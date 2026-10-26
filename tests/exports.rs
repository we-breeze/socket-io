//! Namespace/event exports on the ordinary HTTP listener, without a gateway.
#![cfg(feature = "macros")]
#[allow(dead_code)]
mod common;
use brz_http_server::{ServerConfig, Text};
use brz_socket_io::{Accept, ClientError, Config, RegistryError, Server};
use common::{Client, DEADLINE};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

struct App {
    label: String,
}
brz_socket_io::registry!(dependencies(app: Arc<App>));
brz_http_server::registry!();
#[brz_http_server::get("/health", access = public)]
async fn health() -> Text {
    Text("healthy".into())
}
#[brz_http_server::get("/slow", access = public)]
async fn slow() -> Text {
    tokio::time::sleep(Duration::from_millis(200)).await;
    Text("late".into())
}

struct User;
struct Session;
#[brz_socket_io::middleware]
async fn authorize(#[payload] auth: serde_json::Value) -> Result<User, ClientError> {
    if auth.get("token").and_then(|v| v.as_str()) == Some("ok") {
        Ok(User)
    } else {
        Err(ClientError::new("UNAUTHORIZED", "unauthorized"))
    }
}
#[brz_socket_io::namespace("/one", middleware = authorize)]
async fn one(
    #[auth] _user: &User,
    #[inject(app)] app: &App,
) -> Result<Accept<Session>, ClientError> {
    Ok(Accept::new(Session).join(app.label.clone()))
}
#[brz_socket_io::namespace("/two")]
async fn two() -> Result<Accept, ClientError> {
    Ok(Accept::default().join("shared"))
}
mod events {
    use super::*;
    #[brz_socket_io::event("echo", namespace = "/one")]
    async fn echo(
        #[payload] value: String,
        #[auth] _user: &User,
        #[session] _session: &Session,
        #[inject(app)] app: &App,
    ) -> Result<String, ClientError> {
        Ok(format!("{}:{value}", app.label))
    }
    #[brz_socket_io::event("echo", namespace = "/two")]
    async fn other(#[payload] value: String) -> Result<String, ClientError> {
        Ok(format!("two:{value}"))
    }
}
// Disabled exports must not resolve handlers, dependency fields or groups.
#[brz_socket_io::event("disabled", namespace = "/one", group = missing)]
#[cfg(any())]
async fn disabled(#[inject(missing)] missing: MissingType) {}

#[tokio::test]
async fn http_and_exported_events_share_port_with_namespace_room_and_ack_isolation() {
    let server = Server::builder()
        .with_config(Config {
            path: "/realtime".into(),
            ping_interval: Duration::from_millis(200),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    brz_socket_io::handlers!(
        server.io(),
        app = Arc::new(App {
            label: "shared".into()
        })
    )
    .await
    .unwrap();
    let io = server.io().clone();
    let api = brz_http_server::handlers!().unwrap();
    let service = server
        .http_service(
            api,
            ServerConfig {
                request_timeout: Duration::from_millis(80),
                cors: Some(brz_http_server::Cors::permissive()),
                ..Default::default()
            },
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        brz_http_server::hyper::serve(
            listener,
            move |_| service.clone(),
            async {
                stopped.await.ok();
                server.close().await.unwrap();
            },
            brz_http_server::hyper::Config::default(),
        )
        .await
        .unwrap();
    });
    let http = reqwest::Client::builder()
        .timeout(DEADLINE)
        .build()
        .unwrap();
    assert_eq!(
        http.get(format!("http://{address}/health"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "healthy"
    );
    assert_eq!(
        http.get(format!("http://{address}/slow"))
            .send()
            .await
            .unwrap()
            .status(),
        408
    );
    assert_eq!(
        http.get(format!(
            "http://{address}/realtime-lookalike?EIO=4&transport=polling"
        ))
        .send()
        .await
        .unwrap()
        .status(),
        404
    );

    // Connect middleware runs before a namespace is accepted.
    let mut rejected = Client::open(address, "/realtime").await;
    rejected.send("40/one,{\"token\":\"wrong\"}").await;
    assert_eq!(
        rejected.recv().await,
        "44/one,{\"message\":\"unauthorized\"}"
    );
    rejected.close().await;
    let mut client = Client::open(address, "/realtime").await;
    client
        .join_namespace("/one", serde_json::json!({"token":"ok"}))
        .await;
    client.join_namespace("/two", serde_json::Value::Null).await;
    client.send("42/one,1[\"echo\",\"a\"]").await;
    assert_eq!(
        client.recv().await,
        r#"43/one,1[{"ok":true,"data":"shared:a"}]"#
    );
    client.send("42/two,1[\"echo\",\"b\"]").await;
    assert_eq!(
        client.recv().await,
        r#"43/two,1[{"ok":true,"data":"two:b"}]"#
    );
    io.of("/one")
        .unwrap()
        .to("shared")
        .emit("notice", &"one")
        .await
        .unwrap();
    io.of("/two")
        .unwrap()
        .to("shared")
        .emit("notice", &"two")
        .await
        .unwrap();
    assert_eq!(client.recv().await, "42/one,[\"notice\",\"one\"]");
    assert_eq!(client.recv().await, "42/two,[\"notice\",\"two\"]");
    // Same ACK ID on two namespace sockets must resolve independently.
    let first = io.of("/one").unwrap().sockets().pop().unwrap();
    let second = io.of("/two").unwrap().sockets().pop().unwrap();
    let a = first.emit_with_ack::<_, String>("ask", &"a").unwrap();
    let b = second.emit_with_ack::<_, String>("ask", &"b").unwrap();
    let p = client.recv().await;
    let q = client.recv().await;
    assert!(p.starts_with("42/one,"));
    assert!(q.starts_with("42/two,"));
    let id_a = p.trim_start_matches("42/one,").split('[').next().unwrap();
    let id_b = q.trim_start_matches("42/two,").split('[').next().unwrap();
    client.send(format!("43/two,{id_b}[\"B\"]")).await;
    client.send(format!("43/one,{id_a}[\"A\"]")).await;
    assert_eq!(timeout(DEADLINE, a).await.unwrap().unwrap(), "A");
    assert_eq!(timeout(DEADLINE, b).await.unwrap().unwrap(), "B");

    let preflight = http
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{address}/realtime/"),
        )
        .header("Origin", "https://client.test")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 200);
    assert_eq!(preflight.headers()["access-control-allow-origin"], "*");
    let cors_response = http
        .get(format!("http://{address}/health"))
        .header("Origin", "https://client.test")
        .send()
        .await
        .unwrap();
    assert_eq!(cors_response.headers()["access-control-allow-origin"], "*");
    // Long polling waits past the short HTTP handler deadline.
    let polling = format!("http://{address}/realtime/?EIO=4&transport=polling");
    let open = http
        .get(&polling)
        .header("Origin", "https://client.test")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let open: serde_json::Value = serde_json::from_str(&open[1..]).unwrap();
    let poll = format!("{polling}&sid={}", open["sid"].as_str().unwrap());
    // Drain the initial ping, then wait for the next heartbeat.
    assert_eq!(
        http.get(&poll).send().await.unwrap().text().await.unwrap(),
        "2"
    );
    http.post(&poll)
        .body("3")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let started = tokio::time::Instant::now();
    let response = http.get(&poll).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "2");
    assert!(started.elapsed() >= Duration::from_millis(80));
    assert_eq!(
        http.get(format!(
            "http://{address}/socket.io/?EIO=4&transport=polling"
        ))
        .send()
        .await
        .unwrap()
        .status(),
        404
    );
    client.close().await;
    stop.send(()).unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

brz_socket_io::registry!(group = duplicate);
#[brz_socket_io::namespace("/dup", group = duplicate)]
async fn dup_one() {}
#[brz_socket_io::namespace("/dup", group = duplicate)]
async fn dup_two() {}
brz_socket_io::registry!(group = duplicate_event);
#[brz_socket_io::namespace("/dup", group = duplicate_event)]
async fn dup_connect() {}
#[brz_socket_io::event("echo", namespace = "/dup", group = duplicate_event)]
async fn dup_event_a() {}
#[brz_socket_io::event("echo", namespace = "/dup", group = duplicate_event)]
async fn dup_event_b() {}
brz_socket_io::registry!(group = undeclared);
#[brz_socket_io::event("echo", namespace = "/typo", group = undeclared)]
async fn missing_namespace() {}
#[tokio::test]
async fn registration_rejects_conflicts_before_mounting_any_namespace() {
    let server = Server::builder().build().await.unwrap();
    assert!(matches!(
        brz_socket_io::handlers!(server.io(); group = duplicate).await,
        Err(RegistryError::DuplicateNamespace(_))
    ));
    assert!(matches!(
        brz_socket_io::handlers!(server.io(); group = duplicate_event).await,
        Err(RegistryError::DuplicateEvent { .. })
    ));
    assert!(matches!(
        brz_socket_io::handlers!(server.io(); group = undeclared).await,
        Err(RegistryError::UnknownNamespace { .. })
    ));
    assert_eq!(server.status().namespaces, 0);
    brz_socket_io::handlers!(
        server.io(),
        app = Arc::new(App {
            label: "shared".into()
        })
    )
    .await
    .unwrap();
    assert!(matches!(
        brz_socket_io::handlers!(
            server.io(),
            app = Arc::new(App {
                label: "shared".into()
            })
        )
        .await,
        Err(RegistryError::AlreadyMounted(_))
    ));
    server.close().await.unwrap();
}

#[cfg(feature = "redis")]
mod redis_exports {
    use super::*;
    use brz_socket_io::provider::redis::{RedisAdapter, RedisAdapterConfig, RedisProvider};
    brz_socket_io::registry!(group = exported, adapter = RedisAdapter);
    #[brz_socket_io::namespace("/provided", group = crate::redis_exports::exported)]
    async fn connected() -> Result<Accept, ClientError> {
        Ok(Accept::default().join("shared"))
    }
    #[brz_socket_io::event("echo", namespace = "/provided", group = crate::redis_exports::exported)]
    async fn echo(#[payload] value: String) -> Result<String, ClientError> {
        Ok(value)
    }
    #[tokio::test]
    async fn exported_redis_namespaces_are_ready_before_serving_and_cross_node_emit() {
        let url = std::env::var("BRZ_SOCKET_IO_TEST_REDIS_URL").expect("isolated Redis URL");
        let prefix = format!("brz-exports-{}", std::process::id());
        let mut servers = Vec::new();
        for _ in 0..2 {
            let provider = RedisProvider::new(url.as_str())
                .unwrap()
                .with_adapter_config(RedisAdapterConfig::default().with_prefix(prefix.clone()));
            let server = Server::builder()
                .with_provider(provider)
                .build()
                .await
                .unwrap();
            brz_socket_io::handlers!(server.io(); group = crate::redis_exports::exported)
                .await
                .unwrap();
            assert!(server.io().of("/provided").is_some());
            servers.push(server);
        }
        let second = servers.pop().unwrap();
        let first = servers.pop().unwrap();
        let io = first.io().clone();
        let one = common::Running::start(first).await;
        let two = common::Running::start(second).await;
        let mut client = Client::connect(two.address, "/provided").await;
        client.send("42/provided,1[\"echo\",\"ready\"]").await;
        assert_eq!(
            client.recv().await,
            r#"43/provided,1[{"ok":true,"data":"ready"}]"#
        );
        io.of("/provided")
            .unwrap()
            .to("shared")
            .emit("notice", &"remote")
            .await
            .unwrap();
        assert_eq!(client.recv().await, "42/provided,[\"notice\",\"remote\"]");
        client.close().await;
        one.stop().await;
        two.stop().await;
    }
}
