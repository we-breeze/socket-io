//! Socket.IO contracts over real polling and WebSocket connections.

mod common;

use std::{sync::Arc, time::Duration};

use brz_socket_io::{
    Config, Server, Transports,
    adapter::Adapter,
    extract::{AckSender, Data, SocketRef, State},
    handler::ConnectHandler,
};
use common::{Client, DEADLINE, Running};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::timeout;

fn echo<A: Adapter>(socket: SocketRef<A>) {
    socket.on(
        "echo",
        async |Data(value): Data<Value>, ack: AckSender<A>| {
            ack.send(&value).unwrap();
        },
    );
}

#[tokio::test]
async fn polling_handshake_and_ack_preserve_injected_state() {
    let server = Server::builder()
        .with_state(Arc::new("shared-state".to_string()))
        .build()
        .await
        .unwrap();
    server.io().ns("/", async |socket: SocketRef| {
        socket.on(
            "read",
            async |State(state): State<Arc<String>>, ack: AckSender| {
                ack.send(state.as_str()).unwrap();
            },
        );
    });
    let running = Running::start(server).await;
    let http = reqwest::Client::builder()
        .timeout(DEADLINE)
        .build()
        .unwrap();
    let endpoint = format!(
        "http://{}/socket.io/?EIO=4&transport=polling",
        running.address
    );
    let handshake = http
        .get(&endpoint)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let handshake: Value = serde_json::from_str(handshake.strip_prefix('0').unwrap()).unwrap();
    assert_eq!(handshake["pingInterval"], 25000);
    assert_eq!(handshake["maxPayload"], 1_000_000);
    let endpoint = format!("{endpoint}&sid={}", handshake["sid"].as_str().unwrap());
    assert!(
        http.post(&endpoint)
            .body("40")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let connected = http
        .get(&endpoint)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        connected
            .split('\u{1e}')
            .any(|packet| packet.starts_with("40{"))
    );
    http.post(&endpoint)
        .body("421[\"read\"]")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let response = http
        .get(&endpoint)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    if response.split('\u{1e}').any(|packet| packet == "2") {
        http.post(&endpoint)
            .body("3")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    assert_eq!(
        response
            .split('\u{1e}')
            .filter(|packet| *packet != "2")
            .collect::<Vec<_>>(),
        ["431[\"shared-state\"]"]
    );
    running.stop().await;
}

#[tokio::test]
async fn namespaces_rooms_exclusions_and_disconnect_are_isolated() {
    let server = Server::builder().build().await.unwrap();
    for namespace in ["/one", "/two"] {
        server.io().ns(namespace, async |socket: SocketRef| {
            echo(socket.clone());
            socket.on(
                "join",
                async |socket: SocketRef, Data(room): Data<String>, ack: AckSender| {
                    socket.join(room);
                    ack.send(&true).unwrap();
                },
            );
            socket.on(
                "leave",
                async |socket: SocketRef, Data(room): Data<String>, ack: AckSender| {
                    socket.leave(room);
                    ack.send(&true).unwrap();
                },
            );
        });
    }
    let io = server.io().clone();
    let running = Running::start(server).await;
    let mut one = Client::connect(running.address, "/one").await;
    let mut two = Client::connect(running.address, "/one").await;
    let mut other = Client::connect(running.address, "/two").await;
    for client in [&mut one, &mut two] {
        client.send("42/one,1[\"join\",\"room\"]").await;
        assert_eq!(client.recv().await, "43/one,1[true]");
    }
    two.send("42/one,2[\"join\",\"excluded\"]").await;
    assert_eq!(two.recv().await, "43/one,2[true]");
    other.send("42/two,1[\"join\",\"room\"]").await;
    assert_eq!(other.recv().await, "43/two,1[true]");
    io.of("/one")
        .unwrap()
        .to("room")
        .except("excluded")
        .emit("notice", &json!({"n":1}))
        .await
        .unwrap();
    assert_eq!(one.recv().await, "42/one,[\"notice\",{\"n\":1}]");
    // A following ACK acts as a wire-order barrier: neither excluded socket may
    // have a notice queued before its echo response.
    two.send("42/one,3[\"echo\",\"barrier\"]").await;
    assert_eq!(two.recv().await, "43/one,3[\"barrier\"]");
    other.send("42/two,3[\"echo\",\"barrier\"]").await;
    assert_eq!(other.recv().await, "43/two,3[\"barrier\"]");
    one.send("42/one,4[\"leave\",\"room\"]").await;
    assert_eq!(one.recv().await, "43/one,4[true]");
    assert_eq!(io.of("/one").unwrap().to("room").sockets().len(), 1);
    one.close().await;
    two.close().await;
    other.close().await;
    running.stop().await;
}

#[tokio::test]
async fn server_requests_receive_ack_and_timeout_without_blocking_other_events() {
    let server = Server::builder()
        .with_config(Config {
            ack_timeout: Duration::from_millis(100),
            ..Config::default()
        })
        .build()
        .await
        .unwrap();
    let (sender, mut sockets) = tokio::sync::mpsc::channel(1);
    server.io().ns("/", async move |socket: SocketRef| {
        echo(socket.clone());
        sender.send(socket).await.unwrap();
    });
    let running = Running::start(server).await;
    let mut client = Client::connect(running.address, "/").await;
    let socket = timeout(DEADLINE, sockets.recv()).await.unwrap().unwrap();
    let pending = socket
        .emit_with_ack::<_, Value>("question", &json!({"n":7}))
        .unwrap();
    let packet = client.recv().await;
    let id = packet
        .strip_prefix("42")
        .unwrap()
        .split('[')
        .next()
        .unwrap();
    client.send(format!("43{id}[{{\"answer\":8}}]")).await;
    assert_eq!(pending.await.unwrap(), json!({"answer":8}));
    let pending = socket.emit_with_ack::<_, Value>("unanswered", &()).unwrap();
    client.recv().await;
    assert!(matches!(
        pending.await,
        Err(brz_socket_io::AckError::Timeout)
    ));
    let pending = socket.emit_with_ack::<_, Value>("cancelled", &()).unwrap();
    let packet = client.recv().await;
    drop(pending);
    let id = packet
        .strip_prefix("42")
        .unwrap()
        .split('[')
        .next()
        .unwrap();
    client.send(format!("43{id}[null]")).await;
    client.send("421[\"echo\",\"still-connected\"]").await;
    assert_eq!(client.recv().await, "431[\"still-connected\"]");
    running.stop().await;
}

#[derive(Deserialize)]
struct Auth {
    token: String,
}

#[derive(Clone)]
struct Identity(String);

#[tokio::test]
async fn application_middleware_controls_auth_and_session_extensions() {
    let server = Server::builder().build().await.unwrap();
    let connect = async |socket: SocketRef| {
        socket.on("who", async |socket: SocketRef, ack: AckSender| {
            let identity = socket.extensions.get::<Identity>().unwrap();
            ack.send(&identity.0).unwrap();
        });
    };
    let auth = async |socket: SocketRef, Data(auth): Data<Auth>| -> Result<(), &'static str> {
        if auth.token != "application-token" {
            return Err("unauthorized");
        }
        socket.extensions.insert(Identity("user-1".into()));
        Ok(())
    };
    server.io().ns("/", connect.with(auth));
    let running = Running::start(server).await;
    let mut rejected = Client::open(running.address, "/socket.io").await;
    rejected.send("40{\"token\":\"invalid\"}").await;
    assert!(rejected.recv().await.starts_with("44"));
    let mut accepted = Client::open(running.address, "/socket.io").await;
    accepted
        .join_namespace("/", json!({"token":"application-token"}))
        .await;
    accepted.send("421[\"who\"]").await;
    assert_eq!(accepted.recv().await, "431[\"user-1\"]");
    running.stop().await;
}

#[tokio::test]
async fn binary_attachments_round_trip_with_socketio_framing() {
    let server = Server::builder().build().await.unwrap();
    server.io().ns("/", async |socket: SocketRef| {
        socket.on(
            "binary",
            async |Data(value): Data<serde_bytes::ByteBuf>, ack: AckSender| {
                ack.send(&value).unwrap();
            },
        );
    });
    let running = Running::start(server).await;
    let mut client = Client::connect(running.address, "/").await;
    client
        .send("451-1[\"binary\",{\"_placeholder\":true,\"num\":0}]")
        .await;
    client.binary(vec![0, 1, 255, 2]).await;
    assert_eq!(
        client.recv().await,
        "461-1[{\"_placeholder\":true,\"num\":0}]"
    );
    assert_eq!(client.recv_binary().await, [0, 1, 255, 2]);
    running.stop().await;
}

#[tokio::test]
async fn custom_path_transport_policy_and_buffer_limit_apply() {
    let server = Server::builder()
        .with_config(Config {
            path: "/events".into(),
            transports: Transports::WebSocket,
            max_buffered_packets: 8,
            ..Config::default()
        })
        .build()
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    server.io().ns("/", async move |socket: SocketRef| {
        echo(socket.clone());
        tx.send(socket).await.unwrap();
    });
    let running = Running::start(server).await;
    let http = reqwest::Client::builder()
        .timeout(DEADLINE)
        .build()
        .unwrap();
    assert_eq!(
        http.get(format!(
            "http://{}/socket.io/?EIO=4&transport=polling",
            running.address
        ))
        .send()
        .await
        .unwrap()
        .status(),
        404
    );
    assert_eq!(
        http.get(format!(
            "http://{}/events/?EIO=4&transport=polling",
            running.address
        ))
        .send()
        .await
        .unwrap()
        .status(),
        400
    );
    let mut client = Client::open(running.address, "/events").await;
    client.join_namespace("/", Value::Null).await;
    let socket = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
    // Finish the handshake and drain its ACK before filling the outgoing queue.
    // A one-slot queue can legitimately overflow during heartbeat/connect
    // framing; that is not a deterministic test of the post-connect budget.
    client.send("421[\"echo\",\"ready\"]").await;
    assert_eq!(client.recv().await, "431[\"ready\"]");
    // No await between emits: the consumer cannot drain this current-thread
    // runtime's queue before the capacity assertion.
    for index in 0..8 {
        socket.emit("bounded", &index).unwrap();
    }
    assert!(socket.emit("overflow", &9).is_err());
    for index in 0..8 {
        assert_eq!(client.recv().await, format!("42[\"bounded\",{index}]"));
    }
    running.stop().await;
}
