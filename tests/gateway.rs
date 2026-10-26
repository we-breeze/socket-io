//! Gateway forwarding over real HTTP and WebSocket connections.

#[allow(
    dead_code,
    reason = "Transport helpers are shared across test binaries"
)]
mod common;

use std::time::Duration;

use brz_http_gateway::{Cors, Gateway, OriginService, RouteRule, RouteTable, RoutesConfig};
use brz_socket_io::{
    Server,
    extract::{AckSender, Data, SocketRef},
};
use common::{Client, DEADLINE, Running};
use tokio::{net::TcpListener, sync::oneshot, time::timeout};

#[tokio::test]
async fn gateway_handles_polling_websocket_and_cors_at_one_boundary() {
    let server = Server::builder().build().await.unwrap();
    server.io().ns("/", async |socket: SocketRef| {
        socket.on("echo", async |Data(value): Data<String>, ack: AckSender| {
            ack.send(&value).unwrap();
        });
    });
    let routes = RouteTable::compile(RoutesConfig {
        routes: ["/socket.io", "/socket.io/"]
            .map(|path| RouteRule {
                methods: vec!["GET".into(), "POST".into()],
                path: path.into(),
                admission: None,
            })
            .into(),
        ..Default::default()
    })
    .unwrap();
    let backend = Running::start(server).await;
    let upstream = format!("http://{}", backend.address).parse().unwrap();
    let gateway = Gateway::new(
        routes,
        OriginService::new(&upstream).unwrap(),
        &"http://127.0.0.1:1".parse().unwrap(),
    )
    .unwrap()
    .with_cors(Cors::permissive())
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let shutdown = async {
            stopped.await.unwrap();
        };
        brz_http_gateway::serve(listener, gateway, shutdown, Duration::from_secs(3))
            .await
            .unwrap();
    });
    let http = reqwest::Client::builder()
        .timeout(DEADLINE)
        .build()
        .unwrap();
    let response = http
        .get(format!(
            "http://{address}/socket.io?EIO=4&transport=polling"
        ))
        .header("Origin", "https://example.test")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert!(
        response
            .headers()
            .contains_key("access-control-allow-origin")
    );
    assert!(response.text().await.unwrap().starts_with("0{"));
    let preflight = http
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{address}/socket.io/"),
        )
        .header("Origin", "https://example.test")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 200);
    let mut client = Client::connect(address, "/").await;
    client.send("421[\"echo\",\"via-gateway\"]").await;
    assert_eq!(client.recv().await, "431[\"via-gateway\"]");
    client.close().await;
    stop.send(()).unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
    backend.stop().await;
}
