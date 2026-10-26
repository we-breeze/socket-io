//! Shutdown and connection admission over real sockets.

#[allow(
    dead_code,
    reason = "Transport helpers are shared across test binaries"
)]
mod common;

use std::time::Duration;

use brz_socket_io::{Config, Server, extract::SocketRef};
use common::{Client, DEADLINE, Running};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

#[tokio::test]
async fn shutdown_closes_namespace_sockets_and_partial_http_connections() {
    let server = Server::builder()
        .with_config(Config {
            shutdown_timeout: Duration::from_secs(1),
            ..Config::default()
        })
        .build()
        .await
        .unwrap();
    let (disconnected, mut receiver) = tokio::sync::mpsc::channel(1);
    server.io().ns("/", async move |socket: SocketRef| {
        socket.on_disconnect(async move || {
            disconnected.send(()).await.unwrap();
        });
    });
    let io = server.io().clone();
    let running = Running::start(server).await;
    let _client = Client::connect(running.address, "/").await;
    let mut partial = TcpStream::connect(running.address).await.unwrap();
    partial
        .write_all(b"GET /socket.io/ HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    assert_eq!(io.of("/").unwrap().sockets().len(), 1);
    running.stop().await;
    assert_eq!(timeout(DEADLINE, receiver.recv()).await.unwrap(), Some(()));
    assert!(io.nsps().is_empty());
    let mut byte = [0];
    let result = timeout(DEADLINE, partial.read(&mut byte)).await.unwrap();
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "partial HTTP connection remained open"
    );
}

#[tokio::test]
async fn incomplete_headers_time_out_and_release_the_http_connection_budget() {
    let server = Server::builder()
        .with_config(Config {
            max_http_connections: 1,
            header_read_timeout: Duration::from_millis(100),
            ..Config::default()
        })
        .build()
        .await
        .unwrap();
    let running = Running::start(server).await;
    let mut partial = TcpStream::connect(running.address).await.unwrap();
    partial
        .write_all(b"GET /socket.io/ HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let result = timeout(DEADLINE, partial.read_to_end(&mut bytes))
        .await
        .unwrap();
    assert!(result.is_ok() || result.unwrap_err().kind() == std::io::ErrorKind::ConnectionReset);
    let response = reqwest::Client::builder()
        .timeout(DEADLINE)
        .build()
        .unwrap()
        .get(format!(
            "http://{}/socket.io/?EIO=4&transport=polling",
            running.address
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    running.stop().await;
}
