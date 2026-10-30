//! Advertised heartbeat timing and WebSocket close status on real connections.
#[allow(dead_code)]
mod common;

use std::time::Duration;

use brz_socket_io::{Config, Server, extract::SocketRef};
use common::{DEADLINE, Running};
use futures_util::{SinkExt, StreamExt};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};

const INTERVAL: Duration = Duration::from_millis(100);
const PONG_TIMEOUT: Duration = Duration::from_millis(70);

async fn server() -> Running {
    let server = Server::builder()
        .with_config(Config {
            ping_interval: INTERVAL,
            ping_timeout: PONG_TIMEOUT,
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    server.io().ns("/test", async |_: SocketRef| {});
    Running::start(server).await
}

#[tokio::test]
async fn no_pong_waits_for_the_advertised_interval_and_closes_with_normal_status() {
    let running = server().await;
    let (mut socket, _) = connect_async(format!(
        "ws://{}/socket.io/?EIO=4&transport=websocket",
        running.address
    ))
    .await
    .unwrap();
    let started = Instant::now();
    let mut first_ping = None;
    let mut admitted = false;
    let close = timeout(DEADLINE, async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) if text.starts_with('0') => {
                    let open: serde_json::Value = serde_json::from_str(&text[1..]).unwrap();
                    assert_eq!(open["pingInterval"], 100);
                    assert_eq!(open["pingTimeout"], 70);
                    socket
                        .send(Message::Text("40/test,{}".into()))
                        .await
                        .unwrap();
                }
                Message::Text(text) if text.starts_with("40/test,") => admitted = true,
                Message::Text(text) if text == "2" => {
                    assert!(first_ping.replace(started.elapsed()).is_none());
                    // This client deliberately sends no Engine.IO PONG.
                }
                Message::Text(text) if text == "1" || text.starts_with("41/test,") => {}
                Message::Close(frame) => break frame,
                Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("unexpected heartbeat packet: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let elapsed = started.elapsed();
    running.stop().await;
    assert!(
        admitted,
        "the namespace must be accepted before heartbeat expiry"
    );
    let first_ping = first_ping.expect("a heartbeat must precede its timeout");
    assert!(
        first_ping >= INTERVAL - Duration::from_millis(20),
        "first PING at {first_ping:?}, closed after {elapsed:?}, close={close:?}"
    );
    assert!(elapsed >= INTERVAL + PONG_TIMEOUT - Duration::from_millis(20));
    let close = close.expect("server closure must carry its normal status");
    assert_eq!(close.code, CloseCode::Normal);
    assert!(close.reason.is_empty());
}

#[tokio::test]
async fn pong_keeps_the_connection_alive_and_client_close_status_is_preserved() {
    let running = server().await;
    let (mut socket, _) = connect_async(format!(
        "ws://{}/socket.io/?EIO=4&transport=websocket",
        running.address
    ))
    .await
    .unwrap();
    let mut pings = 0;
    let mut closing = false;
    let frame = timeout(DEADLINE, async {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) if text.starts_with('0') => {
                    socket
                        .send(Message::Text("40/test,{}".into()))
                        .await
                        .unwrap();
                }
                Message::Text(text) if text.starts_with("40/test,") => {}
                Message::Text(text) if text == "2" => {
                    assert!(!closing);
                    pings += 1;
                    socket.send(Message::Text("3".into())).await.unwrap();
                    if pings == 3 {
                        closing = true;
                        socket
                            .send(Message::Close(Some(CloseFrame {
                                code: CloseCode::Away,
                                reason: "client leaves".into(),
                            })))
                            .await
                            .unwrap();
                    }
                }
                Message::Close(frame) => break frame.expect("peer close should be echoed"),
                Message::Ping(bytes) => socket.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("unexpected heartbeat packet: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(pings, 3);
    assert_eq!(frame.code, CloseCode::Away);
    assert_eq!(frame.reason.as_str(), "client leaves");
    running.stop().await;
}
