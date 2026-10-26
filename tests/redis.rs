//! Redis contracts against an explicitly configured Redis 7+ instance.
#![cfg(feature = "redis")]

#[allow(
    dead_code,
    reason = "Transport helpers are shared with the non-Redis test binary"
)]
mod common;

use std::time::Duration;

use brz_socket_io::{
    Config, Server,
    extract::Data,
    provider::redis::{
        RedisAck, RedisAdapter, RedisAdapterConfig, RedisMonitor, RedisProvider, RedisSocket,
    },
};
use common::{Client, DEADLINE, Running};
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};

async fn server(url: &str, prefix: &str) -> (Server<RedisAdapter>, RedisMonitor) {
    let provider = RedisProvider::new(url).unwrap().with_adapter_config(
        RedisAdapterConfig::default()
            .with_prefix(prefix.to_owned())
            .with_request_timeout(Duration::from_secs(1)),
    );
    let monitor = provider.monitor();
    let server = Server::builder()
        .with_provider(provider)
        .build()
        .await
        .unwrap();
    server
        .io()
        .ns("/events", async |socket: RedisSocket| {
            socket.join(socket.id);
            socket.on(
                "join",
                async |socket: RedisSocket, Data(room): Data<String>, ack: RedisAck| {
                    socket.join(room);
                    ack.send(&true).unwrap();
                },
            );
            socket.on("echo", async |Data(value): Data<Value>, ack: RedisAck| {
                ack.send(&value).unwrap();
            });
        })
        .await
        .unwrap();
    (server, monitor)
}

#[tokio::test]
async fn cross_instance_rooms_ack_emitter_and_resubscription() {
    let url = std::env::var("BRZ_SOCKET_IO_TEST_REDIS_URL")
        .expect("set BRZ_SOCKET_IO_TEST_REDIS_URL to an isolated Redis 7+ instance");
    let prefix = format!("brz-test-{}", std::process::id());
    let (first, first_monitor) = server(&url, &prefix).await;
    let (second, second_monitor) = server(&url, &prefix).await;
    // This participant has no HTTP listener or local clients. Its namespace
    // subscription must still route broadcast ACKs from the other processes.
    let (emitter, _) = server(&url, &prefix).await;
    let io = first.io().clone();
    let first = Running::start(first).await;
    let second = Running::start(second).await;
    let mut client = Client::connect(second.address, "/events").await;
    client.send("42/events,1[\"join\",\"updates\"]").await;
    assert_eq!(client.recv().await, "43/events,1[true]");
    assert_eq!(
        io.of("/events")
            .unwrap()
            .fetch_sockets()
            .await
            .unwrap()
            .len(),
        1
    );

    io.of("/events")
        .unwrap()
        .to("updates")
        .emit("notice", &json!({"from":"first"}))
        .await
        .unwrap();
    assert_eq!(
        client.recv().await,
        "42/events,[\"notice\",{\"from\":\"first\"}]"
    );

    let request = emitter
        .io()
        .of("/events")
        .unwrap()
        .to("updates")
        .emit_with_ack::<_, Value>("question", &json!({"n":1}))
        .await
        .unwrap();
    let packet = client.recv().await;
    let id = packet
        .strip_prefix("42/events,")
        .unwrap()
        .split('[')
        .next()
        .unwrap();
    client
        .send(format!("43/events,{id}[{{\"accepted\":true}}]"))
        .await;
    assert_eq!(
        timeout(DEADLINE, request).await.unwrap().unwrap(),
        json!({"accepted":true})
    );

    // Channel prefixes isolate separate applications even with identical
    // namespace and room names.
    let (isolated, _) = server(&url, &format!("{prefix}-isolated")).await;
    assert!(
        isolated
            .io()
            .of("/events")
            .unwrap()
            .fetch_sockets()
            .await
            .unwrap()
            .is_empty()
    );
    isolated.close().await.unwrap();

    let control = redis_client::Client::open(url.as_str()).unwrap();
    let mut control = control.get_multiplexed_async_connection().await.unwrap();
    let killed: u32 = redis_client::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(second_monitor.initial_connection_id())
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(killed, 1);
    let channel = format!("{prefix}-request#/events#");
    timeout(DEADLINE, async {
        loop {
            let (_, count): (String, u32) = redis_client::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(&channel)
                .query_async(&mut control)
                .await
                .unwrap();
            if count == 3 && second_monitor.disconnections() > 0 {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("provider did not restore its namespace subscriptions");
    emitter
        .io()
        .of("/events")
        .unwrap()
        .to("updates")
        .emit("recovered", &true)
        .await
        .unwrap();
    assert_eq!(client.recv().await, "42/events,[\"recovered\",true]");
    assert_eq!(first_monitor.dropped_messages(), 0);
    assert_eq!(second_monitor.dropped_messages(), 0);

    client.close().await;
    first.stop().await;
    second.stop().await;
    emitter.close().await.unwrap();
    let (_, remaining): (String, u32) = redis_client::cmd("PUBSUB")
        .arg("NUMSUB")
        .arg(&channel)
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(
        remaining, 0,
        "closing servers must unsubscribe every namespace"
    );
}

#[tokio::test]
async fn invalid_redis_config_fails_before_network_io_and_debug_redacts_credentials() {
    let provider = RedisProvider::new("redis://username:secret-value@127.0.0.1:1").unwrap();
    let debug = format!("{provider:?}");
    assert!(!debug.contains("username"));
    assert!(!debug.contains("secret-value"));
    let config = RedisAdapterConfig {
        stream_buffer: 0,
        ..RedisAdapterConfig::default()
    };
    let result = Server::builder()
        .with_provider(provider.with_adapter_config(config))
        .build()
        .await;
    assert!(matches!(result, Err(brz_socket_io::Error::Provider(_))));
    let result = Server::builder()
        .with_provider(
            RedisProvider::new("redis://127.0.0.1:1")
                .unwrap()
                .with_timeouts(Duration::ZERO, Duration::ZERO),
        )
        .with_config(Config::default())
        .build()
        .await;
    assert!(matches!(result, Err(brz_socket_io::Error::Provider(_))));
}
