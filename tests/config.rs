//! Configuration and provider construction contracts.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use brz_socket_io::{
    Config, Error, ProviderError, Server, SocketIoBuilder,
    adapter::LocalAdapter,
    provider::{AdapterProvider, Provider},
};

#[test]
fn invalid_paths_and_resource_limits_are_rejected() {
    for path in [
        "",
        "socket.io",
        "/",
        "/socket.io/",
        "/socket.io?x",
        "/socket.io#x",
        "/socket io",
        "/socket\tio",
    ] {
        assert!(
            Config {
                path: path.into(),
                ..Config::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(Config::default().validate().is_ok());
    assert!(
        Config {
            max_buffered_packets: 0,
            ..Config::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            max_payload_bytes: 0,
            ..Config::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            max_binary_attachments: 0,
            ..Config::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            max_http_connections: usize::MAX,
            ..Config::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            ping_timeout: Duration::ZERO,
            ..Config::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        Config {
            ack_timeout: Duration::MAX,
            ..Config::default()
        }
        .validate()
        .is_err()
    );
}

struct FailingProvider(Arc<AtomicBool>);

impl Provider for FailingProvider {
    type Adapter = LocalAdapter;

    async fn configure(self, _: SocketIoBuilder) -> Result<SocketIoBuilder, ProviderError> {
        self.0.store(true, Ordering::SeqCst);
        Err(io::Error::other("provider unavailable").into())
    }
}

#[tokio::test]
async fn invalid_configuration_never_starts_provider_and_provider_errors_propagate() {
    let called = Arc::new(AtomicBool::new(false));
    let result = Server::builder()
        .with_config(Config {
            max_payload_bytes: 0,
            ..Config::default()
        })
        .with_provider(FailingProvider(called.clone()))
        .build()
        .await;
    assert!(matches!(result, Err(Error::Config(_))));
    assert!(!called.load(Ordering::SeqCst));
    let result = Server::builder()
        .with_provider(FailingProvider(called.clone()))
        .build()
        .await;
    assert!(matches!(result, Err(Error::Provider(_))));
    assert!(called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn external_adapter_provider_preserves_protocol_configuration() {
    let server = Server::builder()
        .with_provider(AdapterProvider::<LocalAdapter>::new(()))
        .with_config(Config {
            path: "/events".into(),
            max_payload_bytes: 8192,
            max_buffered_packets: 16,
            ..Config::default()
        })
        .build()
        .await
        .unwrap();
    assert_eq!(server.io().config().engine_config.req_path, "/events");
    assert_eq!(server.io().config().engine_config.max_payload, 8192);
    assert_eq!(server.io().config().engine_config.max_buffer_size, 16);
    assert_eq!(server.status().namespaces, 0);
    server.close().await.unwrap();
}
