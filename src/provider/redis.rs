//! Optional Redis provider using the Socketioxide cluster protocol.
//!
//! Uses RESP3 (Redis 7+) with automatic reconnection and resubscription. Client
//! Socket.IO compatibility does not imply compatibility with other servers'
//! Redis adapter protocols. No application-specific broker formats are included.

use std::{
    collections::HashMap,
    convert::Infallible,
    fmt, io,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use redis_client::{
    AsyncCommands, Client, FromRedisValue, IntoConnectionInfo, ProtocolVersion, PushInfo, PushKind,
    RedisResult,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use socketioxide_redis::{
    CustomRedisAdapter, RedisAdapterCtr,
    drivers::{ChanItem, Driver, MessageStream},
};
use tokio::sync::mpsc;

use crate::{ProviderError, SocketIoBuilder, adapter::Emitter, provider::Provider};

pub use socketioxide_redis::RedisAdapterConfig;

/// Adapter type used by handlers on a Redis-backed server.
pub type RedisAdapter = CustomRedisAdapter<Emitter, RedisDriver>;

/// Redis-backed Socket.IO handle type.
pub type RedisIo = crate::SocketIo<RedisAdapter>;

/// Redis-backed socket extractor type.
pub type RedisSocket = crate::extract::SocketRef<RedisAdapter>;

/// ACK sender extractor for Redis-backed handlers.
pub type RedisAck = crate::extract::AckSender<RedisAdapter>;

type Handlers = Arc<RwLock<HashMap<String, mpsc::Sender<ChanItem>>>>;

/// Shared, non-secret counters for one provider connection.
#[derive(Clone, Default)]
pub struct RedisMonitor(Arc<Counters>);

#[derive(Default)]
struct Counters {
    initial_connection_id: AtomicU64,
    dropped: AtomicU64,
    disconnections: AtomicU64,
}

impl RedisMonitor {
    /// Initial Redis connection ID for diagnostics. Reconnecting creates a new
    /// ID; this value identifies only the connection established during build.
    pub fn initial_connection_id(&self) -> u64 {
        self.0.initial_connection_id.load(Ordering::Relaxed)
    }
    /// Messages rejected because a namespace's bounded input queue was full.
    pub fn dropped_messages(&self) -> u64 {
        self.0.dropped.load(Ordering::Relaxed)
    }

    /// Disconnect notifications observed on the provider connection.
    pub fn disconnections(&self) -> u64 {
        self.0.disconnections.load(Ordering::Relaxed)
    }
}

/// Provider configuration. Its debug representation never exposes credentials.
pub struct RedisProvider {
    client: Client,
    config: RedisAdapterConfig,
    connection_timeout: Duration,
    response_timeout: Duration,
    monitor: RedisMonitor,
}

impl fmt::Debug for RedisProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisProvider")
            .field("prefix", &self.config.prefix)
            .field("connection_timeout", &self.connection_timeout)
            .field("response_timeout", &self.response_timeout)
            .finish_non_exhaustive()
    }
}

impl RedisProvider {
    /// Parse a Redis URL, explicitly select RESP3, and defer connection until build.
    pub fn new(url: impl AsRef<str>) -> RedisResult<Self> {
        let info = url.as_ref().into_connection_info()?;
        let settings = info
            .redis_settings()
            .clone()
            .set_protocol(ProtocolVersion::RESP3);
        Ok(Self {
            client: Client::open(info.set_redis_settings(settings))?,
            config: RedisAdapterConfig::default(),
            connection_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(5),
            monitor: RedisMonitor::default(),
        })
    }

    /// Configure channel isolation, cluster request timeout and bounded queues.
    pub fn with_adapter_config(mut self, config: RedisAdapterConfig) -> Self {
        self.config = config;
        self
    }

    /// Configure connection/initialization and per-command response deadlines.
    pub fn with_timeouts(mut self, connection: Duration, response: Duration) -> Self {
        self.connection_timeout = connection;
        self.response_timeout = response;
        self
    }

    /// Retain counters before moving the provider into a server builder.
    pub fn monitor(&self) -> RedisMonitor {
        self.monitor.clone()
    }

    fn validate(&self) -> Result<(), ProviderError> {
        if self.config.prefix.is_empty() || self.config.prefix.contains('#') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Redis channel prefix must be nonempty and contain no #",
            )
            .into());
        }
        if self.config.stream_buffer == 0 || self.config.ack_response_buffer == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Redis buffers must be positive",
            )
            .into());
        }
        for duration in [
            self.connection_timeout,
            self.response_timeout,
            self.config.request_timeout,
        ] {
            if duration.is_zero() || tokio::time::Instant::now().checked_add(duration).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Redis timeouts must be positive and finite",
                )
                .into());
            }
        }
        Ok(())
    }
}

impl Provider for RedisProvider {
    type Adapter = RedisAdapter;

    async fn configure(
        self,
        builder: SocketIoBuilder,
    ) -> Result<SocketIoBuilder<RedisAdapter>, ProviderError> {
        self.validate()?;
        let handlers = Handlers::default();
        let push_handlers = handlers.clone();
        let monitor = self.monitor.clone();
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(self.connection_timeout))
            .set_response_timeout(Some(self.response_timeout))
            .set_min_delay(Duration::from_millis(100))
            .set_max_delay(Duration::from_secs(2))
            .set_number_of_retries(5)
            .set_automatic_resubscription()
            .set_push_sender(move |message| {
                route_push(message, &push_handlers, &monitor);
                Ok::<(), Infallible>(())
            });
        let mut conn = tokio::time::timeout(
            self.connection_timeout,
            ConnectionManager::new_with_config(self.client, config),
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Redis provider initialization timed out",
            )
        })??;
        let connection_id: u64 = redis_client::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut conn)
            .await?;
        self.monitor
            .0
            .initial_connection_id
            .store(connection_id, Ordering::Relaxed);
        let driver = RedisDriver { conn, handlers };
        let state = RedisAdapterCtr::new_with_driver(driver, self.config);
        Ok(builder.with_adapter::<RedisAdapter>(state))
    }
}

/// Reconnecting Redis driver used by [`RedisProvider`]. Clones share one
/// connection manager and the namespace subscription registry.
#[derive(Clone)]
pub struct RedisDriver {
    conn: ConnectionManager,
    handlers: Handlers,
}

impl Driver for RedisDriver {
    type Error = redis_client::RedisError;

    async fn publish(&self, channel: String, value: Vec<u8>) -> RedisResult<()> {
        self.conn
            .clone()
            .publish::<_, _, usize>(channel, value)
            .await?;
        Ok(())
    }

    async fn subscribe(
        &self,
        channel: String,
        size: usize,
    ) -> RedisResult<MessageStream<ChanItem>> {
        let (sender, receiver) = mpsc::channel(size);
        // Install the receiver before SUBSCRIBE so the first publish cannot race
        // the command acknowledgement and disappear without a handler.
        self.handlers
            .write()
            .expect("subscription lock poisoned")
            .insert(channel.clone(), sender);
        if let Err(error) = self.conn.clone().subscribe(&channel).await {
            self.handlers
                .write()
                .expect("subscription lock poisoned")
                .remove(&channel);
            return Err(error);
        }
        Ok(MessageStream::new(receiver))
    }

    async fn unsubscribe(&self, channel: String) -> RedisResult<()> {
        self.handlers
            .write()
            .expect("subscription lock poisoned")
            .remove(&channel);
        self.conn.clone().unsubscribe(channel).await
    }

    async fn num_serv(&self, channel: &str) -> RedisResult<u16> {
        let (_, count): (String, u16) = redis_client::cmd("PUBSUB")
            .arg("NUMSUB")
            .arg(channel)
            .query_async(&mut self.conn.clone())
            .await?;
        Ok(count)
    }
}

fn route_push(message: PushInfo, handlers: &Handlers, monitor: &RedisMonitor) {
    if message.kind == PushKind::Disconnection {
        monitor.0.disconnections.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("Socket.IO Redis disconnected; provider will reconnect and resubscribe");
        return;
    }
    if message.kind != PushKind::Message || message.data.len() != 2 {
        return;
    }
    let mut values = message.data.into_iter();
    let Ok(channel) = String::from_redis_value(values.next().expect("two values")) else {
        return;
    };
    let Ok(payload) = Vec::<u8>::from_redis_value(values.next().expect("two values")) else {
        return;
    };
    let handlers = handlers.read().expect("subscription lock poisoned");
    if let Some(sender) = handlers.get(&channel)
        && let Err(mpsc::error::TrySendError::Full(_)) = sender.try_send((channel.clone(), payload))
    {
        monitor.0.dropped.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(%channel, "Socket.IO Redis input queue full; message dropped");
    }
}
