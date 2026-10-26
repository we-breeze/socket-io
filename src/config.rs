use std::time::Duration;

use crate::{Error, SocketIoBuilder, TransportType};

/// Permitted Engine.IO transports.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Transports {
    /// HTTP long-polling and WebSocket, including transport upgrades.
    #[default]
    Both,
    /// WebSocket connections only.
    WebSocket,
    /// HTTP long-polling connections only.
    Polling,
}

/// Protocol and standalone listener limits, validated before building a server.
#[derive(Clone, Debug)]
pub struct Config {
    /// Engine.IO HTTP path, independent of Socket.IO namespaces.
    pub path: String,
    /// Permitted transports.
    pub transports: Transports,
    /// Interval between Engine.IO heartbeat pings.
    pub ping_interval: Duration,
    /// Maximum wait for a heartbeat pong.
    pub ping_timeout: Duration,
    /// Maximum duration of a polling-to-WebSocket upgrade.
    pub upgrade_timeout: Duration,
    /// Maximum wait for a client to connect to a namespace.
    pub connect_timeout: Duration,
    /// Default timeout for client acknowledgements.
    pub ack_timeout: Duration,
    /// Maximum incoming payload/message and aggregate binary attachment bytes.
    pub max_payload_bytes: usize,
    /// Maximum buffered outgoing packets per Engine.IO connection.
    pub max_buffered_packets: usize,
    /// Maximum incoming binary attachments in one Socket.IO packet.
    pub max_binary_attachments: usize,
    /// Maximum duration of an incomplete binary Socket.IO packet.
    pub packet_completion_timeout: Duration,
    /// Maximum active TCP connections on the standalone listener, including
    /// upgraded WebSocket sessions.
    pub max_http_connections: usize,
    /// Maximum HTTP header read time on the standalone listener.
    pub header_read_timeout: Duration,
    /// Maximum Hyper HTTP read buffer size; must be at least 8192 bytes.
    pub max_http_buffer_bytes: usize,
    /// Deadline for closing sockets and draining standalone HTTP connections.
    pub shutdown_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: "/socket.io".into(),
            transports: Transports::Both,
            ping_interval: Duration::from_secs(25),
            ping_timeout: Duration::from_secs(20),
            upgrade_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(45),
            ack_timeout: Duration::from_secs(5),
            max_payload_bytes: 1_000_000,
            max_buffered_packets: 128,
            max_binary_attachments: 128,
            packet_completion_timeout: Duration::from_secs(30),
            max_http_connections: 4096,
            header_read_timeout: Duration::from_secs(15),
            max_http_buffer_bytes: 32 * 1024,
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl Config {
    /// Check paths, resource bounds and deadlines without starting any tasks.
    pub fn validate(&self) -> Result<(), Error> {
        if !self.path.starts_with('/')
            || self.path == "/"
            || self.path.ends_with('/')
            || self.path.contains(['?', '#'])
            || self.path.bytes().any(|b| b.is_ascii_control() || b == b' ')
        {
            return Err(Error::Config(
                "path must be an absolute path without a trailing slash, query, fragment or whitespace",
            ));
        }
        for (name, value) in [
            ("max_payload_bytes", self.max_payload_bytes),
            ("max_buffered_packets", self.max_buffered_packets),
            ("max_binary_attachments", self.max_binary_attachments),
            ("max_http_connections", self.max_http_connections),
        ] {
            if value == 0 {
                return Err(Error::Config(name));
            }
        }
        if self.max_http_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(Error::Config(
                "max_http_connections exceeds semaphore capacity",
            ));
        }
        if self.max_http_buffer_bytes < 8192 {
            return Err(Error::Config("max_http_buffer_bytes must be at least 8192"));
        }
        for (name, duration) in [
            ("ping_interval", self.ping_interval),
            ("ping_timeout", self.ping_timeout),
            ("upgrade_timeout", self.upgrade_timeout),
            ("connect_timeout", self.connect_timeout),
            ("ack_timeout", self.ack_timeout),
            ("packet_completion_timeout", self.packet_completion_timeout),
            ("shutdown_timeout", self.shutdown_timeout),
            ("header_read_timeout", self.header_read_timeout),
        ] {
            if duration.is_zero() || tokio::time::Instant::now().checked_add(duration).is_none() {
                return Err(Error::Config(name));
            }
        }
        Ok(())
    }

    pub(crate) fn apply(&self, builder: SocketIoBuilder) -> SocketIoBuilder {
        let builder = builder
            .req_path(self.path.clone())
            .ping_interval(self.ping_interval)
            .ping_timeout(self.ping_timeout)
            .upgrade_timeout(self.upgrade_timeout)
            .connect_timeout(self.connect_timeout)
            .ack_timeout(self.ack_timeout)
            .max_payload(self.max_payload_bytes as u64)
            .max_buffer_size(self.max_buffered_packets)
            .ws_max_message_size(self.max_payload_bytes)
            .ws_max_frame_size(self.max_payload_bytes)
            .with_parser(
                crate::ParserConfig::common()
                    .max_incoming_binary_buf_size(self.max_payload_bytes)
                    .max_incoming_binaries(self.max_binary_attachments)
                    .packet_completion_timeout(self.packet_completion_timeout),
            );
        match self.transports {
            Transports::Both => {
                builder.transports([TransportType::Polling, TransportType::Websocket])
            }
            Transports::Polling => builder.transports([TransportType::Polling]),
            Transports::WebSocket => builder.transports([TransportType::Websocket]),
        }
    }
}
