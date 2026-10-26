//! Socket.IO services with replaceable providers and explicit runtime ownership.
//!
//! Breeze business exports use typed payloads, saved authentication/session
//! state and `Result` returns. The framework owns room actions, ACK packets and
//! metrics; Socketioxide supplies the protocol engine. Applications own identity,
//! authorization and event schemas. The native API is available for adapters:
//!
//! ```no_run
//! use brz_socket_io::{Server, extract::{AckSender, Data, SocketRef}};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let server = Server::builder().build().await?;
//! server.io().ns("/", async |socket: SocketRef| {
//!     socket.on("echo", async |Data(value): Data<String>, ack: AckSender| {
//!         ack.send(&value).ok();
//!     });
//! });
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//! server.serve(listener, std::future::pending()).await?;
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "macros")]
mod business;
mod config;
mod context;
mod emitter;
mod error;
#[cfg_attr(not(feature = "macros"), allow(dead_code))]
mod metrics;
pub mod provider;
#[cfg_attr(not(feature = "macros"), allow(dead_code))]
mod reply;
mod server;

pub use config::{Config, Transports};
pub use context::ConnectionContext;
pub use emitter::{EmitterError, EventEmitter};
pub use error::{Error, ProviderError};
pub use reply::{Accept, ClientError, Reply};
pub use server::{Builder, Server, Service, Status};
pub use socketioxide::{
    AckError, BroadcastError, EmitWithAckError, ParserConfig, ProtocolVersion, SendError,
    SocketError, SocketIo, SocketIoBuilder, TransportType, ack, adapter, extract, handler, layer,
    operators, socket,
};

#[cfg(feature = "macros")]
extern crate self as brz_socket_io;
#[cfg(feature = "macros")]
mod registry;
/// Shared HTTP listener, native HTTP route adapter and protocol mounts.
pub use brz_http_server as http_server;
#[cfg(feature = "macros")]
pub use registry::{NamespaceInit, RegistryError};
#[cfg(feature = "macros")]
pub use socket_io_macros::{disconnect, event, handlers, middleware, namespace, registry};
/// Implementation details for generated namespace and event exports.
#[cfg(feature = "macros")]
#[doc(hidden)]
pub mod __private {
    pub use crate::business::*;
    pub use crate::metrics::{EventMetrics, NamespaceMetrics};
    pub use crate::registry::{
        DisconnectRegistration, EventRegistration, NamespaceRegistration, collect,
    };
    pub use crate::reply::{HandlerResult, IntoAccept, event_reply};
    pub use inventory;
}
