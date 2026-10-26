use std::io;

/// An error returned while constructing a provider.
pub type ProviderError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Construction and listener errors. Handler and ACK errors retain their native types.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid configuration, rejected before opening provider connections.
    #[error("invalid Socket.IO configuration: {0}")]
    Config(&'static str),
    /// Provider initialization failed. No in-memory fallback is attempted.
    #[error("Socket.IO provider initialization failed: {0}")]
    Provider(#[source] ProviderError),
    /// The HTTP listener could not accept a connection.
    #[error("Socket.IO listener failed: {0}")]
    Io(#[from] io::Error),
    /// Shutdown did not complete within its configured deadline.
    #[error("Socket.IO shutdown deadline exceeded")]
    ShutdownTimeout,
}
