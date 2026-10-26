use http::{HeaderMap, Uri};
use std::net::SocketAddr;

/// Immutable identity and handshake metadata for one namespace connection.
/// This contains no protocol send/ACK or room-management operations.
#[derive(Clone)]
pub struct ConnectionContext {
    pub(crate) id: String,
    pub(crate) namespace: String,
    pub(crate) uri: Uri,
    pub(crate) headers: HeaderMap,
    pub(crate) peer_addr: Option<SocketAddr>,
}
impl ConnectionContext {
    /// Namespace socket identifier; suitable for application presence records.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Connected namespace, independent of the HTTP mount path.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    /// URI of the original Engine.IO handshake request.
    pub fn uri(&self) -> &Uri {
        &self.uri
    }
    /// Original handshake headers. Do not log credential-bearing headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    /// TCP peer if the host HTTP runtime supplied this request extension.
    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }
}
