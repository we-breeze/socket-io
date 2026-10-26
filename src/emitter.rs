use crate::{
    AckError, BroadcastError, EmitWithAckError, SocketIo,
    adapter::{Adapter, LocalAdapter},
    metrics::OutgoingMetrics,
    operators::BroadcastOperators,
};
use serde::{Serialize, de::DeserializeOwned};
use std::time::Duration;

/// A failure publishing an event or waiting for a client response.
#[derive(Debug, thiserror::Error)]
pub enum EmitterError {
    /// The namespace is no longer registered on this process.
    #[error("Socket.IO namespace is unavailable: {0}")]
    Namespace(&'static str),
    /// The bounded outgoing queues or provider rejected a broadcast.
    #[error(transparent)]
    Broadcast(#[from] BroadcastError),
    /// A request could not be encoded or published to its recipients.
    #[error(transparent)]
    Publish(#[from] EmitWithAckError),
    /// A response timed out, could not be decoded, or its socket closed.
    #[error(transparent)]
    Ack(#[from] AckError),
    /// Requests must have a positive timeout.
    #[error("Socket.IO request timeout must be positive")]
    InvalidTimeout,
}

/// An outgoing application event, registered once during service construction.
/// Names are static metric identities; rooms and socket IDs never become labels.
/// Business services can own this handle behind their normal application API.
pub struct EventEmitter<A: Adapter = LocalAdapter> {
    io: SocketIo<A>,
    namespace: &'static str,
    event: &'static str,
    rooms: Vec<String>,
    metrics: OutgoingMetrics,
}
impl<A: Adapter> Clone for EventEmitter<A> {
    fn clone(&self) -> Self {
        Self {
            io: self.io.clone(),
            namespace: self.namespace,
            event: self.event,
            rooms: self.rooms.clone(),
            metrics: self.metrics,
        }
    }
}
impl<A: Adapter> EventEmitter<A> {
    pub(crate) fn new(
        io: SocketIo<A>,
        namespace: &'static str,
        event: &'static str,
    ) -> Result<Self, EmitterError> {
        if io.of(namespace).is_none() {
            return Err(EmitterError::Namespace(namespace));
        }
        Ok(Self {
            io,
            namespace,
            event,
            rooms: Vec::new(),
            metrics: OutgoingMetrics::new(namespace, event),
        })
    }
    /// Select a namespace-local room or socket ID. Repeated calls select a union.
    /// Without a target, the event is broadcast to the namespace.
    #[must_use]
    pub fn to(mut self, room: impl Into<String>) -> Self {
        self.rooms.push(room.into());
        self
    }
    fn operators(&self) -> Result<BroadcastOperators<A>, EmitterError> {
        let operators = self
            .io
            .of(self.namespace)
            .ok_or(EmitterError::Namespace(self.namespace))?;
        Ok(if self.rooms.is_empty() {
            operators
        } else {
            operators.to(self.rooms.clone())
        })
    }
    /// Emit typed JSON or binary data. Delivery is best effort, according to
    /// the provider's semantics; a successful publish is not a client ACK.
    pub async fn emit<T: Serialize + ?Sized>(&self, data: &T) -> Result<(), EmitterError> {
        let observation = self.metrics.publish.start();
        let result = async {
            self.operators()?.emit(self.event, data).await?;
            Ok(())
        }
        .await;
        observation.finish(result.is_ok());
        result
    }
    /// Publish once and wait for the first typed client ACK within `timeout`.
    /// A timeout does not cancel remote work or cause an automatic retry.
    pub async fn request<T: Serialize + ?Sized, R: DeserializeOwned>(
        &self,
        data: &T,
        timeout: Duration,
    ) -> Result<R, EmitterError> {
        if timeout.is_zero() {
            return Err(EmitterError::InvalidTimeout);
        }
        let publish = self.metrics.publish.start();
        let stream = async {
            Ok::<_, EmitterError>(
                self.operators()?
                    .timeout(timeout)
                    .emit_with_ack::<_, R>(self.event, data)
                    .await?,
            )
        }
        .await;
        publish.finish(stream.is_ok());
        let stream = stream?;
        let wait = self.metrics.wait.start();
        let result = stream.await;
        wait.finish(result.is_ok());
        if matches!(result, Err(AckError::Timeout)) {
            self.metrics.timed_out();
        }
        Ok(result?)
    }
}
