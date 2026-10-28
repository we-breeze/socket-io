//! Business results; transport packets and acknowledgements are framework-owned.
use serde::Serialize;

/// An explicitly safe error exposed to a client. Convert application errors
/// with `From<AppError>`; never expose arbitrary internal error strings.
#[derive(Clone, Debug, Serialize, thiserror::Error)]
#[error("{message}")]
pub struct ClientError {
    /// Stable application error code.
    pub code: String,
    /// Public, credential-free explanation.
    pub message: String,
}
impl ClientError {
    /// Construct an application-defined public error.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
    pub(crate) fn invalid_payload() -> Self {
        Self::new("INVALID_PAYLOAD", "Invalid event payload")
    }
    pub(crate) fn unavailable() -> Self {
        Self::new("SESSION_CLOSED", "Namespace session is closed")
    }
    pub(crate) fn internal() -> Self {
        Self::new("INTERNAL", "Internal server error")
    }
}
impl From<std::convert::Infallible> for ClientError {
    fn from(error: std::convert::Infallible) -> Self {
        match error {}
    }
}

#[derive(Debug)]
pub(crate) enum RoomAction {
    Join(String),
    Leave(String),
}

pub(crate) struct AfterReply(Box<dyn FnOnce() + Send + Sync>);
impl std::fmt::Debug for AfterReply {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AfterReply(..)")
    }
}
impl AfterReply {
    pub(crate) fn run(self) {
        (self.0)();
    }
}

/// Accept a namespace connection with application-owned, typed session state.
/// The session and authentication result live until namespace disconnection;
/// in-flight handlers can retain them until they complete.
#[derive(Debug)]
pub struct Accept<S = ()> {
    pub(crate) session: S,
    pub(crate) rooms: Vec<RoomAction>,
}
impl<S> Accept<S> {
    /// Store this business session in the namespace socket.
    pub fn new(session: S) -> Self {
        Self {
            session,
            rooms: Vec::new(),
        }
    }
    /// Join a namespace-local room after the handler succeeds.
    #[must_use]
    pub fn join(mut self, room: impl Into<String>) -> Self {
        self.rooms.push(RoomAction::Join(room.into()));
        self
    }
}
impl Default for Accept<()> {
    fn default() -> Self {
        Self::new(())
    }
}

/// Event data plus declarative room changes. Actions run only on success,
/// before the framework sends the optional client acknowledgement.
#[derive(Debug)]
pub struct Reply<T = ()> {
    pub(crate) data: T,
    pub(crate) rooms: Vec<RoomAction>,
    pub(crate) payload_ack: bool,
    pub(crate) after_reply: Vec<AfterReply>,
}
impl<T> Reply<T> {
    /// Reply with business data.
    pub fn new(data: T) -> Self {
        Self {
            data,
            rooms: Vec::new(),
            payload_ack: false,
            after_reply: Vec::new(),
        }
    }
    /// Send the business value as one acknowledgement argument, without the
    /// Breeze success envelope. Arrays, tuples and null remain one argument.
    ///
    /// Room actions and automatic ACK ownership are unchanged. This only
    /// selects successful reply encoding; [`ClientError`] keeps its safe
    /// framework error envelope. Events without an ACK ID receive no reply.
    pub fn payload(data: T) -> Self {
        Self {
            data,
            rooms: Vec::new(),
            payload_ack: true,
            after_reply: Vec::new(),
        }
    }
    /// Invoke an application continuation after successful room actions and
    /// the optional ACK send attempt. An ACK has been queued or failed here;
    /// this does not establish a transport flush or receipt by the peer.
    ///
    /// Continuations run once, in registration order, also for events without
    /// an ACK ID and after ACK serialization/send failures. Business errors or
    /// rejected room actions do not run them. Keep callbacks nonblocking: hand
    /// work to an application-owned queue with its normal shutdown and limits.
    #[must_use]
    pub fn after_reply(mut self, continuation: impl FnOnce() + Send + Sync + 'static) -> Self {
        self.after_reply.push(AfterReply(Box::new(continuation)));
        self
    }
    /// Join a namespace-local room on successful completion.
    #[must_use]
    pub fn join(mut self, room: impl Into<String>) -> Self {
        self.rooms.push(RoomAction::Join(room.into()));
        self
    }
    /// Leave a namespace-local room on successful completion.
    #[must_use]
    pub fn leave(mut self, room: impl Into<String>) -> Self {
        self.rooms.push(RoomAction::Leave(room.into()));
        self
    }
}

#[doc(hidden)]
pub trait HandlerResult {
    type Value;
    fn into_result(self) -> Result<Self::Value, ClientError>;
}
impl<T, E: std::fmt::Debug + Into<ClientError>> HandlerResult for Result<T, E> {
    type Value = T;
    fn into_result(self) -> Result<T, ClientError> {
        self.map_err(|error| {
            tracing::debug!(?error, "Socket.IO business handler failed");
            error.into()
        })
    }
}
impl HandlerResult for () {
    type Value = ();
    fn into_result(self) -> Result<(), ClientError> {
        Ok(())
    }
}

#[doc(hidden)]
pub trait IntoAccept {
    type Session: Send + Sync + 'static;
    fn into_accept(self) -> Accept<Self::Session>;
}
impl<S: Send + Sync + 'static> IntoAccept for Accept<S> {
    type Session = S;
    fn into_accept(self) -> Self {
        self
    }
}
impl IntoAccept for () {
    type Session = ();
    fn into_accept(self) -> Accept<()> {
        Accept::default()
    }
}

#[doc(hidden)]
pub enum Plain {}
#[doc(hidden)]
pub enum Actions {}
#[doc(hidden)]
pub trait IntoReply<Kind> {
    type Data: Serialize;
    fn into_reply(self) -> Reply<Self::Data>;
}
impl<T: Serialize> IntoReply<Plain> for T {
    type Data = T;
    fn into_reply(self) -> Reply<T> {
        Reply::new(self)
    }
}
impl<T: Serialize> IntoReply<Actions> for Reply<T> {
    type Data = T;
    fn into_reply(self) -> Self {
        self
    }
}
#[doc(hidden)]
pub fn event_reply<T: IntoReply<K>, K>(value: T) -> Reply<T::Data> {
    value.into_reply()
}

#[derive(Serialize)]
pub(crate) struct Success<T> {
    pub ok: bool,
    pub data: T,
}
#[derive(Serialize)]
pub(crate) struct Failure {
    pub ok: bool,
    pub error: ClientError,
}
