//! Internal adapter between business exports and the protocol implementation.
#![allow(missing_docs)]
use crate::{
    Accept, ClientError, ConnectionContext, Reply,
    metrics::{EventMetrics, NamespaceMetrics, SessionMetric},
    reply::{Failure, RoomAction, Success},
};
use serde::{Serialize, de::DeserializeOwned};
use socketioxide::{
    adapter::Adapter,
    extract::{AckSender, SocketRef, TryData},
    handler::{
        ConnectHandler, ConnectMiddleware, DisconnectHandler, FromConnectParts, FromMessageParts,
        Value,
    },
    socket::{DisconnectReason, Socket},
};
use std::{
    any::{Any, TypeId, type_name},
    convert::Infallible,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type Identity = Arc<dyn Any + Send + Sync>;
#[derive(Clone, Copy, Debug)]
pub struct StateType {
    pub id: TypeId,
    pub name: &'static str,
}
pub fn state_type<T: Send + Sync + 'static>() -> StateType {
    StateType {
        id: TypeId::of::<T>(),
        name: type_name::<T>(),
    }
}
#[derive(Clone, Copy, Default)]
pub struct StateTypes {
    pub auth: Option<StateType>,
    pub session: Option<StateType>,
}

/// An infallible native extractor: application payload errors are handled by
/// Breeze, so they participate in rejection metrics and safe error responses.
pub struct ConnectInput<A: Adapter> {
    socket: Arc<Socket<A>>,
    auth: Option<Value>,
}
impl<A: Adapter> FromConnectParts<A> for ConnectInput<A> {
    type Error = Infallible;
    fn from_connect_parts(
        socket: &Arc<Socket<A>>,
        auth: &Option<Value>,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            socket: socket.clone(),
            auth: auth.clone(),
        })
    }
}
impl<A: Adapter> ConnectInput<A> {
    pub fn socket(&self) -> SocketRef<A> {
        self.socket.clone().into()
    }
    pub fn context(&self) -> ConnectionContext {
        make_context(&self.socket)
    }
    pub fn payload<T: DeserializeOwned>(&self) -> Result<T, ClientError> {
        let TryData(data) = TryData::<T>::from_connect_parts(&self.socket, &self.auth).unwrap();
        payload(data)
    }
}
pub fn identity<T: Send + Sync + 'static>(value: T) -> Identity {
    Arc::new(value)
}
pub fn internal_error() -> ClientError {
    ClientError::internal()
}
pub fn disconnect_result(result: Result<(), ClientError>) {
    if let Err(error) = result {
        tracing::debug!(code = %error.code, "Socket.IO disconnect hook failed");
    }
}
pub fn auth_as<T: Send + Sync + 'static>(identity: &Identity) -> Result<Arc<T>, ClientError> {
    identity
        .clone()
        .downcast()
        .map_err(|_| ClientError::internal())
}
pub fn payload<T, E: std::fmt::Debug>(data: Result<T, E>) -> Result<T, ClientError> {
    data.map_err(|error| {
        tracing::debug!(?error, "invalid Socket.IO business payload");
        ClientError::invalid_payload()
    })
}
fn make_context<A: Adapter>(socket: &Socket<A>) -> ConnectionContext {
    let request = socket.req_parts();
    ConnectionContext {
        id: socket.id.to_string(),
        namespace: socket.ns().into(),
        uri: request.uri.clone(),
        headers: request.headers.clone(),
        peer_addr: request
            .extensions
            .get::<brz_http_server::hyper::PeerAddr>()
            .map(|peer| peer.0)
            .or_else(|| request.extensions.get::<std::net::SocketAddr>().copied()),
    }
}
pub type DisconnectFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
pub type DisconnectHook = Arc<dyn Fn(Disconnected) -> DisconnectFuture + Send + Sync>;
pub struct Disconnected {
    pub auth: Option<Identity>,
    pub session: Identity,
    pub context: Arc<ConnectionContext>,
}
impl Disconnected {
    pub fn auth<T: Send + Sync + 'static>(&self) -> Result<Arc<T>, ClientError> {
        auth_as(self.auth.as_ref().ok_or_else(ClientError::internal)?)
    }
    pub fn session<T: Send + Sync + 'static>(&self) -> Result<Arc<T>, ClientError> {
        auth_as(&self.session)
    }
}
struct Values {
    auth: Option<Identity>,
    session: Option<Identity>,
    context: Arc<ConnectionContext>,
    online: Option<SessionMetric>,
    active: bool,
}
struct SessionState {
    values: Mutex<Values>,
}
fn stored<A: Adapter>(socket: &SocketRef<A>) -> Result<Arc<SessionState>, ClientError> {
    socket
        .extensions
        .get::<Arc<SessionState>>()
        .ok_or_else(ClientError::unavailable)
}
pub fn ensure_active<A: Adapter>(socket: &SocketRef<A>) -> Result<(), ClientError> {
    let state = stored(socket)?;
    if state.values.lock().unwrap().active && socket.connected() {
        Ok(())
    } else {
        Err(ClientError::unavailable())
    }
}
fn value<T: Send + Sync + 'static, A: Adapter>(
    socket: &SocketRef<A>,
    auth: bool,
) -> Result<Arc<T>, ClientError> {
    let state = stored(socket)?;
    let values = state.values.lock().unwrap();
    if !values.active {
        return Err(ClientError::unavailable());
    }
    let slot = if auth { &values.auth } else { &values.session };
    slot.clone()
        .ok_or_else(ClientError::internal)?
        .downcast()
        .map_err(|_| ClientError::internal())
}
pub fn auth<T: Send + Sync + 'static, A: Adapter>(
    socket: &SocketRef<A>,
) -> Result<Arc<T>, ClientError> {
    value(socket, true)
}
pub fn session<T: Send + Sync + 'static, A: Adapter>(
    socket: &SocketRef<A>,
) -> Result<Arc<T>, ClientError> {
    value(socket, false)
}
pub fn context<A: Adapter>(socket: &SocketRef<A>) -> Result<Arc<ConnectionContext>, ClientError> {
    Ok(stored(socket)?.values.lock().unwrap().context.clone())
}

pub fn prepare<A: Adapter, S: Send + Sync + 'static>(
    socket: &SocketRef<A>,
    auth: Option<Identity>,
    accept: Accept<S>,
    metrics: NamespaceMetrics,
    hook: Option<DisconnectHook>,
) {
    // Socket.IO sockets have a private room for addressed service emissions.
    socket.join(socket.id);
    if !accept.rooms.is_empty() {
        let observation = metrics.actions.start();
        apply(socket, accept.rooms);
        observation.finish(true);
    }
    socket.extensions.insert(Arc::new(SessionState {
        values: Mutex::new(Values {
            auth,
            session: Some(Arc::new(accept.session)),
            context: Arc::new(make_context(socket)),
            online: None,
            active: false,
        }),
    }));
    // Own the native callback. Clear framework data even if application code
    // retains a low-level SocketRef after disconnection.
    socket.on_disconnect(Cleanup { hook });
}
struct Cleanup {
    hook: Option<DisconnectHook>,
}
impl<A: Adapter> DisconnectHandler<A, ()> for Cleanup {
    fn call_with_defer(
        &self,
        socket: Arc<Socket<A>>,
        _: DisconnectReason,
        defer: fn(Arc<Socket<A>>),
    ) {
        let data = socket
            .extensions
            .remove::<Arc<SessionState>>()
            .and_then(|state| {
                let mut values = state.values.lock().unwrap();
                values.active = false;
                values.online.take();
                socket.leave_all();
                values.session.take().map(|session| Disconnected {
                    auth: values.auth.take(),
                    session,
                    context: values.context.clone(),
                })
            });
        if let (Some(hook), Some(data)) = (self.hook.clone(), data) {
            // The deferred removal survives business-hook panic/cancellation.
            let deferred = Deferred {
                socket: Some(socket),
                defer,
            };
            tokio::spawn(async move {
                let _deferred = deferred;
                hook(data).await;
            });
        } else {
            defer(socket);
        }
    }
}
struct Deferred<A: Adapter> {
    socket: Option<Arc<Socket<A>>>,
    defer: fn(Arc<Socket<A>>),
}
impl<A: Adapter> Drop for Deferred<A> {
    fn drop(&mut self) {
        if let Some(socket) = self.socket.take() {
            (self.defer)(socket);
        }
    }
}

/// Unlike a spawned async post-connect callback, this marks the prepared state
/// online synchronously before native dispatch can receive the first event.
struct PreparedNamespace<M, T> {
    middleware: M,
    metrics: NamespaceMetrics,
    marker: PhantomData<fn() -> T>,
}
impl<M: Clone, T> Clone for PreparedNamespace<M, T> {
    fn clone(&self) -> Self {
        Self {
            middleware: self.middleware.clone(),
            metrics: self.metrics,
            marker: PhantomData,
        }
    }
}
impl<A, M, T> ConnectHandler<A, (T,)> for PreparedNamespace<M, T>
where
    A: Adapter,
    M: ConnectMiddleware<A, T>,
    T: Send + Sync + 'static,
{
    fn call(&self, socket: Arc<Socket<A>>, _: Option<Value>) {
        if let Some(state) = socket.extensions.get::<Arc<SessionState>>() {
            let mut values = state.values.lock().unwrap();
            values.active = true;
            values.online = Some(self.metrics.connected());
        }
    }
    fn call_middleware<'a>(
        &'a self,
        socket: Arc<Socket<A>>,
        auth: &'a Option<Value>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn std::fmt::Display + Send>>> + Send + 'a>>
    {
        Box::pin(self.middleware.call(socket, auth))
    }
}
pub fn namespace_handler<A, M, T>(
    middleware: M,
    metrics: NamespaceMetrics,
) -> impl ConnectHandler<A, (T,)>
where
    A: Adapter,
    M: ConnectMiddleware<A, T>,
    T: Send + Sync + 'static,
{
    PreparedNamespace {
        middleware,
        metrics,
        marker: PhantomData,
    }
}

pub struct AckContext<A: Adapter> {
    sender: AckSender<A>,
    requested: bool,
}
impl<A: Adapter> FromMessageParts<A> for AckContext<A> {
    type Error = Infallible;
    fn from_message_parts(
        socket: &Arc<Socket<A>>,
        value: &mut Value,
        ack: &Option<i64>,
    ) -> Result<Self, Infallible> {
        Ok(Self {
            sender: AckSender::from_message_parts(socket, value, ack).unwrap(),
            requested: ack.is_some(),
        })
    }
}
fn apply<A: Adapter>(socket: &SocketRef<A>, actions: Vec<RoomAction>) {
    for action in actions {
        match action {
            RoomAction::Join(room) => socket.join(room),
            RoomAction::Leave(room) => socket.leave(room),
        }
    }
}
fn apply_reply<A: Adapter, T>(
    socket: &SocketRef<A>,
    reply: Reply<T>,
    metrics: EventMetrics,
) -> Result<T, ClientError> {
    let state = stored(socket)?;
    // Serialize framework mutations with teardown; a handler finishing after
    // disconnection cannot re-create orphan room memberships.
    let values = state.values.lock().unwrap();
    let observation = (!reply.rooms.is_empty()).then(|| metrics.actions.start());
    if !values.active || !socket.connected() {
        return Err(ClientError::unavailable());
    }
    apply(socket, reply.rooms);
    if let Some(observation) = observation {
        observation.finish(true);
    }
    Ok(reply.data)
}
pub fn complete_event<A: Adapter, T: Serialize>(
    socket: &SocketRef<A>,
    ack: AckContext<A>,
    result: Result<Reply<T>, ClientError>,
    metrics: EventMetrics,
) {
    let payload_ack = result.as_ref().is_ok_and(|reply| reply.payload_ack);
    let result = result.and_then(|reply| apply_reply(socket, reply, metrics));
    if !ack.requested {
        if let Err(error) = result {
            tracing::debug!(code = %error.code, "unacknowledged Socket.IO event failed");
        }
        return;
    }
    let observation = metrics.ack.start();
    let sent = match result {
        Ok(data) if payload_ack => ack.sender.send(&(data,)),
        Ok(data) => ack.sender.send(&Success { ok: true, data }),
        Err(error) => ack.sender.send(&Failure { ok: false, error }),
    };
    observation.finish(sent.is_ok());
    if let Err(error) = sent {
        tracing::debug!(?error, "Socket.IO acknowledgement failed");
    }
}
