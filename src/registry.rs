use crate::{ProviderError, SocketIo, adapter::Adapter, extract::SocketRef};
use crate::{
    business::{DisconnectFuture, DisconnectHook, Disconnected, StateTypes},
    metrics::{EventMetrics, NamespaceMetrics},
};
use std::future::Future;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// Completion of a provider's namespace initialization. Custom providers can
/// implement this for their own adapter initialization result. Exports wait for
/// readiness before accepting clients or allowing background emits.
pub trait NamespaceInit: Send {
    /// Await subscriptions/resources and propagate startup failures.
    fn ready(self) -> impl Future<Output = Result<(), ProviderError>> + Send;
}
impl NamespaceInit for () {
    async fn ready(self) -> Result<(), ProviderError> {
        Ok(())
    }
}
#[cfg(feature = "redis")]
impl NamespaceInit for socketioxide_redis::InitRes<crate::provider::redis::RedisDriver> {
    async fn ready(self) -> Result<(), ProviderError> {
        self.await.map_err(|error| Box::new(error) as ProviderError)
    }
}

/// A startup registration error. Validation completes before mounting any new
/// namespaces, so duplicate exports cannot silently replace handlers.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// A provider failed while preparing a namespace.
    #[error("Socket.IO namespace {namespace:?} initialization failed: {source}")]
    Initialization {
        /// Namespace whose resources could not be initialized.
        namespace: String,
        /// Underlying provider initialization error.
        #[source]
        source: ProviderError,
    },
    /// More than one connection entrypoint exported the same namespace.
    #[error("duplicate Socket.IO namespace: {0}")]
    DuplicateNamespace(String),
    /// An event was exported more than once within one namespace.
    #[error("duplicate Socket.IO event {event:?} in namespace {namespace:?}")]
    DuplicateEvent {
        /// Namespace containing the conflicting exports.
        namespace: String,
        /// Conflicting event name.
        event: String,
    },
    /// Events must refer to an explicitly exported namespace.
    #[error("event {event:?} refers to undeclared namespace {namespace:?}")]
    UnknownNamespace {
        /// Namespace missing its connection export.
        namespace: String,
        /// Event referring to that namespace.
        event: String,
    },
    /// The native API already registered this namespace.
    #[error("Socket.IO namespace is already mounted: {0}")]
    AlreadyMounted(String),
    /// Only one disconnect hook can own a namespace's business teardown.
    #[error("duplicate Socket.IO disconnect handler: {0}")]
    DuplicateDisconnect(String),
    /// A typed extractor does not match the namespace's declared state.
    #[error("Socket.IO {handler} in namespace {namespace:?} expects incompatible {slot} state")]
    IncompatibleState {
        /// Namespace providing the stored identity and session.
        namespace: String,
        /// Connection, event or disconnect handler with the incompatible binding.
        handler: String,
        /// Either `auth` or `session`.
        slot: &'static str,
    },
}
#[doc(hidden)]
pub type RegisterNamespace<S, A, R> =
    fn(&SocketIo<A>, Arc<S>, Bindings<S, A>, NamespaceMetrics) -> R;
#[doc(hidden)]
pub struct NamespaceRegistration<S, A: Adapter> {
    pub namespace: &'static str,
    pub provides: fn() -> StateTypes,
    pub requires: fn() -> StateTypes,
    pub register: RegisterNamespace<S, A, A::InitRes>,
}
#[doc(hidden)]
pub struct EventRegistration<S, A: Adapter> {
    pub namespace: &'static str,
    pub event: Option<&'static str>,
    pub requires: fn() -> StateTypes,
    pub register: fn(&SocketRef<A>, Arc<S>, EventMetrics),
}
#[doc(hidden)]
pub struct DisconnectRegistration<S> {
    pub namespace: &'static str,
    pub requires: fn() -> StateTypes,
    pub invoke: fn(Disconnected, Arc<S>) -> DisconnectFuture,
}
impl<S> Copy for DisconnectRegistration<S> {}
impl<S> Clone for DisconnectRegistration<S> {
    fn clone(&self) -> Self {
        *self
    }
}
#[doc(hidden)]
pub struct Bindings<S, A: Adapter> {
    pub events: Vec<(EventRegistration<S, A>, EventMetrics)>,
    pub disconnect: Option<DisconnectRegistration<S>>,
}
impl<S: Send + Sync + 'static, A: Adapter> Bindings<S, A> {
    pub fn bind(&self, socket: &SocketRef<A>, state: Arc<S>) {
        for (event, metrics) in &self.events {
            (event.register)(socket, state.clone(), *metrics);
        }
    }
    pub fn disconnect(&self, state: Arc<S>) -> Option<DisconnectHook> {
        self.disconnect.map(|handler| {
            Arc::new(move |data| (handler.invoke)(data, state.clone())) as DisconnectHook
        })
    }
}
impl<S, A: Adapter> Copy for EventRegistration<S, A> {}
impl<S, A: Adapter> Clone for EventRegistration<S, A> {
    fn clone(&self) -> Self {
        *self
    }
}
#[doc(hidden)]
pub async fn collect<S: Send + Sync + 'static, A: Adapter>(
    io: &SocketIo<A>,
    state: S,
    namespaces: &[&NamespaceRegistration<S, A>],
    events: &[&EventRegistration<S, A>],
    disconnects: &[&DisconnectRegistration<S>],
) -> Result<(), RegistryError>
where
    A::InitRes: NamespaceInit,
{
    let mut names = BTreeSet::new();
    for entry in namespaces {
        if !names.insert(entry.namespace) {
            return Err(RegistryError::DuplicateNamespace(entry.namespace.into()));
        }
        if io.of(entry.namespace).is_some() {
            return Err(RegistryError::AlreadyMounted(entry.namespace.into()));
        }
    }
    let mut by_namespace = BTreeMap::<_, Vec<_>>::new();
    let mut keys = BTreeSet::new();
    for entry in events {
        if !names.contains(entry.namespace) {
            return Err(RegistryError::UnknownNamespace {
                namespace: entry.namespace.into(),
                event: entry.event.unwrap_or("<fallback>").into(),
            });
        }
        if !keys.insert((entry.namespace, entry.event)) {
            return Err(RegistryError::DuplicateEvent {
                namespace: entry.namespace.into(),
                event: entry.event.unwrap_or("<fallback>").into(),
            });
        }
        by_namespace
            .entry(entry.namespace)
            .or_default()
            .push((**entry, EventMetrics::new(entry.namespace, entry.event)));
    }
    let mut disconnect_by_namespace = BTreeMap::new();
    for entry in disconnects {
        if !names.contains(entry.namespace) {
            return Err(RegistryError::UnknownNamespace {
                namespace: entry.namespace.into(),
                event: "<disconnect>".into(),
            });
        }
        if disconnect_by_namespace
            .insert(entry.namespace, **entry)
            .is_some()
        {
            return Err(RegistryError::DuplicateDisconnect(entry.namespace.into()));
        }
    }
    for namespace in namespaces {
        let provided = (namespace.provides)();
        validate(
            namespace.namespace,
            "connect",
            (namespace.requires)(),
            provided,
        )?;
        for event in events.iter().filter(|e| e.namespace == namespace.namespace) {
            validate(
                namespace.namespace,
                event.event.unwrap_or("<fallback>"),
                (event.requires)(),
                provided,
            )?;
        }
        if let Some(handler) = disconnect_by_namespace.get(namespace.namespace) {
            validate(
                namespace.namespace,
                "disconnect",
                (handler.requires)(),
                provided,
            )?;
        }
    }
    let state = Arc::new(state);
    let mut mounted = Vec::new();
    for entry in namespaces {
        let init = (entry.register)(
            io,
            state.clone(),
            Bindings {
                events: by_namespace.remove(entry.namespace).unwrap_or_default(),
                disconnect: disconnect_by_namespace.remove(entry.namespace),
            },
            NamespaceMetrics::new(entry.namespace),
        );
        if let Err(source) = init.ready().await {
            io.delete_ns(entry.namespace);
            for namespace in mounted {
                io.delete_ns(namespace);
            }
            return Err(RegistryError::Initialization {
                namespace: entry.namespace.into(),
                source,
            });
        }
        mounted.push(entry.namespace);
    }
    Ok(())
}
fn validate(
    namespace: &str,
    handler: &str,
    required: StateTypes,
    provided: StateTypes,
) -> Result<(), RegistryError> {
    for (slot, needs, has) in [
        ("auth", required.auth, provided.auth),
        ("session", required.session, provided.session),
    ] {
        if let Some(needs) = needs
            && !has.is_some_and(|has| has.id == needs.id)
        {
            tracing::error!(
                namespace,
                handler,
                slot,
                expected = needs.name,
                actual = has.map(|v| v.name),
                "incompatible Socket.IO state binding"
            );
            return Err(RegistryError::IncompatibleState {
                namespace: namespace.into(),
                handler: handler.into(),
                slot,
            });
        }
    }
    Ok(())
}
