//! Provider construction is independent from application handlers and transport.
//!
//! Providers return a configured Socketioxide builder, preserving state injected
//! by the application. An external crate can implement [`Provider`] or wrap an
//! existing adapter with [`AdapterProvider`].

use std::{future::Future, marker::PhantomData};

use crate::{
    ProviderError, SocketIoBuilder,
    adapter::{Adapter, LocalAdapter},
};

#[cfg(feature = "redis")]
pub mod redis;

/// Asynchronous construction of a Socket.IO adapter.
pub trait Provider: Send {
    /// Adapter shared by the server handle and every registered handler.
    type Adapter: Adapter;

    /// Attach this provider to the builder. Implementations must preserve the
    /// caller's protocol configuration and injected application state.
    fn configure(
        self,
        builder: SocketIoBuilder,
    ) -> impl Future<Output = Result<SocketIoBuilder<Self::Adapter>, ProviderError>> + Send;
}

/// Process-local room and socket storage with no external connections.
#[derive(Clone, Copy, Debug, Default)]
pub struct MemoryProvider;

impl Provider for MemoryProvider {
    type Adapter = LocalAdapter;

    async fn configure(self, builder: SocketIoBuilder) -> Result<SocketIoBuilder, ProviderError> {
        Ok(builder)
    }
}

/// Integrate an existing Socketioxide adapter without a Breeze-specific wrapper.
pub struct AdapterProvider<A: Adapter> {
    state: A::State,
    adapter: PhantomData<fn() -> A>,
}

impl<A: Adapter> AdapterProvider<A> {
    /// Use initialized adapter state. The adapter owns namespace initialization.
    pub fn new(state: A::State) -> Self {
        Self {
            state,
            adapter: PhantomData,
        }
    }
}

impl<A: Adapter> Provider for AdapterProvider<A> {
    type Adapter = A;

    async fn configure(
        self,
        builder: SocketIoBuilder,
    ) -> Result<SocketIoBuilder<A>, ProviderError> {
        Ok(builder.with_adapter::<A>(self.state))
    }
}
