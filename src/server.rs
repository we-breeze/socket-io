use std::future::Future;

use tokio::{net::TcpListener, time::timeout};

use crate::{
    Config, Error, SocketIo, SocketIoBuilder,
    adapter::{Adapter, LocalAdapter},
    provider::{MemoryProvider, Provider},
};

/// A clonable Hyper/Tower service for embedding in an existing HTTP runtime.
pub type Service<A = LocalAdapter> =
    socketioxide::service::SocketIoService<engineioxide::service::NotFoundService, A>;

/// Server construction with an application-selected provider.
pub struct Builder<P = MemoryProvider> {
    config: Config,
    builder: SocketIoBuilder,
    provider: P,
}

impl Default for Builder<MemoryProvider> {
    fn default() -> Self {
        Self {
            config: Config::default(),
            builder: SocketIo::builder(),
            provider: MemoryProvider,
        }
    }
}

impl<P: Provider> Builder<P> {
    /// Replace protocol and standalone listener configuration.
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Inject clonable typed application state into all handlers.
    pub fn with_state<T: Clone + Send + Sync + 'static>(mut self, state: T) -> Self {
        self.builder = self.builder.with_state(state);
        self
    }

    /// Choose a provider without changing handlers or transport settings.
    pub fn with_provider<Q: Provider>(self, provider: Q) -> Builder<Q> {
        Builder {
            config: self.config,
            builder: self.builder,
            provider,
        }
    }

    /// Validate settings, initialize the provider and build the service.
    /// Namespaces are explicitly registered by the application after construction.
    pub async fn build(self) -> Result<Server<P::Adapter>, Error> {
        self.config.validate()?;
        let builder = self.config.apply(self.builder);
        let builder = self
            .provider
            .configure(builder)
            .await
            .map_err(Error::Provider)?;
        let (service, io) = builder.build_svc();
        Ok(Server {
            service,
            io,
            config: self.config,
        })
    }
}

/// Snapshot of local Socket.IO namespace and socket membership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Status {
    /// Registered namespaces on this process.
    pub namespaces: usize,
    /// Connected namespace sockets, not unique Engine.IO transport connections.
    pub local_sockets: usize,
}

/// A Socket.IO service and its process-owned event/emitter handle.
///
/// A server can be used without binding a listener: register the required
/// namespaces, then use [`Server::io`] for cross-instance emit and ACK requests.
pub struct Server<A: Adapter = LocalAdapter> {
    service: Service<A>,
    io: SocketIo<A>,
    config: Config,
}

impl Server<LocalAdapter> {
    /// Build a server using in-memory storage unless a provider is selected.
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl<A: Adapter> Server<A> {
    /// Prepare a typed outgoing event and its metrics once during application
    /// startup, after registering the namespace. Clone and target it as needed.
    pub fn emitter(
        &self,
        namespace: &'static str,
        event: &'static str,
    ) -> Result<crate::EventEmitter<A>, crate::EmitterError> {
        crate::EventEmitter::new(self.io.clone(), namespace, event)
    }
    /// Native typed Socket.IO API: namespaces, middleware, rooms, emits and ACKs.
    pub fn io(&self) -> &SocketIo<A> {
        &self.io
    }

    /// Share a listener with normal Breeze HTTP handlers, without a gateway.
    /// The HTTP adapter keeps ordinary handler limits separate from long polls
    /// and WebSocket lifetimes. The caller closes this Socket.IO server when
    /// shutting down the shared listener.
    pub fn http_service<H: brz_http_server::Handler>(
        &self,
        handler: H,
        mut config: brz_http_server::ServerConfig,
    ) -> brz_http_server::Result<
        brz_http_server::hyper::Mount<Service<A>, brz_http_server::hyper::HandlerService<H>>,
    > {
        let cors = config.cors.take();
        let mount = brz_http_server::hyper::Mount::new(
            self.config.path.clone(),
            self.service(),
            brz_http_server::hyper::HandlerService::new(handler, config)?,
        )?;
        match cors {
            Some(cors) => mount.with_cors(cors),
            None => Ok(mount),
        }
    }

    /// Clone the service for use in an existing Hyper or Tower listener.
    pub fn service(&self) -> Service<A> {
        self.service.clone()
    }

    /// Validated protocol and standalone listener configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Obtain a local membership snapshot without querying a remote provider.
    pub fn status(&self) -> Status {
        let namespaces = self.io.nsps();
        Status {
            namespaces: namespaces.len(),
            local_sockets: namespaces.into_iter().map(|ns| ns.sockets().len()).sum(),
        }
    }

    /// Close local sockets and namespace providers within the shutdown deadline.
    /// Call this explicitly when embedding [`Server::service`] in another runtime.
    pub async fn close(&self) -> Result<(), Error> {
        timeout(self.config.shutdown_timeout, self.close_inner())
            .await
            .map_err(|_| Error::ShutdownTimeout)
    }

    async fn close_inner(&self) {
        let namespaces = self.io.nsps();
        let sockets: Vec<_> = namespaces
            .iter()
            .flat_map(|ns| ns.clone().sockets())
            .collect();
        self.io.close().await;
        // Socketioxide 0.18.7 removes its namespace registry before closing
        // transports, so transport callbacks can no longer find these sockets.
        // Retain membership until pending disconnect handlers have completed.
        for socket in sockets {
            if socket.connected() {
                let _ = socket.disconnect();
            }
        }
        while namespaces.iter().any(|ns| !ns.clone().sockets().is_empty()) {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    /// Serve HTTP/1.1 polling and upgraded WebSocket connections until shutdown.
    /// The caller owns address selection, TLS termination and shutdown signals.
    pub async fn serve<F: Future<Output = ()>>(
        self,
        listener: TcpListener,
        shutdown: F,
    ) -> Result<(), Error> {
        let service = self.service();
        let config = brz_http_server::hyper::Config {
            max_connections: self.config.max_http_connections,
            max_request_head_bytes: self.config.max_http_buffer_bytes,
            header_read_timeout: self.config.header_read_timeout,
            // Protocol callbacks/providers already drain under shutdown_timeout
            // before this listener receives its shutdown signal. Any remaining
            // sockets (including rejected namespace attempts) must then close.
            shutdown_grace: std::time::Duration::ZERO,
            ..Default::default()
        };
        let shutdown = async {
            shutdown.await;
            self.close().await
        };
        tokio::pin!(shutdown);
        // Close protocol state before the shared listener drains its sockets.
        let mut close_result = Ok(());
        let signal = async {
            close_result = (&mut shutdown).await;
        };
        let result =
            brz_http_server::hyper::serve(listener, move |_| service.clone(), signal, config).await;
        close_result?;
        // Also release provider/protocol state if the listener failed before
        // the application shutdown future completed.
        self.close().await?;
        result.map_err(Error::Io)
    }
}
