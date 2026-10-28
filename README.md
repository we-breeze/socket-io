# Breeze Socket.IO

`brz-socket-io` provides Socket.IO services with pluggable providers. It reuses
Socketioxide's protocol engine, adding business-facing handlers, automatic
ACKs and metrics, typed namespace sessions, provider initialization and shared
Breeze HTTP listeners.
Application event schemas, identity, authorization and recovery remain in the
application. Requires Rust **1.94+**, matching Socketioxide 0.18.7.

Published releases use crates.io dependencies:

```toml
[dependencies]
brz-socket-io = { version = "=0.0.2", features = ["macros"] }
```

## Services and events

```rust,no_run
use brz_socket_io::{Server, extract::{AckSender, Data, SocketRef}};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let server = Server::builder().build().await?;
server.io().ns("/events", async |socket: SocketRef| {
    // A private room enables SID-addressed emits, including from another node.
    socket.join(socket.id);
    socket.on("echo", async |Data(value): Data<String>, ack: AckSender| {
        ack.send(&value).ok();
    });
});
let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
server.serve(listener, std::future::pending()).await?;
# Ok(())
# }
```

Run `cargo run --example echo` and connect with the official JavaScript client:

```javascript
import { io } from 'socket.io-client'
const socket = io('http://127.0.0.1:3000')
socket.on('connect', async () => {
  console.log(await socket.timeout(3000).emitWithAck('echo', 'hello'))
})
```

Supports Socket.IO protocol v5 / Engine.IO v4 (JavaScript Socket.IO 3.x/4.x),
HTTP polling, WebSocket and polling-to-WebSocket upgrade. A raw WebSocket client
must implement Engine.IO and Socket.IO framing. Namespaces are registered
explicitly, including `/`; the HTTP `Config::path` is independent of namespaces.

The native `SocketIo`, `SocketRef`, `Data`, `TryData`, `AckSender`, `State`,
`Extension`, `ConnectHandler`, room/broadcast operators, dynamic namespaces and
disconnect handlers are re-exported. JSON and binary packets are supported.
Handler extractors use the selected adapter type; see `examples/redis.rs`.

## Exports and a shared HTTP listener

Enable `macros` to export business functions. A namespace owns logical sessions,
authentication and isolated room membership; `(namespace, event)` selects the
incoming handler. Rooms select outgoing recipients.

```rust,ignore
use brz_socket_io::{Accept, ClientError, Reply};
use std::sync::Arc;

brz_socket_io::registry!(dependencies(auth: Arc<AuthService>, chat: Arc<ChatService>));

#[brz_socket_io::middleware]
async fn authorize(
    #[payload] credential: Credential,
    #[inject(auth)] auth: &AuthService,
) -> Result<User, ClientError> {
    auth.verify(credential).await
}

#[brz_socket_io::namespace("/chat", middleware = authorize)]
async fn connected(
    #[auth] user: &User,
    #[inject(chat)] chat: &ChatService,
) -> Result<Accept<ChatSession>, ClientError> {
    let session = chat.open_session(user).await?;
    Ok(Accept::new(session).join(format!("user:{}", user.id)))
}

#[brz_socket_io::event("send_message", namespace = "/chat")]
async fn send_message(
    #[payload] input: SendMessage,
    #[auth] user: &User,
    #[session] session: &ChatSession,
    #[inject(chat)] chat: &ChatService,
) -> Result<Message, ClientError> {
    chat.send_message(user, session, input).await
}

#[brz_socket_io::event("leave_conversation", namespace = "/chat")]
async fn leave_conversation(
    #[payload] input: LeaveConversation,
    #[auth] user: &User,
    #[inject(chat)] chat: &ChatService,
) -> Result<Reply<()>, ClientError> {
    let room = chat.leave_conversation(user, input).await?;
    Ok(Reply::new(()).leave(room))
}

#[brz_socket_io::event(fallback, namespace = "/chat")]
async fn other_event(#[event_name] name: &str, #[payload] input: serde_json::Value)
    -> Result<(), ClientError>
{
    Err(ClientError::new("UNKNOWN_EVENT", "Unsupported event"))
}

#[brz_socket_io::disconnect(namespace = "/chat")]
async fn disconnected(#[session] session: &ChatSession) -> Result<(), ClientError> {
    session.close().await
}

// During startup:
brz_socket_io::handlers!(server.io(), auth, chat).await?;
```

Authentication runs once for each namespace connection. The returned `User` is
saved in that namespace session and borrowed by each event; it does not need
`Clone`. Reconnection authenticates again. Different namespaces on one physical
connection have separate identity/session slots, even if their business types
are identical. The framework releases stored state at disconnection; in-flight
handlers and the optional disconnect hook retain their values until completion.
Use application services to check token expiry, revocation and operation-specific
permissions. The framework does not infer security policy from `User` fields.

Both authentication and the connection handler finish **before** namespace
acceptance. Either can reject with `connect_error`. The underlying engine sends
the safe error message; arbitrary structured `connect_error.data` is not added.
`Accept<S>` saves typed session data and initial rooms. `Result<(), E>` or an
empty `async fn connected() {}` accepts a session without extra business state.
Use application-owned `Arc<Mutex<T>>` or other synchronization inside `S` when
mutable session state is needed. Event handlers run concurrently.

Business handlers never receive a native socket, ACK sender or provider type.
`#[payload]` binds one owned typed DTO (including binary data); `#[auth]` and
`#[session]` borrow the saved values. `#[context] context: &ConnectionContext`
optionally supplies the namespace socket ID, namespace, handshake URI/headers
and TCP peer address. `#[inject(name)]` borrows or clones named service dependencies.
Convert application errors explicitly with `From<AppError> for ClientError`;
only its safe code/message are sent to clients. Errors need `Debug` for internal
logging. Malformed payloads produce `INVALID_PAYLOAD` before calling business code.

The framework automatically sends **one** ACK when the client supplied an ACK ID:

```json
{"ok":true,"data":{"id":"message-123"}}
{"ok":false,"error":{"code":"FORBIDDEN","message":"No permission"}}
```

This envelope is Breeze policy. Events without ACK IDs do not receive an ACK.
For an existing client contract that expects the business value directly,
return `Result<Reply<T>, ClientError>` and construct `Reply::payload(value)`.
The successful ACK contains exactly one argument with that value, including
when the value is an array, tuple or null; binary attachments remain supported.
For example, `Reply::payload(json!({"success": true}))` sends
`[{"success":true}]` as the ACK arguments. Ordinary `Result<T, E>` and
`Reply::new` keep the Breeze envelope. `ClientError` still uses the safe error
envelope; an application-defined error object can be an explicit business value.

`Reply<T>` also declares room changes; successful actions run before
the optional ACK. Disconnect cleanup prevents late handlers from recreating room
membership. Each accepted socket also joins its private ID room for addressed
service emissions. A disconnect hook can read the saved identity/session even
though framework state and room cleanup have already begun; hook failure does
not prevent cleanup. Shutdown bounds hook execution with the server deadline.

Fixed events take priority over the optional namespace fallback. `#[event_name]`
provides the original event name to the fallback (or a named handler). The
framework uses the declared fallback identity for metrics, never dynamic event
names. Duplicate namespace/event/disconnect exports, missing namespaces and
incompatible auth/session types are startup errors before mounting. Separate
collections use `group = name`, including middleware and disconnect exports.
`handlers!` awaits provider subscriptions. Select `adapter = RedisAdapter` only
in `registry!`; the business functions above keep the same signatures.

Use `server.http_service(http_apis, http_config)` with
`brz_http_server::hyper::serve` to share a single listener with ordinary HTTP
macro routes. See the runnable `cargo run --features macros --example http`.
No gateway or additional Socket.IO port is required. The optional Hyper runtime
in `brz-http-server` owns negotiation, upgrades, read-ahead bytes, connection
limits and shutdown; `HandlerService` adapts the ordinary Breeze router.
Native `brz-http-server::Server` remains the default HTTP path without Hyper.
Protocol mounts bypass the short ordinary HTTP handler timeout, so long polls
and WebSocket sessions are not cut off at that deadline. `ServerConfig::cors`
applies one public policy to both the HTTP routes and protocol mount; when a
gateway already owns CORS, leave that inner policy disabled.

The HTTP path is configurable (`Config { path: "/realtime".into(), .. }`),
independent of namespace names. `/socket.io/` is the Socket.IO default. For the
HTTP example, the official client can connect with:

```javascript
io('http://127.0.0.1:3000/chat', {
  path: '/realtime',
  transports: ['websocket']
})
```

## Application state and policy

The macro registry supplies shared dependencies and stores business identity and
session values. The low-level `io()` API remains available for provider/adaptor
integrations; native `Builder::with_state`, `State` and `socket.extensions` are
intended for those integrations. Prefer the business exports for application
handlers. Per-event authorization and state mutation remain explicit business
operations; ordered operations can use application-owned keyed locks or queues.

Choose origin/CORS policy at the public HTTP boundary. The optional Breeze
gateway's `with_cors` handles browser polling preflights and response headers;
the library does not install a second CORS policy. WebSocket Origin admission
and authentication belong in the public boundary or connection middleware.

## Providers

No feature is enabled by default; `MemoryProvider` keeps room/socket state in
one process without external connections. Implement `provider::Provider` to
construct a new adapter while preserving the provided builder's configuration
and application state. `AdapterProvider<A>` accepts initialized state for any
existing Socketioxide adapter. Provider initialization failures are returned;
there is no silent fallback to process-local storage.

Enable `redis` for the Redis provider:

```toml
brz-socket-io = { version = "=0.0.2", features = ["redis"] }
```

```rust,no_run
use brz_socket_io::{Server, provider::redis::{RedisProvider, RedisAdapterConfig, RedisSocket}};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let provider = RedisProvider::new("redis://127.0.0.1:6379")?
    .with_adapter_config(RedisAdapterConfig::default().with_prefix("my-service"));
let monitor = provider.monitor();
let server = Server::builder().with_provider(provider).build().await?;
// Redis namespaces initialize asynchronously; await readiness before serving.
server.io().ns("/events", async |socket: RedisSocket| {
    socket.join(socket.id);
}).await?;
server.io().of("/events").unwrap().to("updates").emit("notice", "hello").await?;
assert_eq!(monitor.dropped_messages(), 0);
server.close().await?;
# Ok(())
# }
```

Requires Redis **7+** and RESP3. The optional Redis driver uses redis-rs's
connection manager with bounded connect/command timeouts, reconnect backoff and
automatic namespace resubscription. Clones share one connection and subscription
registry. Receive handlers are installed before subscribing to avoid losing the
first message. Buffers are bounded: on overflow the newest message is dropped,
a tracing warning is emitted and `RedisMonitor::dropped_messages` increments.
`disconnections` and `initial_connection_id` provide diagnostics without exposing
the Redis URL. Explicit channel prefixes isolate applications sharing Redis.

This is **Socketioxide's cluster protocol**. Redis Pub/Sub is not durable and
does not replay messages missed during an outage. Client protocol compatibility
does not make Redis adapters interoperable with Node Socket.IO or Python
managers. Broker-format migration and any application history/offset recovery
must be implemented outside this crate.

For a background emitter, build the same Redis-backed server and await the
required namespace registration, but do not bind an HTTP listener. The shared
`server.io()` handle supports both broadcasts and ACK requests with no local
clients. This uses the same protocol implementation and version as serving nodes.

## ACKs, rooms and request semantics

Prepare outgoing events once while constructing application services, after
registering namespaces. Their static names are reused by metrics; target IDs
and rooms are not metric dimensions:

```rust,ignore
let notice = server.emitter("/chat", "notice")?;
notice.clone().to("updates").emit(&payload).await?;
let question = server.emitter("/chat", "question")?;
let reply: Answer = question.to(socket_id)
    .request(&payload, std::time::Duration::from_secs(5)).await?;
```

`EventEmitter` supports typed JSON/binary emissions and a typed first-response
request with a positive timeout, using the selected provider. A business service
can hide its provider choice behind its own interface. Native broadcast/ACK
streams remain available for specialized adapters:

```rust,ignore
let replies = server.io().of("/chat").unwrap().to("updates")
    .timeout(std::time::Duration::from_secs(5))
    .emit_with_ack::<_, Answer>("question", &payload).await?;
```

An ACK stream can be awaited for the first response or consumed as a stream of
`(SID, response)` for all selected sockets. A timeout or cancelling the waiting
future does not prove that remote work stopped. Requests are never automatically
replayed by this library. The application owns idempotency and cancellation.

**Upstream ACK lifetime limitation:** Socketioxide 0.18.7 keeps the sender for an
unanswered local ACK until a late response arrives or its namespace socket is
released, even after the waiting future times out or is dropped. Cancellation
stops waiting, but does not remove that entry. Bound application request traffic
and retire unresponsive sockets when this matters; this crate does not claim
automatic ACK-entry reclamation or exactly-once execution. Redis broadcast
response-stream registrations are removed when their stream is dropped.

## Metrics

The default `metrics` feature uses the single `brz-metrics` registry and
`type="SOCKET_IO"`. Macro collection prepares handles once at startup, and the
hot path records through those handles. Metrics cover namespace admissions and
duration, current/cumulative sessions and disconnects, event outcomes/duration/
slow calls, room actions, and requested ACK sends. A successful business return,
action outcome and ACK-send outcome are recorded separately. No-ACK events do
not increment ACK-send metrics. `EventEmitter` separately measures outgoing
publish, response waiting and response timeouts. Direct native `io()` operations
are low-level and do not pass through these business metrics.

Identities contain only declared namespace/event names (length-prefixed to avoid
name collisions), or a declared fallback. SID, user IDs, dynamic room names and
dynamic fallback event names are never labels. The service slow policy is 200 ms.
Disable with `default-features = false`; `macros` also works without metrics.

All Breeze SDKs in one process must use the same metrics registry version.
This release uses `brz-metrics = "=0.0.13"`; no local path patch is required.

## Listener ownership and limits

`Config` validates paths, positive finite deadlines and resource limits before
opening provider connections. Defaults include a 1 MB incoming payload/binary
budget, 128 outgoing buffered packets, 128 binary attachments, 15-second HTTP
header deadline and 10-second shutdown deadline. Exhausted send queues return
the native send error. Binary completion and namespace connection deadlines are
also bounded. The standalone listener serves HTTP/1.1; TLS and HTTP/2 can be
terminated at an outer ingress.

`Server::serve` owns its HTTP connections, stops accepting on the supplied
shutdown signal, closes Socket.IO namespaces and releases remaining HTTP tasks within the
shutdown deadline. Local socket references are retained through transport close
so pending disconnect callbacks run even after the upstream namespace registry
is removed. These callbacks receive `ServerNSDisconnect`; shutdown waits for
their completion within its deadline. Use `Server::close`, rather than calling
the native handle's `close` directly, to obtain this behavior.
Header limits and `max_http_connections` apply to that listener only. The
connection limit includes upgraded WebSocket sessions until their sockets are
released. `Server::status` reports local namespace/socket counts,
not unique transport counts or cluster-wide presence.

Use `server.service()` to embed the Hyper/Tower service in another runtime and
call `server.close().await` during its shutdown. A gateway is optional and uses
`OriginService` to forward the configured HTTP path to this backend (see
`examples/gateway.rs`); the Socket.IO library has no gateway dependency. Only a
successful HTTP 101 changes the gateway into a bidirectional byte relay to the
selected backend. It does not inspect WebSocket frames, namespaces, events,
ACKs or rooms. Ordinary HTTP continues to select a backend per request.

For multiple backends, direct WebSocket transport needs only the established
connection pair. HTTP polling can use different TCP connections for one
Engine.IO session and therefore requires separate affinity if load balanced.
That routing policy is outside this library; the Redis provider does not share
Engine.IO sessions or remove the affinity requirement.

## Validation

```sh
cargo fmt --all -- --check
cargo test --locked --no-default-features
BRZ_SOCKET_IO_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test --locked --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo package --locked --allow-dirty
```

Redis-enabled integration tests require the explicit URL of an isolated Redis
7+ test instance; absence is an error, never a silent skip. One test kills its
own provider's exact Redis connection ID to verify resubscription while keeping
the WebSocket and room membership alive. Tests cover real polling/WebSocket
traffic, typed state and auth hooks, room/namespace/prefix isolation, binary
attachments, ACK timeouts/cancellation, queue limits and shutdown. CI also runs
the official JavaScript client against polling, WebSocket and transport upgrade:

```sh
npm ci --prefix tests/client --ignore-scripts
# Start the echo example in a separate terminal first.
npm test --prefix tests/client
```

Licensed under MIT OR Apache-2.0. Socketioxide and its Redis adapter retain their
own MIT license and attribution through Cargo dependencies.
