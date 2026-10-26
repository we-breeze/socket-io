//! Business exports own neither native sockets nor protocol acknowledgements.
#![cfg(feature = "macros")]
#[allow(dead_code)]
mod common;
use brz_socket_io::{Accept, ClientError, ConnectionContext, RegistryError, Reply, Server};
use common::{Client, DEADLINE, Running};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{sync::Notify, time::timeout};

#[derive(Default)]
struct App {
    authentications: AtomicUsize,
    user_drops: Arc<AtomicUsize>,
    session_drops: Arc<AtomicUsize>,
    events: AtomicUsize,
    disconnected: AtomicUsize,
    changed: Notify,
    started: Notify,
    resume: Notify,
    last_disconnect: Mutex<Option<(String, String, String)>>,
}
// Deliberately identical auth/session types, without Clone: framework slots
// must remain distinct and injection must borrow the saved values.
struct State {
    name: String,
    drops: Arc<AtomicUsize>,
}
impl Drop for State {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Deserialize)]
struct Credential {
    name: String,
    allowed: bool,
}
brz_socket_io::registry!(group = business, dependencies(app: Arc<App>));
#[brz_socket_io::middleware(group = business)]
async fn authorize(
    #[payload] credential: Credential,
    #[inject(app)] app: &App,
) -> Result<State, ClientError> {
    app.authentications.fetch_add(1, Ordering::SeqCst);
    if !credential.allowed {
        return Err(ClientError::new("UNAUTHORIZED", "Access denied"));
    }
    Ok(State {
        name: credential.name,
        drops: app.user_drops.clone(),
    })
}
#[brz_socket_io::namespace("/business", group = business, middleware = authorize)]
async fn connected(
    #[auth] user: &State,
    #[context] context: &ConnectionContext,
    #[inject(app)] app: &App,
) -> Result<Accept<State>, ClientError> {
    if user.name == "reject-init" {
        return Err(ClientError::new(
            "INITIALIZATION",
            "Cannot initialize session",
        ));
    }
    assert!(!context.id().is_empty());
    assert_eq!(context.namespace(), "/business");
    assert!(context.peer_addr().unwrap().ip().is_loopback());
    Ok(Accept::new(State {
        name: format!("session:{}", user.name),
        drops: app.session_drops.clone(),
    })
    .join("updates"))
}
#[brz_socket_io::event("inspect", namespace = "/business", group = business)]
async fn inspect(
    #[payload] input: String,
    #[auth] user: &State,
    #[session] session: &State,
    #[context] context: &ConnectionContext,
    #[inject(app)] app: &App,
) -> Result<Value, ClientError> {
    app.events.fetch_add(1, Ordering::SeqCst);
    Ok(
        json!({"user":user.name,"session":session.name,"namespace":context.namespace(),"input":input}),
    )
}
#[brz_socket_io::event("forbidden", namespace = "/business", group = business)]
async fn forbidden() -> Result<(), ClientError> {
    Err(ClientError::new("FORBIDDEN", "No permission"))
}
#[derive(Deserialize)]
struct Room {
    name: String,
    join: bool,
}
#[brz_socket_io::event("room", namespace = "/business", group = business)]
async fn room(#[payload] room: Room) -> Result<Reply<()>, ClientError> {
    Ok(if room.join {
        Reply::new(()).join(room.name)
    } else {
        Reply::new(()).leave(room.name)
    })
}
#[brz_socket_io::event(fallback, namespace = "/business", group = business)]
async fn fallback(
    #[event_name] event: &str,
    #[payload] input: Value,
) -> Result<Value, ClientError> {
    Ok(json!({"event":event,"input":input}))
}
#[brz_socket_io::event("binary", namespace = "/business", group = business)]
async fn binary(
    #[payload] bytes: serde_bytes::ByteBuf,
) -> Result<serde_bytes::ByteBuf, ClientError> {
    Ok(bytes)
}
#[brz_socket_io::event("blocked", namespace = "/business", group = business)]
async fn blocked(
    #[auth] user: &State,
    #[session] session: &State,
    #[inject(app)] app: &App,
) -> Result<Reply<()>, ClientError> {
    app.started.notify_one();
    app.resume.notified().await;
    assert_eq!(session.name, format!("session:{}", user.name));
    Ok(Reply::new(()).join("must-not-reappear"))
}
#[brz_socket_io::disconnect(namespace = "/business", group = business)]
async fn disconnected(
    #[auth] user: &State,
    #[session] session: &State,
    #[context] context: &ConnectionContext,
    #[inject(app)] app: &App,
) -> Result<(), ClientError> {
    *app.last_disconnect.lock().unwrap() = Some((
        user.name.clone(),
        session.name.clone(),
        context.namespace().into(),
    ));
    app.disconnected.fetch_add(1, Ordering::SeqCst);
    app.changed.notify_one();
    Err(ClientError::new("CLEANUP", "Business cleanup failed"))
}
struct InvalidSerialization;
impl Serialize for InvalidSerialization {
    fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "intentional serialization failure",
        ))
    }
}
#[brz_socket_io::event("bad-reply", namespace = "/business", group = business)]
async fn bad_reply() -> Result<InvalidSerialization, ClientError> {
    Ok(InvalidSerialization)
}

async fn start(app: &Arc<App>) -> (brz_socket_io::SocketIo, Running) {
    let server = Server::builder().build().await.unwrap();
    brz_socket_io::handlers!(server.io(), app = app.clone(); group = business)
        .await
        .unwrap();
    let io = server.io().clone();
    (io, Running::start(server).await)
}
async fn connect(running: &Running, name: &str) -> Client {
    let mut client = Client::open(running.address, "/socket.io").await;
    client
        .join_namespace("/business", json!({"name":name,"allowed":true}))
        .await;
    client
}
fn data(packet: &str) -> Value {
    serde_json::from_str::<Value>(&packet[packet.find('[').unwrap()..]).unwrap()[0].clone()
}
async fn wait_disconnected(app: &App) {
    timeout(DEADLINE, async {
        while app.disconnected.load(Ordering::SeqCst) == 0 {
            app.changed.notified().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn saved_identity_automatic_ack_fallback_binary_and_room_actions() {
    let app = Arc::new(App::default());
    let (io, running) = start(&app).await;
    let mut client = connect(&running, "alice").await;
    let socket = io.of("/business").unwrap().sockets().pop().unwrap();
    assert!(socket.rooms().iter().any(|room| &**room == "updates"));
    client.send(r#"42/business,1["inspect","hello"]"#).await;
    assert_eq!(
        data(&client.recv().await),
        json!({"ok":true,"data":{"user":"alice","session":"session:alice","namespace":"/business","input":"hello"}})
    );
    client.send(r#"42/business,2["inspect",42]"#).await;
    assert_eq!(
        data(&client.recv().await)["error"]["code"],
        "INVALID_PAYLOAD"
    );
    assert_eq!(app.events.load(Ordering::SeqCst), 1);
    client.send(r#"42/business,3["forbidden"]"#).await;
    assert_eq!(
        data(&client.recv().await),
        json!({"ok":false,"error":{"code":"FORBIDDEN","message":"No permission"}})
    );
    client
        .send(r#"42/business,4["response.created",{"task_id":"x"}]"#)
        .await;
    assert_eq!(
        data(&client.recv().await),
        json!({"ok":true,"data":{"event":"response.created","input":{"task_id":"x"}}})
    );
    client
        .send(r#"42/business,5["room",{"name":"updates","join":false}]"#)
        .await;
    assert_eq!(data(&client.recv().await), json!({"ok":true,"data":null}));
    assert!(!socket.rooms().iter().any(|room| &**room == "updates"));
    client
        .send(r#"42/business,6["room",{"name":"new-room","join":true}]"#)
        .await;
    client.recv().await;
    assert!(socket.rooms().iter().any(|room| &**room == "new-room"));
    // Named binary handler wins over the catch-all and preserves attachments.
    client
        .send(r#"451-/business,7["binary",{"_placeholder":true,"num":0}]"#)
        .await;
    client.binary(vec![0, 1, 2, 255]).await;
    assert_eq!(
        client.recv().await,
        r#"461-/business,7[{"ok":true,"data":{"_placeholder":true,"num":0}}]"#
    );
    assert_eq!(client.recv_binary().await, vec![0, 1, 2, 255]);
    // No ACK id produces no reply, and a serialization failure is separate
    // from the business outcome. The next valid reply must be the next frame.
    client
        .send(r#"42/business,["inspect","fire-and-forget"]"#)
        .await;
    client.send(r#"42/business,8["bad-reply"]"#).await;
    client.send(r#"42/business,9["inspect","last"]"#).await;
    assert!(client.recv().await.starts_with("43/business,9["));
    assert_eq!(app.authentications.load(Ordering::SeqCst), 1);
    client.close().await;
    wait_disconnected(&app).await;
    // Retaining the native socket does not retain the application identity.
    assert!(socket.rooms().is_empty());
    timeout(DEADLINE, async {
        while app.user_drops.load(Ordering::SeqCst) != 1
            || app.session_drops.load(Ordering::SeqCst) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        app.last_disconnect.lock().unwrap().as_ref().unwrap().0,
        "alice"
    );
    #[cfg(feature = "metrics")]
    {
        let mut all = std::collections::HashMap::new();
        brz_metrics::visit(|name, kind, value| {
            if kind == "SOCKET_IO" {
                all.insert(name.to_string(), value);
            }
        });
        let inspect = "event:9:/business:7:inspect";
        assert_eq!(all[&format!("{inspect}:handler")].failure, 1);
        assert_eq!(all[&format!("{inspect}:ack")].total, 3);
        assert_eq!(all["event:9:/business:9:bad-reply:handler"].failure, 0);
        assert_eq!(all["event:9:/business:9:bad-reply:ack"].failure, 1);
        assert!(all.contains_key("fallback:9:/business:handler"));
        assert!(!all.keys().any(|key| key.contains("response.created")
            || key.contains("alice")
            || key.contains("new-room")));
    }
    running.stop().await;
}

#[tokio::test]
async fn connection_errors_precede_acceptance_and_disconnect_does_not_rejoin_rooms() {
    let app = Arc::new(App::default());
    let (io, running) = start(&app).await;
    let mut rejected = Client::open(running.address, "/socket.io").await;
    rejected
        .send(r#"40/business,{"name":"alice","allowed":false}"#)
        .await;
    assert_eq!(
        rejected.recv().await,
        r#"44/business,{"message":"Access denied"}"#
    );
    rejected
        .send(r#"40/business,{"name":"reject-init","allowed":true}"#)
        .await;
    assert_eq!(
        rejected.recv().await,
        r#"44/business,{"message":"Cannot initialize session"}"#
    );
    assert!(io.of("/business").unwrap().sockets().is_empty());
    assert_eq!(app.user_drops.load(Ordering::SeqCst), 1);
    rejected.close().await;
    let mut client = connect(&running, "bob").await;
    let socket = io.of("/business").unwrap().sockets().pop().unwrap();
    client.send(r#"42/business,1["blocked"]"#).await;
    timeout(DEADLINE, app.started.notified()).await.unwrap();
    client.close().await;
    wait_disconnected(&app).await;
    // Only the still-running event retains the saved values.
    assert_eq!(app.user_drops.load(Ordering::SeqCst), 1);
    assert_eq!(app.session_drops.load(Ordering::SeqCst), 0);
    app.resume.notify_one();
    timeout(DEADLINE, async {
        while app.user_drops.load(Ordering::SeqCst) != 2
            || app.session_drops.load(Ordering::SeqCst) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(socket.rooms().is_empty());
    running.stop().await;
}

brz_socket_io::registry!(group = wrong_state);
#[brz_socket_io::namespace("/wrong", group = wrong_state)]
async fn empty_namespace() {}
#[brz_socket_io::event("inspect", namespace = "/wrong", group = wrong_state)]
async fn wrong_auth(#[auth] _state: &State) {}
#[tokio::test]
async fn incompatible_saved_identity_binding_fails_at_startup() {
    let server = Server::builder().build().await.unwrap();
    assert!(matches!(
        brz_socket_io::handlers!(server.io(); group = wrong_state).await,
        Err(RegistryError::IncompatibleState { slot: "auth", .. })
    ));
    assert_eq!(server.status().namespaces, 0);
    server.close().await.unwrap();
}

brz_socket_io::registry!(group = outgoing);
#[brz_socket_io::namespace("/outgoing", group = outgoing)]
async fn outgoing_connected() {}
#[tokio::test]
async fn prepared_emitters_address_sockets_and_measure_publish_ack_and_timeout_separately() {
    use std::time::Duration;
    let server = Server::builder().build().await.unwrap();
    brz_socket_io::handlers!(server.io(); group = outgoing)
        .await
        .unwrap();
    let emitter = server.emitter("/outgoing", "question").unwrap();
    let running = Running::start(server).await;
    let mut client = Client::open(running.address, "/socket.io").await;
    let connect = client.join_namespace("/outgoing", Value::Null).await;
    let sid = serde_json::from_str::<Value>(&connect[connect.find('{').unwrap()..]).unwrap()["sid"]
        .as_str()
        .unwrap()
        .to_string();
    let target = emitter.to(sid);
    target
        .emit(&serde_bytes::ByteBuf::from(vec![7, 8, 9]))
        .await
        .unwrap();
    assert_eq!(
        client.recv().await,
        r#"451-/outgoing,["question",{"_placeholder":true,"num":0}]"#
    );
    assert_eq!(client.recv_binary().await, vec![7, 8, 9]);
    let request = target.clone();
    let pending = tokio::spawn(async move {
        request
            .request::<_, Value>(&json!({"input":true}), DEADLINE)
            .await
    });
    let packet = client.recv().await;
    let id = packet
        .trim_start_matches("42/outgoing,")
        .split('[')
        .next()
        .unwrap();
    client
        .send(format!(r#"43/outgoing,{id}[{{"answer":42}}]"#))
        .await;
    assert_eq!(pending.await.unwrap().unwrap(), json!({"answer":42}));
    let request = target.clone();
    let timed_out = tokio::spawn(async move {
        request
            .request::<_, Value>(&"ignored", Duration::from_millis(30))
            .await
    });
    assert!(client.recv().await.starts_with("42/outgoing,"));
    assert!(matches!(
        timed_out.await.unwrap(),
        Err(brz_socket_io::EmitterError::Ack(
            brz_socket_io::AckError::Timeout
        ))
    ));
    #[cfg(feature = "metrics")]
    {
        let mut all = std::collections::HashMap::new();
        brz_metrics::visit(|name, kind, value| {
            if kind == "SOCKET_IO" {
                all.insert(name.to_string(), value);
            }
        });
        assert_eq!(all["outgoing:9:/outgoing:8:question:publish"].total, 3);
        assert_eq!(all["outgoing:9:/outgoing:8:question:publish"].failure, 0);
        assert_eq!(all["outgoing:9:/outgoing:8:question:ack_wait"].total, 2);
        assert_eq!(all["outgoing:9:/outgoing:8:question:ack_wait"].failure, 1);
        use std::io::Read;
        let mut text = String::new();
        brz_metrics::PrometheusReader::new()
            .read_to_string(&mut text)
            .unwrap();
        assert!(text.contains("brz_count_total{type=\"SOCKET_IO\",name=\"outgoing:9:/outgoing:8:question:ack_timeout\"} 1"));
    }
    client.close().await;
    running.stop().await;
}

#[brz_socket_io::namespace("/left", group = business, middleware = authorize)]
async fn left(
    #[auth] user: &State,
    #[inject(app)] app: &App,
) -> Result<Accept<State>, ClientError> {
    Ok(Accept::new(State {
        name: format!("left:{}", user.name),
        drops: app.session_drops.clone(),
    }))
}
#[brz_socket_io::namespace("/right", group = business, middleware = authorize)]
async fn right(
    #[auth] user: &State,
    #[inject(app)] app: &App,
) -> Result<Accept<State>, ClientError> {
    Ok(Accept::new(State {
        name: format!("right:{}", user.name),
        drops: app.session_drops.clone(),
    }))
}
#[brz_socket_io::event("read", namespace = "/left", group = business)]
async fn read_left(#[auth] user: &State, #[session] session: &State) -> Result<Value, ClientError> {
    Ok(json!([user.name, session.name]))
}
#[brz_socket_io::event("read", namespace = "/right", group = business)]
async fn read_right(
    #[auth] user: &State,
    #[session] session: &State,
) -> Result<Value, ClientError> {
    Ok(json!([user.name, session.name]))
}

#[tokio::test]
async fn namespace_disconnect_releases_only_its_identity_on_a_multiplexed_transport() {
    let app = Arc::new(App::default());
    let (io, running) = start(&app).await;
    let mut client = Client::open(running.address, "/socket.io").await;
    client
        .join_namespace("/left", json!({"name":"alice","allowed":true}))
        .await;
    client
        .join_namespace("/right", json!({"name":"bob","allowed":true}))
        .await;
    let left = io.of("/left").unwrap().sockets().pop().unwrap();
    let right = io.of("/right").unwrap().sockets().pop().unwrap();
    client.send(r#"42/left,1["read"]"#).await;
    assert_eq!(
        data(&client.recv().await)["data"],
        json!(["alice", "left:alice"])
    );
    client.send("41/left,").await;
    timeout(DEADLINE, async {
        while app.user_drops.load(Ordering::SeqCst) != 1
            || app.session_drops.load(Ordering::SeqCst) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!left.connected());
    assert!(right.connected());
    client.send(r#"42/right,1["read"]"#).await;
    assert_eq!(
        data(&client.recv().await)["data"],
        json!(["bob", "right:bob"])
    );
    assert_eq!(app.authentications.load(Ordering::SeqCst), 2);
    client.close().await;
    timeout(DEADLINE, async {
        while app.user_drops.load(Ordering::SeqCst) != 2
            || app.session_drops.load(Ordering::SeqCst) != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    running.stop().await;
}
