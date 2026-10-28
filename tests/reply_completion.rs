//! Follow-up work observes framework reply ordering on a real WebSocket.
#![cfg(feature = "macros")]
#[allow(dead_code)]
mod common;

use brz_socket_io::{ClientError, Reply, Server, extract::SocketRef};
use common::{Client, DEADLINE, Running};
use serde::{Serialize, Serializer};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{sync::Notify, time::timeout};

#[derive(Default)]
struct Effects {
    socket: Mutex<Option<SocketRef>>,
    calls: AtomicUsize,
    started: Notify,
    resume: Notify,
    finished: Notify,
}

impl Effects {
    fn emit(&self, label: &str) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.socket
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .emit("follow-up", &label)
            .unwrap();
    }
}

brz_socket_io::registry!(group = completion, dependencies(effects: Arc<Effects>));

#[brz_socket_io::namespace("/completion", group = completion)]
async fn connected() {}

#[brz_socket_io::event("request", namespace = "/completion", group = completion)]
async fn request(
    #[payload] value: Value,
    #[inject(effects)] effects: &Arc<Effects>,
) -> Result<Reply<Value>, ClientError> {
    let first = effects.clone();
    let second = effects.clone();
    Ok(Reply::payload(value)
        .join("ready")
        .after_reply(move || {
            assert!(
                first
                    .socket
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .rooms()
                    .iter()
                    .any(|room| &**room == "ready")
            );
            first.emit("first");
        })
        .after_reply(move || second.emit("second")))
}

struct InvalidSerialization;
impl Serialize for InvalidSerialization {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("synthetic serialization failure"))
    }
}

#[brz_socket_io::event("bad", namespace = "/completion", group = completion)]
async fn bad(
    #[inject(effects)] effects: &Arc<Effects>,
) -> Result<Reply<InvalidSerialization>, ClientError> {
    let effects = effects.clone();
    Ok(Reply::payload(InvalidSerialization).after_reply(move || effects.emit("after-failed-send")))
}

#[brz_socket_io::event("blocked", namespace = "/completion", group = completion)]
async fn blocked(#[inject(effects)] effects: &Arc<Effects>) -> Result<Reply<()>, ClientError> {
    effects.started.notify_one();
    effects.resume.notified().await;
    struct Completion(Arc<Effects>);
    impl Drop for Completion {
        fn drop(&mut self) {
            self.0.finished.notify_one();
        }
    }
    let completion = Completion(effects.clone());
    let effects = effects.clone();
    Ok(Reply::new(()).join("late-room").after_reply(move || {
        let _completion = completion;
        effects.emit("must-not-run");
    }))
}

async fn start() -> (Arc<Effects>, brz_socket_io::SocketIo, Running, Client) {
    let effects = Arc::new(Effects::default());
    let server = Server::builder().build().await.unwrap();
    brz_socket_io::handlers!(server.io(), effects = effects.clone(); group = completion)
        .await
        .unwrap();
    let io = server.io().clone();
    let running = Running::start(server).await;
    let client = Client::connect(running.address, "/completion").await;
    *effects.socket.lock().unwrap() = io.of("/completion").unwrap().sockets().pop();
    (effects, io, running, client)
}

#[tokio::test]
async fn reply_queues_ack_before_follow_up_and_no_ack_events_still_run_continuations() {
    let (effects, _, running, mut client) = start().await;
    client
        .send(format!(
            "42/completion,1{}",
            json!(["request", {"success":true}])
        ))
        .await;
    assert_eq!(client.recv().await, r#"43/completion,1[{"success":true}]"#);
    assert_eq!(
        client.recv().await,
        r#"42/completion,["follow-up","first"]"#
    );
    assert_eq!(
        client.recv().await,
        r#"42/completion,["follow-up","second"]"#
    );
    assert_eq!(effects.calls.load(Ordering::SeqCst), 2);
    client.send(r#"42/completion,["request",null]"#).await;
    assert_eq!(
        client.recv().await,
        r#"42/completion,["follow-up","first"]"#
    );
    assert_eq!(
        client.recv().await,
        r#"42/completion,["follow-up","second"]"#
    );
    assert_eq!(effects.calls.load(Ordering::SeqCst), 4);
    client.close().await;
    running.stop().await;
}

#[tokio::test]
async fn failed_ack_serialization_keeps_application_follow_up() {
    let (effects, _, running, mut client) = start().await;
    client.send(r#"42/completion,7["bad"]"#).await;
    assert_eq!(
        client.recv().await,
        r#"42/completion,["follow-up","after-failed-send"]"#
    );
    assert_eq!(effects.calls.load(Ordering::SeqCst), 1);
    client.close().await;
    running.stop().await;
}

#[tokio::test]
async fn closed_session_does_not_run_reply_continuations_or_restore_rooms() {
    let (effects, io, running, mut client) = start().await;
    client.send(r#"42/completion,1["blocked"]"#).await;
    timeout(DEADLINE, effects.started.notified()).await.unwrap();
    client.close().await;
    timeout(DEADLINE, async {
        while !io.of("/completion").unwrap().sockets().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    effects.resume.notify_one();
    timeout(DEADLINE, effects.finished.notified())
        .await
        .unwrap();
    // The captured guard proves the continuation was run or discarded;
    // observing the business function return alone would be a race.
    running.stop().await;
    assert_eq!(effects.calls.load(Ordering::SeqCst), 0);
    assert!(
        effects
            .socket
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .rooms()
            .is_empty()
    );
}
