use std::{net::SocketAddr, time::Duration};

use brz_socket_io::{Server, adapter::Adapter};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::{net::TcpStream, sync::oneshot, task::JoinHandle, time::timeout};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

pub const DEADLINE: Duration = Duration::from_secs(5);

pub struct Running {
    pub address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), brz_socket_io::Error>>>,
}

impl Running {
    pub async fn start<A: Adapter>(server: Server<A>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(server.serve(listener, async {
            stopped.await.ok();
        }));
        Self {
            address,
            stop: Some(stop),
            task: Some(task),
        }
    }

    pub async fn stop(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        timeout(DEADLINE, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

pub struct Client {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    pub async fn open(address: SocketAddr, path: &str) -> Self {
        let (socket, _) = connect_async(format!("ws://{address}{path}/?EIO=4&transport=websocket"))
            .await
            .unwrap();
        let mut client = Self { socket };
        assert!(client.recv().await.starts_with("0{"));
        client
    }

    pub async fn connect(address: SocketAddr, namespace: &str) -> Self {
        let mut client = Self::open(address, "/socket.io").await;
        client.join_namespace(namespace, Value::Null).await;
        client
    }

    pub async fn join_namespace(&mut self, namespace: &str, auth: Value) -> String {
        let packet = if namespace == "/" {
            format!("40{auth}")
        } else {
            format!("40{namespace},{auth}")
        };
        self.send(packet).await;
        let packet = self.recv().await;
        assert!(packet.starts_with("40"), "connect packet: {packet}");
        packet
    }

    pub async fn send(&mut self, value: impl Into<String>) {
        self.socket
            .send(Message::Text(value.into().into()))
            .await
            .unwrap();
    }

    pub async fn recv(&mut self) -> String {
        timeout(DEADLINE, async {
            loop {
                match self.socket.next().await.unwrap().unwrap() {
                    Message::Text(text) if text == "2" => {
                        self.socket.send(Message::Text("3".into())).await.unwrap();
                    }
                    Message::Text(text) => return text.to_string(),
                    Message::Ping(data) => {
                        self.socket.send(Message::Pong(data)).await.unwrap();
                    }
                    message => panic!("unexpected packet: {message:?}"),
                }
            }
        })
        .await
        .unwrap()
    }

    pub async fn binary(&mut self, data: Vec<u8>) {
        self.socket
            .send(Message::Binary(data.into()))
            .await
            .unwrap();
    }

    pub async fn recv_binary(&mut self) -> Vec<u8> {
        match timeout(DEADLINE, self.socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Binary(bytes) => bytes.to_vec(),
            message => panic!("expected binary packet: {message:?}"),
        }
    }

    pub async fn close(mut self) {
        self.socket.close(None).await.unwrap();
    }
}
