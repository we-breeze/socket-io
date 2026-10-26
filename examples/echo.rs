//! Standalone echo server using the in-memory provider.

use brz_socket_io::{
    Server,
    extract::{AckSender, Data, SocketRef},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::builder().build().await?;
    server.io().ns("/", async |socket: SocketRef| {
        socket.on("echo", async |Data(value): Data<String>, ack: AckSender| {
            ack.send(&value).ok();
        });
    });
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("Socket.IO echo: http://{address}");
    server
        .serve(listener, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
