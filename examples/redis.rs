//! Redis-backed server; register namespaces before accepting connections.

use brz_socket_io::{
    Server,
    extract::Data,
    provider::redis::{RedisAck, RedisProvider, RedisSocket},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let server = Server::builder()
        .with_provider(RedisProvider::new(url)?)
        .build()
        .await?;
    server
        .io()
        .ns("/", async |socket: RedisSocket| {
            socket.on(
                "join",
                async |socket: RedisSocket, Data(room): Data<String>, ack: RedisAck| {
                    socket.join(room);
                    ack.send(&true).ok();
                },
            );
        })
        .await?;
    let address = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    println!("Redis-backed Socket.IO: http://{address}");
    server
        .serve(listener, async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
