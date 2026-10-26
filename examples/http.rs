//! HTTP API macros and Socket.IO exports on one listener, without a gateway.
use brz_socket_io::{
    Accept, ClientError, Config, Server,
    http_server::{self, Text},
};
use std::sync::Arc;
struct App {
    prefix: String,
}
http_server::registry!();
brz_socket_io::registry!(dependencies(app: Arc<App>));
#[http_server::get("/health", access = public)]
async fn health() -> Text {
    Text("ok".into())
}
#[brz_socket_io::namespace("/chat")]
async fn connected() -> Result<Accept, ClientError> {
    Ok(Accept::default().join("updates"))
}
#[brz_socket_io::event("echo", namespace = "/chat")]
async fn echo(#[payload] value: String, #[inject(app)] app: &App) -> Result<String, ClientError> {
    Ok(format!("{}:{value}", app.prefix))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::builder()
        .with_config(Config {
            path: "/realtime".into(),
            ..Default::default()
        })
        .build()
        .await?;
    brz_socket_io::handlers!(
        server.io(),
        app = Arc::new(App {
            prefix: "echo".into()
        })
    )
    .await?;
    let api = http_server::handlers!()?;
    let service = server.http_service(api, http_server::ServerConfig::default())?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    http_server::hyper::serve(
        listener,
        move |_| service.clone(),
        async {
            tokio::signal::ctrl_c().await.ok();
            server.close().await.ok();
        },
        http_server::hyper::Config::default(),
    )
    .await?;
    Ok(())
}
