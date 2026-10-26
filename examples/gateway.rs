//! A forwarding gateway in front of an independent Socket.IO HTTP backend.
use brz_http_gateway::{Gateway, OriginService, RouteRule, RouteTable, RoutesConfig};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Run `cargo run --example echo` with LISTEN_ADDR=127.0.0.1:3001 first.
    let upstream = "http://127.0.0.1:3001".parse()?;
    let routes = RouteTable::compile(RoutesConfig {
        routes: ["/socket.io", "/socket.io/"]
            .map(|path| RouteRule {
                methods: vec!["GET".into(), "POST".into()],
                path: path.into(),
                admission: None,
            })
            .into(),
        ..Default::default()
    })?;
    let gateway = Gateway::new(routes, OriginService::new(&upstream)?, &upstream)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    brz_http_gateway::serve(
        listener,
        gateway,
        async {
            tokio::signal::ctrl_c().await.ok();
        },
        std::time::Duration::from_secs(10),
    )
    .await?;
    Ok(())
}
