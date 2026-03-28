//! `arco-flow-worker` binary entrypoint.

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms)]

use std::net::SocketAddr;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let worker_id =
        std::env::var("ARCO_FLOW_WORKER_ID").unwrap_or_else(|_| "arco-flow-worker".to_string());
    let port = std::env::var("PORT")
        .or_else(|_| std::env::var("ARCO_FLOW_WORKER_PORT"))
        .ok()
        .map(|value| value.parse::<u16>())
        .transpose()?
        .unwrap_or(8080);

    let app = arco_flow_worker::build_app(worker_id);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
