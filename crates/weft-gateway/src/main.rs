//! `weft-gateway` binary entry point.
//!
//! Serves the control-plane REST/WS API (see [`weft_gateway::ROUTES`]). Bind address comes from
//! `WEFT_GATEWAY_ADDR` (default `0.0.0.0:8080`). `--routes` prints the frozen API surface and exits.

#[tokio::main]
async fn main() -> std::io::Result<()> {
    if std::env::args().any(|a| a == "--routes") {
        print!("{}", weft_gateway::openapi_summary());
        return Ok(());
    }
    let addr = std::env::var("WEFT_GATEWAY_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    eprintln!("weft-gateway listening on {addr}");
    weft_gateway::server::serve(&addr).await
}
