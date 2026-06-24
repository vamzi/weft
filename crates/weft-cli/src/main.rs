//! The `weft` command-line entry point.
//!
//! ```text
//! weft spark server --port 50051
//! ```
//! Then point any PySpark client at `sc://localhost:50051`.

use weft_connect::{serve, ServerConfig};

#[tokio::main]
async fn main() {
    // TODO(issue #1): replace this hand-rolled arg handling with clap.
    let args: Vec<String> = std::env::args().collect();
    let want_server = args.iter().any(|a| a == "server");
    let port = parse_port(&args).unwrap_or(50051);

    if !want_server {
        eprintln!("weft {}", env!("CARGO_PKG_VERSION"));
        eprintln!("usage: weft spark server --port <PORT>");
        return;
    }

    eprintln!("Weft Spark Connect server listening on sc://0.0.0.0:{port}");
    if let Err(e) = serve(ServerConfig { port }).await {
        eprintln!("weft: {e}");
        std::process::exit(1);
    }
}

fn parse_port(args: &[String]) -> Option<u16> {
    let i = args.iter().position(|a| a == "--port")?;
    args.get(i + 1)?.parse().ok()
}
