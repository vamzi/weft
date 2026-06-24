//! The `weft` command-line entry point.
//!
//! Target UX (mirrors `sail spark server`):
//! ```text
//! weft spark server --port 50051
//! ```
//! Then point any PySpark client at `sc://localhost:50051`.

use weft_connect::{serve, ServerConfig};

fn main() {
    // TODO(issue #1): replace this hand-rolled arg handling with clap and wire real
    // subcommands (`spark server`, `version`, …).
    let args: Vec<String> = std::env::args().collect();
    let want_server = args.iter().any(|a| a == "server");

    if !want_server {
        eprintln!("weft {}", env!("CARGO_PKG_VERSION"));
        eprintln!("usage: weft spark server --port <PORT>");
        return;
    }

    let config = ServerConfig::default();
    eprintln!(
        "starting Weft Spark Connect server on port {} …",
        config.port
    );
    match serve(config) {
        Ok(()) => {}
        Err(e) => {
            eprintln!("weft: {e}");
            std::process::exit(1);
        }
    }
}
