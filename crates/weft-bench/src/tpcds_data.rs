//! TPC-DS data generation via the DuckDB `tpcds` extension (`dsdgen`) exported as Parquet.
//!
//! Parquet (not CSV) is required so the same harness can target SF100/500/1000 on larger hardware
//! without materializing everything as text. Generation requires a `duckdb` CLI on PATH (or a
//! common install location). Idempotent: skipped when the sentinel + recorded scale factor match.

use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::process::Command;

/// The 24 TPC-DS tables (DuckDB `dsdgen` / official kit).
pub const TABLES: [&str; 24] = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

const SENTINEL: &str = "store_sales.parquet";
const SF_MARKER: &str = "scale_factor.txt";

/// Locate a `duckdb` binary: PATH, else common install locations.
pub fn duckdb_path() -> Option<String> {
    for cand in [
        "duckdb",
        "/opt/homebrew/opt/duckdb/bin/duckdb",
        "/usr/local/bin/duckdb",
    ] {
        if Command::new(cand).arg("--version").output().is_ok() {
            return Some(cand.to_string());
        }
    }
    None
}

/// Generate scale-factor `sf` TPC-DS data as Parquet under `dir`. Idempotent when
/// `store_sales.parquet` exists and `scale_factor.txt` matches `sf`.
pub fn generate(sf: f64, dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let sentinel = dir.join(SENTINEL);
    let marker = dir.join(SF_MARKER);
    if sentinel.exists() {
        if let Ok(prev) = fs::read_to_string(&marker) {
            if prev.trim().parse::<f64>().ok() == Some(sf) {
                return Ok(());
            }
        }
        // Stale or mismatched SF — wipe and regenerate.
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let _ = fs::remove_file(entry.path());
        }
    }

    let duckdb = duckdb_path().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "duckdb CLI not found on PATH (required for TPC-DS data generation via dsdgen)",
        )
    })?;

    // Export into a staging subdir, then move parquet files up — EXPORT also writes schema.sql /
    // load.sql with absolute paths we don't need at the top level.
    let stage = dir.join(".export");
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir_all(&stage)?;

    let stage_str = stage
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "non-UTF8 data dir"))?;
    let script = format!(
        "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf = {sf}); EXPORT DATABASE '{stage_str}' (FORMAT PARQUET);"
    );
    let out = Command::new(&duckdb)
        .args(["-c", &script])
        .output()
        .map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("failed to spawn duckdb: {e}"))
        })?;
    if !out.status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "duckdb dsdgen/export failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ),
        ));
    }

    for t in TABLES {
        let src = stage.join(format!("{t}.parquet"));
        if !src.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("expected {} after EXPORT", src.display()),
            ));
        }
        fs::rename(&src, dir.join(format!("{t}.parquet")))?;
    }
    let _ = fs::remove_dir_all(&stage);

    let mut f = fs::File::create(&marker)?;
    writeln!(f, "{sf}")?;
    Ok(())
}
