//! `weft-datasource` — turn storage into Arrow record batches.
//!
//! Phase 0: Parquet/CSV/JSON. Phase 1 lakehouse reads use a **version-safe resolver** pattern:
//! resolve the table format to its active Parquet file list, then let the engine's native
//! (DataFusion 54) reader scan them — avoiding the DataFusion-version coupling that the
//! `deltalake`/`iceberg` crates impose (they pin older DataFusion). **Delta**:
//! [`delta_active_files`] replays the `_delta_log` JSON (add/remove). **Iceberg**:
//! [`iceberg_active_files`] walks metadata.json → manifest list (Avro) → manifests (Avro).
//! v1 limits: no deletion vectors / merge-on-read deletes / partition pruning yet (those are a
//! delta-kernel / iceberg-rust integration once their DataFusion versions catch up to ours).

use weft_common::{Error, Result};

/// A read request against a source: which columns, what filter, optional row limit.
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// Projected column names; empty = all.
    pub projection: Vec<String>,
    /// Pushed-down filter as a SQL fragment (placeholder; becomes a typed predicate).
    pub filter: Option<String>,
    /// Optional `LIMIT` for top-N / sample pushdown.
    pub limit: Option<usize>,
}

/// Open a source and produce Arrow batches. Implemented in Phase 0/1.
pub fn scan(_uri: &str, _req: &ScanRequest) -> Result<()> {
    Ok(())
}

/// Resolve a Delta Lake table to its active data-file paths by replaying the JSON transaction
/// log (`_delta_log/*.json`): `add` actions introduce files, `remove` actions retire them.
///
/// This is the version-safe Phase-1 read path — it yields plain Parquet paths that the
/// engine's native reader consumes, so it does not couple Weft to a Delta crate's DataFusion
/// version. Limitations (v1): JSON commits only (no checkpoint Parquet), and no deletion
/// vectors / column mapping — those arrive with a `delta-kernel` integration later.
pub fn delta_active_files(table_path: &str) -> Result<Vec<std::path::PathBuf>> {
    use std::collections::HashSet;
    use std::path::Path;

    let base = Path::new(table_path);
    let log_dir = base.join("_delta_log");
    let mut commits: Vec<std::path::PathBuf> = std::fs::read_dir(&log_dir)
        .map_err(|e| Error::Io(format!("reading {}: {e}", log_dir.display())))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    commits.sort(); // 000…0.json, 000…1.json, … apply in version order

    if commits.is_empty() {
        return Err(Error::Io(format!(
            "no _delta_log/*.json under {} (checkpoint-only tables not supported yet)",
            table_path
        )));
    }

    let mut order: Vec<String> = Vec::new();
    let mut present: HashSet<String> = HashSet::new();
    for commit in &commits {
        let content = std::fs::read_to_string(commit)
            .map_err(|e| Error::Io(format!("reading {}: {e}", commit.display())))?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| Error::Io(format!("delta log json: {e}")))?;
            if let Some(p) = v
                .get("add")
                .and_then(|a| a.get("path"))
                .and_then(|p| p.as_str())
            {
                if present.insert(p.to_string()) {
                    order.push(p.to_string());
                }
            } else if let Some(p) = v
                .get("remove")
                .and_then(|r| r.get("path"))
                .and_then(|p| p.as_str())
            {
                if present.remove(p) {
                    order.retain(|x| x != p);
                }
            }
        }
    }
    Ok(order.into_iter().map(|p| base.join(p)).collect())
}

// ---- Iceberg -------------------------------------------------------------------------------

use apache_avro::types::Value as AvroValue;

fn unwrap_union(v: &AvroValue) -> &AvroValue {
    match v {
        AvroValue::Union(_, b) => b,
        other => other,
    }
}
fn avro_field<'a>(v: &'a AvroValue, name: &str) -> Option<&'a AvroValue> {
    match v {
        AvroValue::Record(fields) => fields.iter().find(|(k, _)| k == name).map(|(_, x)| x),
        _ => None,
    }
}
fn avro_str(v: &AvroValue) -> Option<String> {
    match unwrap_union(v) {
        AvroValue::String(s) => Some(s.clone()),
        _ => None,
    }
}
fn avro_int(v: &AvroValue) -> Option<i64> {
    match unwrap_union(v) {
        AvroValue::Int(n) => Some(*n as i64),
        AvroValue::Long(n) => Some(*n),
        _ => None,
    }
}

fn strip_scheme(p: &str) -> String {
    p.strip_prefix("file://")
        .or_else(|| p.strip_prefix("file:"))
        .unwrap_or(p)
        .to_string()
}

fn avro_records(bytes: &[u8]) -> Result<Vec<AvroValue>> {
    let reader = apache_avro::Reader::new(bytes).map_err(|e| Error::Io(format!("avro: {e}")))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Io(format!("avro record: {e}")))
}

/// Resolve an Iceberg table to its active data-file paths: read the current `metadata.json`
/// (JSON), follow the current snapshot's manifest list (Avro) to the manifests (Avro), and
/// collect non-deleted data files. Version-safe (avro + json only; no Iceberg-crate DataFusion
/// coupling). v1 limits: no positional/equality delete application, no partition pruning yet.
pub fn iceberg_active_files(table_path: &str) -> Result<Vec<std::path::PathBuf>> {
    use std::path::Path;
    let base = Path::new(table_path);
    let meta_dir = base.join("metadata");

    // Locate the current metadata.json: prefer version-hint.text, else the latest v*.metadata.json.
    let meta_path = if let Ok(hint) = std::fs::read_to_string(meta_dir.join("version-hint.text")) {
        meta_dir.join(format!("v{}.metadata.json", hint.trim()))
    } else {
        let mut metas: Vec<std::path::PathBuf> = std::fs::read_dir(&meta_dir)
            .map_err(|e| Error::Io(format!("reading {}: {e}", meta_dir.display())))?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.to_string_lossy().ends_with(".metadata.json"))
            .collect();
        metas.sort();
        metas
            .pop()
            .ok_or_else(|| Error::Io(format!("no metadata.json under {}", meta_dir.display())))?
    };

    let meta: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&meta_path).map_err(|e| Error::Io(format!("{e}")))?,
    )
    .map_err(|e| Error::Io(format!("iceberg metadata json: {e}")))?;

    let current = meta.get("current-snapshot-id").and_then(|v| v.as_i64());
    let snapshots = meta
        .get("snapshots")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let snap = snapshots
        .iter()
        .find(|s| current.is_some() && s.get("snapshot-id").and_then(|v| v.as_i64()) == current)
        .or_else(|| snapshots.last())
        .ok_or_else(|| Error::Io("iceberg: no current snapshot".into()))?;
    let manifest_list = snap
        .get("manifest-list")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Io("iceberg: snapshot has no manifest-list".into()))?;

    // manifest list (avro) -> manifest file paths
    let ml_bytes = std::fs::read(strip_scheme(manifest_list))
        .map_err(|e| Error::Io(format!("manifest list: {e}")))?;
    let mut data_files: Vec<std::path::PathBuf> = Vec::new();
    for entry in avro_records(&ml_bytes)? {
        let Some(mp) = avro_field(&entry, "manifest_path").and_then(avro_str) else {
            continue;
        };
        let m_bytes = std::fs::read(strip_scheme(&mp))
            .map_err(|e| Error::Io(format!("manifest {mp}: {e}")))?;
        for me in avro_records(&m_bytes)? {
            // status: 0=EXISTING, 1=ADDED, 2=DELETED — skip deleted.
            if avro_field(&me, "status").and_then(avro_int) == Some(2) {
                continue;
            }
            let Some(df) = avro_field(&me, "data_file").map(unwrap_union) else {
                continue;
            };
            // content: 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES (v1 manifests omit it → data)
            if avro_field(df, "content").and_then(avro_int).unwrap_or(0) != 0 {
                continue;
            }
            if let Some(fp) = avro_field(df, "file_path").and_then(avro_str) {
                data_files.push(std::path::PathBuf::from(strip_scheme(&fp)));
            }
        }
    }
    Ok(data_files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{types::Value, Schema, Writer};

    fn write_avro(path: &std::path::Path, schema_json: &str, rec: Value) {
        let schema = Schema::parse_str(schema_json).unwrap();
        let mut w = Writer::new(&schema, Vec::new());
        w.append(rec).unwrap();
        std::fs::write(path, w.into_inner().unwrap()).unwrap();
    }

    #[test]
    fn iceberg_resolves_data_files() {
        let dir = std::env::temp_dir().join(format!("weft-ice-{}", std::process::id()));
        let meta = dir.join("metadata");
        std::fs::create_dir_all(&meta).unwrap();
        let data = dir.join("data-0.parquet");
        std::fs::write(&data, b"x").unwrap();

        // manifest (one ADDED data file)
        let manifest = dir.join("manifest-0.avro");
        write_avro(
            &manifest,
            r#"{"type":"record","name":"manifest_entry","fields":[
                {"name":"status","type":"int"},
                {"name":"data_file","type":{"type":"record","name":"data_file","fields":[
                    {"name":"content","type":"int"},
                    {"name":"file_path","type":"string"}]}}]}"#,
            Value::Record(vec![
                ("status".into(), Value::Int(1)),
                (
                    "data_file".into(),
                    Value::Record(vec![
                        ("content".into(), Value::Int(0)),
                        (
                            "file_path".into(),
                            Value::String(data.to_string_lossy().into()),
                        ),
                    ]),
                ),
            ]),
        );

        // manifest list -> the manifest above
        let ml = dir.join("snap-0.avro");
        write_avro(
            &ml,
            r#"{"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string"}]}"#,
            Value::Record(vec![(
                "manifest_path".into(),
                Value::String(manifest.to_string_lossy().into()),
            )]),
        );

        // metadata.json + version hint
        let metadata = serde_json::json!({
            "current-snapshot-id": 1,
            "snapshots": [ {"snapshot-id": 1, "manifest-list": ml.to_string_lossy()} ]
        });
        std::fs::write(
            meta.join("v1.metadata.json"),
            serde_json::to_string(&metadata).unwrap(),
        )
        .unwrap();
        std::fs::write(meta.join("version-hint.text"), "1").unwrap();

        let files = iceberg_active_files(dir.to_str().unwrap()).unwrap();
        assert_eq!(files, vec![data]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
