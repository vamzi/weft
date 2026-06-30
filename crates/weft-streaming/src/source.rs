//! Streaming data sources.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use weft_loom::arrow::record_batch::RecordBatch;
use weft_loom::Engine;

/// A micro-batch data source.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Read the next micro-batch. Returns empty when no new data is available.
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>>;
}

/// File-directory source: reads new Parquet/JSON/CSV files not yet in the offset set.
pub struct FileSource {
    path: PathBuf,
    format: String,
    seen: HashSet<String>,
    #[allow(dead_code)]
    table_name: String,
}

impl FileSource {
    pub fn new(path: impl AsRef<Path>, format: &str) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            format: format.to_ascii_lowercase(),
            seen: HashSet::new(),
            table_name: "_weft_stream_src".into(),
        }
    }

    pub fn new_files(&self) -> Vec<PathBuf> {
        let Ok(entries) = std::fs::read_dir(&self.path) else {
            return vec![];
        };
        entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_file() && !self.seen.contains(&p.to_string_lossy().to_string()))
            .collect()
    }
}

#[async_trait::async_trait]
impl Source for FileSource {
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>> {
        let new_files = self.new_files();
        if new_files.is_empty() {
            return Ok(vec![]);
        }
        let mut all = Vec::new();
        for f in &new_files {
            let sql = match self.format.as_str() {
                "parquet" => format!(
                    "SELECT * FROM parquet_scan('{}')",
                    f.display().to_string().replace('\'', "''")
                ),
                "json" => format!(
                    "SELECT * FROM json_scan('{}')",
                    f.display().to_string().replace('\'', "''")
                ),
                "csv" => format!(
                    "SELECT * FROM csv_scan('{}')",
                    f.display().to_string().replace('\'', "''")
                ),
                _ => continue,
            };
            let batches = engine.sql(&sql).await?;
            all.extend(batches);
            self.seen.insert(f.to_string_lossy().to_string());
        }
        Ok(all)
    }
}

/// Rate source for tests: emits N rows per batch.
pub struct MemoryRateSource {
    rows_per_batch: u64,
    batch_count: u64,
    max_batches: u64,
}

impl MemoryRateSource {
    pub fn new(rows_per_batch: u64, max_batches: u64) -> Self {
        Self {
            rows_per_batch,
            batch_count: 0,
            max_batches,
        }
    }
}

#[async_trait::async_trait]
impl Source for MemoryRateSource {
    async fn poll_batch(&mut self, engine: &Engine) -> weft_common::Result<Vec<RecordBatch>> {
        if self.batch_count >= self.max_batches {
            return Ok(vec![]);
        }
        self.batch_count += 1;
        let sql = format!(
            "SELECT id FROM range(0, {}, 1) AS t(id)",
            self.rows_per_batch
        );
        engine.sql(&sql).await
    }
}
