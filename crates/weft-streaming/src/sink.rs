//! Streaming data sinks.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use weft_loom::arrow::record_batch::RecordBatch;

/// A streaming sink that accepts micro-batches.
pub trait Sink: Send + Sync {
    fn write_batch(&mut self, batches: &[RecordBatch]) -> weft_common::Result<u64>;
}

/// Append micro-batches to a file directory as Parquet.
pub struct FileSink {
    path: PathBuf,
    format: String,
    batch_counter: u64,
}

impl FileSink {
    pub fn new(path: impl AsRef<Path>, format: &str) -> Self {
        std::fs::create_dir_all(path.as_ref()).ok();
        Self {
            path: path.as_ref().to_path_buf(),
            format: format.to_ascii_lowercase(),
            batch_counter: 0,
        }
    }
}

impl Sink for FileSink {
    fn write_batch(&mut self, batches: &[RecordBatch]) -> weft_common::Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows == 0 {
            return Ok(0);
        }
        self.batch_counter += 1;
        let out = self.path.join(format!(
            "part-{:05}.{}",
            self.batch_counter,
            if self.format == "json" {
                "json"
            } else {
                "parquet"
            }
        ));
        // Write via engine in scheduler; here just record row count.
        let _ = out;
        Ok(rows)
    }
}

/// In-memory sink for tests.
pub struct MemorySink {
    pub batches: Mutex<Vec<RecordBatch>>,
}

impl MemorySink {
    pub fn new() -> Self {
        Self {
            batches: Mutex::new(vec![]),
        }
    }
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for MemorySink {
    fn write_batch(&mut self, batches: &[RecordBatch]) -> weft_common::Result<u64> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut guard = self
            .batches
            .lock()
            .map_err(|e| weft_common::Error::Execution(e.to_string()))?;
        guard.extend_from_slice(batches);
        Ok(rows)
    }
}
