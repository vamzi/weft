//! Spill hash-shuffle buckets to local disk when in-memory caching would OOM.
//!
//! Activated when `WEFT_SHUFFLE_SPILL_DIR` is set. Buckets are written as Arrow IPC stream
//! files keyed by `(stage_id, partition_id)`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use weft_common::{Error, Result};
use weft_loom::arrow::datatypes::SchemaRef;
use weft_loom::arrow::ipc::reader::StreamReader;
use weft_loom::arrow::ipc::writer::StreamWriter;
use weft_loom::arrow::record_batch::RecordBatch;

/// On-disk spill store for one worker process.
#[derive(Debug, Clone)]
pub struct SpillStore {
    root: PathBuf,
}

impl SpillStore {
    /// Open a spill directory from `WEFT_SHUFFLE_SPILL_DIR`, if set.
    pub fn from_env() -> Option<Self> {
        let root = std::env::var("WEFT_SHUFFLE_SPILL_DIR").ok()?;
        if root.is_empty() {
            return None;
        }
        let store = Self {
            root: PathBuf::from(root),
        };
        std::fs::create_dir_all(&store.root).ok()?;
        Some(store)
    }

    fn path(&self, stage_id: u32, partition: u32) -> PathBuf {
        self.root
            .join(format!("stage_{stage_id}_part_{partition}.arrow"))
    }

    /// Write `batches` for one bucket; returns the file path.
    pub fn write_bucket(
        &self,
        stage_id: u32,
        partition: u32,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<PathBuf> {
        let path = self.path(stage_id, partition);
        let file = std::fs::File::create(&path)
            .map_err(|e| Error::Io(format!("spill create {}: {e}", path.display())))?;
        let mut writer = StreamWriter::try_new(file, &schema)
            .map_err(|e| Error::Io(format!("spill writer: {e}")))?;
        for b in batches {
            writer
                .write(b)
                .map_err(|e| Error::Io(format!("spill write: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| Error::Io(format!("spill finish: {e}")))?;
        Ok(path)
    }

    /// Read a spilled bucket back into memory.
    pub fn read_bucket(&self, stage_id: u32, partition: u32) -> Result<Vec<RecordBatch>> {
        let path = self.path(stage_id, partition);
        if !path.exists() {
            return Ok(Vec::new());
        }
        read_ipc_file(&path)
    }

    /// Remove all spill files for a stage.
    pub fn clear_stage(&self, stage_id: u32) {
        if let Ok(entries) = std::fs::read_dir(&self.root) {
            let prefix = format!("stage_{stage_id}_");
            for ent in entries.flatten() {
                if ent
                    .file_name()
                    .to_string_lossy()
                    .starts_with(&prefix)
                {
                    let _ = std::fs::remove_file(ent.path());
                }
            }
        }
    }
}

fn read_ipc_file(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = std::fs::File::open(path)
        .map_err(|e| Error::Io(format!("spill open {}: {e}", path.display())))?;
    let reader = StreamReader::try_new(file, None)
        .map_err(|e| Error::Io(format!("spill read: {e}")))?;
    reader
        .map(|b| b.map_err(|e| Error::Io(format!("spill batch: {e}"))))
        .collect()
}

/// In-memory bucket set, optionally spilled to disk.
#[derive(Debug)]
pub enum BucketCache {
    Memory(Vec<Vec<RecordBatch>>),
    Spilled {
        schema: SchemaRef,
        spill: Arc<SpillStore>,
        stage_id: u32,
    },
}

impl BucketCache {
    pub fn from_memory(buckets: Vec<Vec<RecordBatch>>) -> Self {
        Self::Memory(buckets)
    }

    pub fn maybe_spill(
        schema: SchemaRef,
        buckets: Vec<Vec<RecordBatch>>,
        stage_id: u32,
        spill: Option<&SpillStore>,
    ) -> Result<Self> {
        let Some(store) = spill else {
            return Ok(Self::Memory(buckets));
        };
        for (i, bucket) in buckets.iter().enumerate() {
            store.write_bucket(stage_id, i as u32, schema.clone(), bucket)?;
        }
        Ok(Self::Spilled {
            schema,
            spill: Arc::new(store.clone()),
            stage_id,
        })
    }

    pub fn read_partition(&self, partition: usize) -> Vec<RecordBatch> {
        match self {
            Self::Memory(buckets) => buckets
                .get(partition)
                .cloned()
                .unwrap_or_default(),
            Self::Spilled {
                schema,
                spill,
                stage_id,
            } => spill
                .read_bucket(*stage_id, partition as u32)
                .unwrap_or_default()
                .into_iter()
                .filter(|b| b.num_rows() > 0)
                .collect::<Vec<_>>()
                .pipe(|data| {
                    if data.is_empty() {
                        vec![RecordBatch::new_empty(schema.clone())]
                    } else {
                        data
                    }
                }),
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Memory(buckets) => buckets
                .iter()
                .find_map(|b| b.first())
                .map(|b| b.schema())
                .unwrap_or_else(|| Arc::new(weft_loom::arrow::datatypes::Schema::empty())),
            Self::Spilled { schema, .. } => schema.clone(),
        }
    }
}

trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
impl<T> Pipe for T {}
