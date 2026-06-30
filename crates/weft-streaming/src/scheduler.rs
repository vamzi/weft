//! Micro-batch trigger scheduling and query manager.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use weft_loom::Engine;

use crate::checkpoint::{CheckpointState, CheckpointStore};
use crate::query::{QueryProgress, QueryStatus, StreamingQuery, StreamingQueryId};
use crate::sink::{MemorySink, Sink};
use crate::source::{MemoryRateSource, Source};

/// Trigger mode for micro-batch execution.
#[derive(Debug, Clone)]
pub enum Trigger {
    /// Fire every `interval` (processing-time).
    ProcessingTime(Duration),
    /// Process all available data once, then stop.
    Once,
    /// Process all currently available data, then idle.
    AvailableNow,
}

/// Manages active streaming queries.
pub struct StreamingQueryManager {
    queries: Arc<RwLock<HashMap<String, ManagedQuery>>>,
}

struct ManagedQuery {
    query: StreamingQuery,
    source: Box<dyn Source>,
    sink: Box<dyn Sink>,
    checkpoint: CheckpointStore,
    #[allow(dead_code)]
    trigger: Trigger,
}

impl StreamingQueryManager {
    pub fn new() -> Self {
        Self {
            queries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start a new streaming query and return its id.
    pub async fn start(
        &self,
        name: String,
        checkpoint_location: String,
        trigger: Trigger,
    ) -> StreamingQueryId {
        let q = StreamingQuery::new(name.clone(), checkpoint_location.clone());
        let id = q.query_id.clone();
        let checkpoint = CheckpointStore::new(&checkpoint_location);
        let _ = checkpoint.init_for_query(&id);
        let managed = ManagedQuery {
            query: q,
            source: Box::new(MemoryRateSource::new(10, 1)),
            sink: Box::new(MemorySink::new()),
            checkpoint,
            trigger,
        };
        self.queries.write().await.insert(id.id.clone(), managed);
        id
    }

    pub async fn status(&self, query_id: &str) -> Option<QueryStatus> {
        self.queries
            .read()
            .await
            .get(query_id)
            .map(|m| m.query.status.clone())
    }

    pub async fn last_progress(&self, query_id: &str) -> Option<QueryProgress> {
        self.queries
            .read()
            .await
            .get(query_id)
            .and_then(|m| m.query.last_progress.clone())
    }

    pub async fn stop(&self, query_id: &str) -> bool {
        if let Some(m) = self.queries.write().await.get_mut(query_id) {
            m.query.status.is_active = false;
            m.query.status.message = "stopped".into();
            true
        } else {
            false
        }
    }

    /// Run one micro-batch for `query_id` using `engine`.
    pub async fn run_batch(&self, query_id: &str, engine: &Engine) -> weft_common::Result<u64> {
        let mut guard = self.queries.write().await;
        let m = guard
            .get_mut(query_id)
            .ok_or_else(|| weft_common::Error::Execution("unknown query".into()))?;
        if !m.query.status.is_active {
            return Ok(0);
        }
        let batches = m.source.poll_batch(engine).await?;
        let rows = m.sink.write_batch(&batches)?;
        m.query.batch_id += 1;
        m.query.status.is_data_available = rows > 0;
        m.query.last_progress = Some(QueryProgress {
            id: m.query.query_id.id.clone(),
            run_id: m.query.query_id.run_id.clone(),
            name: m.query.name.clone(),
            num_input_rows: rows,
            processed_rows_per_second: rows as f64,
            batch_id: m.query.batch_id,
        });
        let state = CheckpointState {
            query_id: m.query.query_id.id.clone(),
            run_id: m.query.query_id.run_id.clone(),
            batch_id: m.query.batch_id,
            source_offsets: vec![],
        };
        let _ = m.checkpoint.save(&state);
        Ok(rows)
    }

    /// Process all available data for `query_id` (for `availableNow` / `once` triggers).
    pub async fn process_all_available(
        &self,
        query_id: &str,
        engine: &Engine,
    ) -> weft_common::Result<u64> {
        let mut total = 0u64;
        loop {
            let rows = self.run_batch(query_id, engine).await?;
            if rows == 0 {
                break;
            }
            total += rows;
        }
        Ok(total)
    }

    pub async fn active_queries(&self) -> Vec<StreamingQueryId> {
        self.queries
            .read()
            .await
            .values()
            .filter(|m| m.query.status.is_active)
            .map(|m| m.query.query_id.clone())
            .collect()
    }
}

impl Default for StreamingQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn start_and_run_batch() {
        let mgr = StreamingQueryManager::new();
        let id = mgr
            .start("test".into(), "/tmp/weft-stream-test".into(), Trigger::Once)
            .await;
        let engine = Engine::new();
        let rows = mgr.process_all_available(&id.id, &engine).await.unwrap();
        assert!(rows > 0);
        let progress = mgr.last_progress(&id.id).await.unwrap();
        assert!(progress.batch_id >= 1);
    }
}
