//! Spark Structured Streaming micro-batch engine for Weft.
//!
//! Implements a subset of Spark's Structured Streaming: file-based sources/sinks, processing-time
//! triggers, checkpoint metadata, and query lifecycle management.

mod checkpoint;
mod query;
mod scheduler;
mod sink;
mod source;

pub use checkpoint::CheckpointStore;
pub use query::{QueryProgress, QueryStatus, StreamingQuery, StreamingQueryId};
pub use scheduler::{StreamingQueryManager, Trigger};
pub use sink::{FileSink, MemorySink, Sink};
pub use source::{FileSource, MemoryRateSource, Source};
