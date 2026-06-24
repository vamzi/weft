//! `weft-execution` — local and distributed execution of physical plans.
//!
//! Local mode runs the physical plan across a thread pool (morsel-driven). Distributed
//! mode (Phase 1 MVP) splits the plan into stages/tasks at shuffle boundaries: a
//! driver/worker actor control plane over Weft gRPC, and an Arrow Flight data plane for
//! shuffle + result return. Out-of-core, partitioned shuffle with spill is a deliberate
//! divergence lane — Sail's cluster mode is new (Feb 2026) and its ClickBench is
//! single-process-per-query.

use weft_common::Result;

/// Execution mode for a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Single process, multi-threaded.
    Local,
    /// Driver + workers, Arrow Flight shuffle.
    Distributed,
}

/// Run a physical plan to completion. Implemented incrementally across Phase 0/1.
pub fn run(_mode: Mode) -> Result<()> {
    weft_loom::execute()
}
