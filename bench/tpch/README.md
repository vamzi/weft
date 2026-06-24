# TPC-H harness

Phase 0 uses a 10-query subset (Q1, Q3, Q5, Q6, Q10 minimum) for correctness vs a Spark
oracle; Phase 1 uses SF100 across nodes to exercise distributed shuffle with zero spill OOM.

- `run-correctness.sh` — run the subset through Weft (`sc://`) and diff vs reference output.
- (Phase 1) `benchmark.sh` — SF1/SF100 timing.
