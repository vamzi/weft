//! `weft-proto` — generated Spark Connect protobuf types.
//!
//! When wired up, this crate vendors the `spark/connect/*.proto` files from a pinned
//! `apache/spark` tag (target **Spark 4.x**) and runs `tonic-build` in `build.rs` to
//! produce the `SparkConnectService` server stubs plus the `Relation` / `Expression` /
//! `DataType` message trees.
//!
//! Surface the server must cover (see `docs/architecture.md` §2):
//! - RPCs: `ExecutePlan`, `AnalyzePlan`, `Config`, `Interrupt`,
//!   `ReattachExecute`/`ReleaseExecute`, `AddArtifacts`/`ArtifactStatus`,
//!   `FetchErrorDetails`.
//! - `Relation.rel_type` oneof (~60 types) and `Expression.expr_type` oneof (~22 types).
//!
//! Today it is an empty placeholder so the workspace builds without `protoc`.

/// The pinned Spark protocol version this crate targets. The Spark Connect wire
/// protocol is version-coupled; we test against an exact `pyspark-client`.
pub const TARGET_SPARK_VERSION: &str = "4.x";
