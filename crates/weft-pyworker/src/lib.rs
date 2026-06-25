//! `weft-pyworker` — the Python UDF execution bridge (Rust side).
//!
//! Notebooks run mixed SQL + Python. SQL/DataFrame cells run on the engine as usual; **Python
//! cells and Python UDFs** run in a Python **sidecar** colocated in each driver/worker pod. The
//! engine dispatches vectorized Arrow batches to the sidecar over Arrow Flight/IPC and splices the
//! result batches back — mirroring Spark's PythonRunner, but Arrow-native end to end.
//!
//! This module freezes the **artifact + UDF descriptor contract** — what `weft-connect`'s
//! `AddArtifacts` records and hands to the sidecar — ahead of the Flight transport and the
//! `runtime/` Python image.

/// A user-supplied artifact (uploaded via Spark Connect `AddArtifacts`), cached for a session and
/// loaded by the sidecar before UDF evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    /// Logical name (e.g. `mylib-1.0-py3-none-any.whl`, `udfs.py`, `closure.pkl`).
    pub name: String,
    /// Artifact kind.
    pub kind: ArtifactKind,
    /// Workspace S3 URI where the bytes are stored (the sidecar fetches from here).
    pub s3_uri: String,
    /// SHA-256 of the contents, for cache identity + integrity.
    pub sha256: String,
}

/// The kind of an uploaded artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactKind {
    /// A Python wheel to `pip install` into the sidecar venv.
    Wheel,
    /// A plain `.py` module to put on `sys.path`.
    PyFile,
    /// A cloudpickled callable (a registered UDF closure).
    Pickle,
    /// An arbitrary data file the UDF reads.
    File,
}

/// How a Python UDF maps batches to batches. v1 targets vectorized (pandas/Arrow) UDFs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfEvalType {
    /// Scalar pandas/Arrow UDF: one input batch → one output column batch of the same length.
    ScalarPandas,
    /// Grouped-map UDF: a group's batch → a transformed batch.
    GroupedMapPandas,
}

/// A registered Python UDF the engine can invoke on the sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfDescriptor {
    /// SQL-visible function name.
    pub name: String,
    /// Evaluation contract.
    pub eval_type: UdfEvalType,
    /// Names of the artifacts that must be loaded to evaluate this UDF.
    pub artifacts: Vec<String>,
    /// Arrow return type, as a DataType string (resolved to a `DataType` by the engine).
    pub return_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_references_artifacts() {
        let udf = UdfDescriptor {
            name: "normalize".into(),
            eval_type: UdfEvalType::ScalarPandas,
            artifacts: vec!["mylib.whl".into()],
            return_type: "Float64".into(),
        };
        assert_eq!(udf.eval_type, UdfEvalType::ScalarPandas);
        assert_eq!(udf.artifacts, vec!["mylib.whl".to_string()]);
    }

    #[test]
    fn artifact_kinds_distinct() {
        let a = Artifact {
            name: "mylib.whl".into(),
            kind: ArtifactKind::Wheel,
            s3_uri: "s3://ws/artifacts/mylib.whl".into(),
            sha256: "deadbeef".into(),
        };
        assert_eq!(a.kind, ArtifactKind::Wheel);
        assert_ne!(ArtifactKind::Wheel, ArtifactKind::PyFile);
    }
}
