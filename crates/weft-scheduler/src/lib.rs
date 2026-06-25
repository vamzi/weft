//! `weft-scheduler` — the job/workflow engine.
//!
//! A long-running service that polls a Postgres job queue (`SELECT … FOR UPDATE SKIP LOCKED`) for
//! due schedules (cron) and triggered runs. A job is a DAG of tasks (SQL or notebook task, run on
//! an existing cluster or an ephemeral **job cluster** the operator spins up per run). Execution is
//! a topological walk with per-task retry/timeout; state is recorded in `weft-meta`.
//!
//! This module freezes the **DAG model** + topological execution order. The [`executor`] module
//! adds the dependency-ordered run with per-task retry and failure-skip (over a [`executor::TaskRunner`]
//! seam); the Postgres queue (`SKIP LOCKED`) + cron wake-up land on top once deps are wired.

pub mod executor;

/// A task in a job DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    /// Unique key within the job.
    pub key: String,
    /// Task kind: `"sql"` or `"notebook"`.
    pub kind: String,
    /// What to run (SQL text or a notebook id), resolved by the executor.
    pub reference: String,
    /// Keys of tasks that must complete before this one runs.
    pub depends_on: Vec<String>,
}

/// A job: a named DAG of tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    /// Job id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// The task DAG.
    pub tasks: Vec<Task>,
}

/// Errors validating/ordering a DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DagError {
    /// A task references a dependency key that does not exist.
    UnknownDependency {
        /// The task with the bad dependency.
        task: String,
        /// The missing dependency key.
        missing: String,
    },
    /// The DAG contains a cycle.
    Cycle,
    /// Two tasks share the same key.
    DuplicateKey(String),
}

impl Job {
    /// Compute a valid execution order (topological sort), or a [`DagError`] if the DAG is
    /// malformed. Ready tasks at the same "level" may run in parallel; this returns one linear
    /// order consistent with the dependencies.
    pub fn execution_order(&self) -> Result<Vec<String>, DagError> {
        use std::collections::{HashMap, HashSet, VecDeque};

        let mut keys = HashSet::new();
        for t in &self.tasks {
            if !keys.insert(t.key.clone()) {
                return Err(DagError::DuplicateKey(t.key.clone()));
            }
        }
        // Build indegree + adjacency, validating dependency references.
        let mut indeg: HashMap<&str, usize> =
            self.tasks.iter().map(|t| (t.key.as_str(), 0)).collect();
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for t in &self.tasks {
            for d in &t.depends_on {
                if !keys.contains(d) {
                    return Err(DagError::UnknownDependency {
                        task: t.key.clone(),
                        missing: d.clone(),
                    });
                }
                adj.entry(d.as_str()).or_default().push(t.key.as_str());
                *indeg.get_mut(t.key.as_str()).unwrap() += 1;
            }
        }
        // Kahn's algorithm; iterate tasks in declared order for deterministic output.
        let mut queue: VecDeque<&str> = self
            .tasks
            .iter()
            .map(|t| t.key.as_str())
            .filter(|k| indeg[k] == 0)
            .collect();
        let mut order = Vec::new();
        while let Some(k) = queue.pop_front() {
            order.push(k.to_string());
            if let Some(children) = adj.get(k) {
                for &c in children {
                    let e = indeg.get_mut(c).unwrap();
                    *e -= 1;
                    if *e == 0 {
                        queue.push_back(c);
                    }
                }
            }
        }
        if order.len() != self.tasks.len() {
            return Err(DagError::Cycle);
        }
        Ok(order)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(key: &str, deps: &[&str]) -> Task {
        Task {
            key: key.into(),
            kind: "sql".into(),
            reference: "SELECT 1".into(),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn linear_dag_orders() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &[]), task("b", &["a"]), task("c", &["b"])],
        };
        assert_eq!(job.execution_order().unwrap(), vec!["a", "b", "c"]);
    }

    #[test]
    fn diamond_dag_orders_dependencies_first() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![
                task("a", &[]),
                task("b", &["a"]),
                task("c", &["a"]),
                task("d", &["b", "c"]),
            ],
        };
        let order = job.execution_order().unwrap();
        let pos = |k: &str| order.iter().position(|x| x == k).unwrap();
        assert!(pos("a") < pos("b") && pos("a") < pos("c"));
        assert!(pos("b") < pos("d") && pos("c") < pos("d"));
    }

    #[test]
    fn cycle_detected() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &["b"]), task("b", &["a"])],
        };
        assert_eq!(job.execution_order(), Err(DagError::Cycle));
    }

    #[test]
    fn unknown_dependency_detected() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &["missing"])],
        };
        assert!(matches!(
            job.execution_order(),
            Err(DagError::UnknownDependency { .. })
        ));
    }
}
