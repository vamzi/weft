//! Dependency-ordered job execution: walk the DAG in topological order, run each task through a
//! [`TaskRunner`], retry transient failures, and **skip** any task whose upstream failed.
//!
//! The control flow (ordering, retry, skip-on-upstream-failure, overall success) is the valuable,
//! testable core and lives here, dependency-free. The concrete async runner — which dispatches a
//! SQL/notebook task to a cluster's Spark Connect endpoint — implements [`TaskRunner`] once the
//! gateway client is wired; the Postgres queue records [`RunReport`] state per `weft-meta`.

use std::collections::{HashMap, HashSet};

use crate::{DagError, Job, Task};

/// The outcome of running one task attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    /// The task succeeded.
    Success,
    /// The task failed, with a reason. Failures are retried up to [`ExecOptions::max_attempts`].
    Failure(String),
}

/// Executes a single task. The seam between the scheduler's control flow and actual execution.
pub trait TaskRunner {
    /// Run `task` (its `attempt`-th attempt, 1-based) and report the outcome.
    fn run(&self, task: &Task, attempt: u32) -> TaskOutcome;
}

/// Execution options.
#[derive(Debug, Clone, Copy)]
pub struct ExecOptions {
    /// Maximum attempts per task (>= 1). A task is retried while it fails, up to this many tries.
    pub max_attempts: u32,
}

impl Default for ExecOptions {
    fn default() -> Self {
        Self { max_attempts: 1 }
    }
}

/// The terminal status of a task within a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Completed successfully.
    Success,
    /// Failed after exhausting retries.
    Failed,
    /// Not run because an upstream dependency failed or was skipped.
    Skipped,
}

/// The result of one task in a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskResult {
    /// The task key.
    pub key: String,
    /// Terminal status.
    pub status: TaskStatus,
    /// Number of attempts made (0 for skipped tasks).
    pub attempts: u32,
    /// The last failure reason, if any.
    pub error: Option<String>,
}

/// The report for a whole job run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    /// Per-task results, in execution order.
    pub results: Vec<TaskResult>,
    /// True iff every task succeeded.
    pub success: bool,
}

impl Job {
    /// Execute the job's DAG with `runner`, honoring `opts`. Returns a [`RunReport`], or a
    /// [`DagError`] if the DAG is malformed.
    ///
    /// Semantics: tasks run in a dependency-respecting order; a task whose any dependency ended in
    /// [`TaskStatus::Failed`] or [`TaskStatus::Skipped`] is itself **skipped** (its branch is dead),
    /// while independent branches continue. A task is retried up to `opts.max_attempts` times.
    pub fn run(&self, runner: &dyn TaskRunner, opts: &ExecOptions) -> Result<RunReport, DagError> {
        let order = self.execution_order()?;
        let by_key: HashMap<&str, &Task> = self.tasks.iter().map(|t| (t.key.as_str(), t)).collect();

        let mut status: HashMap<String, TaskStatus> = HashMap::new();
        let mut results = Vec::with_capacity(order.len());

        for key in &order {
            let task = by_key[key.as_str()];
            // If any dependency didn't succeed, skip this task.
            let blocked = task
                .depends_on
                .iter()
                .any(|d| status.get(d) != Some(&TaskStatus::Success));
            if blocked {
                status.insert(key.clone(), TaskStatus::Skipped);
                results.push(TaskResult {
                    key: key.clone(),
                    status: TaskStatus::Skipped,
                    attempts: 0,
                    error: None,
                });
                continue;
            }

            // Run with retry.
            let max = opts.max_attempts.max(1);
            let mut attempts = 0;
            let mut last_err = None;
            let mut ok = false;
            while attempts < max {
                attempts += 1;
                match runner.run(task, attempts) {
                    TaskOutcome::Success => {
                        ok = true;
                        break;
                    }
                    TaskOutcome::Failure(e) => last_err = Some(e),
                }
            }
            let st = if ok {
                TaskStatus::Success
            } else {
                TaskStatus::Failed
            };
            status.insert(key.clone(), st);
            results.push(TaskResult {
                key: key.clone(),
                status: st,
                attempts,
                error: if ok { None } else { last_err },
            });
        }

        let success = results.iter().all(|r| r.status == TaskStatus::Success);
        Ok(RunReport { results, success })
    }
}

/// Identify the tasks that are *runnable now* given a set of already-succeeded keys — the basis for
/// running independent branches in parallel (the async executor schedules these concurrently).
pub fn ready_tasks<'a>(
    job: &'a Job,
    succeeded: &HashSet<String>,
    done: &HashSet<String>,
) -> Vec<&'a Task> {
    job.tasks
        .iter()
        .filter(|t| !done.contains(&t.key))
        .filter(|t| t.depends_on.iter().all(|d| succeeded.contains(d)))
        .collect()
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

    /// A runner that fails the named keys until a per-key attempt threshold is reached.
    struct ScriptRunner {
        /// key -> attempt number at which it starts succeeding (1 = always succeeds).
        succeed_at: HashMap<String, u32>,
    }
    impl TaskRunner for ScriptRunner {
        fn run(&self, task: &Task, attempt: u32) -> TaskOutcome {
            let threshold = *self.succeed_at.get(&task.key).unwrap_or(&1);
            if attempt >= threshold {
                TaskOutcome::Success
            } else {
                TaskOutcome::Failure(format!("{} flaked on attempt {attempt}", task.key))
            }
        }
    }

    fn runner(pairs: &[(&str, u32)]) -> ScriptRunner {
        ScriptRunner {
            succeed_at: pairs.iter().map(|(k, n)| (k.to_string(), *n)).collect(),
        }
    }

    #[test]
    fn all_success() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &[]), task("b", &["a"])],
        };
        let report = job.run(&runner(&[]), &ExecOptions::default()).unwrap();
        assert!(report.success);
        assert!(report
            .results
            .iter()
            .all(|r| r.status == TaskStatus::Success));
    }

    #[test]
    fn failure_skips_downstream_but_not_independent_branch() {
        // a → b (b fails); c is independent and should still run.
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![
                task("a", &[]),
                task("b", &["a"]),
                task("d", &["b"]),
                task("c", &[]),
            ],
        };
        let report = job
            .run(&runner(&[("b", 99)]), &ExecOptions::default())
            .unwrap();
        assert!(!report.success);
        let st = |k: &str| report.results.iter().find(|r| r.key == k).unwrap().status;
        assert_eq!(st("a"), TaskStatus::Success);
        assert_eq!(st("b"), TaskStatus::Failed);
        assert_eq!(st("d"), TaskStatus::Skipped); // downstream of the failure
        assert_eq!(st("c"), TaskStatus::Success); // independent branch unaffected
    }

    #[test]
    fn retry_recovers_a_flaky_task() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &[])],
        };
        // 'a' succeeds on attempt 2; allow 3 attempts.
        let report = job
            .run(&runner(&[("a", 2)]), &ExecOptions { max_attempts: 3 })
            .unwrap();
        assert!(report.success);
        let a = &report.results[0];
        assert_eq!(a.status, TaskStatus::Success);
        assert_eq!(a.attempts, 2);
    }

    #[test]
    fn exhausting_retries_fails() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &[])],
        };
        let report = job
            .run(&runner(&[("a", 99)]), &ExecOptions { max_attempts: 2 })
            .unwrap();
        assert!(!report.success);
        assert_eq!(report.results[0].attempts, 2);
        assert!(report.results[0].error.is_some());
    }

    #[test]
    fn ready_tasks_tracks_frontier() {
        let job = Job {
            id: "j".into(),
            name: "j".into(),
            tasks: vec![task("a", &[]), task("b", &["a"]), task("c", &["a"])],
        };
        let none = HashSet::new();
        let ready = ready_tasks(&job, &none, &none);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].key, "a");

        let succeeded: HashSet<String> = ["a".to_string()].into_iter().collect();
        let done: HashSet<String> = ["a".to_string()].into_iter().collect();
        let ready2 = ready_tasks(&job, &succeeded, &done);
        let keys: HashSet<&str> = ready2.iter().map(|t| t.key.as_str()).collect();
        assert_eq!(keys, ["b", "c"].into_iter().collect());
    }
}
