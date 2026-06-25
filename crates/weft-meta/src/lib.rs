//! `weft-meta` — the control-plane metadata store.
//!
//! The single source of truth for everything that must survive pod restarts: identity, clusters,
//! the local catalog + governance grants, notebooks, query history, jobs, and dashboards. The
//! canonical schema lives in [`MIGRATIONS`] (SQL under `migrations/`); the repository layer — one
//! module per domain, built on `sqlx` against Postgres — lands on top once deps are wired.
//!
//! Kept dependency-free today (matching the workspace): the migration text and the table/enum
//! name constants are exposed so other crates can reference them without taking a `sqlx`
//! dependency, freezing the schema contract before the storage layer is written.

/// An ordered database migration: `(version, name, sql)`. Applied in ascending `version` order by
/// the future `sqlx::migrate!`-style runner.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// Monotonic version; migrations apply in ascending order.
    pub version: u32,
    /// Human-readable name.
    pub name: &'static str,
    /// The DDL.
    pub sql: &'static str,
}

/// All schema migrations, in apply order. New schema lands as additional entries here (never by
/// editing a shipped migration).
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "init",
    sql: include_str!("../migrations/0001_init.sql"),
}];

/// Table names, referenced by repositories and tests so a rename is a single edit.
pub mod tables {
    /// Users table.
    pub const USERS: &str = "users";
    /// Groups table.
    pub const GROUPS: &str = "groups";
    /// Group membership (nested-group capable).
    pub const GROUP_MEMBERS: &str = "group_members";
    /// Service principals.
    pub const SERVICE_PRINCIPALS: &str = "service_principals";
    /// API/PAT tokens (hashed).
    pub const TOKENS: &str = "tokens";
    /// Compute clusters.
    pub const CLUSTERS: &str = "clusters";
    /// External / local catalog connections.
    pub const CONNECTIONS: &str = "connections";
    /// Local catalogs.
    pub const CATALOGS: &str = "catalogs";
    /// Local schemas.
    pub const SCHEMAS: &str = "schemas";
    /// Local tables.
    pub const TABLES: &str = "tables";
    /// Governance securables.
    pub const SECURABLES: &str = "securables";
    /// Governance grants.
    pub const GRANTS: &str = "grants";
    /// Row filters.
    pub const ROW_FILTERS: &str = "row_filters";
    /// Column masks.
    pub const COLUMN_MASKS: &str = "column_masks";
    /// Audit log.
    pub const AUDIT_LOG: &str = "audit_log";
    /// Notebooks.
    pub const NOTEBOOKS: &str = "notebooks";
    /// Notebook revisions.
    pub const NOTEBOOK_REVISIONS: &str = "notebook_revisions";
    /// Query history.
    pub const QUERIES: &str = "queries";
    /// Jobs (workflow definitions).
    pub const JOBS: &str = "jobs";
    /// Job schedules.
    pub const SCHEDULES: &str = "schedules";
    /// Job runs.
    pub const JOB_RUNS: &str = "job_runs";
    /// Task instances within a run.
    pub const TASKS: &str = "tasks";
    /// Dashboards.
    pub const DASHBOARDS: &str = "dashboards";
    /// Dashboard widgets.
    pub const WIDGETS: &str = "widgets";
}

/// The cluster lifecycle states stored in `clusters.state` (mirrors the operator's state machine).
pub mod cluster_state {
    /// Requested, not yet acted on.
    pub const PENDING: &str = "PENDING";
    /// Pods being created.
    pub const PROVISIONING: &str = "PROVISIONING";
    /// Connect endpoint live, serving queries.
    pub const RUNNING: &str = "RUNNING";
    /// Tearing down.
    pub const TERMINATING: &str = "TERMINATING";
    /// Fully torn down.
    pub const TERMINATED: &str = "TERMINATED";
    /// Failed to provision / crashed.
    pub const ERROR: &str = "ERROR";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migrations_are_ordered_and_nonempty() {
        for (i, m) in MIGRATIONS.iter().enumerate() {
            assert_eq!(
                m.version as usize,
                i + 1,
                "migration versions must be 1..N in order"
            );
            assert!(
                !m.sql.trim().is_empty(),
                "migration {} has empty SQL",
                m.name
            );
        }
    }

    #[test]
    fn init_migration_defines_core_tables() {
        let sql = MIGRATIONS[0].sql;
        for t in [
            tables::USERS,
            tables::CLUSTERS,
            tables::SECURABLES,
            tables::GRANTS,
            tables::QUERIES,
        ] {
            assert!(
                sql.contains(&format!("CREATE TABLE {t} ")),
                "missing table {t}"
            );
        }
    }
}
