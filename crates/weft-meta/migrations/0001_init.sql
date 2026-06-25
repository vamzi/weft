-- weft-meta — canonical control-plane schema (Postgres).
--
-- The single source of truth for everything that must survive pod restarts: identity, clusters,
-- the local catalog + Unity-Catalog-parity governance, notebooks, query history, jobs, and
-- dashboards. Every control-plane service (gateway, clustermgr, scheduler, govern) reads/writes
-- through `weft-meta` repositories built on this schema.
--
-- Conventions: TEXT ids that are app-generated UUIDs (kept TEXT for portability); `created_at`/
-- `updated_at` everywhere; soft references by id (FKs declared where the lifecycle is owned here).

-- ─────────────────────────────────────────── Identity ───────────────────────────────────────────

CREATE TABLE users (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL,
    external_id   TEXT UNIQUE,                 -- SCIM/IdP id
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE groups (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    external_id   TEXT UNIQUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Nested-group capable membership: a member is a user OR another group.
CREATE TABLE group_members (
    group_id        TEXT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    member_user_id  TEXT REFERENCES users(id) ON DELETE CASCADE,
    member_group_id TEXT REFERENCES groups(id) ON DELETE CASCADE,
    CHECK ((member_user_id IS NOT NULL) <> (member_group_id IS NOT NULL))
);
CREATE INDEX idx_group_members_group ON group_members(group_id);

CREATE TABLE service_principals (
    id            TEXT PRIMARY KEY,
    client_id     TEXT NOT NULL UNIQUE,
    display_name  TEXT NOT NULL,
    secret_hash   TEXT NOT NULL,               -- bcrypt of the client secret
    active        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Long-lived API/PAT tokens (hashed). Short-lived session JWTs are not stored.
CREATE TABLE tokens (
    id            TEXT PRIMARY KEY,
    principal_kind TEXT NOT NULL,              -- 'user' | 'service_principal'
    principal_id  TEXT NOT NULL,
    token_hash    TEXT NOT NULL UNIQUE,
    comment       TEXT,
    expires_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at  TIMESTAMPTZ
);

-- ─────────────────────────────────────────── Clusters ───────────────────────────────────────────

CREATE TABLE clusters (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    owner_user_id   TEXT REFERENCES users(id),
    state           TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING|PROVISIONING|RUNNING|TERMINATING|TERMINATED|ERROR
    worker_min      INTEGER NOT NULL DEFAULT 1,
    worker_max      INTEGER NOT NULL DEFAULT 1,
    worker_size     TEXT NOT NULL DEFAULT 'small',    -- pod size class
    idle_timeout_s  INTEGER NOT NULL DEFAULT 1800,
    is_job_cluster  BOOLEAN NOT NULL DEFAULT FALSE,   -- ephemeral, created per job run
    -- In-cluster Spark Connect endpoint the gateway dials (set by the operator once RUNNING).
    connect_endpoint TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_clusters_state ON clusters(state);

-- ──────────────────────────── Catalog + Governance (Unity-Catalog parity) ────────────────────────

-- Connections to external catalogs / systems (UC "connection" securable). Secrets live in
-- AWS Secrets Manager; only the ARN/ref is stored here.
CREATE TABLE connections (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    kind          TEXT NOT NULL,               -- 'hive' | 'glue' | 'unity' | 'local'
    options       JSONB NOT NULL DEFAULT '{}', -- non-secret config (endpoint, region, …)
    secret_ref    TEXT,                        -- Secrets Manager ARN for credentials
    -- Whether Weft enforces its own ACLs on top (governed) or delegates to the source.
    weft_governed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The local catalog's namespace objects (the "default local catalog" created in the UI). External
-- catalogs are resolved live by their provider; only local objects are stored here.
CREATE TABLE catalogs (
    name          TEXT PRIMARY KEY,
    comment       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE schemas (
    catalog_name  TEXT NOT NULL REFERENCES catalogs(name) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    comment       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_name, name)
);

CREATE TABLE tables (
    catalog_name  TEXT NOT NULL,
    schema_name   TEXT NOT NULL,
    name          TEXT NOT NULL,
    location      TEXT NOT NULL,               -- s3:// / file:// URI
    format        TEXT NOT NULL,               -- parquet|delta|iceberg|csv|json
    storage_options JSONB NOT NULL DEFAULT '{}',
    partition_columns TEXT[] NOT NULL DEFAULT '{}',
    comment       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_name, schema_name, name),
    FOREIGN KEY (catalog_name, schema_name) REFERENCES schemas(catalog_name, name) ON DELETE CASCADE
);

-- The securable tree's identity for governance. A securable is (type, dotted-name); rows here
-- exist for any object that has an owner or a grant. Mirrors `weft_govern::Securable`.
CREATE TABLE securables (
    id            TEXT PRIMARY KEY,
    kind          TEXT NOT NULL,               -- metastore|catalog|schema|table|view|volume|function|external_location|storage_credential|connection
    name          TEXT NOT NULL,               -- dotted, e.g. 'main.sales.orders' ('' for metastore)
    owner_principal_kind TEXT,                  -- 'user'|'group'|'service_principal'
    owner_principal_id   TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kind, name)
);

-- Grants: effect privilege ON securable TO principal. Mirrors `weft_govern::Grant`.
CREATE TABLE grants (
    id            TEXT PRIMARY KEY,
    securable_id  TEXT NOT NULL REFERENCES securables(id) ON DELETE CASCADE,
    privilege     TEXT NOT NULL,               -- USE CATALOG, SELECT, MODIFY, ALL PRIVILEGES, …
    principal_kind TEXT NOT NULL,              -- 'user'|'group'|'service_principal'
    principal_id  TEXT NOT NULL,
    effect        TEXT NOT NULL DEFAULT 'allow', -- 'allow'|'deny'
    granted_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (securable_id, privilege, principal_kind, principal_id, effect)
);
CREATE INDEX idx_grants_securable ON grants(securable_id);

-- Row filters and column masks (UDF-backed). Applied by the analyzer's plan rewriter.
CREATE TABLE row_filters (
    table_securable_id TEXT NOT NULL REFERENCES securables(id) ON DELETE CASCADE,
    function_name      TEXT NOT NULL,
    on_columns         TEXT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (table_securable_id)
);

CREATE TABLE column_masks (
    table_securable_id TEXT NOT NULL REFERENCES securables(id) ON DELETE CASCADE,
    column_name        TEXT NOT NULL,
    function_name      TEXT NOT NULL,
    using_columns      TEXT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (table_securable_id, column_name)
);

-- Append-only audit log of grant changes and (sampled) query access.
CREATE TABLE audit_log (
    id            BIGSERIAL PRIMARY KEY,
    at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    actor         TEXT,                        -- user/sp id
    action        TEXT NOT NULL,               -- 'grant'|'revoke'|'query'|'login'|…
    securable     TEXT,
    detail        JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_audit_at ON audit_log(at);

-- ────────────────────────────────────── Notebooks + Queries ──────────────────────────────────────

CREATE TABLE notebooks (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    owner_user_id TEXT REFERENCES users(id),
    -- Cells as an ordered JSON array: [{ "kind": "sql|python|markdown", "source": "…" }, …]
    cells         JSONB NOT NULL DEFAULT '[]',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE notebook_revisions (
    id            BIGSERIAL PRIMARY KEY,
    notebook_id   TEXT NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    cells         JSONB NOT NULL,
    author_user_id TEXT REFERENCES users(id),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_notebook_revisions_nb ON notebook_revisions(notebook_id);

CREATE TABLE queries (
    id            TEXT PRIMARY KEY,
    cluster_id    TEXT REFERENCES clusters(id),
    user_id       TEXT REFERENCES users(id),
    sql_text      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'RUNNING', -- RUNNING|FINISHED|FAILED|CANCELLED
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    row_count     BIGINT,
    bytes_scanned BIGINT,
    -- Pointer to the full Arrow IPC result set in the workspace S3 bucket.
    result_s3_uri TEXT,
    error         TEXT
);
CREATE INDEX idx_queries_user ON queries(user_id, started_at DESC);

-- ───────────────────────────────────── Jobs + Workflows ─────────────────────────────────────────

CREATE TABLE jobs (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    owner_user_id TEXT REFERENCES users(id),
    -- DAG of tasks: [{ "key": "t1", "type": "sql|notebook", "ref": "…", "depends_on": [] }, …]
    tasks         JSONB NOT NULL DEFAULT '[]',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE schedules (
    id            TEXT PRIMARY KEY,
    job_id        TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    cron          TEXT NOT NULL,
    timezone      TEXT NOT NULL DEFAULT 'UTC',
    paused        BOOLEAN NOT NULL DEFAULT FALSE,
    next_run_at   TIMESTAMPTZ
);
CREATE INDEX idx_schedules_next ON schedules(next_run_at) WHERE NOT paused;

CREATE TABLE job_runs (
    id            TEXT PRIMARY KEY,
    job_id        TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    trigger       TEXT NOT NULL,               -- 'schedule'|'manual'
    status        TEXT NOT NULL DEFAULT 'PENDING', -- PENDING|RUNNING|SUCCESS|FAILED|CANCELLED
    job_cluster_id TEXT REFERENCES clusters(id),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_job_runs_job ON job_runs(job_id, created_at DESC);

CREATE TABLE tasks (
    id            TEXT PRIMARY KEY,
    job_run_id    TEXT NOT NULL REFERENCES job_runs(id) ON DELETE CASCADE,
    task_key      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING',
    attempt       INTEGER NOT NULL DEFAULT 0,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    error         TEXT
);
CREATE INDEX idx_tasks_run ON tasks(job_run_id);

-- ──────────────────────────────────────── Dashboards ────────────────────────────────────────────

CREATE TABLE dashboards (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    owner_user_id TEXT REFERENCES users(id),
    layout        JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE widgets (
    id            TEXT PRIMARY KEY,
    dashboard_id  TEXT NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    -- A saved query plus a Vega-Lite/ECharts viz spec.
    query_text    TEXT NOT NULL,
    viz_spec      JSONB NOT NULL DEFAULT '{}',
    position      JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX idx_widgets_dashboard ON widgets(dashboard_id);
