/*
 * Typed gateway client.
 *
 * The browser speaks ONLY to this REST edge (served SAME-ORIGIN by the gateway),
 * never gRPC. Auth, clusters, SQL, and admin (users/groups/grants) are LIVE —
 * they hit the real `/api/...` endpoints. Catalog, dashboards, and jobs have no
 * live backend yet, so those keep returning mock fixtures (clearly noted in the
 * UI). Every live call funnels through the single `request()` chokepoint, which
 * attaches the bearer token and, on 401, clears it and bounces to the login.
 */

/**
 * `USE_MOCK` toggles the demo-only sections (catalog / dashboards / jobs). The
 * live sections ignore it entirely — they always hit the gateway.
 */
const USE_MOCK = true;

/** Same-origin: the SPA is served by the gateway, so all paths are relative. */
const API_BASE = "";

// ---------------------------------------------------------------------------
// Token storage (localStorage) — the single source of the bearer token.
// ---------------------------------------------------------------------------

const TOKEN_KEY = "weft_token";

export function getToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
}

export function setToken(token: string): void {
  try {
    localStorage.setItem(TOKEN_KEY, token);
  } catch {
    // ignore storage failures (private mode, etc.)
  }
}

export function clearToken(): void {
  try {
    localStorage.removeItem(TOKEN_KEY);
  } catch {
    // ignore
  }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

export type ClusterState = "running" | "stopped" | "pending" | "terminating" | "error";

export type ClusterSize = "small" | "medium" | "large" | "xlarge";

export interface Cluster {
  id: string;
  name: string;
  state: ClusterState;
  size: ClusterSize;
  minWorkers: number;
  maxWorkers: number;
  activeWorkers: number;
  runtime: string;
  creator: string;
  createdAt: string;
  /** `sc://host:port` the user points PySpark at — only set once RUNNING. */
  connect_endpoint: string | null;
}

/** One lifecycle event from `GET /api/clusters/:id/events`. */
export interface ClusterEvent {
  /** Unix seconds. */
  at: number;
  message: string;
}

export interface CreateClusterInput {
  name: string;
  size: ClusterSize;
  minWorkers: number;
  maxWorkers: number;
}

export type CatalogKind = "catalog" | "schema" | "table" | "view";

export interface CatalogObject {
  id: string;
  name: string;
  kind: CatalogKind;
  parent: string | null;
  owner: string;
  rows?: number;
}

export interface Column {
  name: string;
  type: string;
  nullable: boolean;
  comment?: string;
}

/** Rich detail for a single table/view (GET /api/catalog/:fqn). */
export interface TableDetail {
  fqn: string; // catalog.schema.table
  name: string;
  kind: CatalogKind;
  owner: string;
  format: string; // delta | parquet | iceberg | view
  location: string;
  rows?: number;
  sizeBytes?: number;
  createdAt: string;
  columns: Column[];
}

// Live catalog (GET /api/catalog) -------------------------------------------

/** One column of a real, queryable table. */
export interface CatalogColumn {
  name: string;
  data_type: string;
}

/** A real table with its column schema. */
export interface CatalogTable {
  name: string;
  columns: CatalogColumn[];
}

/** A schema (namespace) holding tables. */
export interface CatalogSchema {
  name: string;
  tables: CatalogTable[];
}

/** A top-level catalog/namespace holding schemas. */
export interface CatalogNamespace {
  name: string;
  schemas: CatalogSchema[];
}

export type ExternalCatalogType = "hms" | "glue" | "unity" | "local";

export interface AttachCatalogInput {
  name: string;
  type: ExternalCatalogType;
  uri: string;
  comment?: string;
}

export interface QueryResult {
  columns: string[];
  rows: (string | number | null)[][];
  rowCount: number;
  durationMs: number;
}

/** Structured NL→SQL response (mirrors the gateway's schema-constrained JSON). */
export interface AiSqlResult {
  sql: string;
}

export type QueryStatus = "finished" | "running" | "failed" | "canceled";

export interface Query {
  id: string;
  text: string;
  status: QueryStatus;
  clusterId: string;
  user: string;
  durationMs: number;
  startedAt: string;
}

export interface Notebook {
  id: string;
  name: string;
  language: "sql" | "python" | "scala";
  owner: string;
  updatedAt: string;
  cells: number;
}

// Notebook editing model -----------------------------------------------------

export type CellKind = "sql" | "python" | "markdown";

export interface NotebookCell {
  id: string;
  kind: CellKind;
  source: string;
}

/** A notebook opened for editing: ordered cells. */
export interface NotebookDoc {
  id: string;
  name: string;
  cells: NotebookCell[];
}

/**
 * Result of running one cell. The shape depends on the cell kind:
 *  - sql      → a tabular result (`table`)
 *  - python   → captured stdout / repr (`text`)
 *  - markdown → the rendered source echoed back (`text`)
 */
export interface CellResult {
  kind: CellKind;
  table?: QueryResult;
  text?: string;
  durationMs: number;
}

/** AI-generated notebook skeleton (cells without ids — the client assigns them). */
export interface AiNotebook {
  cells: { kind: CellKind; source: string }[];
}

export type JobStatus = "succeeded" | "running" | "failed" | "scheduled" | "paused";

export interface Job {
  id: string;
  name: string;
  status: JobStatus;
  schedule: string;
  lastRun: string;
  owner: string;
}

export interface Dashboard {
  id: string;
  name: string;
  owner: string;
  updatedAt: string;
  tiles: number;
}

// Dashboard widgets ----------------------------------------------------------

export type ChartType = "bar" | "line" | "table";

export interface Widget {
  id: string;
  title: string;
  chart: ChartType;
  query: string;
}

/** A dashboard opened for viewing/editing: its widgets. */
export interface DashboardDoc {
  id: string;
  name: string;
  widgets: Widget[];
}

/** Materialized data for one widget: a labelled series the chart renders. */
export interface WidgetData {
  columns: string[]; // [label, value] for bar/line; full columns for table
  rows: (string | number | null)[][];
}

export interface AddWidgetInput {
  title: string;
  chart: ChartType;
  query: string;
}

// Job DAGs + runs ------------------------------------------------------------

export type TaskStatus = "pending" | "running" | "success" | "failed";

/** One node in a job's task DAG. */
export interface JobTask {
  id: string;
  name: string;
  dependsOn: string[];
}

/** A job opened for viewing: its task DAG. */
export interface JobDoc {
  id: string;
  name: string;
  schedule: string;
  tasks: JobTask[];
}

export type RunStatus = "PENDING" | "RUNNING" | "SUCCESS" | "FAILED";

/** Per-task outcome within a single run. */
export interface TaskRun {
  taskId: string;
  status: RunStatus;
  durationMs: number;
}

export interface JobRun {
  id: string;
  status: RunStatus;
  startedAt: string;
  durationMs: number;
  taskRuns: TaskRun[];
}

/** The signed-in user, as `GET /api/me` returns it. */
export interface Principal {
  user: string;
  groups: string[];
  authenticated: boolean;
}

/** `POST /api/auth/login` response. */
export interface LoginResult {
  token: string;
  user: string;
  groups: string[];
}

/** Whether the signed-in principal is an administrator (member of `admins`). */
export function isAdmin(me: Principal | null): boolean {
  return !!me?.groups?.includes("admins");
}

// Admin: users / groups / grants (mirror the gateway DTOs) -------------------

export interface AdminUser {
  username: string;
  groups: string[];
}

export interface CreateUserInput {
  username: string;
  password: string;
  groups: string[];
}

export interface AdminGroup {
  name: string;
  members: string[];
}

export interface CreateGroupInput {
  name: string;
  members: string[];
}

export type SecurableType =
  | "catalog"
  | "schema"
  | "table"
  | "view"
  | "metastore"
  | "connection";

/** A grant exactly as the gateway exchanges it (`GET/POST/DELETE /api/grants`). */
export interface GrantDto {
  securable_type: SecurableType;
  securable_name: string;
  privilege: string;
  principal_kind: PrincipalType;
  principal_id: string;
  effect: GrantEffect;
}

// Governance / Unity-Catalog-style grants -----------------------------------

export type Privilege =
  | "SELECT"
  | "MODIFY"
  | "USE CATALOG"
  | "USE SCHEMA"
  | "ALL PRIVILEGES"
  | "BROWSE"
  | "CREATE TABLE"
  | "MANAGE";

export type PrincipalType = "user" | "group";

export type GrantEffect = "allow" | "deny";

/** A securable object that grants can be attached to (catalog/schema/table). */
export interface Securable {
  fqn: string; // e.g. main, main.sales, main.sales.orders
  kind: CatalogKind;
  label: string;
}

export interface Grant {
  id: string;
  principal: string; // user email or group name
  principalType: PrincipalType;
  privilege: Privilege;
  effect: GrantEffect;
}

export interface GrantInput {
  securable: string; // fqn
  principal: string;
  principalType: PrincipalType;
  privilege: Privilege;
  effect: GrantEffect;
}

// ---------------------------------------------------------------------------
// Transport — single chokepoint so going live is a one-line change per path.
// ---------------------------------------------------------------------------

/**
 * The single network chokepoint. Attaches `Authorization: Bearer <token>` to
 * every call, and on 401 clears the token and reloads to the login gate. Set
 * `auth: false` for the login call itself (no token yet). Empty bodies (204) and
 * non-JSON responses resolve to `undefined`.
 */
async function request<T>(
  method: "GET" | "POST" | "PUT" | "DELETE",
  path: string,
  body?: unknown,
  opts: { auth?: boolean } = {},
): Promise<T> {
  const { auth = true } = opts;
  const headers: Record<string, string> = {};
  if (body !== undefined) headers["content-type"] = "application/json";
  if (auth) {
    const token = getToken();
    if (token) headers["authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(`${API_BASE}${path}`, {
    method,
    headers,
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

  if (res.status === 401 && auth) {
    // Token missing/expired — drop it and bounce to the login gate.
    clearToken();
    if (typeof window !== "undefined") window.location.reload();
    throw new Error("unauthorized");
  }
  if (!res.ok) {
    throw new Error(`${method} ${path} → ${res.status} ${res.statusText}`);
  }

  // 204 / empty body → no JSON to parse.
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

const delay = (ms = 250) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Mock fixtures
// ---------------------------------------------------------------------------

const mockCatalog: CatalogObject[] = [
  { id: "cat-main", name: "main", kind: "catalog", parent: null, owner: "admin" },
  { id: "sch-sales", name: "sales", kind: "schema", parent: "main", owner: "sales-lead" },
  { id: "tbl-orders", name: "orders", kind: "table", parent: "main.sales", owner: "sales-lead", rows: 12_840_221 },
  { id: "tbl-lineitem", name: "lineitem", kind: "table", parent: "main.sales", owner: "sales-lead", rows: 59_986_052 },
  { id: "view-rev", name: "monthly_revenue", kind: "view", parent: "main.sales", owner: "analyst" },
];

const mockQueries: Query[] = [
  {
    id: "q-1001",
    text: "SELECT l_returnflag, SUM(l_quantity) FROM lineitem GROUP BY 1",
    status: "finished",
    clusterId: "c-7f3a",
    user: "analyst",
    durationMs: 1840,
    startedAt: "2026-06-24T08:01:00Z",
  },
  {
    id: "q-1002",
    text: "SELECT * FROM orders WHERE o_orderdate > '1998-01-01'",
    status: "running",
    clusterId: "c-7f3a",
    user: "analyst",
    durationMs: 0,
    startedAt: "2026-06-24T08:05:00Z",
  },
];

/** Rich table/view details keyed by fully-qualified name. */
const mockTableDetails: Record<string, TableDetail> = {
  "main.sales.orders": {
    fqn: "main.sales.orders",
    name: "orders",
    kind: "table",
    owner: "sales-lead",
    format: "delta",
    location: "s3://weft-lake/main/sales/orders",
    rows: 12_840_221,
    sizeBytes: 2_140_998_144,
    createdAt: "2026-03-01T10:00:00Z",
    columns: [
      { name: "o_orderkey", type: "bigint", nullable: false, comment: "Primary key" },
      { name: "o_custkey", type: "bigint", nullable: false, comment: "FK → customer" },
      { name: "o_orderstatus", type: "char(1)", nullable: false },
      { name: "o_totalprice", type: "decimal(15,2)", nullable: true },
      { name: "o_orderdate", type: "date", nullable: true },
      { name: "o_orderpriority", type: "varchar(15)", nullable: true },
      { name: "o_clerk", type: "varchar(15)", nullable: true },
    ],
  },
  "main.sales.lineitem": {
    fqn: "main.sales.lineitem",
    name: "lineitem",
    kind: "table",
    owner: "sales-lead",
    format: "delta",
    location: "s3://weft-lake/main/sales/lineitem",
    rows: 59_986_052,
    sizeBytes: 9_881_223_168,
    createdAt: "2026-03-01T10:02:00Z",
    columns: [
      { name: "l_orderkey", type: "bigint", nullable: false, comment: "FK → orders" },
      { name: "l_partkey", type: "bigint", nullable: false },
      { name: "l_quantity", type: "decimal(15,2)", nullable: true },
      { name: "l_extendedprice", type: "decimal(15,2)", nullable: true },
      { name: "l_discount", type: "decimal(15,2)", nullable: true },
      { name: "l_returnflag", type: "char(1)", nullable: true },
      { name: "l_shipdate", type: "date", nullable: true },
    ],
  },
  "main.sales.monthly_revenue": {
    fqn: "main.sales.monthly_revenue",
    name: "monthly_revenue",
    kind: "view",
    owner: "analyst",
    format: "view",
    location: "—",
    createdAt: "2026-04-10T14:30:00Z",
    columns: [
      { name: "month", type: "date", nullable: true },
      { name: "revenue", type: "decimal(18,2)", nullable: true },
      { name: "orders", type: "bigint", nullable: true },
    ],
  },
};

const mockNotebooks: Notebook[] = [
  { id: "nb-01", name: "Revenue exploration", language: "python", owner: "analyst", updatedAt: "2026-06-23T17:20:00Z", cells: 14 },
  { id: "nb-02", name: "TPC-H Q1 deep dive", language: "sql", owner: "data-platform", updatedAt: "2026-06-20T10:02:00Z", cells: 6 },
];

const mockDashboards: Dashboard[] = [
  { id: "db-01", name: "Sales overview", owner: "sales-lead", updatedAt: "2026-06-22T12:00:00Z", tiles: 8 },
  { id: "db-02", name: "Cluster utilization", owner: "data-platform", updatedAt: "2026-06-24T07:00:00Z", tiles: 5 },
];

const mockJobs: Job[] = [
  { id: "j-01", name: "nightly-etl", status: "succeeded", schedule: "0 2 * * *", lastRun: "2026-06-24T02:00:00Z", owner: "ingest-team" },
  { id: "j-02", name: "feature-refresh", status: "running", schedule: "@hourly", lastRun: "2026-06-24T08:00:00Z", owner: "ml-eng" },
  { id: "j-03", name: "weekly-report", status: "scheduled", schedule: "0 6 * * 1", lastRun: "2026-06-17T06:00:00Z", owner: "analyst" },
];

/** Opened notebook documents keyed by id (ordered cells). */
const mockNotebookDocs: Record<string, NotebookDoc> = {
  "nb-01": {
    id: "nb-01",
    name: "Revenue exploration",
    cells: [
      { id: "cell-1", kind: "markdown", source: "# Revenue exploration\n\nMonthly revenue across the sales lake, then a quick Python sanity check." },
      { id: "cell-2", kind: "sql", source: "SELECT\n  date_trunc('month', o_orderdate) AS month,\n  SUM(l_extendedprice * (1 - l_discount)) AS revenue\nFROM main.sales.orders o\nJOIN main.sales.lineitem l ON l.l_orderkey = o.o_orderkey\nGROUP BY 1\nORDER BY 1;" },
      { id: "cell-3", kind: "python", source: "import pandas as pd\n\ndf = spark.sql('SELECT * FROM main.sales.monthly_revenue').toPandas()\nprint(df.describe())" },
    ],
  },
  "nb-02": {
    id: "nb-02",
    name: "TPC-H Q1 deep dive",
    cells: [
      { id: "cell-1", kind: "markdown", source: "## TPC-H Q1\n\nPricing summary report over `lineitem`." },
      { id: "cell-2", kind: "sql", source: "SELECT\n  l_returnflag,\n  l_linestatus,\n  SUM(l_quantity) AS sum_qty,\n  COUNT(*) AS count_order\nFROM main.sales.lineitem\nGROUP BY l_returnflag, l_linestatus\nORDER BY l_returnflag, l_linestatus;" },
    ],
  },
};

/** Opened dashboard documents keyed by id (their widgets). */
const mockDashboardDocs: Record<string, DashboardDoc> = {
  "db-01": {
    id: "db-01",
    name: "Sales overview",
    widgets: [
      { id: "w-1", title: "Revenue by month", chart: "line", query: "SELECT month, revenue FROM main.sales.monthly_revenue ORDER BY month" },
      { id: "w-2", title: "Orders by status", chart: "bar", query: "SELECT o_orderstatus, COUNT(*) FROM main.sales.orders GROUP BY 1" },
      { id: "w-3", title: "Top regions", chart: "table", query: "SELECT region, SUM(revenue) AS revenue FROM main.sales.by_region GROUP BY 1 ORDER BY 2 DESC" },
    ],
  },
  "db-02": {
    id: "db-02",
    name: "Cluster utilization",
    widgets: [
      { id: "w-1", title: "Active workers", chart: "line", query: "SELECT hour, active_workers FROM ops.cluster_metrics ORDER BY hour" },
      { id: "w-2", title: "Queries per cluster", chart: "bar", query: "SELECT cluster, COUNT(*) FROM ops.queries GROUP BY 1" },
    ],
  },
};

/** Opened job documents keyed by id (their task DAGs). */
const mockJobDocs: Record<string, JobDoc> = {
  "j-01": {
    id: "j-01",
    name: "nightly-etl",
    schedule: "0 2 * * *",
    tasks: [
      { id: "ingest", name: "ingest-raw", dependsOn: [] },
      { id: "clean", name: "clean-stage", dependsOn: ["ingest"] },
      { id: "dim", name: "build-dimensions", dependsOn: ["clean"] },
      { id: "fact", name: "build-facts", dependsOn: ["clean"] },
      { id: "publish", name: "publish-marts", dependsOn: ["dim", "fact"] },
    ],
  },
  "j-02": {
    id: "j-02",
    name: "feature-refresh",
    schedule: "@hourly",
    tasks: [
      { id: "extract", name: "extract-events", dependsOn: [] },
      { id: "features", name: "compute-features", dependsOn: ["extract"] },
      { id: "materialize", name: "materialize-store", dependsOn: ["features"] },
    ],
  },
  "j-03": {
    id: "j-03",
    name: "weekly-report",
    schedule: "0 6 * * 1",
    tasks: [
      { id: "rollup", name: "weekly-rollup", dependsOn: [] },
      { id: "render", name: "render-report", dependsOn: ["rollup"] },
      { id: "email", name: "email-stakeholders", dependsOn: ["render"] },
    ],
  },
};

/** Run history keyed by job id (most recent first). */
const mockJobRuns: Record<string, JobRun[]> = {
  "j-01": [
    {
      id: "run-1042",
      status: "SUCCESS",
      startedAt: "2026-06-24T02:00:00Z",
      durationMs: 742_000,
      taskRuns: [
        { taskId: "ingest", status: "SUCCESS", durationMs: 180_000 },
        { taskId: "clean", status: "SUCCESS", durationMs: 220_000 },
        { taskId: "dim", status: "SUCCESS", durationMs: 90_000 },
        { taskId: "fact", status: "SUCCESS", durationMs: 160_000 },
        { taskId: "publish", status: "SUCCESS", durationMs: 92_000 },
      ],
    },
    {
      id: "run-1041",
      status: "FAILED",
      startedAt: "2026-06-23T02:00:00Z",
      durationMs: 410_000,
      taskRuns: [
        { taskId: "ingest", status: "SUCCESS", durationMs: 175_000 },
        { taskId: "clean", status: "SUCCESS", durationMs: 210_000 },
        { taskId: "dim", status: "SUCCESS", durationMs: 25_000 },
        { taskId: "fact", status: "FAILED", durationMs: 0 },
        { taskId: "publish", status: "PENDING", durationMs: 0 },
      ],
    },
  ],
  "j-02": [
    {
      id: "run-9920",
      status: "RUNNING",
      startedAt: "2026-06-24T08:00:00Z",
      durationMs: 0,
      taskRuns: [
        { taskId: "extract", status: "SUCCESS", durationMs: 40_000 },
        { taskId: "features", status: "RUNNING", durationMs: 0 },
        { taskId: "materialize", status: "PENDING", durationMs: 0 },
      ],
    },
    {
      id: "run-9919",
      status: "SUCCESS",
      startedAt: "2026-06-24T07:00:00Z",
      durationMs: 138_000,
      taskRuns: [
        { taskId: "extract", status: "SUCCESS", durationMs: 38_000 },
        { taskId: "features", status: "SUCCESS", durationMs: 71_000 },
        { taskId: "materialize", status: "SUCCESS", durationMs: 29_000 },
      ],
    },
  ],
  "j-03": [
    {
      id: "run-5510",
      status: "SUCCESS",
      startedAt: "2026-06-17T06:00:00Z",
      durationMs: 96_000,
      taskRuns: [
        { taskId: "rollup", status: "SUCCESS", durationMs: 52_000 },
        { taskId: "render", status: "SUCCESS", durationMs: 31_000 },
        { taskId: "email", status: "SUCCESS", durationMs: 13_000 },
      ],
    },
  ],
};

const mockMe: Principal = {
  user: "vamsi",
  groups: ["data-platform", "admins"],
  authenticated: true,
};

const mockSecurables: Securable[] = [
  { fqn: "main", kind: "catalog", label: "main (catalog)" },
  { fqn: "main.sales", kind: "schema", label: "main.sales (schema)" },
  { fqn: "main.sales.orders", kind: "table", label: "main.sales.orders (table)" },
  { fqn: "main.sales.lineitem", kind: "table", label: "main.sales.lineitem (table)" },
  { fqn: "main.sales.monthly_revenue", kind: "view", label: "main.sales.monthly_revenue (view)" },
];

/** Grants keyed by securable fqn (Unity-Catalog-style governance model). */
const mockGrants: Record<string, Grant[]> = {
  main: [
    { id: "g-1", principal: "data-platform", principalType: "group", privilege: "USE CATALOG", effect: "allow" },
    { id: "g-2", principal: "analysts", principalType: "group", privilege: "USE CATALOG", effect: "allow" },
    { id: "g-3", principal: "admins", principalType: "group", privilege: "ALL PRIVILEGES", effect: "allow" },
  ],
  "main.sales": [
    { id: "g-4", principal: "analysts", principalType: "group", privilege: "USE SCHEMA", effect: "allow" },
    { id: "g-5", principal: "contractor@ext.io", principalType: "user", privilege: "BROWSE", effect: "deny" },
  ],
  "main.sales.orders": [
    { id: "g-6", principal: "analysts", principalType: "group", privilege: "SELECT", effect: "allow" },
    { id: "g-7", principal: "sales-lead", principalType: "user", privilege: "MODIFY", effect: "allow" },
  ],
  "main.sales.lineitem": [
    { id: "g-8", principal: "analysts", principalType: "group", privilege: "SELECT", effect: "allow" },
  ],
  "main.sales.monthly_revenue": [
    { id: "g-9", principal: "analysts", principalType: "group", privilege: "SELECT", effect: "allow" },
  ],
};

/** Deterministic-ish mock result set for the SQL editor grid. */
const mockResult: QueryResult = {
  columns: ["l_returnflag", "l_linestatus", "sum_qty", "sum_base_price", "avg_disc", "count_order"],
  rows: [
    ["A", "F", 37734107, "56586554400.73", "0.0500", 1478493],
    ["N", "F", 991417, "1487504710.38", "0.0497", 38854],
    ["N", "O", 74476040, "111701729697.74", "0.0500", 2920374],
    ["R", "F", 37719753, "56568041380.90", "0.0500", 1478870],
  ],
  rowCount: 4,
  durationMs: 1843,
};

let mockHistory: Query[] = [
  {
    id: "h-1",
    text: "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty\nFROM lineitem\nGROUP BY l_returnflag, l_linestatus",
    status: "finished",
    clusterId: "c-7f3a",
    user: "analyst",
    durationMs: 1843,
    startedAt: "2026-06-24T08:01:00Z",
  },
  {
    id: "h-2",
    text: "SELECT COUNT(*) FROM orders WHERE o_orderdate >= DATE '1998-01-01'",
    status: "finished",
    clusterId: "c-7f3a",
    user: "analyst",
    durationMs: 412,
    startedAt: "2026-06-24T07:55:00Z",
  },
  {
    id: "h-3",
    text: "SELECT * FROM monthly_revenue ORDER BY month DESC LIMIT 100",
    status: "failed",
    clusterId: "c-91b2",
    user: "analyst",
    durationMs: 0,
    startedAt: "2026-06-24T07:40:00Z",
  },
];

// ---------------------------------------------------------------------------
// API surface — each fn maps to a route in ROUTES.
// ---------------------------------------------------------------------------

export const api = {
  // Auth (LIVE) -----------------------------------------------------------
  /** POST /api/auth/login — store the token on success; throws on 401. */
  async login(username: string, password: string): Promise<LoginResult> {
    const res = await request<LoginResult>(
      "POST",
      "/api/auth/login",
      { username, password },
      { auth: false },
    );
    setToken(res.token);
    return res;
  },

  /** POST /api/auth/logout — clears the local token regardless of the result. */
  async logout(): Promise<void> {
    try {
      await request<void>("POST", "/api/auth/logout");
    } catch {
      // best-effort; we clear locally either way
    }
    clearToken();
  },

  /** GET /api/me (LIVE) — the signed-in principal; throws/401s if not authed. */
  async me(): Promise<Principal> {
    return request("GET", "/api/me");
  },

  // Clusters (LIVE) -------------------------------------------------------
  /** GET /api/clusters */
  async listClusters(): Promise<Cluster[]> {
    const raw = await request<GatewayCluster[]>("GET", "/api/clusters");
    return raw.map(fromGatewayCluster);
  },

  /** POST /api/clusters */
  async createCluster(input: CreateClusterInput): Promise<Cluster> {
    const raw = await request<GatewayCluster>("POST", "/api/clusters", {
      name: input.name,
      worker_min: input.minWorkers,
      worker_max: input.maxWorkers,
      worker_size: input.size,
    });
    return fromGatewayCluster(raw);
  },

  /** POST /api/clusters/:id/start */
  async startCluster(id: string): Promise<Cluster> {
    const raw = await request<GatewayCluster>("POST", `/api/clusters/${id}/start`);
    return fromGatewayCluster(raw);
  },

  /** POST /api/clusters/:id/stop */
  async stopCluster(id: string): Promise<Cluster> {
    const raw = await request<GatewayCluster>("POST", `/api/clusters/${id}/stop`);
    return fromGatewayCluster(raw);
  },

  /** DELETE /api/clusters/:id */
  async deleteCluster(id: string): Promise<void> {
    await request<void>("DELETE", `/api/clusters/${id}`);
  },

  /** GET /api/clusters/:id/events — lifecycle events (oldest first). */
  async clusterEvents(id: string): Promise<ClusterEvent[]> {
    return request("GET", `/api/clusters/${id}/events`);
  },

  // Admin: users / groups / grants (LIVE) ---------------------------------
  /** GET /api/admin/users */
  async listUsers(): Promise<AdminUser[]> {
    return request("GET", "/api/admin/users");
  },

  /** POST /api/admin/users */
  async createUser(input: CreateUserInput): Promise<void> {
    await request<void>("POST", "/api/admin/users", input);
  },

  /** GET /api/admin/groups */
  async listGroups(): Promise<AdminGroup[]> {
    return request("GET", "/api/admin/groups");
  },

  /** POST /api/admin/groups */
  async createGroup(input: CreateGroupInput): Promise<void> {
    await request<void>("POST", "/api/admin/groups", input);
  },

  /** GET /api/grants */
  async listGrants(): Promise<GrantDto[]> {
    return request("GET", "/api/grants");
  },

  /** POST /api/grants */
  async createGrant(grant: GrantDto): Promise<void> {
    await request<void>("POST", "/api/grants", grant);
  },

  /** DELETE /api/grants — body is the grant to revoke. */
  async revokeGrant(grant: GrantDto): Promise<void> {
    await request<void>("DELETE", "/api/grants", grant);
  },

  /**
   * POST /api/sql (LIVE) — run a query on the engine. Returns the raw gateway
   * shape ({columns, rows, row_count, error}); the caller renders `error` in a
   * banner when present.
   */
  async runSql(sql: string, clusterId?: string): Promise<SqlResponse> {
    return request("POST", "/api/sql", { sql, cluster_id: clusterId });
  },

  // Catalog ---------------------------------------------------------------
  /**
   * GET /api/catalog (LIVE) — the real, queryable catalog tree
   * (namespace → schema → table → columns). Goes through `request()`, which
   * attaches the bearer token.
   */
  async getCatalog(): Promise<CatalogNamespace[]> {
    return request("GET", "/api/catalog");
  },

  /** GET /api/catalog */
  async listCatalog(): Promise<CatalogObject[]> {
    if (!USE_MOCK) return request("GET", "/api/catalog");
    await delay();
    return [...mockCatalog];
  },

  /** GET /api/catalog/:fqn — rich detail for one table/view. */
  async tableDetail(fqn: string): Promise<TableDetail> {
    if (!USE_MOCK) return request("GET", `/api/catalog/${encodeURIComponent(fqn)}`);
    await delay();
    const detail = mockTableDetails[fqn];
    if (!detail) throw new Error(`no detail for ${fqn}`);
    return detail;
  },

  /** POST /api/catalog/connections — attach an external catalog (HMS/Glue/Unity/local). */
  async attachCatalog(input: AttachCatalogInput): Promise<{ ok: true; name: string }> {
    if (!USE_MOCK) return request("POST", "/api/catalog/connections", input);
    await delay(400);
    return { ok: true, name: input.name };
  },

  // SQL / queries ---------------------------------------------------------
  /** GET /api/queries */
  async listQueries(): Promise<Query[]> {
    if (!USE_MOCK) return request("GET", "/api/queries");
    await delay();
    return [...mockQueries];
  },

  /** GET /api/queries/history — past runs for the SQL editor. */
  async queryHistory(): Promise<Query[]> {
    if (!USE_MOCK) return request("GET", "/api/queries/history");
    await delay();
    return [...mockHistory];
  },

  /**
   * POST /api/sql — run a query on a cluster.
   * Live: streams Arrow IPC over the /api/sql WebSocket into the grid. The mock
   * returns a fixed result set and records the run into history.
   */
  async runQuery(sql: string, clusterId: string): Promise<QueryResult> {
    if (!USE_MOCK) return request("POST", "/api/sql", { sql, clusterId });
    await delay(600);
    mockHistory = [
      {
        id: `h-${Math.random().toString(16).slice(2, 6)}`,
        text: sql,
        status: "finished",
        clusterId,
        user: mockMe.user,
        durationMs: mockResult.durationMs,
        startedAt: new Date().toISOString(),
      },
      ...mockHistory,
    ];
    return mockResult;
  },

  /**
   * POST /api/ai/generate — NL→SQL.
   * Live: the model returns schema-constrained JSON ({ sql }) grounded on the
   * governed catalog. The mock echoes a plausible query for the prompt.
   */
  async aiGenerateSql(prompt: string): Promise<AiSqlResult> {
    if (!USE_MOCK) return request("POST", "/api/ai/generate", { prompt });
    await delay(700);
    return { sql: mockSqlForPrompt(prompt) };
  },

  // Notebooks -------------------------------------------------------------
  /** GET /api/notebooks */
  async listNotebooks(): Promise<Notebook[]> {
    if (!USE_MOCK) return request("GET", "/api/notebooks");
    await delay();
    return [...mockNotebooks];
  },

  /** GET /api/notebooks/:id — open a notebook with its ordered cells. */
  async getNotebook(id: string): Promise<NotebookDoc> {
    if (!USE_MOCK) return request("GET", `/api/notebooks/${id}`);
    await delay();
    const doc = mockNotebookDocs[id];
    if (!doc) throw new Error(`no notebook ${id}`);
    // Return a deep-ish copy so the editor mutates its own state, not the mock.
    return { ...doc, cells: doc.cells.map((c) => ({ ...c })) };
  },

  /**
   * PUT /api/notebooks/:id — autosave the whole notebook (cells + name).
   * The mock just records it; live this persists the document.
   */
  async saveNotebook(doc: NotebookDoc): Promise<{ ok: true; savedAt: string }> {
    if (!USE_MOCK) return request("PUT", `/api/notebooks/${doc.id}`, doc);
    await delay(150);
    mockNotebookDocs[doc.id] = { ...doc, cells: doc.cells.map((c) => ({ ...c })) };
    return { ok: true, savedAt: new Date().toISOString() };
  },

  /**
   * POST /api/notebooks/:id/run — run one cell.
   * Live: streams output over the /api/notebooks/:id/run WebSocket (per-cell,
   * incremental). The mock synthesizes a kind-appropriate result.
   */
  async runCell(cell: NotebookCell): Promise<CellResult> {
    if (!USE_MOCK)
      return request("POST", `/api/notebooks/${cell.id}/run`, { cell });
    await delay(500);
    if (cell.kind === "sql") {
      return { kind: "sql", table: mockResult, durationMs: mockResult.durationMs };
    }
    if (cell.kind === "markdown") {
      return { kind: "markdown", text: cell.source, durationMs: 0 };
    }
    // python — echo a plausible stdout for the snippet.
    return {
      kind: "python",
      text: mockPythonOutput(cell.source),
      durationMs: 318,
    };
  },

  /**
   * POST /api/ai/notebook — NL→notebook.
   * Live: the model returns a schema-grounded notebook skeleton. The mock
   * returns a small, prompt-flavored set of cells.
   */
  async aiGenerateNotebook(prompt: string): Promise<AiNotebook> {
    if (!USE_MOCK) return request("POST", "/api/ai/notebook", { prompt });
    await delay(800);
    return { cells: mockNotebookForPrompt(prompt) };
  },

  // Dashboards ------------------------------------------------------------
  /** GET /api/dashboards */
  async listDashboards(): Promise<Dashboard[]> {
    if (!USE_MOCK) return request("GET", "/api/dashboards");
    await delay();
    return [...mockDashboards];
  },

  /** GET /api/dashboards/:id — open a dashboard with its widgets. */
  async getDashboard(id: string): Promise<DashboardDoc> {
    if (!USE_MOCK) return request("GET", `/api/dashboards/${id}`);
    await delay();
    const doc = mockDashboardDocs[id];
    if (!doc) throw new Error(`no dashboard ${id}`);
    return { ...doc, widgets: doc.widgets.map((w) => ({ ...w })) };
  },

  /** GET /api/dashboards/:dashboardId/widgets/:id/data — materialized widget data. */
  async widgetData(widgetId: string): Promise<WidgetData> {
    if (!USE_MOCK) return request("GET", `/api/widgets/${widgetId}/data`);
    await delay(200);
    return mockWidgetData(widgetId);
  },

  /**
   * POST /api/dashboards/:id/widgets — append a widget.
   * The mock assigns an id and returns it; data is fetched separately.
   */
  async addWidget(dashboardId: string, input: AddWidgetInput): Promise<Widget> {
    if (!USE_MOCK)
      return request("POST", `/api/dashboards/${dashboardId}/widgets`, input);
    await delay(250);
    const widget: Widget = {
      id: `w-${Math.random().toString(16).slice(2, 6)}`,
      title: input.title,
      chart: input.chart,
      query: input.query,
    };
    const doc = mockDashboardDocs[dashboardId];
    if (doc) doc.widgets = [...doc.widgets, widget];
    return widget;
  },

  // Jobs ------------------------------------------------------------------
  /** GET /api/jobs */
  async listJobs(): Promise<Job[]> {
    if (!USE_MOCK) return request("GET", "/api/jobs");
    await delay();
    return [...mockJobs];
  },

  /** GET /api/jobs/:id — open a job with its task DAG. */
  async getJob(id: string): Promise<JobDoc> {
    if (!USE_MOCK) return request("GET", `/api/jobs/${id}`);
    await delay();
    const doc = mockJobDocs[id];
    if (!doc) throw new Error(`no job ${id}`);
    return { ...doc, tasks: doc.tasks.map((t) => ({ ...t })) };
  },

  /** GET /api/jobs/:id/runs — run history for a job. */
  async jobRuns(id: string): Promise<JobRun[]> {
    if (!USE_MOCK) return request("GET", `/api/jobs/${id}/runs`);
    await delay();
    return (mockJobRuns[id] ?? []).map((r) => ({ ...r, taskRuns: r.taskRuns.map((t) => ({ ...t })) }));
  },

  /**
   * POST /api/jobs/:id/run — trigger a run now.
   * The mock fabricates a fresh RUNNING run (all tasks pending/running) and
   * prepends it to history; live this enqueues the DAG on a cluster.
   */
  async runJob(id: string): Promise<JobRun> {
    if (!USE_MOCK) return request("POST", `/api/jobs/${id}/run`);
    await delay(400);
    const doc = mockJobDocs[id];
    const tasks = doc?.tasks ?? [];
    const run: JobRun = {
      id: `run-${Math.random().toString(16).slice(2, 6)}`,
      status: "RUNNING",
      startedAt: new Date().toISOString(),
      durationMs: 0,
      taskRuns: tasks.map((t, i) => ({
        taskId: t.id,
        status: i === 0 ? "RUNNING" : "PENDING",
        durationMs: 0,
      })),
    };
    mockJobRuns[id] = [run, ...(mockJobRuns[id] ?? [])];
    return run;
  },

  // Permissions / governance ----------------------------------------------
  /** GET /api/securables — objects that grants can be attached to. */
  async listSecurables(): Promise<Securable[]> {
    if (!USE_MOCK) return request("GET", "/api/securables");
    await delay();
    return [...mockSecurables];
  },

  /** GET /api/grants?securable=:fqn */
  async grants(securable: string): Promise<Grant[]> {
    if (!USE_MOCK) return request("GET", `/api/grants?securable=${encodeURIComponent(securable)}`);
    await delay();
    return [...(mockGrants[securable] ?? [])];
  },

  /** POST /api/grants — GRANT a privilege to a principal. */
  async grant(input: GrantInput): Promise<Grant> {
    if (!USE_MOCK) return request("POST", "/api/grants", input);
    await delay(300);
    const grant: Grant = {
      id: `g-${Math.random().toString(16).slice(2, 6)}`,
      principal: input.principal,
      principalType: input.principalType,
      privilege: input.privilege,
      effect: input.effect,
    };
    mockGrants[input.securable] = [...(mockGrants[input.securable] ?? []), grant];
    return grant;
  },

  /** DELETE /api/grants/:id?securable=:fqn — REVOKE a grant. */
  async revoke(securable: string, grantId: string): Promise<void> {
    if (!USE_MOCK)
      return request("DELETE", `/api/grants/${grantId}?securable=${encodeURIComponent(securable)}`);
    await delay(300);
    mockGrants[securable] = (mockGrants[securable] ?? []).filter((g) => g.id !== grantId);
  },
};

/** Tiny heuristic so the mock AI assist returns something prompt-relevant. */
function mockSqlForPrompt(prompt: string): string {
  const p = prompt.toLowerCase();
  if (p.includes("revenue") || p.includes("sales")) {
    return `-- ${prompt}\nSELECT\n  date_trunc('month', o_orderdate) AS month,\n  SUM(l_extendedprice * (1 - l_discount)) AS revenue\nFROM main.sales.orders o\nJOIN main.sales.lineitem l ON l.l_orderkey = o.o_orderkey\nGROUP BY 1\nORDER BY 1;`;
  }
  if (p.includes("count") || p.includes("how many")) {
    return `-- ${prompt}\nSELECT COUNT(*) AS n\nFROM main.sales.orders;`;
  }
  if (p.includes("top") || p.includes("customer")) {
    return `-- ${prompt}\nSELECT o_custkey, SUM(o_totalprice) AS spend\nFROM main.sales.orders\nGROUP BY o_custkey\nORDER BY spend DESC\nLIMIT 10;`;
  }
  return `-- ${prompt}\nSELECT *\nFROM main.sales.orders\nLIMIT 100;`;
}

/** Plausible Python stdout for a notebook python cell (mock only). */
function mockPythonOutput(source: string): string {
  if (/print\s*\(/.test(source) && /describe/.test(source)) {
    return [
      "              revenue       orders",
      "count   1.200000e+01    12.000000",
      "mean    9.418e+09       2.4e+06",
      "std     1.204e+09       3.1e+05",
      "min     7.512e+09       1.9e+06",
      "max     1.117e+11       2.9e+06",
    ].join("\n");
  }
  if (/print\s*\(/.test(source)) {
    return "ok";
  }
  return "[1] executed in 0.31s — 0 rows materialized";
}

/** Prompt-flavored notebook skeleton for the mock AI generator. */
function mockNotebookForPrompt(prompt: string): { kind: CellKind; source: string }[] {
  const p = prompt.toLowerCase();
  const title = prompt.trim() || "Generated notebook";
  if (p.includes("revenue") || p.includes("sales")) {
    return [
      { kind: "markdown", source: `# ${title}\n\nGenerated analysis grounded on \`main.sales\`.` },
      { kind: "sql", source: "SELECT\n  date_trunc('month', o_orderdate) AS month,\n  SUM(l_extendedprice * (1 - l_discount)) AS revenue\nFROM main.sales.orders o\nJOIN main.sales.lineitem l ON l.l_orderkey = o.o_orderkey\nGROUP BY 1\nORDER BY 1;" },
      { kind: "python", source: "df = _.toPandas()\ndf.plot(x='month', y='revenue')" },
    ];
  }
  return [
    { kind: "markdown", source: `# ${title}` },
    { kind: "sql", source: "SELECT *\nFROM main.sales.orders\nLIMIT 100;" },
    { kind: "python", source: "print(_.count())" },
  ];
}

/** Deterministic mock data for a dashboard widget, by id hash. */
function mockWidgetData(widgetId: string): WidgetData {
  // Stable pseudo-random from the id so re-fetches look consistent.
  let seed = 0;
  for (const ch of widgetId) seed = (seed * 31 + ch.charCodeAt(0)) >>> 0;
  const rand = () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed / 0xffffffff;
  };
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug"];
  const rows: (string | number)[][] = months.map((m) => [
    m,
    Math.round(40 + rand() * 60),
  ]);
  return { columns: ["label", "value"], rows };
}

// ---------------------------------------------------------------------------
// Gateway ↔ UI mapping for the live cluster endpoints.
// ---------------------------------------------------------------------------

/** The cluster shape the gateway returns (`{id,name,state,worker_*}`). */
interface GatewayCluster {
  id: string;
  name: string;
  state: string;
  worker_min: number;
  worker_max: number;
  worker_size: string;
  connect_endpoint?: string | null;
}

const KNOWN_SIZES: ClusterSize[] = ["small", "medium", "large", "xlarge"];

/** Map the gateway's lifecycle string onto the UI's `ClusterState`. */
function toClusterState(state: string): ClusterState {
  const s = state.toLowerCase();
  if (s === "running") return "running";
  if (s === "stopped" || s === "terminated") return "stopped";
  if (s === "terminating") return "terminating";
  if (s === "error" || s === "failed") return "error";
  return "pending"; // pending / provisioning / unknown
}

/** Widen the gateway cluster into the richer UI `Cluster` (defaults for fields the API doesn't carry). */
function fromGatewayCluster(c: GatewayCluster): Cluster {
  const size = (KNOWN_SIZES as string[]).includes(c.worker_size)
    ? (c.worker_size as ClusterSize)
    : "small";
  const state = toClusterState(c.state);
  return {
    id: c.id,
    name: c.name,
    state,
    size,
    minWorkers: c.worker_min,
    maxWorkers: c.worker_max,
    activeWorkers: state === "running" ? c.worker_min : 0,
    runtime: "weft (Spark Connect)",
    creator: "—",
    createdAt: new Date().toISOString(),
    connect_endpoint: c.connect_endpoint ?? null,
  };
}

/** `POST /api/sql` response — the raw gateway shape. */
export interface SqlResponse {
  columns: string[];
  rows: string[][];
  row_count: number;
  error: string | null;
}
