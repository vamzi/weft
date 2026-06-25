//! The axum HTTP server: serves the [`crate::ROUTES`] surface **and** the web SPA on one origin.
//!
//! Auth is local username/password (bcrypt-verified) issuing a session **JWT** — the break-glass
//! admin path from the plan; OIDC/SAML/SCIM layer on top of the same session-JWT seam later.
//! `/api/*` (except login) is gated by a Bearer-token middleware; the SPA is served same-origin so
//! the browser's `fetch('/api/...')` carries the token without CORS. Cluster lifecycle runs on an
//! in-memory store today (persistence via `weft-meta` is the next layer).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::{from_fn_with_state, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Extension, Json, Router};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tower_http::services::ServeDir;
use weft_clustermgr::Phase;
use weft_govern::{Effect, Grant, Principal, Privilege, Securable, SecurableType};
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

use crate::cloud;
use crate::cluster_client;
use weft_catalog_glue::GlueCatalog;

// ─────────────────────────────────────────── Auth ───────────────────────────────────────────────

/// Session JWT claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject — the username.
    pub sub: String,
    /// The principal's groups (drives governance + UI).
    pub groups: Vec<String>,
    /// Expiry (unix seconds).
    pub exp: usize,
}

/// A local user record.
#[derive(Clone)]
struct UserRecord {
    password_hash: String,
    groups: Vec<String>,
}

/// `POST /api/auth/login` body.
#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    /// Username.
    pub username: String,
    /// Password.
    pub password: String,
}

/// `POST /api/auth/login` response.
#[derive(Debug, Serialize)]
pub struct LoginResponse {
    /// The bearer session token.
    pub token: String,
    /// The signed-in username.
    pub user: String,
    /// Resolved groups.
    pub groups: Vec<String>,
}

const TOKEN_TTL_SECS: usize = 24 * 60 * 60;

// ───────────────────────────────────────── Clusters ─────────────────────────────────────────────

/// A cluster as the API exposes it (mirrors the `clusters` table + the operator's status).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    /// Stable id.
    pub id: String,
    /// Display name.
    pub name: String,
    /// Lifecycle state (`PENDING`/`PROVISIONING`/`RUNNING`/…), from [`Phase::as_str`].
    pub state: String,
    /// Autoscale floor.
    pub worker_min: u32,
    /// Autoscale ceiling.
    pub worker_max: u32,
    /// Pod size class.
    pub worker_size: String,
    /// The cluster's real Spark Connect endpoint once `RUNNING` (e.g. `sc://host:51001`).
    #[serde(default)]
    pub connect_endpoint: Option<String>,
    /// Backing EC2 instance id (set for EC2-backed clusters) — persisted so the cluster can be
    /// re-adopted (deleted / auto-terminated) after a control-plane restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
}

/// One lifecycle event for a cluster (the reconcile trace the UI shows).
#[derive(Debug, Clone, Serialize)]
pub struct ClusterEvent {
    /// Unix seconds.
    pub at: u64,
    /// Human-readable message.
    pub message: String,
}

/// The real compute backing a `RUNNING` cluster: either a dedicated **EC2 instance** (production
/// path) or a local OS process (fallback for local/dev when no EC2 config is set).
enum ClusterRuntime {
    /// A `weft spark server` process on the control-plane node.
    Process(tokio::process::Child),
    /// A dedicated EC2 instance running `weft spark server`.
    Ec2 { instance_id: String },
}

/// Config for launching a real EC2 instance per cluster. Present (from env) → create-cluster spins
/// up an actual instance; absent → fall back to a local process.
#[derive(Clone)]
struct Ec2ClusterConfig {
    /// AMI for cluster instances.
    ami: String,
    /// Security group (must allow the Spark Connect port).
    sg: String,
    /// Optional subnet.
    subnet: Option<String>,
    /// Optional SSH key name.
    key: Option<String>,
    /// Public URL to download the `weft` binary onto the cluster instance.
    weft_url: String,
    /// AWS region.
    region: String,
    /// Path to the AWS CLI.
    aws_bin: String,
    /// Launch clusters in a private subnet with no public IP (reach them by private IP from the
    /// in-VPC gateway). Requires a route to S3 (gateway endpoint) + Glue for catalogs.
    private: bool,
    /// IAM instance profile to attach to cluster instances (e.g. so they can read Glue/S3).
    instance_profile: Option<String>,
}

impl Ec2ClusterConfig {
    /// Build from env; returns `None` (→ local-process clusters) unless the required vars are set:
    /// `WEFT_CLUSTER_AMI`, `WEFT_CLUSTER_SG`, `WEFT_CLUSTER_WEFT_URL`.
    fn from_env() -> Option<Self> {
        let ami = std::env::var("WEFT_CLUSTER_AMI").ok()?;
        let sg = std::env::var("WEFT_CLUSTER_SG").ok()?;
        let weft_url = std::env::var("WEFT_CLUSTER_WEFT_URL").ok()?;
        Some(Self {
            ami,
            sg,
            subnet: std::env::var("WEFT_CLUSTER_SUBNET").ok(),
            key: std::env::var("WEFT_CLUSTER_KEY").ok(),
            weft_url,
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string()),
            aws_bin: std::env::var("WEFT_AWS_BIN").unwrap_or_else(|_| "aws".to_string()),
            private: std::env::var("WEFT_CLUSTER_PRIVATE")
                .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
                .unwrap_or(false),
            instance_profile: std::env::var("WEFT_CLUSTER_INSTANCE_PROFILE").ok(),
        })
    }
}

/// Map a cluster's `worker_size` to an EC2 instance type.
fn instance_type_for(size: &str) -> &'static str {
    match size {
        "small" => "t3.medium",
        "medium" => "t3.large",
        "large" => "c6a.xlarge",
        "xlarge" => "c6a.2xlarge",
        _ => "t3.medium",
    }
}

/// Body for `POST /api/clusters`.
#[derive(Debug, Deserialize)]
pub struct CreateCluster {
    /// Display name.
    pub name: String,
    /// Autoscale floor (default 1).
    #[serde(default = "one")]
    pub worker_min: u32,
    /// Autoscale ceiling (default 1).
    #[serde(default = "one")]
    pub worker_max: u32,
    /// Pod size class (default `small`).
    #[serde(default = "small")]
    pub worker_size: String,
}

fn one() -> u32 {
    1
}
fn small() -> String {
    "small".into()
}

// ─────────────────────────────────────────── State ──────────────────────────────────────────────

/// The shared application state. In-memory today; a `weft-meta` repository handle later. The
/// embedded [`Engine`] runs SQL in-process (the demo stand-in for routing to a cluster's Spark
/// Connect endpoint — same engine, real results).
#[derive(Clone)]
pub struct AppState {
    clusters: Arc<Mutex<HashMap<String, Cluster>>>,
    runtimes: Arc<tokio::sync::Mutex<HashMap<String, ClusterRuntime>>>,
    events: Arc<Mutex<HashMap<String, Vec<ClusterEvent>>>>,
    next_id: Arc<Mutex<u64>>,
    next_port: Arc<Mutex<u16>>,
    users: Arc<Mutex<HashMap<String, UserRecord>>>,
    groups: Arc<Mutex<HashMap<String, Vec<String>>>>,
    grants: Arc<Mutex<Vec<Grant>>>,
    connections: Arc<Mutex<Vec<ConnectionInfo>>>,
    /// Saved notebooks (the Workspace).
    notebooks: Arc<Mutex<Vec<NotebookDoc>>>,
    /// Saved SQL queries (the Workspace).
    queries: Arc<Mutex<Vec<SavedQuery>>>,
    /// DynamoDB table holding the `clusters` + `connections` collection blobs (durable, survives a
    /// control-plane restart/replacement). Workspace docs go to S3 instead.
    ddb_table: Arc<String>,
    /// S3 URIs for the workspace blobs (notebooks, saved queries).
    nb_uri: Arc<String>,
    q_uri: Arc<String>,
    engine: Arc<Engine>,
    jwt_secret: Arc<Vec<u8>>,
    web_dir: Arc<PathBuf>,
    weft_bin: Arc<String>,
    public_host: Arc<String>,
    ec2: Arc<Option<Ec2ClusterConfig>>,
    /// Last "use" time per cluster (unix secs) — drives idle auto-termination.
    last_activity: Arc<Mutex<HashMap<String, u64>>>,
    /// Seconds of inactivity before a running cluster is auto-terminated (0 = never).
    idle_secs: u64,
}

impl AppState {
    /// Build state seeding a single local admin (`username`/`password`) and serving the SPA from
    /// `web_dir`. The JWT secret is provided by the caller (env in production).
    pub fn new(username: &str, password: &str, jwt_secret: Vec<u8>, web_dir: PathBuf) -> Self {
        Self::with_runtime(
            username,
            password,
            jwt_secret,
            web_dir,
            String::new(),
            "127.0.0.1".into(),
        )
    }

    /// Build state with cluster-runtime config: `weft_bin` (the `weft` binary spawned per cluster)
    /// and `public_host` (the address advertised in a cluster's Spark Connect endpoint).
    pub fn with_runtime(
        username: &str,
        password: &str,
        jwt_secret: Vec<u8>,
        web_dir: PathBuf,
        weft_bin: String,
        public_host: String,
    ) -> Self {
        let mut users = HashMap::new();
        let hash = bcrypt::hash(password, bcrypt::DEFAULT_COST).expect("bcrypt hash");
        users.insert(
            username.to_string(),
            UserRecord {
                password_hash: hash,
                groups: vec!["admins".to_string()],
            },
        );
        let mut groups = HashMap::new();
        groups.insert("admins".to_string(), vec![username.to_string()]);
        let engine = Arc::new(Engine::new());
        seed_sample_data(&engine);
        let ddb_table =
            std::env::var("WEFT_DDB_TABLE").unwrap_or_else(|_| "weft-control-plane".into());
        // Workspace S3 prefix (e.g. s3://bucket/control-plane); notebooks/queries are blobs under it.
        let ws_prefix = std::env::var("WEFT_WORKSPACE_S3")
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();
        let nb_uri = format!("{ws_prefix}/notebooks.json");
        let q_uri = format!("{ws_prefix}/queries.json");
        let st = Self {
            clusters: Arc::new(Mutex::new(HashMap::new())),
            runtimes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            events: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
            next_port: Arc::new(Mutex::new(51000)),
            users: Arc::new(Mutex::new(users)),
            groups: Arc::new(Mutex::new(groups)),
            grants: Arc::new(Mutex::new(Vec::new())),
            connections: Arc::new(Mutex::new(Vec::new())),
            notebooks: Arc::new(Mutex::new(Vec::new())),
            queries: Arc::new(Mutex::new(Vec::new())),
            ddb_table: Arc::new(ddb_table),
            nb_uri: Arc::new(nb_uri),
            q_uri: Arc::new(q_uri),
            engine,
            jwt_secret: Arc::new(jwt_secret),
            web_dir: Arc::new(web_dir),
            weft_bin: Arc::new(weft_bin),
            public_host: Arc::new(public_host),
            ec2: Arc::new(Ec2ClusterConfig::from_env()),
            last_activity: Arc::new(Mutex::new(HashMap::new())),
            idle_secs: std::env::var("WEFT_CLUSTER_IDLE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1800),
        };
        st
    }

    /// Allocate a unique id with the given prefix (e.g. `nb-7`, `q-3`).
    fn new_oid(&self, prefix: &str) -> String {
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        format!("{prefix}-{n}")
    }

    // Durable persistence (best-effort, off the request path). Workspace → S3; clusters +
    // connections → DynamoDB. Each collection is a single JSON blob; loads happen at startup.

    fn save_notebooks(&self) {
        let body = serde_json::to_string(&*self.notebooks.lock().unwrap()).unwrap_or_default();
        tokio::spawn(cloud::s3_put((*self.nb_uri).clone(), body));
    }

    fn save_queries(&self) {
        let body = serde_json::to_string(&*self.queries.lock().unwrap()).unwrap_or_default();
        tokio::spawn(cloud::s3_put((*self.q_uri).clone(), body));
    }

    fn save_connections(&self) {
        let body = serde_json::to_string(&*self.connections.lock().unwrap()).unwrap_or_default();
        tokio::spawn(cloud::ddb_put(
            (*self.ddb_table).clone(),
            "connections".into(),
            body,
        ));
    }

    fn save_clusters(&self) {
        let snapshot: Vec<Cluster> = self.clusters.lock().unwrap().values().cloned().collect();
        let body = serde_json::to_string(&snapshot).unwrap_or_default();
        tokio::spawn(cloud::ddb_put(
            (*self.ddb_table).clone(),
            "clusters".into(),
            body,
        ));
    }

    /// Load all durable state at startup (called from `serve`, async): connections (re-register
    /// their catalogs), workspace docs, and clusters (re-adopt EC2 instances; mark dead local ones).
    async fn load_from_cloud(&self) {
        // Connections (DynamoDB) → re-register each provider into the engine.
        if let Some(body) = cloud::ddb_get(&self.ddb_table, "connections").await {
            if let Ok(saved) = serde_json::from_str::<Vec<ConnectionInfo>>(&body) {
                for c in saved {
                    match build_connection_provider(&c.kind, &c.name, &c.options) {
                        Ok(p) => {
                            self.engine.register_catalog(&c.name, p);
                            self.connections.lock().unwrap().push(c);
                        }
                        Err(e) => eprintln!("warn: skipping connection `{}`: {e}", c.name),
                    }
                }
            }
        }
        // Workspace (S3).
        if let Some(body) = cloud::s3_get(&self.nb_uri).await {
            if let Ok(v) = serde_json::from_str::<Vec<NotebookDoc>>(&body) {
                *self.notebooks.lock().unwrap() = v;
            }
        }
        if let Some(body) = cloud::s3_get(&self.q_uri).await {
            if let Ok(v) = serde_json::from_str::<Vec<SavedQuery>>(&body) {
                *self.queries.lock().unwrap() = v;
            }
        }
        // Clusters (DynamoDB) → re-adopt so the idle reaper + delete can manage them again.
        if let Some(body) = cloud::ddb_get(&self.ddb_table, "clusters").await {
            if let Ok(saved) = serde_json::from_str::<Vec<Cluster>>(&body) {
                self.adopt_clusters(saved).await;
            }
        }
    }

    /// Re-adopt persisted clusters after a restart. EC2-backed clusters get their runtime handle
    /// rebuilt from the stored `instance_id` (so delete/auto-terminate work again) and a fresh idle
    /// window; local-process clusters can't survive a restart, so they're marked `STOPPED`.
    async fn adopt_clusters(&self, saved: Vec<Cluster>) {
        for mut c in saved {
            match c.instance_id.clone() {
                Some(instance_id) => {
                    self.runtimes
                        .lock()
                        .await
                        .insert(c.id.clone(), ClusterRuntime::Ec2 { instance_id });
                    self.touch(&c.id); // fresh idle window so the reaper doesn't kill it instantly
                }
                None => {
                    // A local-process cluster can't survive a control-plane restart.
                    if c.state == Phase::Running.as_str() || c.state == Phase::Provisioning.as_str()
                    {
                        c.state = Phase::Terminated.as_str().to_string();
                        c.connect_endpoint = None;
                    }
                }
            }
            self.clusters.lock().unwrap().insert(c.id.clone(), c);
        }
    }

    /// Mark a cluster as just-used (resets its idle timer).
    fn touch(&self, id: &str) {
        self.last_activity
            .lock()
            .unwrap()
            .insert(id.to_string(), now_secs() as u64);
    }

    fn add_event(&self, id: &str, message: impl Into<String>) {
        let at = now_secs() as u64;
        self.events
            .lock()
            .unwrap()
            .entry(id.to_string())
            .or_default()
            .push(ClusterEvent {
                at,
                message: message.into(),
            });
    }

    fn set_state(&self, id: &str, state: Phase, endpoint: Option<String>) {
        let changed = {
            let mut clusters = self.clusters.lock().unwrap();
            if let Some(c) = clusters.get_mut(id) {
                c.state = state.as_str().to_string();
                if endpoint.is_some() {
                    c.connect_endpoint = endpoint;
                }
                true
            } else {
                false
            }
        };
        if changed {
            self.save_clusters();
        }
    }

    /// Record the EC2 instance backing a cluster (so it can be re-adopted after a restart).
    fn set_cluster_instance(&self, id: &str, instance_id: &str) {
        if let Some(c) = self.clusters.lock().unwrap().get_mut(id) {
            c.instance_id = Some(instance_id.to_string());
        }
        self.save_clusters();
    }

    fn alloc_port(&self) -> u16 {
        let mut p = self.next_port.lock().unwrap();
        let port = *p;
        *p += 1;
        port
    }

    fn new_id(&self) -> String {
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        format!("cluster-{n}")
    }

    fn issue_token(&self, user: &str, groups: &[String]) -> Option<String> {
        let exp = now_secs() + TOKEN_TTL_SECS;
        let claims = Claims {
            sub: user.to_string(),
            groups: groups.to_vec(),
            exp,
        };
        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(&self.jwt_secret),
        )
        .ok()
    }

    fn verify_token(&self, token: &str) -> Option<Claims> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(&self.jwt_secret),
            &Validation::default(),
        )
        .ok()
        .map(|d| d.claims)
    }
}

fn now_secs() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as usize
}

/// Current time as an ISO-8601 UTC string (e.g. `2026-06-25T14:40:00Z`) — what the web renders for
/// `updatedAt`. Computed from unix seconds (civil-from-days) so we avoid a date-library dependency.
fn now_iso() -> String {
    let secs = now_secs() as i64;
    let (days, rem) = (secs.div_euclid(86400), secs.rem_euclid(86400));
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Howard Hinnant's civil_from_days.
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z")
}

// ─────────────────────────────────────────── Router ─────────────────────────────────────────────

/// Build the gateway router: public auth + health, Bearer-gated `/api/*`, and the SPA fallback.
pub fn app(state: AppState) -> Router {
    let protected = Router::new()
        .route("/api/me", get(me))
        .route("/api/clusters", get(list_clusters).post(create_cluster))
        .route("/api/clusters/:id", get(get_cluster).delete(delete_cluster))
        .route("/api/clusters/:id/start", post(start_cluster))
        .route("/api/clusters/:id/stop", post(stop_cluster))
        .route("/api/clusters/:id/events", get(list_cluster_events))
        .route("/api/sql", post(run_sql))
        .route("/api/catalog", get(get_catalog))
        .route(
            "/api/connections",
            get(list_connections).post(create_connection),
        )
        .route("/api/connections/:name", delete(delete_connection))
        // Workspace: notebooks + saved SQL queries
        .route("/api/notebooks", get(list_notebooks).post(create_notebook))
        .route(
            "/api/notebooks/:id",
            get(get_notebook).put(save_notebook).delete(delete_notebook),
        )
        .route("/api/queries", get(list_queries).post(create_query))
        .route(
            "/api/queries/:id",
            get(get_query).put(save_query).delete(delete_query),
        )
        .route("/api/admin/users", get(list_users).post(create_user))
        .route("/api/admin/groups", get(list_groups).post(create_group))
        .route(
            "/api/grants",
            get(list_grants).post(create_grant).delete(revoke_grant),
        )
        .route_layer(from_fn_with_state(state.clone(), auth_mw));

    // SPA: serve hashed assets directly; any other path (`/`, `/admin`, `/sql`, refreshes, deep
    // links) returns index.html so the client-side router takes over. This is the robust SPA
    // pattern — a plain ServeDir 404s on client routes.
    let assets = ServeDir::new(state.web_dir.join("assets"));

    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .merge(protected)
        .nest_service("/assets", assets)
        .fallback(spa_index)
        .with_state(state)
}

/// Serve `index.html` for any unmatched path (the SPA entry; client-side routing handles the rest).
async fn spa_index(State(st): State<AppState>) -> Response {
    match tokio::fs::read(st.web_dir.join("index.html")).await {
        Ok(bytes) => (
            [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
            bytes,
        )
            .into_response(),
        Err(_) => (StatusCode::NOT_FOUND, "web UI not built").into_response(),
    }
}

async fn healthz() -> &'static str {
    "ok"
}

/// Bearer-token gate for `/api/*` (except login). On success the [`Claims`] are inserted as a
/// request extension for downstream handlers.
async fn auth_mw(
    State(st): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let claims = st.verify_token(token).ok_or(StatusCode::UNAUTHORIZED)?;
    req.extensions_mut().insert(claims);
    Ok(next.run(req).await)
}

async fn login(
    State(st): State<AppState>,
    Json(body): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, StatusCode> {
    let (hash, groups) = {
        let users = st.users.lock().unwrap();
        let u = users.get(&body.username).ok_or(StatusCode::UNAUTHORIZED)?;
        (u.password_hash.clone(), u.groups.clone())
    };
    if !bcrypt::verify(&body.password, &hash).unwrap_or(false) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let token = st
        .issue_token(&body.username, &groups)
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(LoginResponse {
        token,
        user: body.username,
        groups,
    }))
}

async fn logout() -> StatusCode {
    // Stateless JWT: the client discards the token. (A denylist lands with the persistent store.)
    StatusCode::NO_CONTENT
}

/// The current principal, from the validated token.
async fn me(Extension(claims): Extension<Claims>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "user": claims.sub,
        "groups": claims.groups,
        "authenticated": true
    }))
}

async fn list_clusters(State(st): State<AppState>) -> Json<Vec<Cluster>> {
    let mut v: Vec<Cluster> = st.clusters.lock().unwrap().values().cloned().collect();
    v.sort_by(|a, b| a.id.cmp(&b.id));
    Json(v)
}

async fn create_cluster(
    State(st): State<AppState>,
    Json(body): Json<CreateCluster>,
) -> (StatusCode, Json<Cluster>) {
    let id = st.new_id();
    let cluster = Cluster {
        id: id.clone(),
        name: body.name,
        state: Phase::Pending.as_str().to_string(),
        worker_min: body.worker_min,
        worker_max: body.worker_max,
        worker_size: body.worker_size,
        connect_endpoint: None,
        instance_id: None,
    };
    st.clusters
        .lock()
        .unwrap()
        .insert(id.clone(), cluster.clone());
    st.save_clusters();
    st.add_event(
        &id,
        format!(
            "Cluster created (requested {} worker(s))",
            cluster.worker_min
        ),
    );
    // Provision a real compute backend (Databricks-style: create implies start). Async so the API
    // returns immediately while the cluster comes up — poll the list / events to watch it advance.
    st.touch(&id);
    tokio::spawn(provision(st.clone(), id));
    (StatusCode::CREATED, Json(cluster))
}

/// Build the `WEFT_CATALOG_CONF` value (`;`-separated `spark.sql.catalog.<name>.*` entries) from the
/// attached connections, so a freshly provisioned cluster registers the same catalogs as the
/// control plane. Glue → `type=glue;region=…`; Hive → `type=hive;uri=…`.
fn cluster_catalog_conf(st: &AppState, default_region: &str) -> String {
    let conns = st.connections.lock().unwrap();
    let mut parts: Vec<String> = Vec::new();
    for c in conns.iter() {
        let p = format!("spark.sql.catalog.{}", c.name);
        parts.push(format!("{p}.type={}", c.kind));
        match c.kind.as_str() {
            "glue" => {
                let region = c
                    .options
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or(default_region);
                parts.push(format!("{p}.region={region}"));
            }
            "hive" => {
                if let Some(uri) = c.options.get("uri") {
                    parts.push(format!("{p}.uri={uri}"));
                }
            }
            _ => {}
        }
    }
    parts.join(";")
}

/// Materialize a cluster into a real Spark Connect server process and advance its state. On EKS the
/// operator would create driver + worker pods here; on this single node it spawns a `weft spark
/// server` on an allocated port — a genuine, connectable endpoint.
async fn provision(st: AppState, id: String) {
    st.set_state(&id, Phase::Provisioning, None);
    if st.ec2.as_ref().is_some() {
        provision_ec2(st, id).await;
    } else {
        provision_process(st, id).await;
    }
}

/// Launch a real EC2 instance running `weft spark server` for this cluster.
async fn provision_ec2(st: AppState, id: String) {
    use tokio::net::TcpStream;
    use tokio::time::{sleep, Duration};
    let cfg = st.ec2.as_ref().clone().expect("ec2 config");

    let size = st
        .clusters
        .lock()
        .unwrap()
        .get(&id)
        .map(|c| c.worker_size.clone())
        .unwrap_or_else(|| "small".into());
    let itype = instance_type_for(&size);
    st.add_event(
        &id,
        format!("Launching EC2 instance ({itype}) for cluster compute"),
    );

    // Propagate every attached catalog connection so the cluster's engine registers them too —
    // a cluster automatically "sees" the same catalogs as the control plane.
    let catalog_conf = cluster_catalog_conf(&st, &cfg.region);
    if !catalog_conf.is_empty() {
        st.add_event(
            &id,
            format!(
                "Seeding cluster with {} attached catalog(s)",
                st.connections.lock().unwrap().len()
            ),
        );
    }

    // user-data: download the weft binary and run the Spark Connect server on boot, with AWS region
    // + catalog config in the environment so external catalogs (Glue/Hive) resolve on the cluster.
    let user_data = format!(
        "#!/bin/bash\nset -e\ncurl -fsSL '{}' -o /usr/local/bin/weft\nchmod +x /usr/local/bin/weft\nexport AWS_REGION='{}'\nexport WEFT_CATALOG_CONF='{}'\n/usr/local/bin/weft spark server --port 50051 > /var/log/weft.log 2>&1 &\n",
        cfg.weft_url, cfg.region, catalog_conf
    );
    let user_data_b64 = base64_encode(user_data.as_bytes());

    let mut args: Vec<String> = vec![
        "ec2".into(), "run-instances".into(),
        "--image-id".into(), cfg.ami.clone(),
        "--instance-type".into(), itype.to_string(),
        "--security-group-ids".into(), cfg.sg.clone(),
        "--user-data".into(), user_data_b64,
        "--tag-specifications".into(),
        format!("ResourceType=instance,Tags=[{{Key=Name,Value=weft-cluster-{id}}},{{Key=project,Value=weft-cluster}}]"),
        "--region".into(), cfg.region.clone(),
        "--query".into(), "Instances[0].InstanceId".into(),
        "--output".into(), "text".into(),
    ];
    if let Some(subnet) = &cfg.subnet {
        args.push("--subnet-id".into());
        args.push(subnet.clone());
    }
    if let Some(key) = &cfg.key {
        args.push("--key-name".into());
        args.push(key.clone());
    }
    if let Some(profile) = &cfg.instance_profile {
        // Lets the cluster read Glue/S3 via the instance role (no static keys).
        args.push("--iam-instance-profile".into());
        args.push(format!("Name={profile}"));
    }
    // Private clusters get no public IP — the in-VPC gateway reaches them by private IP.
    args.push("--associate-public-ip-address".into());
    if cfg.private {
        args.last_mut()
            .map(|s| *s = "--no-associate-public-ip-address".into());
    }

    let instance_id = match run_aws(&cfg.aws_bin, &args).await {
        Ok(out) => out.trim().to_string(),
        Err(e) => {
            st.set_state(&id, Phase::Error, None);
            st.add_event(&id, format!("Failed to launch EC2 instance: {e}"));
            return;
        }
    };
    st.runtimes.lock().await.insert(
        id.clone(),
        ClusterRuntime::Ec2 {
            instance_id: instance_id.clone(),
        },
    );
    // Persist the instance id so the cluster can be re-adopted (deleted/auto-terminated) if the
    // control plane restarts before the instance is torn down.
    st.set_cluster_instance(&id, &instance_id);
    st.add_event(
        &id,
        format!("EC2 instance {instance_id} launching; waiting for it to boot"),
    );

    // Wait for the instance's IP (private when running in a private subnet, else public) — ~120s.
    let ip_field = if cfg.private {
        "Reservations[0].Instances[0].PrivateIpAddress"
    } else {
        "Reservations[0].Instances[0].PublicIpAddress"
    };
    let mut ip = None;
    for _ in 0..60 {
        if let Ok(out) = run_aws(
            &cfg.aws_bin,
            &[
                "ec2".into(),
                "describe-instances".into(),
                "--instance-ids".into(),
                instance_id.clone(),
                "--region".into(),
                cfg.region.clone(),
                "--query".into(),
                ip_field.into(),
                "--output".into(),
                "text".into(),
            ],
        )
        .await
        {
            let v = out.trim();
            if !v.is_empty() && v != "None" {
                ip = Some(v.to_string());
                break;
            }
        }
        sleep(Duration::from_secs(2)).await;
    }
    let Some(ip) = ip else {
        st.set_state(&id, Phase::Error, None);
        st.add_event(&id, "EC2 instance did not report an IP in time");
        return;
    };
    st.add_event(&id, format!("Instance running at {ip}; waiting for Spark Connect to accept (booting + installing weft)"));

    // Wait for the Spark Connect port to accept — the instance boots, downloads weft, and starts
    // the server (up to ~3 min).
    let mut up = false;
    for _ in 0..90 {
        if TcpStream::connect((ip.as_str(), 50051)).await.is_ok() {
            up = true;
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }
    if up {
        let endpoint = format!("sc://{ip}:50051");
        st.set_state(&id, Phase::Running, Some(endpoint.clone()));
        st.add_event(
            &id,
            format!("Cluster RUNNING on EC2 {instance_id} — Spark Connect endpoint {endpoint}"),
        );
    } else {
        st.set_state(&id, Phase::Error, None);
        st.add_event(
            &id,
            "Spark Connect did not come up on the EC2 instance in time",
        );
    }
}

/// Fallback: run `weft spark server` as a local process (no EC2 config).
async fn provision_process(st: AppState, id: String) {
    use tokio::net::TcpStream;
    use tokio::time::{sleep, Duration};

    let port = st.alloc_port();
    st.add_event(
        &id,
        format!("Provisioning compute (local process, Spark Connect port {port})"),
    );
    if st.weft_bin.is_empty() {
        st.set_state(&id, Phase::Error, None);
        st.add_event(
            &id,
            "No weft binary configured (WEFT_BIN) — cannot start compute",
        );
        return;
    }
    let child = tokio::process::Command::new(st.weft_bin.as_str())
        .args(["spark", "server", "--port", &port.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    let child = match child {
        Ok(c) => c,
        Err(e) => {
            st.set_state(&id, Phase::Error, None);
            st.add_event(&id, format!("Failed to launch compute: {e}"));
            return;
        }
    };
    st.runtimes
        .lock()
        .await
        .insert(id.clone(), ClusterRuntime::Process(child));
    st.add_event(
        &id,
        "Compute process launched; waiting for the Spark Connect endpoint to accept",
    );
    let mut up = false;
    for _ in 0..40 {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            up = true;
            break;
        }
        sleep(Duration::from_millis(500)).await;
    }
    if up {
        let endpoint = format!("sc://{}:{port}", st.public_host);
        st.set_state(&id, Phase::Running, Some(endpoint.clone()));
        st.add_event(
            &id,
            format!("Cluster RUNNING — Spark Connect endpoint {endpoint}"),
        );
    } else {
        st.set_state(&id, Phase::Error, None);
        st.add_event(&id, "Compute did not become ready in time");
    }
}

/// Run an `aws` CLI command, returning stdout on success.
async fn run_aws(aws_bin: &str, args: &[String]) -> Result<String, String> {
    let out = tokio::process::Command::new(aws_bin)
        .args(args)
        .output()
        .await
        .map_err(|e| format!("exec aws: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

/// Minimal base64 (standard alphabet) for the EC2 user-data payload.
fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

async fn get_cluster(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    st.clusters
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// The lifecycle events for a cluster (the reconcile trace shown in the UI).
async fn list_cluster_events(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Json<Vec<ClusterEvent>> {
    Json(
        st.events
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .unwrap_or_default(),
    )
}

async fn delete_cluster(State(st): State<AppState>, Path(id): Path<String>) -> StatusCode {
    let existed = st.clusters.lock().unwrap().remove(&id).is_some();
    if !existed {
        return StatusCode::NOT_FOUND;
    }
    kill_runtime(&st, &id).await;
    st.events.lock().unwrap().remove(&id);
    st.save_clusters();
    StatusCode::NO_CONTENT
}

async fn start_cluster(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    let exists = st.clusters.lock().unwrap().contains_key(&id);
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }
    st.touch(&id);
    let already_up = st.runtimes.lock().await.contains_key(&id);
    if !already_up {
        st.add_event(&id, "Start requested");
        tokio::spawn(provision(st.clone(), id.clone()));
    }
    st.clusters
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn stop_cluster(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    let exists = st.clusters.lock().unwrap().contains_key(&id);
    if !exists {
        return Err(StatusCode::NOT_FOUND);
    }
    kill_runtime(&st, &id).await;
    st.set_state(&id, Phase::Terminated, None);
    st.add_event(&id, "Cluster stopped (compute terminated)");
    st.clusters
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Kill the OS process backing a cluster, if any.
async fn kill_runtime(st: &AppState, id: &str) {
    let rt = st.runtimes.lock().await.remove(id);
    match rt {
        Some(ClusterRuntime::Process(mut child)) => {
            let _ = child.kill().await;
        }
        Some(ClusterRuntime::Ec2 { instance_id }) => {
            if let Some(cfg) = st.ec2.as_ref().as_ref() {
                let _ = run_aws(
                    &cfg.aws_bin,
                    &[
                        "ec2".into(),
                        "terminate-instances".into(),
                        "--instance-ids".into(),
                        instance_id,
                        "--region".into(),
                        cfg.region.clone(),
                        "--output".into(),
                        "text".into(),
                    ],
                )
                .await;
            }
        }
        None => {}
    }
}

// ───────────────────────────────────────── Run SQL ──────────────────────────────────────────────

/// `POST /api/sql` body.
#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    /// The SQL to run (Spark dialect; passed through the dialect shim).
    pub sql: String,
    /// Optional target cluster (ignored by the embedded engine; the routing seam).
    #[serde(default)]
    pub cluster_id: Option<String>,
}

/// `POST /api/sql` response — a column header + rows of display strings (capped for the UI).
#[derive(Debug, Serialize)]
pub struct SqlResponse {
    /// Column names.
    pub columns: Vec<String>,
    /// Up to 1000 result rows, each a vector of stringified cell values.
    pub rows: Vec<Vec<String>>,
    /// Total rows produced (before the UI cap).
    pub row_count: usize,
    /// Error message if the query failed (then `rows` is empty).
    pub error: Option<String>,
}

/// Run a SQL query on the embedded engine and return rows. Spark-dialect input is passed through
/// [`weft_sql::dialect`] first. In production this routes to the target cluster's Spark Connect
/// endpoint; here it runs in-process on the same engine — real execution, real results.
async fn run_sql(State(st): State<AppState>, Json(req): Json<SqlRequest>) -> Json<SqlResponse> {
    const MAX_ROWS: usize = 1000;
    // If a RUNNING cluster is selected, route execution to its Spark Connect endpoint (the real
    // data-plane hop); otherwise run on the gateway's embedded engine. A non-running selection
    // falls back to the embedded engine.
    if let Some(cid) = req.cluster_id.as_deref().filter(|s| !s.is_empty()) {
        st.touch(cid); // counts as activity → resets the cluster's idle timer
        let endpoint = {
            let clusters = st.clusters.lock().unwrap();
            clusters.get(cid).and_then(|c| {
                if c.state == Phase::Running.as_str() {
                    c.connect_endpoint.clone()
                } else {
                    None
                }
            })
        };
        if let Some(ep) = endpoint {
            return match cluster_client::run_sql_on_cluster(&ep, &req.sql, MAX_ROWS).await {
                Ok(batches) => Json(batches_to_response(&batches)),
                Err(e) => Json(SqlResponse {
                    columns: vec![],
                    rows: vec![],
                    row_count: 0,
                    error: Some(format!("cluster `{cid}`: {e}")),
                }),
            };
        }
    }
    let sql = weft_sql::dialect::to_datafusion_sql(&req.sql);
    // Cap execution at the UI display limit so an unbounded `SELECT *` over a huge external table
    // (e.g. 100M-row ClickBench `hits`) reads only enough row groups instead of materializing the
    // whole result into memory (which previously OOMed → "load failed"). LIMIT pushes into the scan.
    let result = async {
        let df = st.engine.ctx().sql(&sql).await?;
        let df = df.limit(0, Some(MAX_ROWS))?;
        df.collect().await
    }
    .await;
    match result {
        Ok(batches) => Json(batches_to_response(&batches)),
        Err(e) => Json(SqlResponse {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            error: Some(format!("{e}")),
        }),
    }
}

fn batches_to_response(batches: &[weft_loom::arrow::record_batch::RecordBatch]) -> SqlResponse {
    let Some(first) = batches.first() else {
        return SqlResponse {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            error: None,
        };
    };
    let columns: Vec<String> = first
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let opts = FormatOptions::default();
    let mut rows: Vec<Vec<String>> = Vec::new();
    for b in batches {
        let Ok(fmts) = b
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c, &opts))
            .collect::<Result<Vec<_>, _>>()
        else {
            continue;
        };
        for r in 0..b.num_rows() {
            rows.push(fmts.iter().map(|f| f.value(r).to_string()).collect());
        }
    }
    let row_count = rows.len();
    rows.truncate(1000); // cap for the UI grid
    SqlResponse {
        columns,
        rows,
        row_count,
        error: None,
    }
}

// ──────────────────────────────── Admin: users / groups / grants ────────────────────────────────

/// A user as the admin panel lists it.
#[derive(Debug, Serialize)]
pub struct UserDto {
    /// Username.
    pub username: String,
    /// Group memberships.
    pub groups: Vec<String>,
}

/// `POST /api/admin/users` body.
#[derive(Debug, Deserialize)]
pub struct CreateUser {
    /// Username.
    pub username: String,
    /// Password (bcrypt-hashed on store).
    pub password: String,
    /// Initial group memberships.
    #[serde(default)]
    pub groups: Vec<String>,
}

async fn list_users(State(st): State<AppState>) -> Json<Vec<UserDto>> {
    let users = st.users.lock().unwrap();
    let mut out: Vec<UserDto> = users
        .iter()
        .map(|(u, r)| UserDto {
            username: u.clone(),
            groups: r.groups.clone(),
        })
        .collect();
    out.sort_by(|a, b| a.username.cmp(&b.username));
    Json(out)
}

async fn create_user(State(st): State<AppState>, Json(b): Json<CreateUser>) -> StatusCode {
    let hash = match bcrypt::hash(&b.password, bcrypt::DEFAULT_COST) {
        Ok(h) => h,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR,
    };
    st.users.lock().unwrap().insert(
        b.username.clone(),
        UserRecord {
            password_hash: hash,
            groups: b.groups.clone(),
        },
    );
    // Reflect membership in the group store.
    let mut groups = st.groups.lock().unwrap();
    for g in &b.groups {
        groups
            .entry(g.clone())
            .or_default()
            .push(b.username.clone());
    }
    StatusCode::CREATED
}

/// A group as the admin panel lists it.
#[derive(Debug, Serialize)]
pub struct GroupDto {
    /// Group name.
    pub name: String,
    /// Member usernames (and nested group names).
    pub members: Vec<String>,
}

/// `POST /api/admin/groups` body.
#[derive(Debug, Deserialize)]
pub struct CreateGroup {
    /// Group name.
    pub name: String,
    /// Initial members.
    #[serde(default)]
    pub members: Vec<String>,
}

async fn list_groups(State(st): State<AppState>) -> Json<Vec<GroupDto>> {
    let groups = st.groups.lock().unwrap();
    let mut out: Vec<GroupDto> = groups
        .iter()
        .map(|(n, m)| GroupDto {
            name: n.clone(),
            members: m.clone(),
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Json(out)
}

async fn create_group(State(st): State<AppState>, Json(b): Json<CreateGroup>) -> StatusCode {
    st.groups.lock().unwrap().insert(b.name, b.members);
    StatusCode::CREATED
}

/// A grant as the admin panel exchanges it (Unity-Catalog model, string-typed for the wire).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantDto {
    /// Securable type: `catalog`/`schema`/`table`/`view`/`metastore`/…
    pub securable_type: String,
    /// Dotted securable name (e.g. `main.sales.orders`; empty for the metastore).
    pub securable_name: String,
    /// Privilege: `SELECT`/`MODIFY`/`USE CATALOG`/`ALL PRIVILEGES`/`BROWSE`/…
    pub privilege: String,
    /// Principal kind: `user`/`group`/`service_principal`.
    pub principal_kind: String,
    /// Principal id (username / group name).
    pub principal_id: String,
    /// `allow` or `deny`.
    pub effect: String,
}

async fn list_grants(State(st): State<AppState>) -> Json<Vec<GrantDto>> {
    Json(st.grants.lock().unwrap().iter().map(grant_to_dto).collect())
}

async fn create_grant(
    State(st): State<AppState>,
    Json(d): Json<GrantDto>,
) -> Result<StatusCode, (StatusCode, String)> {
    let grant = dto_to_grant(&d).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let mut grants = st.grants.lock().unwrap();
    if !grants.contains(&grant) {
        grants.push(grant);
    }
    Ok(StatusCode::CREATED)
}

async fn revoke_grant(State(st): State<AppState>, Json(d): Json<GrantDto>) -> StatusCode {
    match dto_to_grant(&d) {
        Ok(g) => {
            st.grants.lock().unwrap().retain(|x| x != &g);
            StatusCode::NO_CONTENT
        }
        Err(_) => StatusCode::BAD_REQUEST,
    }
}

fn grant_to_dto(g: &Grant) -> GrantDto {
    let (kind, id) = match &g.principal {
        Principal::User(u) => ("user", u.clone()),
        Principal::Group(x) => ("group", x.clone()),
        Principal::ServicePrincipal(s) => ("service_principal", s.clone()),
    };
    GrantDto {
        securable_type: format!("{:?}", g.securable.kind).to_lowercase(),
        securable_name: g.securable.name.join("."),
        privilege: format!("{:?}", g.privilege),
        principal_kind: kind.into(),
        principal_id: id,
        effect: if g.effect == Effect::Deny {
            "deny".into()
        } else {
            "allow".into()
        },
    }
}

fn dto_to_grant(d: &GrantDto) -> Result<Grant, String> {
    let kind = match d.securable_type.to_lowercase().as_str() {
        "metastore" => SecurableType::Metastore,
        "catalog" => SecurableType::Catalog,
        "schema" | "database" => SecurableType::Schema,
        "table" => SecurableType::Table,
        "view" => SecurableType::View,
        "volume" => SecurableType::Volume,
        "function" => SecurableType::Function,
        "connection" => SecurableType::Connection,
        other => return Err(format!("unknown securable type: {other}")),
    };
    let name: Vec<String> = d
        .securable_name
        .split('.')
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    let privilege = privilege_from_str(&d.privilege.to_uppercase())
        .ok_or_else(|| format!("unknown privilege: {}", d.privilege))?;
    let principal = match d.principal_kind.as_str() {
        "user" => Principal::User(d.principal_id.clone()),
        "group" => Principal::Group(d.principal_id.clone()),
        "service_principal" => Principal::ServicePrincipal(d.principal_id.clone()),
        other => return Err(format!("unknown principal kind: {other}")),
    };
    let effect = if d.effect.eq_ignore_ascii_case("deny") {
        Effect::Deny
    } else {
        Effect::Allow
    };
    Ok(Grant {
        securable: Securable { kind, name },
        privilege,
        principal,
        effect,
    })
}

fn privilege_from_str(p: &str) -> Option<Privilege> {
    Some(match p {
        "SELECT" => Privilege::Select,
        "MODIFY" => Privilege::Modify,
        "EXECUTE" => Privilege::Execute,
        "USE CATALOG" => Privilege::UseCatalog,
        "USE SCHEMA" => Privilege::UseSchema,
        "CREATE CATALOG" => Privilege::CreateCatalog,
        "CREATE SCHEMA" => Privilege::CreateSchema,
        "CREATE TABLE" => Privilege::CreateTable,
        "BROWSE" => Privilege::Browse,
        "ALL PRIVILEGES" => Privilege::AllPrivileges,
        "MANAGE" => Privilege::Manage,
        _ => return None,
    })
}

// ───────────────────────────────── Connections (external catalogs) ──────────────────────────────

/// An attached catalog connection, as the UI lists it. Also the persisted record (round-tripped
/// to `WEFT_CONNECTIONS_FILE`) and the source for a cluster's startup catalog config, so `options`
/// carries everything needed to re-register the provider (Glue `region`, Hive `uri`, …).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// The registered catalog name (use it as the first part of `name.db.table`).
    pub name: String,
    /// Connection kind: `glue` | `hive`.
    pub kind: String,
    /// Region (for Glue) — surfaced to the UI for convenience; also present in `options`.
    pub region: Option<String>,
    /// Full kind-specific options (Glue → `region`/`aws_bin`; Hive → `uri`). Used to rebuild the
    /// provider on restart and to seed a cluster's `spark.sql.catalog.<name>.*` config.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

/// `POST /api/connections` body — attach an external catalog.
#[derive(Debug, Deserialize)]
pub struct CreateConnection {
    /// Catalog name to register it under.
    pub name: String,
    /// `glue` (AWS Glue Data Catalog) or `hive` (Hive Metastore over Thrift).
    pub kind: String,
    /// Kind-specific options: Glue → `region`; Hive → `uri` (`thrift://host:port`).
    #[serde(default)]
    pub options: HashMap<String, String>,
}

async fn list_connections(State(st): State<AppState>) -> Json<Vec<ConnectionInfo>> {
    Json(st.connections.lock().unwrap().clone())
}

/// Build a [`weft_catalog::CatalogProvider`] for a connection `kind` + `options`, used both by the
/// live attach path and when re-registering persisted connections on startup.
fn build_connection_provider(
    kind: &str,
    name: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn weft_catalog::CatalogProvider>, String> {
    match kind {
        "glue" => Ok(Arc::new(GlueCatalog::from_config(name, options))),
        "hive" => Ok(Arc::new(
            weft_catalog_hive::HiveCatalog::from_config(name, options)
                .map_err(|e| format!("{e:?}"))?,
        )),
        other => Err(format!("unsupported connection kind: {other}")),
    }
}

/// Attach an external catalog: construct its [`weft_catalog::CatalogProvider`] and register it on
/// the engine, so its databases/tables show up in the catalog browser and become queryable as
/// `name.database.table`. The connection is persisted so it survives a restart and is propagated
/// to every cluster the gateway provisions.
async fn create_connection(
    State(st): State<AppState>,
    Json(b): Json<CreateConnection>,
) -> Result<StatusCode, (StatusCode, String)> {
    let provider = build_connection_provider(&b.kind, &b.name, &b.options)
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    st.engine.register_catalog(&b.name, provider);
    {
        let mut conns = st.connections.lock().unwrap();
        conns.retain(|c| c.name != b.name); // replace any prior connection with the same name
        conns.push(ConnectionInfo {
            name: b.name.clone(),
            kind: b.kind,
            region: b.options.get("region").cloned(),
            options: b.options,
        });
    }
    st.save_connections();
    Ok(StatusCode::CREATED)
}

/// Detach an external catalog by name. Removes it from the persisted set so it no longer appears in
/// the catalog browser or seeds new clusters. (DataFusion can't deregister a live catalog, so the
/// provider lingers in memory until the next restart; the catalog browser filters it out by name.)
async fn delete_connection(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let removed = {
        let mut conns = st.connections.lock().unwrap();
        let before = conns.len();
        conns.retain(|c| c.name != name);
        before != conns.len()
    };
    if !removed {
        return Err((StatusCode::NOT_FOUND, format!("no connection `{name}`")));
    }
    st.save_connections();
    Ok(StatusCode::NO_CONTENT)
}

// ───────────────────────────── Workspace: notebooks + saved SQL queries ─────────────────────────

/// One cell in a notebook. `kind` is `sql` | `python` | `markdown`.
#[derive(Clone, Serialize, Deserialize)]
pub struct NotebookCell {
    pub id: String,
    pub kind: String,
    pub source: String,
}

/// A saved notebook (ordered cells). Persisted to `WEFT_NOTEBOOKS_FILE`.
#[derive(Clone, Serialize, Deserialize)]
pub struct NotebookDoc {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub cells: Vec<NotebookCell>,
    #[serde(default, rename = "updatedAt")]
    pub updated_at: String,
}

/// List-view summary of a notebook (matches the web `Notebook` shape).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NotebookSummary {
    pub id: String,
    pub name: String,
    pub language: String,
    pub owner: String,
    pub updated_at: String,
    pub cells: usize,
}

/// A saved SQL query (the Workspace). Persisted to `WEFT_QUERIES_FILE`.
#[derive(Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub sql: String,
    #[serde(default, rename = "updatedAt")]
    pub updated_at: String,
}

/// `POST /api/notebooks` body.
#[derive(Deserialize)]
pub struct CreateNotebook {
    #[serde(default)]
    pub name: String,
}

/// `POST /api/queries` body.
#[derive(Deserialize)]
pub struct CreateQuery {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub sql: String,
}

async fn list_notebooks(State(st): State<AppState>) -> Json<Vec<NotebookSummary>> {
    let nbs = st.notebooks.lock().unwrap();
    Json(
        nbs.iter()
            .map(|n| NotebookSummary {
                id: n.id.clone(),
                name: n.name.clone(),
                language: n
                    .cells
                    .first()
                    .map(|c| c.kind.clone())
                    .unwrap_or_else(|| "sql".into()),
                owner: "admin".into(),
                updated_at: n.updated_at.clone(),
                cells: n.cells.len(),
            })
            .collect(),
    )
}

async fn create_notebook(
    State(st): State<AppState>,
    Json(b): Json<CreateNotebook>,
) -> Json<NotebookDoc> {
    let name = if b.name.trim().is_empty() {
        "Untitled notebook".to_string()
    } else {
        b.name
    };
    let doc = NotebookDoc {
        id: st.new_oid("nb"),
        name,
        cells: vec![NotebookCell {
            id: st.new_oid("cell"),
            kind: "sql".into(),
            source: "SELECT * FROM main.sales.lineitem LIMIT 10".into(),
        }],
        updated_at: now_iso(),
    };
    st.notebooks.lock().unwrap().push(doc.clone());
    st.save_notebooks();
    Json(doc)
}

async fn get_notebook(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<NotebookDoc>, (StatusCode, String)> {
    st.notebooks
        .lock()
        .unwrap()
        .iter()
        .find(|n| n.id == id)
        .cloned()
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, format!("no notebook `{id}`")))
}

async fn save_notebook(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(mut doc): Json<NotebookDoc>,
) -> Json<serde_json::Value> {
    doc.id = id.clone();
    doc.updated_at = now_iso();
    {
        let mut nbs = st.notebooks.lock().unwrap();
        match nbs.iter_mut().find(|n| n.id == id) {
            Some(slot) => *slot = doc.clone(),
            None => nbs.push(doc.clone()), // upsert (e.g. first save of a client-created doc)
        }
    }
    st.save_notebooks();
    Json(serde_json::json!({ "ok": true, "savedAt": doc.updated_at }))
}

async fn delete_notebook(State(st): State<AppState>, Path(id): Path<String>) -> StatusCode {
    let removed = {
        let mut nbs = st.notebooks.lock().unwrap();
        let before = nbs.len();
        nbs.retain(|n| n.id != id);
        before != nbs.len()
    };
    if removed {
        st.save_notebooks();
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn list_queries(State(st): State<AppState>) -> Json<Vec<SavedQuery>> {
    Json(st.queries.lock().unwrap().clone())
}

async fn create_query(State(st): State<AppState>, Json(b): Json<CreateQuery>) -> Json<SavedQuery> {
    let name = if b.name.trim().is_empty() {
        "Untitled query".to_string()
    } else {
        b.name
    };
    let q = SavedQuery {
        id: st.new_oid("q"),
        name,
        sql: b.sql,
        updated_at: now_iso(),
    };
    st.queries.lock().unwrap().push(q.clone());
    st.save_queries();
    Json(q)
}

async fn get_query(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<SavedQuery>, (StatusCode, String)> {
    st.queries
        .lock()
        .unwrap()
        .iter()
        .find(|q| q.id == id)
        .cloned()
        .map(Json)
        .ok_or((StatusCode::NOT_FOUND, format!("no query `{id}`")))
}

async fn save_query(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(mut q): Json<SavedQuery>,
) -> Json<serde_json::Value> {
    q.id = id.clone();
    q.updated_at = now_iso();
    {
        let mut qs = st.queries.lock().unwrap();
        match qs.iter_mut().find(|x| x.id == id) {
            Some(slot) => *slot = q.clone(),
            None => qs.push(q.clone()),
        }
    }
    st.save_queries();
    Json(serde_json::json!({ "ok": true, "savedAt": q.updated_at }))
}

async fn delete_query(State(st): State<AppState>, Path(id): Path<String>) -> StatusCode {
    let removed = {
        let mut qs = st.queries.lock().unwrap();
        let before = qs.len();
        qs.retain(|q| q.id != id);
        before != qs.len()
    };
    if removed {
        st.save_queries();
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// ─────────────────────────────── Catalog: sample data + introspection ───────────────────────────

/// Register a small set of sample tables under `main.sales` so the SQL editor + catalog browser
/// have real, queryable data out of the box (e.g. `SELECT * FROM main.sales.monthly_revenue`). In
/// production these come from the user's registered catalogs (local + external HMS/Glue/UC).
fn seed_sample_data(engine: &Engine) {
    use weft_loom::arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::arrow::record_batch::RecordBatch;

    // 2026-01-01 as days since the Unix epoch (Date32). Orders/lineitems spread over ~4 months.
    const BASE: i32 = 20454;
    const N_CUST: i64 = 10;
    const N_ORD: i64 = 30;
    let segments = [
        "BUILDING",
        "AUTOMOBILE",
        "MACHINERY",
        "HOUSEHOLD",
        "FURNITURE",
    ];
    let statuses = ["O", "F", "P"];
    let prio = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
    let flags = ["A", "N", "R"];

    // monthly_revenue — a simple roll-up table for quick demos.
    let revenue = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("month", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("revenue", DataType::Float64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![
                "2026-01", "2026-01", "2026-02", "2026-02", "2026-03", "2026-03",
            ])),
            Arc::new(StringArray::from(vec!["US", "EU", "US", "EU", "US", "EU"])),
            Arc::new(Float64Array::from(vec![
                120000.0, 88000.0, 135000.0, 91000.0, 142000.0, 99000.0,
            ])),
        ],
    );

    // customer — TPC-H-shaped.
    let customer = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Float64, false),
            Field::new("c_nationkey", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from((1..=N_CUST).collect::<Vec<_>>())),
            Arc::new(StringArray::from(
                (1..=N_CUST)
                    .map(|i| format!("Customer#{i:03}"))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (1..=N_CUST)
                    .map(|i| segments[(i as usize - 1) % segments.len()].to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (1..=N_CUST)
                    .map(|i| 1000.0 + i as f64 * 137.5)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (1..=N_CUST).map(|i| i % 5).collect::<Vec<_>>(),
            )),
        ],
    );

    // orders — TPC-H-shaped, with a real Date32 o_orderdate.
    let orders = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Float64, false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from((1..=N_ORD).collect::<Vec<_>>())),
            Arc::new(Int64Array::from(
                (1..=N_ORD).map(|i| (i % N_CUST) + 1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (1..=N_ORD)
                    .map(|i| statuses[i as usize % 3].to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (1..=N_ORD)
                    .map(|i| 5000.0 + i as f64 * 321.0)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                (1..=N_ORD)
                    .map(|i| BASE + (i as i32 * 13) % 120)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                (1..=N_ORD)
                    .map(|i| prio[i as usize % 5].to_string())
                    .collect::<Vec<_>>(),
            )),
        ],
    );

    // lineitem — TPC-H-shaped, 2–4 lines per order.
    let (mut lo, mut ll, mut lp, mut lq, mut lep, mut ld, mut lt, mut lf, mut ls) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for ok in 1..=N_ORD {
        for ln in 1..=(2 + ok % 3) {
            lo.push(ok);
            ll.push(ln);
            lp.push((ok * 7 + ln) % 200 + 1);
            let qty = 5.0 + ((ok + ln) % 40) as f64;
            lq.push(qty);
            lep.push(qty * (100.0 + (ok * 3 % 900) as f64));
            ld.push(((ok + ln) % 10) as f64 / 100.0);
            lt.push((ok % 8) as f64 / 100.0);
            lf.push(flags[((ok + ln) as usize) % 3].to_string());
            ls.push(BASE + (ok as i32 * 13) % 120 + ln as i32 * 2);
        }
    }
    let lineitem = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(lo)),
            Arc::new(Int64Array::from(ll)),
            Arc::new(Int64Array::from(lp)),
            Arc::new(Float64Array::from(lq)),
            Arc::new(Float64Array::from(lep)),
            Arc::new(Float64Array::from(ld)),
            Arc::new(Float64Array::from(lt)),
            Arc::new(StringArray::from(lf)),
            Arc::new(Date32Array::from(ls)),
        ],
    );

    for (table, batch) in [
        ("monthly_revenue", revenue),
        ("customer", customer),
        ("orders", orders),
        ("lineitem", lineitem),
    ] {
        match batch {
            Ok(b) => {
                if let Err(e) = register_namespaced(engine, "main", "sales", table, b) {
                    eprintln!("seed {table}: {e}");
                }
            }
            Err(e) => eprintln!("build {table}: {e}"),
        }
    }
}

/// Register `batch` as `catalog.schema.table` in the engine, creating the catalog/schema if needed.
fn register_namespaced(
    engine: &Engine,
    catalog: &str,
    schema: &str,
    table: &str,
    batch: weft_loom::arrow::record_batch::RecordBatch,
) -> Result<(), String> {
    use datafusion::catalog::{
        CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    };
    use datafusion::datasource::MemTable;

    let ctx = engine.ctx();
    let mem = MemTable::try_new(batch.schema(), vec![vec![batch]]).map_err(|e| e.to_string())?;

    let cat = match ctx.catalog(catalog) {
        Some(c) => c,
        None => {
            let c: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::new());
            ctx.register_catalog(catalog, c.clone());
            c
        }
    };
    let sch = match cat.schema(schema) {
        Some(s) => s,
        None => {
            let s: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
            cat.register_schema(schema, s.clone())
                .map_err(|e| e.to_string())?;
            s
        }
    };
    sch.register_table(table.to_string(), Arc::new(mem))
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// A column in the catalog browser.
#[derive(Debug, Serialize)]
pub struct CatalogColumn {
    /// Column name.
    pub name: String,
    /// Arrow data type (display).
    pub data_type: String,
}

/// A table in the catalog browser.
#[derive(Debug, Serialize)]
pub struct CatalogTable {
    /// Table name.
    pub name: String,
    /// Columns.
    pub columns: Vec<CatalogColumn>,
}

/// A schema (namespace) in the catalog browser.
#[derive(Debug, Serialize)]
pub struct CatalogSchema {
    /// Schema name.
    pub name: String,
    /// Tables.
    pub tables: Vec<CatalogTable>,
}

/// A catalog in the catalog browser.
#[derive(Debug, Serialize)]
pub struct CatalogNamespace {
    /// Catalog name.
    pub name: String,
    /// Schemas.
    pub schemas: Vec<CatalogSchema>,
}

/// Introspect the engine's real catalogs/schemas/tables/columns — what the SQL editor can query.
async fn get_catalog(State(st): State<AppState>) -> Json<Vec<CatalogNamespace>> {
    let ctx = st.engine.ctx();
    let mut out = Vec::new();

    // 1) Built-in `main` catalog — DataFusion enumerates it eagerly (seeded tables resolved).
    if let Some(cat) = ctx.catalog("main") {
        let mut schemas = Vec::new();
        for sch_name in cat.schema_names() {
            if sch_name == "information_schema" {
                continue;
            }
            let Some(sch) = cat.schema(&sch_name) else {
                continue;
            };
            let mut tables = Vec::new();
            for tbl_name in sch.table_names() {
                if let Ok(Some(tbl)) = sch.table(&tbl_name).await {
                    tables.push(CatalogTable {
                        name: tbl_name,
                        columns: fields_to_columns(tbl.schema().fields()),
                    });
                }
            }
            schemas.push(CatalogSchema {
                name: sch_name,
                tables,
            });
        }
        if schemas.iter().any(|s| !s.tables.is_empty()) {
            out.push(CatalogNamespace {
                name: "main".into(),
                schemas,
            });
        }
    }

    // 2) Attached external catalogs (Glue / Hive). The DataFusion bridge lists these *lazily*
    // (table_names() is empty until a query resolves a table), so we enumerate through the catalog
    // provider's own async API — list databases → list tables — and resolve each table's columns
    // through the engine bridge (best-effort; a Glue/Hive hiccup just yields fewer entries).
    let conns: Vec<ConnectionInfo> = st.connections.lock().unwrap().clone();
    for conn in conns {
        let Ok(provider) = build_connection_provider(&conn.kind, &conn.name, &conn.options) else {
            continue;
        };
        let Ok(namespaces) = provider.list_namespaces(&[]).await else {
            continue;
        };
        let mut schemas = Vec::new();
        for ns in namespaces {
            let db = ns.join(".");
            let tables = provider.list_tables(&ns).await.unwrap_or_default();
            let mut tbls = Vec::new();
            for tname in tables {
                let columns = resolve_columns(ctx, &conn.name, &db, &tname).await;
                tbls.push(CatalogTable {
                    name: tname,
                    columns,
                });
            }
            schemas.push(CatalogSchema { name: db, tables: tbls });
        }
        if !schemas.is_empty() {
            out.push(CatalogNamespace {
                name: conn.name,
                schemas,
            });
        }
    }
    Json(out)
}

/// Map Arrow fields to the catalog-browser column shape.
fn fields_to_columns(fields: &weft_loom::arrow::datatypes::Fields) -> Vec<CatalogColumn> {
    fields
        .iter()
        .map(|f| CatalogColumn {
            name: f.name().clone(),
            data_type: format!("{}", f.data_type()),
        })
        .collect()
}

/// Resolve a `catalog.db.table`'s columns through the engine bridge (reads the table's schema,
/// e.g. a Parquet footer for a Glue table). Best-effort: returns `[]` if it can't be resolved.
async fn resolve_columns(
    ctx: &datafusion::prelude::SessionContext,
    catalog: &str,
    db: &str,
    table: &str,
) -> Vec<CatalogColumn> {
    let Some(cat) = ctx.catalog(catalog) else {
        return vec![];
    };
    let Some(sch) = cat.schema(db) else {
        return vec![];
    };
    match sch.table(table).await {
        Ok(Some(tbl)) => fields_to_columns(tbl.schema().fields()),
        _ => vec![],
    }
}

/// Bind and serve the gateway on `addr`. Admin credentials come from `WEFT_ADMIN_USER` /
/// `WEFT_ADMIN_PASSWORD` (defaults `admin` / `admin`), the JWT secret from `WEFT_JWT_SECRET`, and
/// the SPA directory from `WEFT_WEB_DIR` (default `web/dist`).
pub async fn serve(addr: &str) -> std::io::Result<()> {
    let user = std::env::var("WEFT_ADMIN_USER").unwrap_or_else(|_| "admin".into());
    let password = std::env::var("WEFT_ADMIN_PASSWORD").unwrap_or_else(|_| "admin".into());
    let secret =
        std::env::var("WEFT_JWT_SECRET").unwrap_or_else(|_| "weft-dev-secret-change-me".into());
    let web_dir =
        PathBuf::from(std::env::var("WEFT_WEB_DIR").unwrap_or_else(|_| "web/dist".into()));
    // Cluster runtime: the `weft` binary to spawn per cluster, and the host advertised in the
    // cluster's connect endpoint.
    let weft_bin = std::env::var("WEFT_BIN").unwrap_or_default();
    let public_host = std::env::var("WEFT_PUBLIC_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let state = AppState::with_runtime(
        &user,
        &password,
        secret.into_bytes(),
        web_dir,
        weft_bin,
        public_host,
    );
    // Restore durable state (clusters/connections from DynamoDB, workspace from S3) before serving.
    state.load_from_cloud().await;
    // Background reaper: auto-terminate idle running clusters.
    tokio::spawn(idle_reaper(state.clone()));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(state)).await
}

/// Periodically terminate `RUNNING` clusters that have had no activity for `idle_secs` — so
/// forgotten compute (and its EC2 cost) doesn't linger.
async fn idle_reaper(st: AppState) {
    use tokio::time::{sleep, Duration};
    if st.idle_secs == 0 {
        return;
    }
    loop {
        sleep(Duration::from_secs(60)).await;
        let now = now_secs() as u64;
        // Find running clusters idle beyond the timeout.
        let stale: Vec<String> = {
            let clusters = st.clusters.lock().unwrap();
            let activity = st.last_activity.lock().unwrap();
            clusters
                .iter()
                .filter(|(_, c)| c.state == Phase::Running.as_str())
                .filter(|(id, _)| {
                    let last = activity.get(id.as_str()).copied().unwrap_or(0);
                    now.saturating_sub(last) >= st.idle_secs
                })
                .map(|(id, _)| id.clone())
                .collect()
        };
        for id in stale {
            let mins = st.idle_secs / 60;
            st.add_event(
                &id,
                format!("Auto-terminating: idle for {mins}+ minutes (no activity)"),
            );
            kill_runtime(&st, &id).await;
            st.set_state(&id, Phase::Terminated, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn state() -> AppState {
        AppState::new(
            "admin",
            "secretsecret1234",
            b"test-secret".to_vec(),
            PathBuf::from("web/dist"),
        )
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    async fn login_token(st: &AppState, user: &str, pass: &str) -> Option<String> {
        let resp = app(st.clone())
            .oneshot(
                Request::post("/api/auth/login")
                    .header("content-type", "application/json")
                    .body(Body::from(format!(
                        r#"{{"username":"{user}","password":"{pass}"}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        if resp.status() != StatusCode::OK {
            return None;
        }
        body_json(resp).await["token"].as_str().map(String::from)
    }

    #[tokio::test]
    async fn login_success_and_failure() {
        let st = state();
        assert!(login_token(&st, "admin", "secretsecret1234")
            .await
            .is_some());
        // Wrong password → no token (401).
        assert!(login_token(&st, "admin", "wrong").await.is_none());
        // Unknown user → 401.
        assert!(login_token(&st, "nobody", "secretsecret1234")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn protected_routes_require_token() {
        let st = state();
        // No token → 401.
        let resp = app(st.clone())
            .oneshot(Request::get("/api/clusters").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        // With token → 200, and /api/me reflects the principal.
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let resp = app(st.clone())
            .oneshot(
                Request::get("/api/me")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let me = body_json(resp).await;
        assert_eq!(me["user"], "admin");
        assert_eq!(me["authenticated"], true);
    }

    #[tokio::test]
    async fn healthz_is_public() {
        let resp = app(state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn run_sql_executes_on_engine() {
        let st = state();
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let resp = app(st)
            .oneshot(
                Request::post("/api/sql")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(r#"{"sql":"SELECT 1 AS a, 'hi' AS b"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(j["columns"][0], "a");
        assert_eq!(j["columns"][1], "b");
        assert_eq!(j["rows"][0][0], "1");
        assert_eq!(j["rows"][0][1], "hi");
        assert!(j["error"].is_null());
    }

    #[tokio::test]
    async fn seeded_catalog_is_queryable() {
        let st = state();
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let auth = format!("Bearer {token}");
        // The seeded three-level table is queryable.
        let resp = app(st.clone())
            .oneshot(
                Request::post("/api/sql")
                    .header("content-type", "application/json")
                    .header("authorization", &auth)
                    .body(Body::from(
                        r#"{"sql":"SELECT region, sum(revenue) r FROM main.sales.monthly_revenue GROUP BY region ORDER BY region"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let j = body_json(resp).await;
        assert!(j["error"].is_null(), "query errored: {j:?}");
        assert_eq!(j["columns"][0], "region");
        assert_eq!(j["row_count"], 2);
        // The catalog endpoint surfaces it.
        let resp = app(st)
            .oneshot(
                Request::get("/api/catalog")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cat = body_json(resp).await;
        let s = serde_json::to_string(&cat).unwrap();
        assert!(s.contains("\"main\"") && s.contains("\"sales\"") && s.contains("monthly_revenue"));
    }

    #[tokio::test]
    async fn grant_create_and_list() {
        let st = state();
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let auth = format!("Bearer {token}");
        let resp = app(st.clone())
            .oneshot(
                Request::post("/api/grants")
                    .header("content-type", "application/json")
                    .header("authorization", &auth)
                    .body(Body::from(
                        r#"{"securable_type":"table","securable_name":"main.sales.orders","privilege":"SELECT","principal_kind":"group","principal_id":"analysts","effect":"allow"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let resp = app(st)
            .oneshot(
                Request::get("/api/grants")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j = body_json(resp).await;
        assert_eq!(j[0]["privilege"], "Select");
        assert_eq!(j[0]["principal_id"], "analysts");
    }

    #[tokio::test]
    async fn cluster_crud_with_auth() {
        let st = state();
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let auth = format!("Bearer {token}");

        let resp = app(st.clone())
            .oneshot(
                Request::post("/api/clusters")
                    .header("content-type", "application/json")
                    .header("authorization", &auth)
                    .body(Body::from(
                        r#"{"name":"analytics","worker_min":2,"worker_max":8}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let created = body_json(resp).await;
        let id = created["id"].as_str().unwrap().to_string();
        // Create kicks off async provisioning; with no `weft` binary configured in tests it can't
        // come up, so we don't assert RUNNING here. A create event is recorded, and stop terminates.
        let resp = app(st.clone())
            .oneshot(
                Request::get(format!("/api/clusters/{id}/events"))
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let events = body_json(resp).await;
        assert!(events
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["message"].as_str().unwrap().contains("created")));

        // Stop transitions to TERMINATED (no process needed).
        let resp = app(st.clone())
            .oneshot(
                Request::post(format!("/api/clusters/{id}/stop"))
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["state"], "TERMINATED");
    }
}
