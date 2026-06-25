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
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tower_http::services::ServeDir;
use weft_clustermgr::Phase;
use weft_govern::{Effect, Grant, Principal, Privilege, Securable, SecurableType};
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

use crate::glue::{glue_options, GlueCatalog};

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
        Self {
            clusters: Arc::new(Mutex::new(HashMap::new())),
            runtimes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            events: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
            next_port: Arc::new(Mutex::new(51000)),
            users: Arc::new(Mutex::new(users)),
            groups: Arc::new(Mutex::new(groups)),
            grants: Arc::new(Mutex::new(Vec::new())),
            connections: Arc::new(Mutex::new(Vec::new())),
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
        if let Some(c) = self.clusters.lock().unwrap().get_mut(id) {
            c.state = state.as_str().to_string();
            if endpoint.is_some() {
                c.connect_endpoint = endpoint;
            }
        }
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
    };
    st.clusters
        .lock()
        .unwrap()
        .insert(id.clone(), cluster.clone());
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

    // user-data: download the weft binary and run the Spark Connect server on boot.
    let user_data = format!(
        "#!/bin/bash\nset -e\ncurl -fsSL '{}' -o /usr/local/bin/weft\nchmod +x /usr/local/bin/weft\n/usr/local/bin/weft spark server --port 50051 > /var/log/weft.log 2>&1 &\n",
        cfg.weft_url
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
    st.add_event(
        &id,
        format!("EC2 instance {instance_id} launching; waiting for it to boot"),
    );

    // Wait for a public IP (instance running) — up to ~120s.
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
                "Reservations[0].Instances[0].PublicIpAddress".into(),
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
        st.add_event(&id, "EC2 instance did not report a public IP in time");
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
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
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
    // Running SQL against a cluster counts as activity (resets its idle timer).
    if let Some(cid) = &req.cluster_id {
        st.touch(cid);
    }
    let sql = weft_sql::dialect::to_datafusion_sql(&req.sql);
    match st.engine.sql(&sql).await {
        Ok(batches) => Json(batches_to_response(&batches)),
        Err(e) => Json(SqlResponse {
            columns: vec![],
            rows: vec![],
            row_count: 0,
            error: Some(format!("{e:?}")),
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

/// An attached catalog connection, as the UI lists it.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectionInfo {
    /// The registered catalog name (use it as the first part of `name.db.table`).
    pub name: String,
    /// Connection kind: `glue` | `hive`.
    pub kind: String,
    /// Region (for Glue).
    pub region: Option<String>,
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

/// Attach an external catalog: construct its [`weft_catalog::CatalogProvider`] and register it on
/// the engine, so its databases/tables show up in the catalog browser and become queryable as
/// `name.database.table`.
async fn create_connection(
    State(st): State<AppState>,
    Json(b): Json<CreateConnection>,
) -> Result<StatusCode, (StatusCode, String)> {
    match b.kind.as_str() {
        "glue" => {
            let (region, aws_bin) = glue_options(&b.options);
            let provider = Arc::new(GlueCatalog::new(b.name.clone(), region.clone(), aws_bin));
            st.engine.register_catalog(&b.name, provider);
            st.connections.lock().unwrap().push(ConnectionInfo {
                name: b.name,
                kind: b.kind,
                region: Some(region),
            });
            Ok(StatusCode::CREATED)
        }
        "hive" => {
            let provider = weft_catalog_hive::HiveCatalog::from_config(&b.name, &b.options)
                .map_err(|e| (StatusCode::BAD_REQUEST, format!("{e:?}")))?;
            st.engine.register_catalog(&b.name, Arc::new(provider));
            st.connections.lock().unwrap().push(ConnectionInfo {
                name: b.name,
                kind: b.kind,
                region: None,
            });
            Ok(StatusCode::CREATED)
        }
        other => Err((
            StatusCode::BAD_REQUEST,
            format!("unsupported connection kind: {other}"),
        )),
    }
}

// ─────────────────────────────── Catalog: sample data + introspection ───────────────────────────

/// Register a small set of sample tables under `main.sales` so the SQL editor + catalog browser
/// have real, queryable data out of the box (e.g. `SELECT * FROM main.sales.monthly_revenue`). In
/// production these come from the user's registered catalogs (local + external HMS/Glue/UC).
fn seed_sample_data(engine: &Engine) {
    use weft_loom::arrow::array::{Float64Array, Int64Array, StringArray};
    use weft_loom::arrow::datatypes::{DataType, Field, Schema};
    use weft_loom::arrow::record_batch::RecordBatch;

    let revenue_schema = Arc::new(Schema::new(vec![
        Field::new("month", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("revenue", DataType::Float64, false),
    ]));
    let revenue = RecordBatch::try_new(
        revenue_schema,
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

    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("customer", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("status", DataType::Utf8, false),
    ]));
    let orders = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(Int64Array::from(vec![1001, 1002, 1003, 1004, 1005])),
            Arc::new(StringArray::from(vec![
                "acme", "globex", "acme", "initech", "globex",
            ])),
            Arc::new(Float64Array::from(vec![
                2500.0, 1800.0, 3200.0, 950.0, 4100.0,
            ])),
            Arc::new(StringArray::from(vec![
                "shipped",
                "pending",
                "shipped",
                "cancelled",
                "shipped",
            ])),
        ],
    );

    let customers_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("segment", DataType::Utf8, false),
    ]));
    let customers = RecordBatch::try_new(
        customers_schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Acme Corp", "Globex", "Initech"])),
            Arc::new(StringArray::from(vec!["enterprise", "smb", "enterprise"])),
        ],
    );

    for (table, batch) in [
        ("monthly_revenue", revenue),
        ("orders", orders),
        ("customers", customers),
    ] {
        if let Ok(b) = batch {
            if let Err(e) = register_namespaced(engine, "main", "sales", table, b) {
                eprintln!("seed {table}: {e}");
            }
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
    for cat_name in ctx.catalog_names() {
        let Some(cat) = ctx.catalog(&cat_name) else {
            continue;
        };
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
                    let columns = tbl
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| CatalogColumn {
                            name: f.name().clone(),
                            data_type: format!("{}", f.data_type()),
                        })
                        .collect();
                    tables.push(CatalogTable {
                        name: tbl_name,
                        columns,
                    });
                }
            }
            schemas.push(CatalogSchema {
                name: sch_name,
                tables,
            });
        }
        // Only surface catalogs that actually hold schemas with tables (skip empty internals).
        if schemas.iter().any(|s| !s.tables.is_empty()) {
            out.push(CatalogNamespace {
                name: cat_name,
                schemas,
            });
        }
    }
    Json(out)
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
