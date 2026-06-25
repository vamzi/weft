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
    next_id: Arc<Mutex<u64>>,
    users: Arc<Mutex<HashMap<String, UserRecord>>>,
    groups: Arc<Mutex<HashMap<String, Vec<String>>>>,
    grants: Arc<Mutex<Vec<Grant>>>,
    engine: Arc<Engine>,
    jwt_secret: Arc<Vec<u8>>,
    web_dir: Arc<PathBuf>,
}

impl AppState {
    /// Build state seeding a single local admin (`username`/`password`) and serving the SPA from
    /// `web_dir`. The JWT secret is provided by the caller (env in production).
    pub fn new(username: &str, password: &str, jwt_secret: Vec<u8>, web_dir: PathBuf) -> Self {
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
        Self {
            clusters: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
            users: Arc::new(Mutex::new(users)),
            groups: Arc::new(Mutex::new(groups)),
            grants: Arc::new(Mutex::new(Vec::new())),
            engine: Arc::new(Engine::new()),
            jwt_secret: Arc::new(jwt_secret),
            web_dir: Arc::new(web_dir),
        }
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
        .route("/api/sql", post(run_sql))
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
    let cluster = Cluster {
        id: st.new_id(),
        name: body.name,
        state: Phase::Pending.as_str().to_string(),
        worker_min: body.worker_min,
        worker_max: body.worker_max,
        worker_size: body.worker_size,
    };
    st.clusters
        .lock()
        .unwrap()
        .insert(cluster.id.clone(), cluster.clone());
    (StatusCode::CREATED, Json(cluster))
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

async fn delete_cluster(State(st): State<AppState>, Path(id): Path<String>) -> StatusCode {
    if st.clusters.lock().unwrap().remove(&id).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn start_cluster(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    transition(&st, &id, Phase::Running)
}

async fn stop_cluster(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Cluster>, StatusCode> {
    transition(&st, &id, Phase::Terminated)
}

fn transition(st: &AppState, id: &str, to: Phase) -> Result<Json<Cluster>, StatusCode> {
    let mut map = st.clusters.lock().unwrap();
    let c = map.get_mut(id).ok_or(StatusCode::NOT_FOUND)?;
    c.state = to.as_str().to_string();
    Ok(Json(c.clone()))
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
    let state = AppState::new(&user, &password, secret.into_bytes(), web_dir);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(state)).await
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
        let id = body_json(resp).await["id"].as_str().unwrap().to_string();

        let resp = app(st.clone())
            .oneshot(
                Request::post(format!("/api/clusters/{id}/start"))
                    .header("authorization", &auth)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["state"], "RUNNING");
    }
}
