//! The axum HTTP server: serves the [`crate::ROUTES`] surface.
//!
//! This Wave-1 implementation wires the health probe, the current-principal endpoint, and the
//! **cluster lifecycle** endpoints against an in-memory store, so the web Clusters page and the
//! cluster-manager integration have a live API to build against. Persistence (`weft-meta` over
//! Postgres), SSO/JWT auth middleware, and the Spark Connect client pool that routes SQL to a
//! cluster's endpoint layer on top without changing this surface.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use weft_clustermgr::Phase;

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

/// The shared application state. In-memory today; a `weft-meta` repository handle later.
#[derive(Clone, Default)]
pub struct AppState {
    clusters: Arc<Mutex<HashMap<String, Cluster>>>,
    next_id: Arc<Mutex<u64>>,
}

impl AppState {
    fn new_id(&self) -> String {
        let mut n = self.next_id.lock().unwrap();
        *n += 1;
        format!("cluster-{n}")
    }
}

/// Build the gateway router. Pure function of [`AppState`] so tests can drive it via
/// `tower::ServiceExt::oneshot` without binding a socket.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/api/me", get(me))
        .route("/api/clusters", get(list_clusters).post(create_cluster))
        .route("/api/clusters/:id", get(get_cluster).delete(delete_cluster))
        .route("/api/clusters/:id/start", post(start_cluster))
        .route("/api/clusters/:id/stop", post(stop_cluster))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

/// The current principal (mock until OIDC/JWT middleware lands).
async fn me() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "user": "dev@example.com",
        "groups": ["admins"],
        "authenticated": false
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

/// Set a cluster's state (the in-memory stand-in for the operator reconciling a `WeftCluster`).
fn transition(st: &AppState, id: &str, to: Phase) -> Result<Json<Cluster>, StatusCode> {
    let mut map = st.clusters.lock().unwrap();
    let c = map.get_mut(id).ok_or(StatusCode::NOT_FOUND)?;
    c.state = to.as_str().to_string();
    Ok(Json(c.clone()))
}

/// Bind and serve the gateway on `addr` (e.g. `0.0.0.0:8080`).
pub async fn serve(addr: &str) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app(AppState::default())).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt; // for `oneshot`

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    }

    #[tokio::test]
    async fn healthz_ok() {
        let resp = app(AppState::default())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cluster_crud_lifecycle() {
        let state = AppState::default();

        // Create.
        let resp = app(state.clone())
            .oneshot(
                Request::post("/api/clusters")
                    .header("content-type", "application/json")
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
        assert_eq!(created["state"], "PENDING");
        assert_eq!(created["worker_max"], 8);

        // Start → RUNNING.
        let resp = app(state.clone())
            .oneshot(
                Request::post(format!("/api/clusters/{id}/start"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["state"], "RUNNING");

        // Stop → TERMINATED.
        let resp = app(state.clone())
            .oneshot(
                Request::post(format!("/api/clusters/{id}/stop"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(body_json(resp).await["state"], "TERMINATED");

        // List shows one.
        let resp = app(state.clone())
            .oneshot(Request::get("/api/clusters").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(body_json(resp).await.as_array().unwrap().len(), 1);

        // Delete → 204, then 404.
        let resp = app(state.clone())
            .oneshot(
                Request::delete(format!("/api/clusters/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        let resp = app(state)
            .oneshot(
                Request::get(format!("/api/clusters/{id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
