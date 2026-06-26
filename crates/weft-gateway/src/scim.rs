//! **SCIM 2.0** provisioning (a pragmatic subset of RFC 7643/7644) so an external IdP — Okta, Azure
//! AD/Entra, etc. — can push users and groups into the gateway's directory.
//!
//! It maps directly onto the same in-memory `users`/`groups` store the local admin and OIDC login
//! use: SCIM `userName` → the user store key, `displayName` → the group name, `members[].value` /
//! `groups[].value` → membership. Responses are minimally RFC-compliant (`schemas`, `id`, `meta`),
//! which is enough for the standard Okta/Azure SCIM connectors.
//!
//! All routes sit behind [`scim_guard`]: a static bearer token (`WEFT_SCIM_TOKEN`). If the env var is
//! unset, SCIM is disabled (503); a present-but-mismatched token is 401.

use axum::extract::{Path, Query, Request, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::server::AppState;

const USER_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
const GROUP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";
const LIST_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:ListResponse";
const ERROR_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:Error";

// ─────────────────────────────────────────── Guard ─────────────────────────────────────────────

/// Bearer-token gate for `/scim/*`. 503 if `WEFT_SCIM_TOKEN` is unset (SCIM disabled); 401 if the
/// `Authorization: Bearer <token>` is absent or doesn't match.
pub async fn scim_guard(
    State(st): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = st.scim_token().as_ref().clone() else {
        return scim_error(StatusCode::SERVICE_UNAVAILABLE, "SCIM provisioning is not enabled");
    };
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match presented {
        Some(tok) if constant_time_eq(tok.as_bytes(), expected.as_bytes()) => next.run(req).await,
        _ => scim_error(StatusCode::UNAUTHORIZED, "invalid or missing bearer token"),
    }
}

/// Length-independent constant-ish-time compare (avoids leaking the token via early-exit timing).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A SCIM error response (`schemas` + `status` + `detail`).
fn scim_error(status: StatusCode, detail: &str) -> Response {
    (
        status,
        Json(json!({
            "schemas": [ERROR_SCHEMA],
            "status": status.as_u16().to_string(),
            "detail": detail,
        })),
    )
        .into_response()
}

// ─────────────────────────────────────── Shaping helpers ───────────────────────────────────────

/// Shape a stored user as a SCIM User resource. `groups` are surfaced read-only (Okta tolerates the
/// minimal form). `id` doubles as the `userName` (we key users by name).
fn user_resource(username: &str, groups: &[String]) -> Value {
    json!({
        "schemas": [USER_SCHEMA],
        "id": username,
        "userName": username,
        "active": true,
        "groups": groups.iter().map(|g| json!({ "value": g, "display": g })).collect::<Vec<_>>(),
        "meta": {
            "resourceType": "User",
            "location": format!("/scim/v2/Users/{username}"),
        }
    })
}

/// Shape a stored group as a SCIM Group resource. `id` doubles as the `displayName`.
fn group_resource(name: &str, members: &[String]) -> Value {
    json!({
        "schemas": [GROUP_SCHEMA],
        "id": name,
        "displayName": name,
        "members": members.iter().map(|m| json!({ "value": m, "display": m })).collect::<Vec<_>>(),
        "meta": {
            "resourceType": "Group",
            "location": format!("/scim/v2/Groups/{name}"),
        }
    })
}

/// Wrap resources in a SCIM ListResponse.
fn list_response(resources: Vec<Value>) -> Value {
    json!({
        "schemas": [LIST_SCHEMA],
        "totalResults": resources.len(),
        "startIndex": 1,
        "itemsPerPage": resources.len(),
        "Resources": resources,
    })
}

/// Parse a SCIM `?filter=attr eq "value"` into `(attr, value)` (the only filter Okta/Azure send for
/// these resources). Best-effort: returns `None` if it isn't that shape.
fn parse_eq_filter(filter: &str) -> Option<(String, String)> {
    let mut parts = filter.splitn(3, char::is_whitespace);
    let attr = parts.next()?.trim();
    let op = parts.next()?.trim();
    let raw = parts.next()?.trim();
    if !op.eq_ignore_ascii_case("eq") {
        return None;
    }
    let value = raw.trim_matches('"').to_string();
    Some((attr.to_string(), value))
}

/// Query for the list endpoints (`?filter=...`).
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    /// SCIM filter expression (we support `attr eq "value"`).
    #[serde(default)]
    pub filter: Option<String>,
}

// ─────────────────────────────────────────── Users ─────────────────────────────────────────────

/// Pull `userName` (+ any `groups[].value`) out of a SCIM User request body.
fn parse_user_body(body: &Value) -> Option<(String, Vec<String>)> {
    let username = body.get("userName")?.as_str()?.to_string();
    if username.is_empty() {
        return None;
    }
    let groups = body
        .get("groups")
        .and_then(|g| g.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| m.get("value").and_then(|v| v.as_str()).map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some((username, groups))
}

/// `POST /scim/v2/Users` — create (or upsert) a user.
pub async fn create_user(State(st): State<AppState>, Json(body): Json<Value>) -> Response {
    let Some((username, groups)) = parse_user_body(&body) else {
        return scim_error(StatusCode::BAD_REQUEST, "userName is required");
    };
    st.scim_upsert_user(&username, &groups);
    st.save_users();
    st.save_groups();
    (StatusCode::CREATED, Json(user_resource(&username, &groups))).into_response()
}

/// `GET /scim/v2/Users` — list, with optional `?filter=userName eq "x"`.
pub async fn list_users(State(st): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let all = st.scim_list_users();
    let filtered: Vec<Value> = match q.filter.as_deref().and_then(parse_eq_filter) {
        Some((attr, value)) if attr.eq_ignore_ascii_case("userName") => all
            .into_iter()
            .filter(|(u, _)| *u == value)
            .map(|(u, g)| user_resource(&u, &g))
            .collect(),
        _ => all.into_iter().map(|(u, g)| user_resource(&u, &g)).collect(),
    };
    Json(list_response(filtered)).into_response()
}

/// `GET /scim/v2/Users/:id` — fetch one user (id == userName).
pub async fn get_user(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    match st.scim_get_user(&id) {
        Some(groups) => Json(user_resource(&id, &groups)).into_response(),
        None => scim_error(StatusCode::NOT_FOUND, "user not found"),
    }
}

/// `PUT /scim/v2/Users/:id` — replace a user (id == userName; groups from body).
pub async fn put_user(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let groups = body
        .get("groups")
        .and_then(|g| g.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| m.get("value").and_then(|v| v.as_str()).map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    st.scim_upsert_user(&id, &groups);
    st.save_users();
    st.save_groups();
    Json(user_resource(&id, &groups)).into_response()
}

/// `PATCH /scim/v2/Users/:id` — best-effort: Okta/Azure most commonly toggle `active`. We accept the
/// op and return the current resource (deactivation maps to delete).
pub async fn patch_user(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    if st.scim_get_user(&id).is_none() {
        return scim_error(StatusCode::NOT_FOUND, "user not found");
    }
    // Look for an `active: false` set op → treat as soft delete (de-provision).
    if patch_sets_active_false(&body) {
        st.scim_delete_user(&id);
        st.save_users();
        st.save_groups();
        return (StatusCode::NO_CONTENT, "").into_response();
    }
    let groups = st.scim_get_user(&id).unwrap_or_default();
    Json(user_resource(&id, &groups)).into_response()
}

/// True if a SCIM PATCH body contains an op setting `active` to `false`.
fn patch_sets_active_false(body: &Value) -> bool {
    let Some(ops) = body.get("Operations").and_then(|o| o.as_array()) else {
        return false;
    };
    ops.iter().any(|op| {
        let is_replace_or_add = op
            .get("op")
            .and_then(|o| o.as_str())
            .map(|o| o.eq_ignore_ascii_case("replace") || o.eq_ignore_ascii_case("add"))
            .unwrap_or(false);
        if !is_replace_or_add {
            return false;
        }
        // `value` can be `{ "active": false }` or the path can be `active` with `value: false`.
        let path_active = op
            .get("path")
            .and_then(|p| p.as_str())
            .map(|p| p.eq_ignore_ascii_case("active"))
            .unwrap_or(false);
        if path_active {
            return op.get("value").and_then(|v| v.as_bool()) == Some(false);
        }
        op.get("value")
            .and_then(|v| v.get("active"))
            .and_then(|v| v.as_bool())
            == Some(false)
    })
}

/// `DELETE /scim/v2/Users/:id` — de-provision a user.
pub async fn delete_user(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    if st.scim_delete_user(&id) {
        st.save_users();
        st.save_groups();
        (StatusCode::NO_CONTENT, "").into_response()
    } else {
        scim_error(StatusCode::NOT_FOUND, "user not found")
    }
}

// ─────────────────────────────────────────── Groups ────────────────────────────────────────────

/// Pull `displayName` (+ `members[].value`) from a SCIM Group request body.
fn parse_group_body(body: &Value) -> Option<(String, Vec<String>)> {
    let name = body.get("displayName")?.as_str()?.to_string();
    if name.is_empty() {
        return None;
    }
    let members = group_members(body);
    Some((name, members))
}

/// Extract `members[].value` from a SCIM Group body.
fn group_members(body: &Value) -> Vec<String> {
    body.get("members")
        .and_then(|m| m.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|m| m.get("value").and_then(|v| v.as_str()).map(String::from))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

/// `POST /scim/v2/Groups` — create (or upsert) a group.
pub async fn create_group(State(st): State<AppState>, Json(body): Json<Value>) -> Response {
    let Some((name, members)) = parse_group_body(&body) else {
        return scim_error(StatusCode::BAD_REQUEST, "displayName is required");
    };
    st.scim_set_group(&name, members.clone());
    st.save_groups();
    st.save_users();
    (StatusCode::CREATED, Json(group_resource(&name, &members))).into_response()
}

/// `GET /scim/v2/Groups` — list, with optional `?filter=displayName eq "x"`.
pub async fn list_groups(State(st): State<AppState>, Query(q): Query<ListQuery>) -> Response {
    let all = st.scim_list_groups();
    let filtered: Vec<Value> = match q.filter.as_deref().and_then(parse_eq_filter) {
        Some((attr, value)) if attr.eq_ignore_ascii_case("displayName") => all
            .into_iter()
            .filter(|(n, _)| *n == value)
            .map(|(n, m)| group_resource(&n, &m))
            .collect(),
        _ => all.into_iter().map(|(n, m)| group_resource(&n, &m)).collect(),
    };
    Json(list_response(filtered)).into_response()
}

/// `GET /scim/v2/Groups/:id` — fetch one group (id == displayName).
pub async fn get_group(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    match st.scim_get_group(&id) {
        Some(members) => Json(group_resource(&id, &members)).into_response(),
        None => scim_error(StatusCode::NOT_FOUND, "group not found"),
    }
}

/// `PUT /scim/v2/Groups/:id` — replace a group's membership.
pub async fn put_group(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let members = group_members(&body);
    st.scim_set_group(&id, members.clone());
    st.save_groups();
    st.save_users();
    Json(group_resource(&id, &members)).into_response()
}

/// `PATCH /scim/v2/Groups/:id` — apply the common Okta/Azure member ops (`add`/`remove`/`replace`).
pub async fn patch_group(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Response {
    let Some(mut members) = st.scim_get_group(&id) else {
        return scim_error(StatusCode::NOT_FOUND, "group not found");
    };
    if let Some(ops) = body.get("Operations").and_then(|o| o.as_array()) {
        for op in ops {
            apply_member_op(&mut members, op);
        }
    }
    st.scim_set_group(&id, members.clone());
    st.save_groups();
    st.save_users();
    Json(group_resource(&id, &members)).into_response()
}

/// Apply one SCIM PATCH operation to a group's member list. Handles the member-targeting ops Okta
/// and Azure emit: `add`/`replace` (members from `value[].value`) and `remove` (by `path` filter or
/// `value[].value`). Best-effort — anything unrecognized is ignored.
fn apply_member_op(members: &mut Vec<String>, op: &Value) {
    let kind = op
        .get("op")
        .and_then(|o| o.as_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    let path = op.get("path").and_then(|p| p.as_str()).unwrap_or("");
    // Only member ops are relevant to our model.
    let targets_members = path.is_empty() || path.starts_with("members");

    let values_in_op: Vec<String> = op
        .get("value")
        .map(|v| extract_member_values(v))
        .unwrap_or_default();

    match kind.as_str() {
        "add" if targets_members => {
            for m in values_in_op {
                if !members.contains(&m) {
                    members.push(m);
                }
            }
        }
        "replace" if targets_members && !values_in_op.is_empty() => {
            *members = values_in_op;
        }
        "remove" if targets_members => {
            if let Some(target) = member_from_remove_path(path) {
                members.retain(|m| m != &target);
            } else {
                for m in values_in_op {
                    members.retain(|x| x != &m);
                }
            }
            // `remove` on the whole `members` path with no filter clears it.
            if path == "members" && member_from_remove_path(path).is_none() && op.get("value").is_none() {
                members.clear();
            }
        }
        _ => {}
    }
}

/// Extract member id strings from a PATCH op `value` (either `[{value: "x"}]`, `["x"]`, or `"x"`).
fn extract_member_values(value: &Value) -> Vec<String> {
    match value {
        Value::Array(arr) => arr
            .iter()
            .filter_map(|m| {
                m.get("value")
                    .and_then(|v| v.as_str())
                    .or_else(|| m.as_str())
                    .map(String::from)
            })
            .collect(),
        Value::String(s) => vec![s.clone()],
        _ => vec![],
    }
}

/// Parse a SCIM `remove` path like `members[value eq "alice"]` → `alice`.
fn member_from_remove_path(path: &str) -> Option<String> {
    let start = path.find('[')? + 1;
    let end = path.rfind(']')?;
    let inner = path.get(start..end)?;
    // inner ~ `value eq "alice"`
    let q1 = inner.find('"')? + 1;
    let q2 = inner.rfind('"')?;
    if q2 > q1 {
        inner.get(q1..q2).map(String::from)
    } else {
        None
    }
}

/// `DELETE /scim/v2/Groups/:id` — remove a group.
pub async fn delete_group(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    if st.scim_delete_group(&id) {
        st.save_groups();
        (StatusCode::NO_CONTENT, "").into_response()
    } else {
        scim_error(StatusCode::NOT_FOUND, "group not found")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_eq_filter_username() {
        let f = parse_eq_filter(r#"userName eq "alice@example.com""#).unwrap();
        assert_eq!(f.0, "userName");
        assert_eq!(f.1, "alice@example.com");
        let g = parse_eq_filter(r#"displayName eq "analysts""#).unwrap();
        assert_eq!(g, ("displayName".to_string(), "analysts".to_string()));
        // Unsupported op → None.
        assert!(parse_eq_filter(r#"userName co "ali""#).is_none());
    }

    #[test]
    fn parse_user_body_extracts_name_and_groups() {
        let body = json!({
            "schemas": [USER_SCHEMA],
            "userName": "carol",
            "groups": [{ "value": "admins" }, { "value": "analysts" }],
        });
        let (u, g) = parse_user_body(&body).unwrap();
        assert_eq!(u, "carol");
        assert_eq!(g, vec!["admins", "analysts"]);
        // Missing userName → None.
        assert!(parse_user_body(&json!({ "active": true })).is_none());
    }

    #[test]
    fn user_resource_is_scim_shaped() {
        let r = user_resource("dave", &["admins".into()]);
        assert_eq!(r["schemas"][0], USER_SCHEMA);
        assert_eq!(r["id"], "dave");
        assert_eq!(r["userName"], "dave");
        assert_eq!(r["active"], true);
        assert_eq!(r["meta"]["resourceType"], "User");
        assert_eq!(r["meta"]["location"], "/scim/v2/Users/dave");
        assert_eq!(r["groups"][0]["value"], "admins");
    }

    #[test]
    fn group_resource_and_list_shape() {
        let g = group_resource("analysts", &["alice".into(), "bob".into()]);
        assert_eq!(g["schemas"][0], GROUP_SCHEMA);
        assert_eq!(g["displayName"], "analysts");
        assert_eq!(g["members"][1]["value"], "bob");
        let list = list_response(vec![g]);
        assert_eq!(list["schemas"][0], LIST_SCHEMA);
        assert_eq!(list["totalResults"], 1);
        assert_eq!(list["Resources"][0]["displayName"], "analysts");
    }

    #[test]
    fn patch_member_add_remove_replace() {
        let mut members = vec!["alice".to_string()];
        // add bob
        apply_member_op(
            &mut members,
            &json!({ "op": "add", "path": "members", "value": [{ "value": "bob" }] }),
        );
        assert_eq!(members, vec!["alice", "bob"]);
        // add duplicate alice → no-op
        apply_member_op(
            &mut members,
            &json!({ "op": "Add", "value": [{ "value": "alice" }] }),
        );
        assert_eq!(members, vec!["alice", "bob"]);
        // remove bob via path filter
        apply_member_op(
            &mut members,
            &json!({ "op": "remove", "path": "members[value eq \"bob\"]" }),
        );
        assert_eq!(members, vec!["alice"]);
        // replace with a fresh set
        apply_member_op(
            &mut members,
            &json!({ "op": "replace", "path": "members", "value": [{ "value": "carol" }, { "value": "dave" }] }),
        );
        assert_eq!(members, vec!["carol", "dave"]);
    }

    #[test]
    fn patch_detects_active_false() {
        assert!(patch_sets_active_false(&json!({
            "Operations": [{ "op": "replace", "value": { "active": false } }]
        })));
        assert!(patch_sets_active_false(&json!({
            "Operations": [{ "op": "Replace", "path": "active", "value": false }]
        })));
        assert!(!patch_sets_active_false(&json!({
            "Operations": [{ "op": "replace", "value": { "active": true } }]
        })));
        assert!(!patch_sets_active_false(&json!({ "Operations": [] })));
    }

    #[test]
    fn constant_time_eq_matches() {
        assert!(constant_time_eq(b"secret-token", b"secret-token"));
        assert!(!constant_time_eq(b"secret-token", b"secret-toker"));
        assert!(!constant_time_eq(b"short", b"longer-token"));
    }
}
