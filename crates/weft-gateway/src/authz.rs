//! Authorization helpers: identity resolution + admin gating for the control plane.
//!
//! Server-side membership is the source of truth. A principal's groups and admin status are resolved
//! from the gateway's user/group store keyed by the token `sub`, **not** trusted from the JWT body,
//! so a token cannot self-assert `groups:["admins"]` to reach a privileged route. (The JWT secret is
//! additionally required to be strong at startup — see [`crate::server::serve`] — so the token's
//! `sub` itself can't be forged.)
//!
//! Two seams use these:
//! - control-plane mutations (create/delete cluster, attach/detach connection, user/group/grant
//!   management) call [`require_admin`];
//! - workspace objects (notebooks, saved queries) call [`owns_or_admin`] so a principal reaches only
//!   objects it owns (closing the IDOR where any authenticated user could read another's by id);
//! - the SQL data path builds a governance [`weft_govern::Identity`] via [`identity_of`].

use axum::http::StatusCode;
use weft_govern::Identity;

use crate::server::{AppState, Claims};

/// Resolve the caller's governance [`Identity`] from server-side group membership (never the token
/// body). Feeds the `GovernedCatalog` enforcement on the SQL data path.
pub fn identity_of(st: &AppState, claims: &Claims) -> Identity {
    Identity::user(&claims.sub).with_groups(st.groups_of(&claims.sub))
}

/// Gate a privileged (admin-only) control-plane action. `403 Forbidden` unless the caller is a
/// member of the `admins` group, resolved server-side.
pub fn require_admin(st: &AppState, claims: &Claims) -> Result<(), StatusCode> {
    if st.is_admin(&claims.sub) {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

/// Whether `claims` may access an object owned by `owner` — the owner themselves, or an admin.
pub fn owns_or_admin(st: &AppState, claims: &Claims, owner: &str) -> bool {
    claims.sub == owner || st.is_admin(&claims.sub)
}
