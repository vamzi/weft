//! External-IdP **SSO via OpenID Connect** (authorization-code + PKCE).
//!
//! This layers on top of the gateway's existing session-JWT seam: after a successful OIDC login we
//! upsert the federated user into the same in-memory `users`/`groups` store the local admin uses,
//! then mint a session token with [`crate::server::AppState::issue_token`] — exactly the token a
//! local password login produces. The browser receives it via a fragment redirect (`/#token=...`).
//!
//! Provider integration uses the [`openidconnect`] crate (discovery, the PKCE code exchange, and
//! id_token validation: issuer, audience, nonce, expiry, and JWKS signature) with [`reqwest`] as
//! the async HTTP backend. Discovered provider metadata is cached on the [`AppState`] after the
//! first call so each login doesn't re-fetch `.well-known/openid-configuration`.
//!
//! Everything here is **public** (wired outside the Bearer-gated `/api/*` group): the browser hits
//! `/api/auth/sso/login` (302 → IdP), the IdP redirects back to `/api/auth/callback`, and
//! `/api/auth/config` lets the SPA decide whether to show the SSO button.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Redirect, Response};
use axum::http::StatusCode;
use axum::{Extension, Json};
use openidconnect::core::{CoreClient, CoreProviderMetadata, CoreResponseType};
use openidconnect::{
    AuthenticationFlow, AuthorizationCode, ClientId, ClientSecret, CsrfToken, IssuerUrl, Nonce,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
};
use serde::{Deserialize, Serialize};

use crate::server::{AppState, Claims};

/// How long a pending PKCE/state entry lives before it's pruned (the user must complete the
/// redirect dance within this window).
const PENDING_TTL: Duration = Duration::from_secs(300);

// ───────────────────────────────────────── Config ──────────────────────────────────────────────

/// OIDC SSO configuration, read from the environment. SSO is *disabled* (these handlers return 503)
/// unless [`OidcConfig::from_env`] returns `Some` — i.e. all of issuer + client id + client secret +
/// redirect URL are set.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// The IdP issuer URL (e.g. `https://cognito-idp.us-west-2.amazonaws.com/us-west-2_xxx`).
    pub issuer: String,
    /// The registered OAuth client id.
    pub client_id: String,
    /// The registered OAuth client secret.
    pub client_secret: String,
    /// Where the IdP redirects back (must match the app's `/api/auth/callback` URL exactly).
    pub redirect_url: String,
    /// The id_token claim holding the user's groups (an array). Default `cognito:groups`.
    pub groups_claim: String,
    /// The id_token claim used as the username/store key. Default `email`.
    pub username_claim: String,
    /// A human label for the SSO button in the UI. Default `SSO`.
    pub provider_label: String,
}

impl OidcConfig {
    /// Build from env. Returns `None` (SSO disabled) unless `WEFT_OIDC_ISSUER`,
    /// `WEFT_OIDC_CLIENT_ID`, `WEFT_OIDC_CLIENT_SECRET`, and `WEFT_OIDC_REDIRECT_URL` are all set.
    pub fn from_env() -> Option<Self> {
        let issuer = non_empty(std::env::var("WEFT_OIDC_ISSUER").ok())?;
        let client_id = non_empty(std::env::var("WEFT_OIDC_CLIENT_ID").ok())?;
        let client_secret = non_empty(std::env::var("WEFT_OIDC_CLIENT_SECRET").ok())?;
        let redirect_url = non_empty(std::env::var("WEFT_OIDC_REDIRECT_URL").ok())?;
        Some(Self {
            issuer,
            client_id,
            client_secret,
            redirect_url,
            groups_claim: std::env::var("WEFT_OIDC_GROUPS_CLAIM")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "cognito:groups".into()),
            username_claim: std::env::var("WEFT_OIDC_USERNAME_CLAIM")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "email".into()),
            provider_label: std::env::var("WEFT_OIDC_PROVIDER_LABEL")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "SSO".into()),
        })
    }

    /// Build from explicit fields (the admin runtime-config path). Optional claim/label fields fall
    /// back to the documented defaults when empty.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        issuer: String,
        client_id: String,
        client_secret: String,
        redirect_url: String,
        groups_claim: Option<String>,
        username_claim: Option<String>,
        provider_label: Option<String>,
    ) -> Self {
        Self {
            issuer,
            client_id,
            client_secret,
            redirect_url,
            groups_claim: groups_claim
                .filter(|s| !s.is_empty())
                .unwrap_or_else(default_groups_claim),
            username_claim: username_claim
                .filter(|s| !s.is_empty())
                .unwrap_or_else(default_username_claim),
            provider_label: provider_label
                .filter(|s| !s.is_empty())
                .unwrap_or_else(default_provider_label),
        }
    }
}

fn default_groups_claim() -> String {
    "cognito:groups".into()
}
fn default_username_claim() -> String {
    "email".into()
}
fn default_provider_label() -> String {
    "SSO".into()
}

/// The serializable form of [`OidcConfig`] for DynamoDB persistence. Carries **all** fields including
/// the client secret (the persisted blob is trusted control-plane state, never returned to clients).
/// An `enabled: false` marker (no other fields) records that SSO was explicitly disabled, so a
/// previously-configured env seed doesn't silently re-enable it on the next restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOidc {
    /// Whether SSO is configured/enabled. `false` → a disabled marker; the other fields are ignored.
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    #[serde(default)]
    pub redirect_url: String,
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
    #[serde(default = "default_username_claim")]
    pub username_claim: String,
    #[serde(default = "default_provider_label")]
    pub provider_label: String,
}

fn default_true() -> bool {
    true
}

impl StoredOidc {
    /// The explicit "SSO is disabled" marker persisted on `DELETE /api/admin/sso`.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            issuer: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            redirect_url: String::new(),
            groups_claim: default_groups_claim(),
            username_claim: default_username_claim(),
            provider_label: default_provider_label(),
        }
    }
}

impl From<&OidcConfig> for StoredOidc {
    fn from(c: &OidcConfig) -> Self {
        Self {
            enabled: true,
            issuer: c.issuer.clone(),
            client_id: c.client_id.clone(),
            client_secret: c.client_secret.clone(),
            redirect_url: c.redirect_url.clone(),
            groups_claim: c.groups_claim.clone(),
            username_claim: c.username_claim.clone(),
            provider_label: c.provider_label.clone(),
        }
    }
}

impl StoredOidc {
    /// Reconstruct the in-memory [`OidcConfig`] from a stored blob, or `None` for a disabled marker
    /// (or a blob missing the required issuer/client fields).
    pub fn into_config(self) -> Option<OidcConfig> {
        if !self.enabled
            || self.issuer.is_empty()
            || self.client_id.is_empty()
            || self.redirect_url.is_empty()
        {
            return None;
        }
        Some(OidcConfig {
            issuer: self.issuer,
            client_id: self.client_id,
            client_secret: self.client_secret,
            redirect_url: self.redirect_url,
            groups_claim: self.groups_claim,
            username_claim: self.username_claim,
            provider_label: self.provider_label,
        })
    }
}

fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.trim().is_empty())
}

/// Derive the `/api/auth/callback` redirect URL the IdP must be configured with, from
/// `WEFT_PUBLIC_BASE` (trailing slash trimmed). Empty base → a relative `/api/auth/callback`.
pub(crate) fn callback_url() -> String {
    let base = std::env::var("WEFT_PUBLIC_BASE")
        .unwrap_or_default()
        .trim_end_matches('/')
        .to_string();
    format!("{base}/api/auth/callback")
}

/// 403 unless the principal is in the `admins` group. Used to gate the admin config surface.
pub(crate) fn require_admin(claims: &Claims) -> Result<(), (StatusCode, String)> {
    if claims.groups.iter().any(|g| g == "admins") {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, "admin privileges required".into()))
    }
}

// ───────────────────────────────── Pending-auth (PKCE/state) store ──────────────────────────────

/// One in-flight authorization request, keyed by the opaque `state` value we hand the IdP. Holds the
/// PKCE verifier + nonce we must replay when the IdP redirects back. Pruned by TTL.
pub struct PendingAuth {
    /// The PKCE code verifier (replayed at the token exchange).
    pub verifier: String,
    /// The nonce we asked the IdP to echo into the id_token (replay-protection).
    pub nonce: String,
    /// When this entry was created (for TTL pruning).
    pub created: Instant,
}

/// The PKCE/state store: `state -> PendingAuth`, with TTL pruning on insert.
#[derive(Default)]
pub struct PendingStore {
    inner: HashMap<String, PendingAuth>,
}

impl PendingStore {
    /// Insert a pending entry, pruning anything past its TTL first.
    pub fn insert(&mut self, state: String, pending: PendingAuth) {
        let now = Instant::now();
        self.inner
            .retain(|_, p| now.duration_since(p.created) < PENDING_TTL);
        self.inner.insert(state, pending);
    }

    /// Remove and return the entry for `state` (single-use), if present and not expired.
    pub fn take(&mut self, state: &str) -> Option<PendingAuth> {
        let p = self.inner.remove(state)?;
        if Instant::now().duration_since(p.created) >= PENDING_TTL {
            None
        } else {
            Some(p)
        }
    }
}

// ─────────────────────────────────── Claim extraction (pure) ────────────────────────────────────

/// What we resolve out of a validated id_token: the store username + the user's groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedIdentity {
    /// The username / store key.
    pub username: String,
    /// The user's groups (possibly empty).
    pub groups: Vec<String>,
}

/// Extract `(username, groups)` from a decoded-claims JSON object given the configured claim names.
///
/// Pure and provider-agnostic so it can be unit-tested with a `serde_json::Value` fixture (no live
/// IdP). Username resolution: `username_claim` → `email` → `sub`. Groups: `groups_claim` as a JSON
/// array of strings (empty if absent or not an array). Returns `None` only if no username at all can
/// be resolved (no username claim, no email, no sub).
pub fn extract_identity(
    claims: &serde_json::Value,
    username_claim: &str,
    groups_claim: &str,
) -> Option<ResolvedIdentity> {
    let str_at = |key: &str| -> Option<String> {
        claims
            .get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
    };
    let username = str_at(username_claim)
        .or_else(|| str_at("email"))
        .or_else(|| str_at("sub"))?;
    let groups = claims
        .get(groups_claim)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|g| g.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Some(ResolvedIdentity { username, groups })
}

// ─────────────────────────────────────── Provider client ───────────────────────────────────────

/// Discover (or reuse the cached) provider metadata, then build the auth-code-flow [`CoreClient`].
///
/// openidconnect 4.x encodes which endpoints are configured in the client's *type* (the
/// `EndpointSet`/`EndpointNotSet` typestate). `from_provider_metadata` populates the auth/token/etc.
/// endpoints from discovery and `set_redirect_uri` marks the redirect set, so the result is a fully
/// usable client — but its concrete type differs from the bare [`CoreClient`] alias, which is why we
/// let it infer through `impl Trait`-style returns rather than naming it.
async fn discover_metadata(st: &AppState, cfg: &OidcConfig) -> Result<CoreProviderMetadata, String> {
    let cached = { st.oidc_meta().lock().unwrap().clone() };
    if let Some(m) = cached {
        return Ok(m);
    }
    let http = http_client()?;
    let issuer = IssuerUrl::new(cfg.issuer.clone()).map_err(|e| format!("issuer: {e}"))?;
    let m = CoreProviderMetadata::discover_async(issuer, &http)
        .await
        .map_err(|e| format!("discovery: {e}"))?;
    *st.oidc_meta().lock().unwrap() = Some(m.clone());
    Ok(m)
}

/// Construct the configured OIDC client (redirect URI set) from discovered `metadata` + `cfg`.
///
/// A macro (not a function) because openidconnect 4.x's client type carries an unnameable endpoint
/// typestate; expanding inline lets each call site infer it. Yields a `Result<_, String>`.
macro_rules! oidc_client {
    ($metadata:expr, $cfg:expr) => {{
        match RedirectUrl::new($cfg.redirect_url.clone()) {
            Ok(redirect) => Ok(CoreClient::from_provider_metadata(
                $metadata,
                ClientId::new($cfg.client_id.clone()),
                Some(ClientSecret::new($cfg.client_secret.clone())),
            )
            .set_redirect_uri(redirect)),
            Err(e) => Err(format!("redirect: {e}")),
        }
    }};
}

/// A reqwest client configured for the OIDC flows: redirects disabled (per openidconnect's SSRF
/// guidance — the token/discovery endpoints must not be auto-followed).
fn http_client() -> Result<reqwest::Client, String> {
    reqwest::ClientBuilder::new()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| format!("http client: {e}"))
}

// ────────────────────────────────────────── Handlers ───────────────────────────────────────────

/// `GET /api/auth/config` (PUBLIC): whether SSO is configured + the provider label, so the SPA can
/// render the SSO button conditionally.
pub async fn auth_config(State(st): State<AppState>) -> Json<serde_json::Value> {
    let cfg = st.oidc().read().unwrap().clone();
    let (enabled, label) = match cfg {
        Some(cfg) => (true, cfg.provider_label.clone()),
        None => (false, "SSO".to_string()),
    };
    Json(serde_json::json!({ "sso_enabled": enabled, "provider_label": label }))
}

/// `GET /api/auth/sso/login` (PUBLIC): begin the OIDC PKCE flow. 503 if SSO disabled; otherwise
/// 302-redirect to the IdP's authorization endpoint (scopes `openid email profile`).
pub async fn sso_login(State(st): State<AppState>) -> Response {
    let Some(cfg) = st.oidc().read().unwrap().clone() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "SSO not configured").into_response();
    };
    let metadata = match discover_metadata(&st, &cfg).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("warn: OIDC discovery failed: {e}");
            return (StatusCode::BAD_GATEWAY, "SSO provider unavailable").into_response();
        }
    };
    let client = match oidc_client!(metadata, cfg) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("warn: OIDC client build failed: {e}");
            return (StatusCode::BAD_GATEWAY, "SSO provider unavailable").into_response();
        }
    };
    let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf_state, nonce) = client
        .authorize_url(
            AuthenticationFlow::<CoreResponseType>::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        .add_scope(Scope::new("openid".to_string()))
        .add_scope(Scope::new("email".to_string()))
        .add_scope(Scope::new("profile".to_string()))
        .set_pkce_challenge(challenge)
        .url();
    st.oidc_pending().lock().unwrap().insert(
        csrf_state.secret().clone(),
        PendingAuth {
            verifier: verifier.secret().clone(),
            nonce: nonce.secret().clone(),
            created: Instant::now(),
        },
    );
    Redirect::to(auth_url.as_str()).into_response()
}

/// Query for `GET /api/auth/callback`.
#[derive(Debug, Deserialize)]
pub struct CallbackQuery {
    /// The authorization code from the IdP.
    #[serde(default)]
    pub code: Option<String>,
    /// The opaque `state` we issued (looks up the pending PKCE entry).
    #[serde(default)]
    pub state: Option<String>,
    /// Set by the IdP on a denied/failed authorization.
    #[serde(default)]
    pub error: Option<String>,
}

/// `GET /api/auth/callback` (PUBLIC): finish the flow. Exchange the code (with the stored PKCE
/// verifier + client secret), validate the id_token, upsert the federated user + groups, mint a
/// session JWT, and 302-redirect to the SPA with the token in the URL fragment. Any failure
/// redirects to `/#sso_error=<reason>` (no internals leaked).
pub async fn callback(State(st): State<AppState>, Query(q): Query<CallbackQuery>) -> Response {
    match callback_inner(&st, q).await {
        Ok(jwt) => {
            let base = std::env::var("WEFT_PUBLIC_BASE").unwrap_or_default();
            Redirect::to(&format!("{base}/#token={jwt}")).into_response()
        }
        Err(reason) => Redirect::to(&format!("/#sso_error={reason}")).into_response(),
    }
}

/// The fallible body of [`callback`]. Returns the session JWT on success, or a *short* error reason
/// (safe to surface in the redirect — no provider internals).
async fn callback_inner(st: &AppState, q: CallbackQuery) -> Result<String, String> {
    if let Some(err) = q.error {
        return Err(sanitize_reason(&err));
    }
    let cfg = st.oidc().read().unwrap().clone().ok_or("sso_disabled")?;
    let code = q.code.ok_or("missing_code")?;
    let state = q.state.ok_or("missing_state")?;
    // Single-use: look up + remove the pending entry (CSRF/replay protection).
    let pending = st
        .oidc_pending()
        .lock()
        .unwrap()
        .take(&state)
        .ok_or("invalid_state")?;

    let metadata = discover_metadata(st, &cfg).await.map_err(|_| "provider_error")?;
    let client = oidc_client!(metadata, cfg).map_err(|_| "provider_error")?;
    let http = http_client().map_err(|_| "provider_error")?;

    let token_response = client
        .exchange_code(AuthorizationCode::new(code))
        .map_err(|_| "exchange_failed")?
        .set_pkce_verifier(PkceCodeVerifier::new(pending.verifier))
        .request_async(&http)
        .await
        .map_err(|_| "exchange_failed")?;

    let id_token =
        openidconnect::TokenResponse::id_token(&token_response).ok_or("no_id_token")?;
    let verifier = client.id_token_verifier();
    // openidconnect performs the security-critical validation here: issuer, audience, nonce, expiry,
    // and the JWKS signature. We bind to the default (empty) additional-claims type for validation,
    // then read the *full* claim set (including provider-specific ones like `cognito:groups`) out of
    // the now-trusted raw JWT payload, so the configurable `groups_claim`/`username_claim` resolve.
    id_token
        .claims(&verifier, &Nonce::new(pending.nonce))
        .map_err(|_| "id_token_invalid")?;
    let raw_claims = decode_jwt_payload(&id_token.to_string()).ok_or("id_token_invalid")?;
    let identity =
        extract_identity(&raw_claims, &cfg.username_claim, &cfg.groups_claim).ok_or("no_username")?;

    // Upsert the federated user + groups into the shared store, then persist.
    st.upsert_sso_user(&identity.username, &identity.groups);
    st.save_users();
    st.save_groups();

    st.issue_token(&identity.username, &identity.groups)
        .ok_or_else(|| "token_error".to_string())
}

// ──────────────────────────────────── Admin runtime config ─────────────────────────────────────

/// `PUT /api/admin/sso` body: the admin-supplied OIDC settings. `client_secret` may be empty/omitted
/// when editing an existing config (the stored secret is then reused). The `redirect_url` is *not*
/// accepted from the client — it's derived from `WEFT_PUBLIC_BASE` so it always matches this origin.
#[derive(Debug, Deserialize)]
pub struct SsoConfigInput {
    /// The IdP issuer URL.
    pub issuer: String,
    /// The registered OAuth client id.
    pub client_id: String,
    /// The OAuth client secret. Empty/omitted on edit → reuse the existing secret.
    #[serde(default)]
    pub client_secret: String,
    /// Optional human label for the SSO button.
    #[serde(default)]
    pub provider_label: Option<String>,
    /// Optional id_token claim for groups (default `cognito:groups`).
    #[serde(default)]
    pub groups_claim: Option<String>,
    /// Optional id_token claim for the username (default `email`).
    #[serde(default)]
    pub username_claim: Option<String>,
}

/// `GET /api/admin/sso` (ADMIN): the current SSO config for the admin panel. **Never** includes the
/// client secret — only `has_secret` signals whether one is stored. `callback_url` is the redirect
/// URI the admin must register at the IdP.
pub async fn get_sso(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&claims)?;
    let cfg = st.oidc().read().unwrap().clone();
    let body = match cfg {
        Some(c) => serde_json::json!({
            "enabled": true,
            "issuer": c.issuer,
            "client_id": c.client_id,
            "provider_label": c.provider_label,
            "groups_claim": c.groups_claim,
            "username_claim": c.username_claim,
            "has_secret": !c.client_secret.is_empty(),
            "callback_url": callback_url(),
        }),
        None => serde_json::json!({
            "enabled": false,
            "issuer": "",
            "client_id": "",
            "provider_label": default_provider_label(),
            "groups_claim": default_groups_claim(),
            "username_claim": default_username_claim(),
            "has_secret": false,
            "callback_url": callback_url(),
        }),
    };
    Ok(Json(body))
}

/// `PUT /api/admin/sso` (ADMIN): set/replace the SSO config. Validates the issuer by running OIDC
/// discovery (400 on failure), reuses the stored secret if the body omits one, swaps the config into
/// the live `RwLock`, clears the discovery cache (so the next login rediscovers the new issuer), and
/// persists to DynamoDB.
pub async fn put_sso(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
    Json(input): Json<SsoConfigInput>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    require_admin(&claims)?;
    let issuer = input.issuer.trim().to_string();
    let client_id = input.client_id.trim().to_string();
    if issuer.is_empty() || client_id.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "issuer and client_id are required".into(),
        ));
    }

    // Secret-reuse: if the body omits a secret and a config already exists, keep the stored one.
    let existing_secret = st
        .oidc()
        .read()
        .unwrap()
        .as_ref()
        .map(|c| c.client_secret.clone());
    let client_secret = if input.client_secret.trim().is_empty() {
        existing_secret.filter(|s| !s.is_empty()).ok_or((
            StatusCode::BAD_REQUEST,
            "client_secret is required (no existing secret to reuse)".to_string(),
        ))?
    } else {
        input.client_secret.trim().to_string()
    };

    let cfg = OidcConfig::from_parts(
        issuer,
        client_id,
        client_secret,
        callback_url(),
        input.groups_claim,
        input.username_claim,
        input.provider_label,
    );

    // Validate the issuer by actually discovering it (the same path login uses), uncached.
    discover_uncached(&cfg)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("could not discover OIDC issuer: {e}")))?;

    // Swap into the live config, invalidate the cached metadata, persist.
    *st.oidc().write().unwrap() = Some(cfg.clone());
    *st.oidc_meta().lock().unwrap() = None;
    st.save_sso();

    Ok(Json(serde_json::json!({
        "enabled": true,
        "callback_url": callback_url(),
    })))
}

/// `DELETE /api/admin/sso` (ADMIN): disable SSO. Clears the live config + discovery cache and
/// persists the disabled marker so it stays off across restarts (even if env vars are still set).
pub async fn delete_sso(
    State(st): State<AppState>,
    Extension(claims): Extension<Claims>,
) -> Result<StatusCode, (StatusCode, String)> {
    require_admin(&claims)?;
    *st.oidc().write().unwrap() = None;
    *st.oidc_meta().lock().unwrap() = None;
    st.save_sso();
    Ok(StatusCode::NO_CONTENT)
}

/// Run OIDC discovery against `cfg.issuer` **without** touching the shared cache — used to validate
/// an admin-supplied issuer before committing it.
async fn discover_uncached(cfg: &OidcConfig) -> Result<(), String> {
    let http = http_client()?;
    let issuer = IssuerUrl::new(cfg.issuer.clone()).map_err(|e| format!("issuer url: {e}"))?;
    CoreProviderMetadata::discover_async(issuer, &http)
        .await
        .map_err(|e| format!("{e}"))?;
    Ok(())
}

/// Decode the **payload** (claims) of a compact JWT (`header.payload.signature`) into JSON, *without*
/// verifying anything — the signature/issuer/aud/nonce/exp were already validated by openidconnect
/// before this is called. We only use this to read the raw claim set so a *configurable* claim name
/// (e.g. `cognito:groups`) can be pulled out by string key.
fn decode_jwt_payload(jwt: &str) -> Option<serde_json::Value> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let bytes = base64url_decode(payload_b64)?;
    serde_json::from_slice(&bytes).ok()
}

/// Decode unpadded base64url (RFC 7515 §2) into bytes.
fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    }
    let cleaned: Vec<u8> = input.bytes().filter(|&b| b != b'=').collect();
    let mut out = Vec::with_capacity(cleaned.len() * 3 / 4);
    for chunk in cleaned.chunks(4) {
        let mut buf = [0u8; 4];
        let mut n = 0;
        for (i, &c) in chunk.iter().enumerate() {
            buf[i] = val(c)?;
            n += 1;
        }
        let b0 = (buf[0] << 2) | (buf[1] >> 4);
        out.push(b0);
        if n >= 3 {
            out.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if n >= 4 {
            out.push((buf[2] << 6) | buf[3]);
        }
    }
    Some(out)
}

/// Reduce an arbitrary IdP `error` string to a short, safe token for the `#sso_error=` fragment.
fn sanitize_reason(raw: &str) -> String {
    raw.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .take(40)
        .collect::<String>()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // `from_env` reads process-wide env; serialize the env-mutating cases so they don't race.
    fn clear_oidc_env() {
        for k in [
            "WEFT_OIDC_ISSUER",
            "WEFT_OIDC_CLIENT_ID",
            "WEFT_OIDC_CLIENT_SECRET",
            "WEFT_OIDC_REDIRECT_URL",
            "WEFT_OIDC_GROUPS_CLAIM",
            "WEFT_OIDC_USERNAME_CLAIM",
            "WEFT_OIDC_PROVIDER_LABEL",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn from_env_gating_and_defaults() {
        // Guard against parallel env mutation from other env-touching tests in this binary.
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _g = LOCK.lock().unwrap();

        clear_oidc_env();
        // Nothing set → disabled.
        assert!(OidcConfig::from_env().is_none());

        // Partial config (no secret/redirect) → still disabled.
        std::env::set_var("WEFT_OIDC_ISSUER", "https://issuer.example");
        std::env::set_var("WEFT_OIDC_CLIENT_ID", "cid");
        assert!(OidcConfig::from_env().is_none());

        // All four required → enabled, with documented defaults.
        std::env::set_var("WEFT_OIDC_CLIENT_SECRET", "secret");
        std::env::set_var("WEFT_OIDC_REDIRECT_URL", "https://app/api/auth/callback");
        let cfg = OidcConfig::from_env().expect("enabled");
        assert_eq!(cfg.issuer, "https://issuer.example");
        assert_eq!(cfg.groups_claim, "cognito:groups");
        assert_eq!(cfg.username_claim, "email");
        assert_eq!(cfg.provider_label, "SSO");

        // Overrides take effect.
        std::env::set_var("WEFT_OIDC_GROUPS_CLAIM", "groups");
        std::env::set_var("WEFT_OIDC_USERNAME_CLAIM", "preferred_username");
        std::env::set_var("WEFT_OIDC_PROVIDER_LABEL", "Okta");
        let cfg = OidcConfig::from_env().expect("enabled");
        assert_eq!(cfg.groups_claim, "groups");
        assert_eq!(cfg.username_claim, "preferred_username");
        assert_eq!(cfg.provider_label, "Okta");

        clear_oidc_env();
    }

    #[test]
    fn extract_identity_prefers_username_claim() {
        let claims = json!({
            "email": "alice@example.com",
            "sub": "uuid-123",
            "cognito:groups": ["admins", "analysts"],
        });
        let id = extract_identity(&claims, "email", "cognito:groups").unwrap();
        assert_eq!(id.username, "alice@example.com");
        assert_eq!(id.groups, vec!["admins", "analysts"]);
    }

    #[test]
    fn extract_identity_falls_back_email_then_sub() {
        // username_claim absent → falls back to email.
        let claims = json!({ "email": "bob@example.com", "sub": "uuid-9" });
        let id = extract_identity(&claims, "preferred_username", "groups").unwrap();
        assert_eq!(id.username, "bob@example.com");
        assert!(id.groups.is_empty());

        // email also absent → falls back to sub.
        let claims = json!({ "sub": "uuid-only" });
        let id = extract_identity(&claims, "preferred_username", "groups").unwrap();
        assert_eq!(id.username, "uuid-only");

        // Nothing resolvable → None.
        let claims = json!({ "unrelated": "x" });
        assert!(extract_identity(&claims, "preferred_username", "groups").is_none());
    }

    #[test]
    fn extract_identity_groups_non_array_is_empty() {
        // A scalar (not an array) groups claim yields empty groups, not an error.
        let claims = json!({ "sub": "u", "groups": "not-an-array" });
        let id = extract_identity(&claims, "x", "groups").unwrap();
        assert!(id.groups.is_empty());
        // Filters non-string array members.
        let claims = json!({ "sub": "u", "groups": ["ok", 7, null, "two"] });
        let id = extract_identity(&claims, "x", "groups").unwrap();
        assert_eq!(id.groups, vec!["ok", "two"]);
    }

    #[test]
    fn pending_store_single_use() {
        let mut s = PendingStore::default();
        s.insert(
            "state1".into(),
            PendingAuth {
                verifier: "v".into(),
                nonce: "n".into(),
                created: Instant::now(),
            },
        );
        assert!(s.take("state1").is_some());
        // Second take of the same state → gone (single-use).
        assert!(s.take("state1").is_none());
        // Unknown state → None.
        assert!(s.take("nope").is_none());
    }

    #[test]
    fn sanitize_reason_strips_unsafe_chars() {
        assert_eq!(sanitize_reason("access_denied"), "access_denied");
        assert_eq!(sanitize_reason("Bad Request! <xss>"), "badrequestxss");
    }

    #[test]
    fn from_parts_applies_defaults() {
        let cfg = OidcConfig::from_parts(
            "https://issuer.example".into(),
            "cid".into(),
            "secret".into(),
            "https://app/api/auth/callback".into(),
            None,
            Some(String::new()), // empty → default
            Some("Okta".into()),
        );
        assert_eq!(cfg.groups_claim, "cognito:groups");
        assert_eq!(cfg.username_claim, "email");
        assert_eq!(cfg.provider_label, "Okta");
    }

    #[test]
    fn stored_oidc_round_trips() {
        let cfg = OidcConfig::from_parts(
            "https://issuer.example".into(),
            "cid".into(),
            "topsecret".into(),
            "https://app/api/auth/callback".into(),
            Some("groups".into()),
            Some("preferred_username".into()),
            Some("Okta".into()),
        );
        let stored = StoredOidc::from(&cfg);
        let json = serde_json::to_string(&stored).unwrap();
        // The secret is part of the persisted blob (trusted control-plane state).
        assert!(json.contains("topsecret"));
        let back: StoredOidc = serde_json::from_str(&json).unwrap();
        let cfg2 = back.into_config().expect("enabled");
        assert_eq!(cfg2.issuer, cfg.issuer);
        assert_eq!(cfg2.client_id, cfg.client_id);
        assert_eq!(cfg2.client_secret, "topsecret");
        assert_eq!(cfg2.groups_claim, "groups");
        assert_eq!(cfg2.username_claim, "preferred_username");
        assert_eq!(cfg2.provider_label, "Okta");
    }

    #[test]
    fn stored_oidc_disabled_marker_yields_none() {
        let stored = StoredOidc::disabled();
        let json = serde_json::to_string(&stored).unwrap();
        let back: StoredOidc = serde_json::from_str(&json).unwrap();
        assert!(back.into_config().is_none());
    }

    #[test]
    fn require_admin_gates_on_group() {
        let admin = Claims {
            sub: "a".into(),
            groups: vec!["admins".into()],
            exp: 0,
        };
        let user = Claims {
            sub: "u".into(),
            groups: vec!["analysts".into()],
            exp: 0,
        };
        assert!(require_admin(&admin).is_ok());
        let err = require_admin(&user).unwrap_err();
        assert_eq!(err.0, StatusCode::FORBIDDEN);
    }
}
