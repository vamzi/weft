//! `weft-gateway` — the control plane's public REST/WebSocket edge.
//!
//! The **only** component that holds a Spark Connect gRPC client; the browser speaks REST/WS to
//! it and never gRPC. Responsibilities: identity (OIDC/SAML/SCIM + session JWT), routing a user's
//! SQL/notebook to the right cluster's Connect endpoint, coarse governance checks at the edge,
//! and streaming Arrow IPC results back (and to S3 for history).
//!
//! [`ROUTES`] is the route table every web feature team mocks against and the engine's identity
//! interceptor mirrors. [`server`] is the axum implementation: it serves the health probe,
//! current-principal, and cluster-lifecycle endpoints against an in-memory store today, with
//! persistence/auth/Spark-Connect-routing layering on top of the same surface.

pub mod cloud;
pub mod server;

/// HTTP methods used by the gateway API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    /// GET.
    Get,
    /// POST.
    Post,
    /// PUT.
    Put,
    /// DELETE.
    Delete,
    /// WebSocket upgrade.
    Ws,
}

/// One route in the gateway's public API.
#[derive(Debug, Clone, Copy)]
pub struct Route {
    /// HTTP method (or `Ws` for a WebSocket endpoint).
    pub method: Method,
    /// Path, with `:param` placeholders.
    pub path: &'static str,
    /// One-line description (feeds the OpenAPI summary).
    pub summary: &'static str,
}

/// The frozen public API surface. Web feature areas build against these paths (mocked until the
/// backend lands); the identity interceptor and OpenAPI doc derive from the same list.
pub const ROUTES: &[Route] = &[
    // Auth / identity
    Route {
        method: Method::Get,
        path: "/api/auth/login",
        summary: "Begin OIDC/SAML SSO login",
    },
    Route {
        method: Method::Get,
        path: "/api/auth/callback",
        summary: "SSO redirect callback → session JWT",
    },
    Route {
        method: Method::Post,
        path: "/api/auth/logout",
        summary: "End session",
    },
    Route {
        method: Method::Get,
        path: "/api/me",
        summary: "Current principal + resolved groups",
    },
    Route {
        method: Method::Post,
        path: "/scim/v2/Users",
        summary: "SCIM user provisioning",
    },
    Route {
        method: Method::Post,
        path: "/scim/v2/Groups",
        summary: "SCIM group provisioning",
    },
    // Clusters
    Route {
        method: Method::Get,
        path: "/api/clusters",
        summary: "List clusters",
    },
    Route {
        method: Method::Post,
        path: "/api/clusters",
        summary: "Create a cluster",
    },
    Route {
        method: Method::Get,
        path: "/api/clusters/:id",
        summary: "Cluster detail + state",
    },
    Route {
        method: Method::Post,
        path: "/api/clusters/:id/start",
        summary: "Start a cluster",
    },
    Route {
        method: Method::Post,
        path: "/api/clusters/:id/stop",
        summary: "Stop a cluster",
    },
    Route {
        method: Method::Delete,
        path: "/api/clusters/:id",
        summary: "Delete a cluster",
    },
    // Catalog + governance
    Route {
        method: Method::Get,
        path: "/api/catalog",
        summary: "Browse catalogs/schemas/tables (governed)",
    },
    Route {
        method: Method::Post,
        path: "/api/connections",
        summary: "Attach an external catalog (HMS/Glue/UC) or create local",
    },
    Route {
        method: Method::Get,
        path: "/api/grants/:securable",
        summary: "Show grants on a securable (self-filtered)",
    },
    Route {
        method: Method::Post,
        path: "/api/grants",
        summary: "GRANT/REVOKE a privilege",
    },
    // SQL editor
    Route {
        method: Method::Post,
        path: "/api/complete",
        summary: "Editor autocomplete (governed catalog symbols)",
    },
    Route {
        method: Method::Ws,
        path: "/api/sql",
        summary: "Run SQL on a cluster; stream Arrow IPC results",
    },
    Route {
        method: Method::Post,
        path: "/api/sql/:id/cancel",
        summary: "Interrupt a running query",
    },
    Route {
        method: Method::Get,
        path: "/api/queries",
        summary: "Query history",
    },
    // Notebooks
    Route {
        method: Method::Get,
        path: "/api/notebooks",
        summary: "List notebooks",
    },
    Route {
        method: Method::Post,
        path: "/api/notebooks",
        summary: "Create a notebook",
    },
    Route {
        method: Method::Put,
        path: "/api/notebooks/:id",
        summary: "Autosave notebook cells (+ revision)",
    },
    Route {
        method: Method::Ws,
        path: "/api/notebooks/:id/run",
        summary: "Execute cells; stream per-cell output",
    },
    // AI assist
    Route {
        method: Method::Ws,
        path: "/api/ai/generate",
        summary: "NL → SQL / NL → notebook (structured, streamed)",
    },
    // Dashboards + jobs
    Route {
        method: Method::Get,
        path: "/api/dashboards",
        summary: "List dashboards",
    },
    Route {
        method: Method::Post,
        path: "/api/dashboards",
        summary: "Create/update a dashboard",
    },
    Route {
        method: Method::Get,
        path: "/api/jobs",
        summary: "List jobs",
    },
    Route {
        method: Method::Post,
        path: "/api/jobs",
        summary: "Create/update a job (DAG + schedule)",
    },
    Route {
        method: Method::Post,
        path: "/api/jobs/:id/run",
        summary: "Trigger a job run",
    },
    Route {
        method: Method::Get,
        path: "/api/jobs/:id/runs",
        summary: "Job run history",
    },
    // Ops
    Route {
        method: Method::Get,
        path: "/healthz",
        summary: "Liveness/readiness probe",
    },
];

/// A compact, human-readable OpenAPI-style summary of the route table (placeholder for the real
/// `utoipa`-generated spec).
pub fn openapi_summary() -> String {
    let mut s = String::from("Weft Gateway API\n");
    for r in ROUTES {
        s.push_str(&format!("{:?} {} — {}\n", r.method, r.path, r.summary));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_are_unique_and_described() {
        let mut seen = std::collections::HashSet::new();
        for r in ROUTES {
            assert!(!r.summary.is_empty(), "{} missing summary", r.path);
            assert!(
                seen.insert((r.method, r.path)),
                "duplicate route {:?} {}",
                r.method,
                r.path
            );
        }
        assert!(ROUTES.iter().any(|r| r.path == "/healthz"));
        assert!(ROUTES
            .iter()
            .any(|r| matches!(r.method, Method::Ws) && r.path == "/api/sql"));
    }
}
