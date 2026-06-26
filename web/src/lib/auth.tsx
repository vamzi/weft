import { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { api, clearToken, getToken, setToken, type Principal } from "./api";

interface AuthState {
  /** The signed-in principal, or null when logged out. */
  me: Principal | null;
  /** True until the initial `GET /api/me` probe settles. */
  loading: boolean;
  /** Sign in with username/password (throws on bad creds), then refresh `me`. */
  login: (username: string, password: string) => Promise<void>;
  /** Sign out: hit the gateway, clear the token, drop to the login gate. */
  logout: () => Promise<void>;
  /**
   * A one-time SSO sign-in failure surfaced from the `#sso_error=` fragment the
   * gateway redirects back with. `null` on a normal load; the LoginPage shows
   * it in its error banner.
   */
  ssoError: string | null;
}

const AuthContext = createContext<AuthState | null>(null);

/**
 * Consume an SSO handoff from the URL fragment BEFORE any token/`/api/me`
 * logic runs. Two shapes:
 *  - `#token=<jwt>` — store it as the bearer token (the subsequent `/api/me`
 *    probe then logs the user in), and return `{ ... }` with no error.
 *  - `#sso_error=<reason>` — return the (decoded) reason so the login gate can
 *    show it.
 * Either way the fragment is stripped via `replaceState` so a reload/back is
 * clean. A normal load with no relevant fragment is a no-op.
 */
function consumeSsoFragment(): { ssoError: string | null } {
  if (typeof window === "undefined") return { ssoError: null };
  const hash = window.location.hash;
  if (!hash || hash.length < 2) return { ssoError: null };

  // Tolerate both `#token=…` and `#a=b&token=…` by parsing as URL params.
  const params = new URLSearchParams(hash.slice(1));
  const token = params.get("token");
  const error = params.get("sso_error");
  if (!token && !error) return { ssoError: null };

  if (token) setToken(token);

  // Strip the fragment so the JWT/error never lingers in the address bar.
  window.history.replaceState(
    null,
    "",
    window.location.pathname + window.location.search,
  );

  return { ssoError: token ? null : error };
}

/**
 * Auth gate provider. On mount, if a token exists it probes `GET /api/me`;
 * a 401 there (handled inside `request()`) clears the token and reloads. With no
 * token we render the login immediately (no probe).
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const [me, setMe] = useState<Principal | null>(null);
  const [loading, setLoading] = useState(true);
  // Captured once, synchronously, from the URL fragment on first render so the
  // JWT/error is off the address bar before anything else (incl. the `/api/me`
  // probe below) reads the token. `useState`'s initializer runs exactly once.
  const [ssoError] = useState<string | null>(() => consumeSsoFragment().ssoError);

  useEffect(() => {
    let alive = true;
    if (!getToken()) {
      setLoading(false);
      return;
    }
    api
      .me()
      .then((p) => {
        if (alive) setMe(p);
      })
      .catch(() => {
        // 401 reloads inside request(); any other error → treat as logged out.
        clearToken();
        if (alive) setMe(null);
      })
      .finally(() => {
        if (alive) setLoading(false);
      });
    return () => {
      alive = false;
    };
  }, []);

  const login = useCallback(async (username: string, password: string) => {
    await api.login(username, password);
    const p = await api.me();
    setMe(p);
  }, []);

  const logout = useCallback(async () => {
    await api.logout();
    setMe(null);
  }, []);

  return (
    <AuthContext.Provider value={{ me, loading, login, logout, ssoError }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
  return ctx;
}
