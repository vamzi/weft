import { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { api, clearToken, getToken, type Principal } from "./api";

interface AuthState {
  /** The signed-in principal, or null when logged out. */
  me: Principal | null;
  /** True until the initial `GET /api/me` probe settles. */
  loading: boolean;
  /** Sign in with username/password (throws on bad creds), then refresh `me`. */
  login: (username: string, password: string) => Promise<void>;
  /** Sign out: hit the gateway, clear the token, drop to the login gate. */
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthState | null>(null);

/**
 * Auth gate provider. On mount, if a token exists it probes `GET /api/me`;
 * a 401 there (handled inside `request()`) clears the token and reloads. With no
 * token we render the login immediately (no probe).
 */
export function AuthProvider({ children }: { children: ReactNode }) {
  const [me, setMe] = useState<Principal | null>(null);
  const [loading, setLoading] = useState(true);

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
    <AuthContext.Provider value={{ me, loading, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
  return ctx;
}
