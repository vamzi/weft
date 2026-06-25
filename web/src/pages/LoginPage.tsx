import { useState } from "react";
import { useTheme } from "../lib/theme";
import { useAuth } from "../lib/auth";
import { MoonIcon, SunIcon } from "../components/icons";

/**
 * The login gate. Centered, ollama-styled card with username + password. On a
 * 401 it surfaces a "wrong credentials" message; on success the AuthProvider
 * flips to the app. The theme toggle stays available here too.
 */
export function LoginPage() {
  const { theme, toggle } = useTheme();
  const { login } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const valid = username.trim().length > 0 && password.length > 0;

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || busy) return;
    setBusy(true);
    setError(null);
    try {
      await login(username.trim(), password);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "";
      setError(
        /401/.test(msg) || /unauthorized/i.test(msg)
          ? "Incorrect username or password."
          : "Sign-in failed. Check the gateway is reachable and try again.",
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="grid min-h-full place-items-center bg-bg-subtle px-4 py-10">
      <div className="absolute right-4 top-4">
        <button
          type="button"
          onClick={toggle}
          aria-label="Toggle theme"
          className="weft-btn-ghost h-8 w-8 px-0"
        >
          {theme === "dark" ? <SunIcon width={16} height={16} /> : <MoonIcon width={16} height={16} />}
        </button>
      </div>

      <div className="w-full max-w-sm">
        <div className="mb-6 flex flex-col items-center gap-3 text-center">
          <span
            className="grid h-11 w-11 place-items-center rounded-weft text-lg font-bold text-accent-contrast"
            style={{ backgroundColor: "var(--weft-accent)" }}
          >
            W
          </span>
          <div>
            <h1 className="text-lg font-semibold tracking-tight text-body">Sign in to Weft</h1>
            <p className="mt-1 text-sm text-muted">The Weft control plane.</p>
          </div>
        </div>

        <form onSubmit={onSubmit} className="weft-card flex flex-col gap-4 px-6 py-6">
          <div>
            <label className="weft-label" htmlFor="login-user">
              Username
            </label>
            <input
              id="login-user"
              className="weft-input"
              autoComplete="username"
              autoFocus
              value={username}
              onChange={(e) => setUsername(e.target.value)}
            />
          </div>
          <div>
            <label className="weft-label" htmlFor="login-pass">
              Password
            </label>
            <input
              id="login-pass"
              type="password"
              className="weft-input"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </div>

          {error && (
            <div
              className="rounded-weft-sm px-3 py-2 text-sm"
              style={{
                color: "var(--weft-danger)",
                backgroundColor: "color-mix(in srgb, var(--weft-danger) 12%, transparent)",
              }}
              role="alert"
            >
              {error}
            </div>
          )}

          <button
            type="submit"
            className="weft-btn-primary mt-1 justify-center"
            disabled={!valid || busy}
          >
            {busy ? "Signing in…" : "Sign in"}
          </button>
        </form>
      </div>
    </div>
  );
}
