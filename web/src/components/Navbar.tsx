import { Link } from "react-router-dom";
import { useEffect, useState } from "react";
import { useTheme } from "../lib/theme";
import { MoonIcon, SunIcon } from "./icons";
import { api, type Principal } from "../lib/api";

export function Navbar() {
  const { theme, toggle } = useTheme();
  const [me, setMe] = useState<Principal | null>(null);

  useEffect(() => {
    api.me().then(setMe).catch(() => setMe(null));
  }, []);

  return (
    <header className="sticky top-0 z-30 border-b border-hairline bg-bg/80 backdrop-blur">
      <div className="flex h-14 items-center justify-between px-5">
        <Link to="/" className="flex items-center gap-2.5">
          <span
            className="grid h-7 w-7 place-items-center rounded-weft-sm text-sm font-bold text-accent-contrast"
            style={{ backgroundColor: "var(--weft-accent)" }}
          >
            W
          </span>
          <span className="text-[15px] font-semibold tracking-tight text-body">Weft</span>
          <span className="hidden text-xs text-muted sm:inline">control plane</span>
        </Link>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={toggle}
            aria-label="Toggle theme"
            className="weft-btn-ghost h-8 w-8 px-0"
          >
            {theme === "dark" ? <SunIcon width={16} height={16} /> : <MoonIcon width={16} height={16} />}
          </button>

          {me ? (
            <button type="button" className="weft-btn-ghost">
              <span
                className="grid h-5 w-5 place-items-center rounded-full text-[10px] font-semibold text-accent-contrast"
                style={{ backgroundColor: "var(--weft-accent)" }}
              >
                {me.displayName.charAt(0)}
              </span>
              <span className="hidden sm:inline">{me.displayName}</span>
            </button>
          ) : (
            <a href="/api/auth/login" className="weft-btn-primary">
              Sign in
            </a>
          )}
        </div>
      </div>
    </header>
  );
}
