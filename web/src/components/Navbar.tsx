import { Link } from "react-router-dom";
import { useTheme } from "../lib/theme";
import { useAuth } from "../lib/auth";
import { MoonIcon, SunIcon } from "./icons";

export function Navbar() {
  const { theme, toggle } = useTheme();
  const { me, logout } = useAuth();

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

          {me && (
            <div className="flex items-center gap-1.5 pl-1">
              <span className="flex items-center gap-2 rounded-weft-sm px-2 py-1 text-sm text-body">
                <span
                  className="grid h-5 w-5 place-items-center rounded-full text-[10px] font-semibold uppercase text-accent-contrast"
                  style={{ backgroundColor: "var(--weft-accent)" }}
                >
                  {me.user.charAt(0)}
                </span>
                <span className="hidden sm:inline">{me.user}</span>
              </span>
              <button type="button" className="weft-btn-ghost" onClick={() => void logout()}>
                Sign out
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}
