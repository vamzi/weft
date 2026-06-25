import { Link, NavLink } from "react-router-dom";
import { useTheme } from "../lib/theme";

const REPO = "https://github.com/vamzi/weft";

function ThemeToggle() {
  const { theme, toggle } = useTheme();
  return (
    <button
      onClick={toggle}
      aria-label="Toggle theme"
      className="weft-btn-ghost h-9 w-9 px-0"
      title={theme === "dark" ? "Switch to light" : "Switch to dark"}
    >
      {theme === "dark" ? "☀" : "☾"}
    </button>
  );
}

export default function Navbar() {
  const link = ({ isActive }: { isActive: boolean }) =>
    `text-sm font-medium transition-colors ${
      isActive ? "text-accent" : "text-muted hover:text-body"
    }`;
  return (
    <header className="sticky top-0 z-20 border-b border-hairline bg-bg/80 backdrop-blur">
      <div className="weft-container flex h-14 items-center justify-between">
        <Link to="/" className="flex items-center gap-2">
          <img src="weft.svg" alt="Weft" className="h-7 w-7" />
          <span className="text-base font-semibold tracking-tight">Weft</span>
        </Link>
        <nav className="flex items-center gap-6">
          <NavLink to="/" end className={link}>
            Overview
          </NavLink>
          <NavLink to="/performance" className={link}>
            Performance
          </NavLink>
          <a
            href={`${REPO}#readme`}
            className="text-sm font-medium text-muted transition-colors hover:text-body"
          >
            Docs
          </a>
          <ThemeToggle />
          <a href={REPO} className="weft-btn-primary hidden sm:inline-flex">
            GitHub
          </a>
        </nav>
      </div>
    </header>
  );
}
