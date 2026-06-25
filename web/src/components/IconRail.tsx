import { NavLink } from "react-router-dom";
import type { ComponentType, SVGProps } from "react";
import {
  CatalogIcon,
  ClustersIcon,
  DashboardIcon,
  JobsIcon,
  NotebookIcon,
  PermissionsIcon,
  SqlIcon,
} from "./icons";

interface Section {
  to: string;
  label: string;
  Icon: ComponentType<SVGProps<SVGSVGElement>>;
}

export const SECTIONS: Section[] = [
  { to: "/clusters", label: "Clusters", Icon: ClustersIcon },
  { to: "/catalog", label: "Catalog", Icon: CatalogIcon },
  { to: "/sql", label: "SQL", Icon: SqlIcon },
  { to: "/permissions", label: "Access", Icon: PermissionsIcon },
  { to: "/notebooks", label: "Notebooks", Icon: NotebookIcon },
  { to: "/dashboards", label: "Dashboards", Icon: DashboardIcon },
  { to: "/jobs", label: "Jobs", Icon: JobsIcon },
];

export function IconRail() {
  return (
    <nav className="flex w-16 shrink-0 flex-col items-center gap-1 border-r border-hairline bg-bg-subtle py-3">
      {SECTIONS.map(({ to, label, Icon }) => (
        <NavLink
          key={to}
          to={to}
          title={label}
          className={({ isActive }) =>
            [
              "group flex w-12 flex-col items-center gap-1 rounded-weft-sm py-2 text-[10px] font-medium transition-colors",
              isActive ? "text-accent" : "text-muted hover:text-body hover:bg-surface",
            ].join(" ")
          }
        >
          {({ isActive }) => (
            <>
              <span
                className="grid h-9 w-9 place-items-center rounded-weft-sm transition-colors"
                style={isActive ? { backgroundColor: "color-mix(in srgb, var(--weft-accent) 12%, transparent)" } : undefined}
              >
                <Icon />
              </span>
              {label}
            </>
          )}
        </NavLink>
      ))}
    </nav>
  );
}
