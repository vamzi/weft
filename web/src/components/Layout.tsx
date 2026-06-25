import type { ReactNode } from "react";
import { Outlet } from "react-router-dom";
import { Navbar } from "./Navbar";
import { IconRail } from "./IconRail";

export function Layout() {
  return (
    <div className="flex h-full flex-col">
      <Navbar />
      <div className="flex min-h-0 flex-1">
        <IconRail />
        <main className="min-w-0 flex-1 overflow-auto">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

/** Shared page scaffold: title + subtitle + body. Used by every section. */
export function Page({
  title,
  subtitle,
  actions,
  children,
}: {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="mx-auto max-w-6xl px-6 py-8">
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-body">{title}</h1>
          {subtitle && <p className="mt-1 text-sm text-muted">{subtitle}</p>}
        </div>
        {actions}
      </div>
      {children}
    </div>
  );
}
