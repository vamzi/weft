import { Navigate, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { ClustersPage } from "./pages/ClustersPage";
import { CatalogPage } from "./pages/CatalogPage";
import { SqlPage } from "./pages/SqlPage";
import { PermissionsPage } from "./pages/PermissionsPage";
import { NotebooksPage } from "./pages/NotebooksPage";
import { WorkspacePage } from "./pages/WorkspacePage";
import { DashboardsPage } from "./pages/DashboardsPage";
import { JobsPage } from "./pages/JobsPage";
import { AdminPage } from "./pages/AdminPage";
import { NotFoundPage } from "./pages/StubPages";
import { LoginPage } from "./pages/LoginPage";
import { useAuth } from "./lib/auth";

export function App() {
  const { me, loading } = useAuth();

  // While the initial /api/me probe is in flight, render nothing (avoids a
  // login flash for already-authenticated sessions).
  if (loading) {
    return (
      <div className="grid h-full place-items-center bg-bg-subtle">
        <p className="text-sm text-muted">Loading…</p>
      </div>
    );
  }

  // No session → the login gate is the whole app.
  if (!me) {
    return <LoginPage />;
  }

  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/clusters" replace />} />
        <Route path="/workspace" element={<WorkspacePage />} />
        <Route path="/clusters" element={<ClustersPage />} />
        <Route path="/catalog" element={<CatalogPage />} />
        <Route path="/sql" element={<SqlPage />} />
        <Route path="/permissions" element={<PermissionsPage />} />
        <Route path="/notebooks" element={<NotebooksPage />} />
        <Route path="/dashboards" element={<DashboardsPage />} />
        <Route path="/jobs" element={<JobsPage />} />
        <Route path="/admin" element={<AdminPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  );
}
