import { Navigate, Route, Routes } from "react-router-dom";
import { Layout } from "./components/Layout";
import { ClustersPage } from "./pages/ClustersPage";
import { CatalogPage } from "./pages/CatalogPage";
import { SqlPage } from "./pages/SqlPage";
import { PermissionsPage } from "./pages/PermissionsPage";
import { NotebooksPage } from "./pages/NotebooksPage";
import { DashboardsPage } from "./pages/DashboardsPage";
import { JobsPage } from "./pages/JobsPage";
import { NotFoundPage } from "./pages/StubPages";

export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Navigate to="/clusters" replace />} />
        <Route path="/clusters" element={<ClustersPage />} />
        <Route path="/catalog" element={<CatalogPage />} />
        <Route path="/sql" element={<SqlPage />} />
        <Route path="/permissions" element={<PermissionsPage />} />
        <Route path="/notebooks" element={<NotebooksPage />} />
        <Route path="/dashboards" element={<DashboardsPage />} />
        <Route path="/jobs" element={<JobsPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  );
}
