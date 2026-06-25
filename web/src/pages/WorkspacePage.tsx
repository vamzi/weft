import { useCallback, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { Page } from "../components/Layout";
import { NotebookIcon, SqlIcon, TrashIcon, ChevronRightIcon } from "../components/icons";
import { api, type Notebook, type SavedQuery } from "../lib/api";

/** Unified row model: a notebook or a saved SQL query. */
type Item =
  | { type: "notebook"; id: string; name: string; updatedAt: string }
  | { type: "query"; id: string; name: string; updatedAt: string };

export function WorkspacePage() {
  const navigate = useNavigate();
  const [items, setItems] = useState<Item[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [notebooks, queries] = await Promise.all([
        api.listNotebooks(),
        api.listSavedQueries(),
      ]);
      const merged: Item[] = [
        ...notebooks.map((nb: Notebook) => ({
          type: "notebook" as const,
          id: nb.id,
          name: nb.name,
          updatedAt: nb.updatedAt,
        })),
        ...queries.map((q: SavedQuery) => ({
          type: "query" as const,
          id: q.id,
          name: q.name,
          updatedAt: q.updatedAt,
        })),
      ];
      merged.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
      setItems(merged);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  async function newNotebook() {
    const name = window.prompt("Notebook name", "Untitled notebook");
    if (!name?.trim() || busy) return;
    setBusy(true);
    try {
      const doc = await api.createNotebook(name.trim());
      navigate(`/notebooks?open=${doc.id}`);
    } finally {
      setBusy(false);
    }
  }

  async function newQuery() {
    const name = window.prompt("Query name", "Untitled query");
    if (!name?.trim() || busy) return;
    setBusy(true);
    try {
      const q = await api.createSavedQuery(name.trim(), "SELECT 1");
      navigate(`/sql?query=${q.id}`);
    } finally {
      setBusy(false);
    }
  }

  function open(item: Item) {
    if (item.type === "notebook") navigate(`/notebooks?open=${item.id}`);
    else navigate(`/sql?query=${item.id}`);
  }

  async function remove(item: Item) {
    if (!window.confirm(`Delete "${item.name}"?`)) return;
    if (item.type === "notebook") await api.deleteNotebook(item.id);
    else await api.deleteSavedQuery(item.id);
    await load();
  }

  return (
    <Page
      title="Workspace"
      subtitle="Browse, open, and manage your notebooks and saved SQL queries."
      actions={
        <div className="flex items-center gap-2">
          <button type="button" className="weft-btn-ghost" onClick={newQuery} disabled={busy}>
            New SQL Query
          </button>
          <button type="button" className="weft-btn-primary" onClick={newNotebook} disabled={busy}>
            New Notebook
          </button>
        </div>
      }
    >
      {loading ? (
        <p className="text-sm text-muted">Loading workspace…</p>
      ) : items.length === 0 ? (
        <div className="weft-card px-6 py-12 text-center">
          <p className="text-sm font-medium text-body">Your workspace is empty</p>
          <p className="mt-1 text-sm text-muted">
            Create a notebook or a saved SQL query to get started.
          </p>
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {items.map((item) => (
            <div
              key={`${item.type}-${item.id}`}
              className="weft-card flex items-center gap-4 px-5 py-4 transition-colors hover:bg-bg-subtle"
            >
              <button
                type="button"
                onClick={() => open(item)}
                className="flex min-w-0 flex-1 items-center gap-4 text-left"
              >
                <span className="grid h-9 w-9 shrink-0 place-items-center rounded-weft-sm bg-bg-subtle text-muted">
                  {item.type === "notebook" ? (
                    <NotebookIcon width={18} height={18} />
                  ) : (
                    <SqlIcon width={18} height={18} />
                  )}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-3">
                    <span className="truncate text-sm font-semibold text-body">{item.name}</span>
                    <span className="rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] uppercase tracking-wide text-muted">
                      {item.type === "notebook" ? "Notebook" : "SQL"}
                    </span>
                  </div>
                  <div className="mt-1.5 text-xs text-muted">
                    updated {new Date(item.updatedAt).toLocaleString()}
                  </div>
                </div>
                <ChevronRightIcon width={16} height={16} className="shrink-0 text-muted" />
              </button>
              <button
                type="button"
                className="weft-btn-ghost px-2 py-1"
                style={{ color: "var(--weft-danger)" }}
                onClick={() => remove(item)}
                aria-label={`Delete ${item.name}`}
              >
                <TrashIcon width={15} height={15} />
              </button>
            </div>
          ))}
        </div>
      )}
    </Page>
  );
}
