import { useEffect, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import Editor, { loader, type Monaco, type OnMount } from "@monaco-editor/react";
import * as monaco from "monaco-editor";
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";

// Self-host Monaco from the bundled package (same setup as the SQL editor) so
// the notebook cell editors work offline / behind the gateway. The SQL,
// Python, and Markdown Monarch grammars all ship with monaco-editor; none of
// them needs a dedicated language-service worker, so the base editor worker is
// enough for every cell kind.
self.MonacoEnvironment = {
  getWorker: () => new editorWorker(),
};
loader.config({ monaco });

import { Page } from "../components/Layout";
import { DemoNote } from "../components/DemoNote";
import { PlayIcon, PlusIcon, SparklesIcon, TrashIcon, ChevronRightIcon } from "../components/icons";
import { useTheme } from "../lib/theme";
import { setupMonacoSql, WEFT_DARK, WEFT_LIGHT } from "../lib/monaco";
import {
  api,
  type CellKind,
  type CellResult,
  type Cluster,
  type Notebook,
  type NotebookCell,
  type NotebookDoc,
} from "../lib/api";

const CELL_KINDS: { value: CellKind; label: string; lang: string }[] = [
  { value: "sql", label: "SQL", lang: "sql" },
  { value: "python", label: "Python", lang: "python" },
  { value: "markdown", label: "Markdown", lang: "markdown" },
];

const langFor = (kind: CellKind) => CELL_KINDS.find((c) => c.value === kind)?.lang ?? "sql";

let cellSeq = 0;
const newCell = (kind: CellKind, source = ""): NotebookCell => ({
  id: `cell-new-${Date.now()}-${cellSeq++}`,
  kind,
  source,
});

export function NotebooksPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [notebooks, setNotebooks] = useState<Notebook[]>([]);
  const [loading, setLoading] = useState(true);
  const [openId, setOpenId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  function refresh() {
    setLoading(true);
    return api.listNotebooks().then((nbs) => {
      setNotebooks(nbs);
      setLoading(false);
    });
  }

  useEffect(() => {
    refresh();
  }, []);

  // Deep-link: ?open=<id> opens that notebook's editor on mount.
  useEffect(() => {
    const open = searchParams.get("open");
    if (open) setOpenId(open);
  }, [searchParams]);

  function closeEditor() {
    setOpenId(null);
    if (searchParams.has("open")) {
      searchParams.delete("open");
      setSearchParams(searchParams, { replace: true });
    }
    refresh();
  }

  async function newNotebook() {
    const name = window.prompt("Notebook name", "Untitled notebook");
    if (!name?.trim() || busy) return;
    setBusy(true);
    try {
      const doc = await api.createNotebook(name.trim());
      setOpenId(doc.id);
    } finally {
      setBusy(false);
    }
  }

  async function deleteNotebook(id: string, name: string) {
    if (!window.confirm(`Delete "${name}"?`)) return;
    await api.deleteNotebook(id);
    await refresh();
  }

  if (openId) {
    return <NotebookEditor id={openId} onClose={closeEditor} />;
  }

  return (
    <Page
      title="Notebooks"
      subtitle="Multi-language notebooks with per-cell output (SQL, Python, Markdown)."
      actions={
        <button type="button" className="weft-btn-primary" onClick={newNotebook} disabled={busy}>
          <PlusIcon width={16} height={16} />
          New notebook
        </button>
      }
    >
      <DemoNote text="SQL cells execute live on the engine; Python & Markdown cells are demo-only for now." />
      {loading ? (
        <p className="text-sm text-muted">Loading notebooks…</p>
      ) : notebooks.length === 0 ? (
        <div className="weft-card px-6 py-12 text-center">
          <p className="text-sm font-medium text-body">No notebooks yet</p>
          <p className="mt-1 text-sm text-muted">Create one to get started.</p>
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {notebooks.map((nb) => (
            <div
              key={nb.id}
              className="weft-card flex items-center gap-4 px-5 py-4 transition-colors hover:bg-bg-subtle"
            >
              <button
                type="button"
                onClick={() => setOpenId(nb.id)}
                className="flex min-w-0 flex-1 items-center gap-4 text-left"
              >
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-3">
                    <span className="truncate text-sm font-semibold text-body">{nb.name}</span>
                    <span className="rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] uppercase tracking-wide text-muted">
                      {nb.language}
                    </span>
                  </div>
                  <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
                    <span className="font-mono">{nb.id}</span>
                    <span>{nb.cells} cells</span>
                    <span>by {nb.owner}</span>
                    <span>updated {new Date(nb.updatedAt).toLocaleDateString()}</span>
                  </div>
                </div>
                <ChevronRightIcon width={16} height={16} className="shrink-0 text-muted" />
              </button>
              <button
                type="button"
                className="weft-btn-ghost px-2 py-1"
                style={{ color: "var(--weft-danger)" }}
                onClick={() => deleteNotebook(nb.id, nb.name)}
                aria-label={`Delete ${nb.name}`}
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

type SaveState = "idle" | "saving" | "saved";

function NotebookEditor({ id, onClose }: { id: string; onClose: () => void }) {
  const [doc, setDoc] = useState<NotebookDoc | null>(null);
  const [results, setResults] = useState<Record<string, CellResult>>({});
  const [running, setRunning] = useState<Record<string, boolean>>({});
  const [saveState, setSaveState] = useState<SaveState>("idle");
  const [addKind, setAddKind] = useState<CellKind>("sql");
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [clusterId, setClusterId] = useState("");
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const firstLoad = useRef(true);

  useEffect(() => {
    api.getNotebook(id).then(setDoc);
  }, [id]);

  // Cluster picker (same model as the SQL editor): all cells in a notebook run on the chosen
  // cluster; default to a RUNNING one, else the embedded control-plane engine.
  useEffect(() => {
    api.listClusters().then((cs) => {
      setClusters(cs);
      const running = cs.find((c) => c.state === "running");
      setClusterId(running?.id ?? "");
    });
  }, []);

  // Autosave (debounced) whenever the doc changes after the initial load.
  useEffect(() => {
    if (!doc) return;
    if (firstLoad.current) {
      firstLoad.current = false;
      return;
    }
    setSaveState("saving");
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      // Live: PUT /api/notebooks/:id persists the document.
      api.saveNotebook(doc).then(() => setSaveState("saved"));
    }, 700);
    return () => {
      if (saveTimer.current) clearTimeout(saveTimer.current);
    };
  }, [doc]);

  function updateCell(cellId: string, source: string) {
    setDoc((d) =>
      d ? { ...d, cells: d.cells.map((c) => (c.id === cellId ? { ...c, source } : c)) } : d,
    );
  }

  function setCellKind(cellId: string, kind: CellKind) {
    setDoc((d) =>
      d ? { ...d, cells: d.cells.map((c) => (c.id === cellId ? { ...c, kind } : c)) } : d,
    );
    setResults((r) => {
      const next = { ...r };
      delete next[cellId];
      return next;
    });
  }

  function addCell() {
    setDoc((d) => (d ? { ...d, cells: [...d.cells, newCell(addKind)] } : d));
  }

  function deleteCell(cellId: string) {
    setDoc((d) => (d ? { ...d, cells: d.cells.filter((c) => c.id !== cellId) } : d));
  }

  function moveCell(cellId: string, dir: -1 | 1) {
    setDoc((d) => {
      if (!d) return d;
      const i = d.cells.findIndex((c) => c.id === cellId);
      const j = i + dir;
      if (i < 0 || j < 0 || j >= d.cells.length) return d;
      const cells = [...d.cells];
      [cells[i], cells[j]] = [cells[j], cells[i]];
      return { ...d, cells };
    });
  }

  async function runCell(cell: NotebookCell) {
    setRunning((r) => ({ ...r, [cell.id]: true }));
    try {
      let result: CellResult;
      if (cell.kind === "sql") {
        // LIVE: SQL cells execute on the engine via POST /api/sql.
        const startedAt = performance.now();
        const r = await api.runSql(cell.source, clusterId || undefined);
        const durationMs = Math.round(performance.now() - startedAt);
        if (r.error) {
          result = { kind: "sql", text: r.error, durationMs };
        } else {
          result = {
            kind: "sql",
            table: { columns: r.columns, rows: r.rows, rowCount: r.row_count, durationMs },
            durationMs,
          };
        }
      } else {
        // Python / Markdown don't execute server-side yet — the mock echoes them.
        result = await api.runCell(cell);
      }
      setResults((r) => ({ ...r, [cell.id]: result }));
    } finally {
      setRunning((r) => ({ ...r, [cell.id]: false }));
    }
  }

  async function loadAiNotebook(prompt: string) {
    // Live: POST /api/ai/notebook returns a schema-grounded notebook skeleton.
    const { cells } = await api.aiGenerateNotebook(prompt);
    setDoc((d) =>
      d ? { ...d, cells: cells.map((c) => newCell(c.kind, c.source)) } : d,
    );
    setResults({});
  }

  if (!doc) {
    return (
      <Page title="Notebook" subtitle="Loading…">
        <p className="text-sm text-muted">Loading notebook…</p>
      </Page>
    );
  }

  return (
    <Page
      title={doc.name}
      subtitle="SQL cells run live on the engine; Python & Markdown cells are demo-only for now."
      actions={
        <div className="flex items-center gap-2">
          <select
            aria-label="Cluster"
            className="weft-input w-auto"
            value={clusterId}
            onChange={(e) => setClusterId(e.target.value)}
          >
            <option value="">embedded engine</option>
            {clusters.map((c) => (
              <option key={c.id} value={c.id}>
                {c.name} ({c.state})
              </option>
            ))}
          </select>
          <SaveIndicator state={saveState} />
          <button type="button" className="weft-btn-ghost" onClick={onClose}>
            Back to notebooks
          </button>
        </div>
      }
    >
      <AiNotebookBar onGenerate={loadAiNotebook} />

      <div className="flex flex-col gap-4">
        {doc.cells.map((cell, idx) => (
          <CellCard
            key={cell.id}
            cell={cell}
            index={idx}
            count={doc.cells.length}
            result={results[cell.id]}
            running={!!running[cell.id]}
            onChange={(src) => updateCell(cell.id, src)}
            onKind={(k) => setCellKind(cell.id, k)}
            onRun={() => runCell(cell)}
            onDelete={() => deleteCell(cell.id)}
            onMove={(dir) => moveCell(cell.id, dir)}
          />
        ))}
      </div>

      <div className="mt-4 flex items-center gap-2">
        <button type="button" className="weft-btn-primary" onClick={addCell}>
          <PlusIcon width={16} height={16} />
          Add cell
        </button>
        <select
          aria-label="New cell type"
          className="weft-input w-auto"
          value={addKind}
          onChange={(e) => setAddKind(e.target.value as CellKind)}
        >
          {CELL_KINDS.map((k) => (
            <option key={k.value} value={k.value}>
              {k.label}
            </option>
          ))}
        </select>
      </div>
    </Page>
  );
}

function SaveIndicator({ state }: { state: SaveState }) {
  const text = state === "saving" ? "Saving…" : state === "saved" ? "All changes saved" : "Autosave on";
  return <span className="text-xs text-muted">{text}</span>;
}

function CellCard({
  cell,
  index,
  count,
  result,
  running,
  onChange,
  onKind,
  onRun,
  onDelete,
  onMove,
}: {
  cell: NotebookCell;
  index: number;
  count: number;
  result: CellResult | undefined;
  running: boolean;
  onChange: (src: string) => void;
  onKind: (k: CellKind) => void;
  onRun: () => void;
  onDelete: () => void;
  onMove: (dir: -1 | 1) => void;
}) {
  const { theme } = useTheme();
  const monacoTheme = theme === "dark" ? WEFT_DARK : WEFT_LIGHT;

  const onMount: OnMount = (_ed, m: Monaco) => {
    setupMonacoSql(m);
    m.editor.setTheme(monacoTheme);
  };

  // Editor height scales with content (clamped), keeps cells compact.
  const lines = cell.source.split("\n").length;
  const height = Math.min(Math.max(lines, 3), 18) * 19 + 24;

  return (
    <div className="weft-card overflow-hidden">
      <div className="flex items-center gap-2 border-b border-hairline px-3 py-2">
        <span className="font-mono text-xs text-muted">[{index + 1}]</span>
        <select
          aria-label="Cell type"
          className="weft-input w-auto py-1 text-xs"
          value={cell.kind}
          onChange={(e) => onKind(e.target.value as CellKind)}
        >
          {CELL_KINDS.map((k) => (
            <option key={k.value} value={k.value}>
              {k.label}
            </option>
          ))}
        </select>
        <div className="ml-auto flex items-center gap-1">
          <button
            type="button"
            className="weft-btn-ghost px-2 py-1"
            disabled={index === 0}
            onClick={() => onMove(-1)}
            aria-label="Move cell up"
          >
            ↑
          </button>
          <button
            type="button"
            className="weft-btn-ghost px-2 py-1"
            disabled={index === count - 1}
            onClick={() => onMove(1)}
            aria-label="Move cell down"
          >
            ↓
          </button>
          <button type="button" className="weft-btn-primary px-2.5 py-1" disabled={running} onClick={onRun}>
            <PlayIcon width={14} height={14} />
            {running ? "Running…" : "Run"}
          </button>
          <button
            type="button"
            className="weft-btn-ghost px-2 py-1"
            style={{ color: "var(--weft-danger)" }}
            onClick={onDelete}
            aria-label="Delete cell"
          >
            <TrashIcon width={14} height={14} />
          </button>
        </div>
      </div>

      <Editor
        height={`${height}px`}
        language={langFor(cell.kind)}
        value={cell.source}
        onChange={(v) => onChange(v ?? "")}
        onMount={onMount}
        theme={monacoTheme}
        options={{
          fontFamily: "var(--weft-font-mono)",
          fontSize: 13,
          minimap: { enabled: false },
          scrollBeyondLastLine: false,
          automaticLayout: true,
          padding: { top: 8, bottom: 8 },
          lineNumbersMinChars: 3,
          renderLineHighlight: "line",
          tabSize: 2,
        }}
      />

      <CellOutput result={result} running={running} />
    </div>
  );
}

function CellOutput({ result, running }: { result: CellResult | undefined; running: boolean }) {
  if (running) {
    return (
      <div className="border-t border-hairline px-4 py-3 text-xs text-muted">Executing cell…</div>
    );
  }
  if (!result) return null;

  if (result.kind === "sql" && result.table) {
    const t = result.table;
    return (
      <div className="border-t border-hairline">
        <div className="flex items-center justify-between px-4 py-1.5 text-[11px] text-muted">
          <span>
            {t.rowCount} row{t.rowCount === 1 ? "" : "s"}
          </span>
          <span>{t.durationMs} ms</span>
        </div>
        <div className="overflow-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {t.columns.map((col) => (
                  <th
                    key={col}
                    className="border-y border-hairline bg-bg-subtle px-3 py-1.5 text-left font-mono text-[11px] font-semibold text-muted"
                  >
                    {col}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {t.rows.map((row, i) => (
                <tr key={i} className="hover:bg-bg-subtle">
                  {row.map((cell, j) => (
                    <td key={j} className="border-b border-hairline px-3 py-1 font-mono text-[11px] text-body">
                      {cell === null ? <span className="text-muted">NULL</span> : String(cell)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  if (result.kind === "markdown") {
    return (
      <div className="border-t border-hairline px-5 py-4">
        <MarkdownView source={result.text ?? ""} />
      </div>
    );
  }

  // python / text output → dark code surface.
  return (
    <pre
      className="overflow-auto border-t border-hairline px-4 py-3 text-xs leading-relaxed"
      style={{
        backgroundColor: "var(--weft-code-bg)",
        color: "var(--weft-code-text)",
        fontFamily: "var(--weft-font-mono)",
      }}
    >
      {result.text}
    </pre>
  );
}

/**
 * Minimal, dependency-free Markdown renderer — handles the subset notebooks
 * use (headings, bold/italic/inline-code, list items, paragraphs). Source is
 * escaped first so it is safe to render the resulting inline HTML.
 */
function MarkdownView({ source }: { source: string }) {
  const html = renderMarkdown(source);
  return (
    <div
      className="text-sm leading-relaxed text-body [&_code]:rounded [&_code]:bg-bg-subtle [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-mono [&_code]:text-xs [&_h1]:mb-2 [&_h1]:text-lg [&_h1]:font-semibold [&_h2]:mb-2 [&_h2]:text-base [&_h2]:font-semibold [&_h3]:mb-1 [&_h3]:text-sm [&_h3]:font-semibold [&_li]:ml-5 [&_li]:list-disc [&_p]:mb-2"
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderInline(s: string): string {
  return escapeHtml(s)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/\*([^*]+)\*/g, "<em>$1</em>");
}

function renderMarkdown(source: string): string {
  const out: string[] = [];
  for (const raw of source.split("\n")) {
    const line = raw.trimEnd();
    if (line.trim() === "") continue;
    const h = /^(#{1,3})\s+(.*)$/.exec(line);
    if (h) {
      const level = h[1].length;
      out.push(`<h${level}>${renderInline(h[2])}</h${level}>`);
      continue;
    }
    const li = /^[-*]\s+(.*)$/.exec(line);
    if (li) {
      out.push(`<li>${renderInline(li[1])}</li>`);
      continue;
    }
    out.push(`<p>${renderInline(line)}</p>`);
  }
  return out.join("");
}

function AiNotebookBar({ onGenerate }: { onGenerate: (prompt: string) => Promise<void> }) {
  const [prompt, setPrompt] = useState("");
  const [busy, setBusy] = useState(false);

  async function generate() {
    if (!prompt.trim() || busy) return;
    setBusy(true);
    try {
      await onGenerate(prompt.trim());
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="weft-card mb-4 flex flex-wrap items-center gap-2 px-4 py-3">
      <div className="flex items-center gap-1.5 text-sm font-semibold text-body">
        <SparklesIcon width={16} height={16} />
        AI
      </div>
      <input
        className="weft-input min-w-[200px] flex-1"
        placeholder="Generate a notebook — e.g. monthly revenue trend with a chart"
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && generate()}
      />
      <button
        type="button"
        className="weft-btn-primary"
        onClick={generate}
        disabled={busy || !prompt.trim()}
      >
        <SparklesIcon width={15} height={15} />
        {busy ? "Generating…" : "Generate notebook"}
      </button>
    </div>
  );
}
