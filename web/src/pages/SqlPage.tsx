import { useEffect, useRef, useState } from "react";
import Editor, { loader, type Monaco, type OnMount } from "@monaco-editor/react";
import * as monaco from "monaco-editor";
import type { editor } from "monaco-editor";
import editorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";

// Self-host Monaco from the bundled `monaco-editor` package instead of the
// default jsdelivr CDN, so the editor works offline / behind the gateway.
// SQL has no language-service worker, so the base editor worker is enough.
self.MonacoEnvironment = {
  getWorker: () => new editorWorker(),
};
loader.config({ monaco });
import { Page } from "../components/Layout";
import { StatusBadge } from "../components/StatusBadge";
import { PlayIcon, SparklesIcon } from "../components/icons";
import { useTheme } from "../lib/theme";
import { setupMonacoSql, WEFT_DARK, WEFT_LIGHT } from "../lib/monaco";
import { api, type Cluster, type Query } from "../lib/api";

/** Live result for the grid: the gateway's columns/rows plus a client-side timing. */
interface LiveResult {
  columns: string[];
  rows: (string | number | null)[][];
  rowCount: number;
  durationMs: number;
}

// A simple default query that runs anywhere — proves the live path end to end.
const STARTER_SQL = `SELECT 1 AS a, 'hello' AS b;`;

const STATUS_TONE: Record<Query["status"], "success" | "warning" | "danger" | "muted"> = {
  finished: "success",
  running: "warning",
  failed: "danger",
  canceled: "muted",
};

export function SqlPage() {
  const { theme } = useTheme();
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [clusterId, setClusterId] = useState("");
  const [sql, setSql] = useState(STARTER_SQL);
  const [result, setResult] = useState<LiveResult | null>(null);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<Query[]>([]);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<Monaco | null>(null);

  useEffect(() => {
    api.listClusters().then((cs) => {
      setClusters(cs);
      const running = cs.find((c) => c.state === "running");
      setClusterId(running?.id ?? cs[0]?.id ?? "");
    });
  }, []);

  // Keep Monaco theme in sync with the app theme toggle.
  useEffect(() => {
    monacoRef.current?.editor.setTheme(theme === "dark" ? WEFT_DARK : WEFT_LIGHT);
  }, [theme]);

  const onMount: OnMount = (ed, monaco) => {
    editorRef.current = ed;
    monacoRef.current = monaco;
    setupMonacoSql(monaco);
    monaco.editor.setTheme(theme === "dark" ? WEFT_DARK : WEFT_LIGHT);
  };

  async function run() {
    if (running) return; // cluster is optional — the engine runs in-process
    setRunning(true);
    setError(null);
    const startedAt = performance.now();
    try {
      const r = await api.runSql(sql, clusterId || undefined);
      const durationMs = Math.round(performance.now() - startedAt);
      if (r.error) {
        setError(r.error);
        setResult(null);
        recordHistory(sql, clusterId, durationMs, "failed");
      } else {
        setResult({ columns: r.columns, rows: r.rows, rowCount: r.row_count, durationMs });
        recordHistory(sql, clusterId, durationMs, "finished");
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Query failed");
      setResult(null);
    } finally {
      setRunning(false);
    }
  }

  function recordHistory(text: string, cluster: string, durationMs: number, status: Query["status"]) {
    setHistory((h) => [
      {
        id: `h-${Date.now()}`,
        text,
        status,
        clusterId: cluster,
        user: "you",
        durationMs,
        startedAt: new Date().toISOString(),
      },
      ...h,
    ].slice(0, 25));
  }

  function applyGenerated(generated: string) {
    setSql(generated);
    editorRef.current?.setValue(generated);
    editorRef.current?.focus();
  }

  return (
    <Page
      title="SQL editor"
      subtitle="Write catalog-aware SQL, run it on a cluster, and inspect results."
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
          <button
            type="button"
            className="weft-btn-primary"
            onClick={run}
            disabled={running}
          >
            <PlayIcon width={15} height={15} />
            {running ? "Running…" : "Run"}
          </button>
        </div>
      }
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[1fr_320px]">
        <div className="flex min-w-0 flex-col gap-4">
          <div className="weft-card overflow-hidden">
            <Editor
              height="320px"
              language="sql"
              value={sql}
              onChange={(v) => setSql(v ?? "")}
              onMount={onMount}
              theme={theme === "dark" ? WEFT_DARK : WEFT_LIGHT}
              options={{
                fontFamily: "var(--weft-font-mono)",
                fontSize: 13,
                minimap: { enabled: false },
                scrollBeyondLastLine: false,
                automaticLayout: true,
                padding: { top: 12, bottom: 12 },
                lineNumbersMinChars: 3,
                renderLineHighlight: "line",
                tabSize: 2,
              }}
            />
          </div>

          <ResultGrid result={result} error={error} running={running} />
          <HistoryList history={history} onPick={applyGenerated} />
        </div>

        <AiAssistPanel onInsert={applyGenerated} />
      </div>
    </Page>
  );
}

function ResultGrid({
  result,
  error,
  running,
}: {
  result: LiveResult | null;
  error: string | null;
  running: boolean;
}) {
  if (error) {
    return (
      <div
        className="weft-card px-4 py-3 text-sm"
        role="alert"
        style={{
          color: "var(--weft-danger)",
          backgroundColor: "color-mix(in srgb, var(--weft-danger) 8%, transparent)",
        }}
      >
        <span className="font-semibold">Query error: </span>
        <span className="font-mono">{error}</span>
      </div>
    );
  }
  if (running) {
    return <div className="weft-card px-4 py-6 text-center text-sm text-muted">Executing query…</div>;
  }
  if (!result) {
    return (
      <div className="weft-card px-4 py-6 text-center text-sm text-muted">
        Run a query to see results.
      </div>
    );
  }
  return (
    <div className="weft-card overflow-hidden">
      <div className="flex items-center justify-between border-b border-hairline px-4 py-2 text-xs text-muted">
        <span>
          {result.rowCount} row{result.rowCount === 1 ? "" : "s"}
        </span>
        <span>{result.durationMs} ms</span>
      </div>
      <div className="overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr>
              {result.columns.map((col) => (
                <th
                  key={col}
                  className="border-b border-hairline bg-bg-subtle px-3 py-2 text-left font-mono text-xs font-semibold text-muted"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {result.rows.map((row, i) => (
              <tr key={i} className="hover:bg-bg-subtle">
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className="border-b border-hairline px-3 py-1.5 font-mono text-xs text-body"
                  >
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

function HistoryList({
  history,
  onPick,
}: {
  history: Query[];
  onPick: (sql: string) => void;
}) {
  if (history.length === 0) return null;
  return (
    <div className="weft-card overflow-hidden">
      <div className="border-b border-hairline px-4 py-2 text-xs font-semibold text-muted">
        Query history
      </div>
      <ul>
        {history.map((q) => (
          <li key={q.id} className="border-b border-hairline last:border-b-0">
            <button
              type="button"
              onClick={() => onPick(q.text)}
              className="flex w-full items-center gap-3 px-4 py-2 text-left hover:bg-bg-subtle"
            >
              <StatusBadge tone={STATUS_TONE[q.status]} label={q.status} />
              <span className="min-w-0 flex-1 truncate font-mono text-xs text-body">
                {q.text.replace(/\s+/g, " ")}
              </span>
              <span className="shrink-0 text-xs text-muted">
                {q.durationMs ? `${q.durationMs} ms` : "—"}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

function AiAssistPanel({ onInsert }: { onInsert: (sql: string) => void }) {
  const [prompt, setPrompt] = useState("");
  const [busy, setBusy] = useState(false);
  const [generated, setGenerated] = useState<string | null>(null);

  async function generate() {
    if (!prompt.trim() || busy) return;
    setBusy(true);
    try {
      // No AI backend yet: this returns a canned, prompt-flavored example query.
      const { sql } = await api.aiGenerateSql(prompt.trim());
      setGenerated(sql);
    } finally {
      setBusy(false);
    }
  }

  return (
    <aside className="weft-card flex h-fit flex-col gap-3 px-4 py-4">
      <div className="flex items-center gap-2 text-sm font-semibold text-body">
        <SparklesIcon width={16} height={16} />
        AI assist
        <span className="rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-muted">
          example
        </span>
      </div>
      <p className="text-xs text-muted">
        Demo only — no AI backend is wired yet. "Generate SQL" inserts a canned example query you can
        edit and run.
      </p>
      <textarea
        className="weft-input min-h-[88px] resize-y font-sans"
        placeholder="e.g. monthly revenue from sales for the last year"
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
      />
      <button
        type="button"
        className="weft-btn-primary justify-center"
        onClick={generate}
        disabled={busy || !prompt.trim()}
      >
        <SparklesIcon width={15} height={15} />
        {busy ? "Generating…" : "Generate SQL"}
      </button>

      {generated && (
        <div className="flex flex-col gap-2">
          <pre
            className="max-h-48 overflow-auto rounded-weft-sm px-3 py-2 text-xs leading-relaxed"
            style={{
              backgroundColor: "var(--weft-code-bg)",
              color: "var(--weft-code-text)",
              fontFamily: "var(--weft-font-mono)",
            }}
          >
            {generated}
          </pre>
          <button
            type="button"
            className="weft-btn-ghost justify-center"
            onClick={() => onInsert(generated)}
          >
            Insert into editor
          </button>
        </div>
      )}
    </aside>
  );
}
