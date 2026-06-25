import { useEffect, useState } from "react";
import { Page } from "../components/Layout";
import { DemoNote } from "../components/DemoNote";
import { ChevronRightIcon, PlusIcon } from "../components/icons";
import {
  api,
  type AddWidgetInput,
  type ChartType,
  type Dashboard,
  type DashboardDoc,
  type Widget,
  type WidgetData,
} from "../lib/api";

export function DashboardsPage() {
  const [dashboards, setDashboards] = useState<Dashboard[]>([]);
  const [openId, setOpenId] = useState<string | null>(null);

  useEffect(() => {
    api.listDashboards().then(setDashboards);
  }, []);

  if (openId) {
    return <DashboardView id={openId} onClose={() => setOpenId(null)} />;
  }

  return (
    <Page title="Dashboards" subtitle="Saved queries rendered as charts on a widget grid.">
      <DemoNote text="Demo data — live dashboards wiring pending." />
      {dashboards.length === 0 ? (
        <p className="text-sm text-muted">Loading dashboards…</p>
      ) : (
        <div className="flex flex-col gap-3">
          {dashboards.map((db) => (
            <button
              key={db.id}
              type="button"
              onClick={() => setOpenId(db.id)}
              className="weft-card flex items-center gap-4 px-5 py-4 text-left transition-colors hover:bg-bg-subtle"
            >
              <div className="min-w-0 flex-1">
                <span className="truncate text-sm font-semibold text-body">{db.name}</span>
                <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
                  <span className="font-mono">{db.id}</span>
                  <span>{db.tiles} widgets</span>
                  <span>by {db.owner}</span>
                  <span>updated {new Date(db.updatedAt).toLocaleDateString()}</span>
                </div>
              </div>
              <ChevronRightIcon width={16} height={16} className="shrink-0 text-muted" />
            </button>
          ))}
        </div>
      )}
    </Page>
  );
}

function DashboardView({ id, onClose }: { id: string; onClose: () => void }) {
  const [doc, setDoc] = useState<DashboardDoc | null>(null);
  const [showForm, setShowForm] = useState(false);

  useEffect(() => {
    api.getDashboard(id).then(setDoc);
  }, [id]);

  async function onAdd(input: AddWidgetInput) {
    // Live: POST /api/dashboards/:id/widgets appends and persists the widget.
    const widget = await api.addWidget(id, input);
    setDoc((d) => (d ? { ...d, widgets: [...d.widgets, widget] } : d));
    setShowForm(false);
  }

  if (!doc) {
    return (
      <Page title="Dashboard" subtitle="Loading…">
        <p className="text-sm text-muted">Loading dashboard…</p>
      </Page>
    );
  }

  return (
    <Page
      title={doc.name}
      subtitle="Each widget is a saved query rendered as a chart."
      actions={
        <div className="flex items-center gap-2">
          <button type="button" className="weft-btn-primary" onClick={() => setShowForm((v) => !v)}>
            <PlusIcon width={16} height={16} />
            Add widget
          </button>
          <button type="button" className="weft-btn-ghost" onClick={onClose}>
            Back to dashboards
          </button>
        </div>
      }
    >
      {showForm && <AddWidgetForm onSubmit={onAdd} onCancel={() => setShowForm(false)} />}

      {doc.widgets.length === 0 ? (
        <div className="weft-card grid place-items-center px-6 py-16 text-center">
          <p className="text-sm text-muted">No widgets yet. Add one to get started.</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          {doc.widgets.map((w) => (
            <WidgetCard key={w.id} widget={w} />
          ))}
        </div>
      )}
    </Page>
  );
}

function WidgetCard({ widget }: { widget: Widget }) {
  const [data, setData] = useState<WidgetData | null>(null);

  useEffect(() => {
    // Live: GET /api/widgets/:id/data materializes the saved query.
    api.widgetData(widget.id).then(setData);
  }, [widget.id]);

  return (
    <div className="weft-card flex flex-col overflow-hidden">
      <div className="border-b border-hairline px-4 py-3">
        <span className="text-sm font-semibold text-body">{widget.title}</span>
        <span className="ml-2 rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] uppercase tracking-wide text-muted">
          {widget.chart}
        </span>
      </div>

      <div className="px-4 py-4">
        {!data ? (
          <div className="grid h-[160px] place-items-center text-sm text-muted">Loading…</div>
        ) : widget.chart === "table" ? (
          <DataTable data={data} />
        ) : widget.chart === "line" ? (
          <LineChart data={data} />
        ) : (
          <BarChart data={data} />
        )}
      </div>

      <pre
        className="overflow-auto border-t border-hairline px-4 py-2 text-[11px] leading-relaxed"
        style={{
          backgroundColor: "var(--weft-code-bg)",
          color: "var(--weft-code-text)",
          fontFamily: "var(--weft-font-mono)",
        }}
      >
        {widget.query}
      </pre>
    </div>
  );
}

// --- Lightweight SVG charts (zero dependencies, token-driven colors) ---------

const CHART_W = 480;
const CHART_H = 160;
const PAD = { top: 12, right: 12, bottom: 24, left: 36 };

function seriesOf(data: WidgetData): { label: string; value: number }[] {
  return data.rows.map((r) => ({ label: String(r[0] ?? ""), value: Number(r[1] ?? 0) }));
}

function BarChart({ data }: { data: WidgetData }) {
  const pts = seriesOf(data);
  const innerW = CHART_W - PAD.left - PAD.right;
  const innerH = CHART_H - PAD.top - PAD.bottom;
  const max = Math.max(1, ...pts.map((p) => p.value));
  const bandW = innerW / Math.max(1, pts.length);
  const barW = bandW * 0.6;

  return (
    <svg viewBox={`0 0 ${CHART_W} ${CHART_H}`} className="h-[160px] w-full" role="img" aria-label="Bar chart">
      <Axes max={max} />
      {pts.map((p, i) => {
        const h = (p.value / max) * innerH;
        const x = PAD.left + i * bandW + (bandW - barW) / 2;
        const y = PAD.top + innerH - h;
        return (
          <g key={i}>
            <rect x={x} y={y} width={barW} height={h} rx={2} fill="var(--weft-accent)" />
            <text
              x={x + barW / 2}
              y={CHART_H - 8}
              textAnchor="middle"
              fontSize={9}
              fill="var(--weft-text-muted)"
            >
              {p.label}
            </text>
          </g>
        );
      })}
    </svg>
  );
}

function LineChart({ data }: { data: WidgetData }) {
  const pts = seriesOf(data);
  const innerW = CHART_W - PAD.left - PAD.right;
  const innerH = CHART_H - PAD.top - PAD.bottom;
  const max = Math.max(1, ...pts.map((p) => p.value));
  const stepX = innerW / Math.max(1, pts.length - 1);
  const coords = pts.map((p, i) => ({
    x: PAD.left + i * stepX,
    y: PAD.top + innerH - (p.value / max) * innerH,
    label: p.label,
  }));
  const path = coords.map((c, i) => `${i === 0 ? "M" : "L"}${c.x},${c.y}`).join(" ");

  return (
    <svg viewBox={`0 0 ${CHART_W} ${CHART_H}`} className="h-[160px] w-full" role="img" aria-label="Line chart">
      <Axes max={max} />
      <path d={path} fill="none" stroke="var(--weft-accent)" strokeWidth={2} />
      {coords.map((c, i) => (
        <g key={i}>
          <circle cx={c.x} cy={c.y} r={2.5} fill="var(--weft-accent)" />
          <text x={c.x} y={CHART_H - 8} textAnchor="middle" fontSize={9} fill="var(--weft-text-muted)">
            {c.label}
          </text>
        </g>
      ))}
    </svg>
  );
}

function Axes({ max }: { max: number }) {
  const innerH = CHART_H - PAD.top - PAD.bottom;
  return (
    <g>
      <line
        x1={PAD.left}
        y1={PAD.top}
        x2={PAD.left}
        y2={PAD.top + innerH}
        stroke="var(--weft-border)"
        strokeWidth={1}
      />
      <line
        x1={PAD.left}
        y1={PAD.top + innerH}
        x2={CHART_W - PAD.right}
        y2={PAD.top + innerH}
        stroke="var(--weft-border)"
        strokeWidth={1}
      />
      <text x={PAD.left - 6} y={PAD.top + 4} textAnchor="end" fontSize={9} fill="var(--weft-text-muted)">
        {max}
      </text>
      <text
        x={PAD.left - 6}
        y={PAD.top + innerH}
        textAnchor="end"
        fontSize={9}
        fill="var(--weft-text-muted)"
      >
        0
      </text>
    </g>
  );
}

function DataTable({ data }: { data: WidgetData }) {
  return (
    <div className="max-h-[160px] overflow-auto">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr>
            {data.columns.map((col) => (
              <th
                key={col}
                className="border-b border-hairline bg-bg-subtle px-3 py-1.5 text-left font-mono text-[11px] font-semibold text-muted"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.map((row, i) => (
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
  );
}

const CHART_TYPES: ChartType[] = ["bar", "line", "table"];

function AddWidgetForm({
  onSubmit,
  onCancel,
}: {
  onSubmit: (input: AddWidgetInput) => void;
  onCancel: () => void;
}) {
  const [title, setTitle] = useState("");
  const [chart, setChart] = useState<ChartType>("bar");
  const [query, setQuery] = useState("SELECT label, COUNT(*) FROM main.sales.orders GROUP BY 1");
  const [submitting, setSubmitting] = useState(false);

  const valid = title.trim().length > 0 && query.trim().length > 0;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      await onSubmit({ title: title.trim(), chart, query: query.trim() });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={submit} className="weft-card mb-5 px-5 py-5">
      <h2 className="mb-4 text-sm font-semibold text-body">New widget</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <div className="sm:col-span-2">
          <label className="weft-label" htmlFor="w-title">
            Title
          </label>
          <input
            id="w-title"
            className="weft-input"
            placeholder="Revenue by month"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            autoFocus
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="w-chart">
            Chart type
          </label>
          <select
            id="w-chart"
            className="weft-input"
            value={chart}
            onChange={(e) => setChart(e.target.value as ChartType)}
          >
            {CHART_TYPES.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </div>
        <div className="sm:col-span-3">
          <label className="weft-label" htmlFor="w-query">
            Query
          </label>
          <textarea
            id="w-query"
            className="weft-input min-h-[72px] resize-y font-mono text-xs"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
      </div>
      <div className="mt-5 flex justify-end gap-2">
        <button type="button" className="weft-btn-ghost" onClick={onCancel}>
          Cancel
        </button>
        <button type="submit" className="weft-btn-primary" disabled={!valid || submitting}>
          {submitting ? "Adding…" : "Add widget"}
        </button>
      </div>
    </form>
  );
}
