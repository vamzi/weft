import { useEffect, useRef, useState } from "react";
import { Page } from "../components/Layout";
import { StatusBadge } from "../components/StatusBadge";
import { ChevronRightIcon, PlayIcon, PlusIcon, StopIcon, TrashIcon } from "../components/icons";
import {
  api,
  type Cluster,
  type ClusterEvent,
  type ClusterSize,
  type ClusterState,
  type CreateClusterInput,
} from "../lib/api";

const STATE_TONE: Record<ClusterState, "success" | "warning" | "danger" | "muted"> = {
  running: "success",
  pending: "warning",
  terminating: "warning",
  stopped: "muted",
  error: "danger",
};

const SIZES: ClusterSize[] = ["small", "medium", "large", "xlarge"];

/** How often the list (and any expanded events feed) re-polls the gateway. */
const REFRESH_MS = 3000;

export function ClustersPage() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);

  // Auto-refresh the list every ~3s so state visibly advances
  // (PENDING → PROVISIONING → RUNNING) without the user reloading.
  useEffect(() => {
    let alive = true;
    const refresh = () =>
      api
        .listClusters()
        .then((cs) => {
          if (alive) setClusters(cs);
        })
        .catch(() => {
          /* transient poll error — keep the last good list */
        })
        .finally(() => {
          if (alive) setLoading(false);
        });

    refresh();
    const timer = setInterval(refresh, REFRESH_MS);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  async function withBusy(id: string, fn: () => Promise<void>) {
    setBusy(id);
    try {
      await fn();
    } finally {
      setBusy(null);
    }
  }

  async function onStart(id: string) {
    await withBusy(id, async () => {
      const updated = await api.startCluster(id);
      setClusters((cs) => cs.map((c) => (c.id === id ? updated : c)));
    });
  }

  async function onStop(id: string) {
    await withBusy(id, async () => {
      const updated = await api.stopCluster(id);
      setClusters((cs) => cs.map((c) => (c.id === id ? updated : c)));
    });
  }

  async function onDelete(id: string) {
    await withBusy(id, async () => {
      await api.deleteCluster(id);
      setClusters((cs) => cs.filter((c) => c.id !== id));
      setExpanded((e) => (e === id ? null : e));
    });
  }

  async function onCreate(input: CreateClusterInput) {
    const created = await api.createCluster(input);
    setClusters((cs) => [created, ...cs]);
    setShowForm(false);
  }

  async function onSaveAutoTerminate(id: string, mins: number | null) {
    await withBusy(id, async () => {
      const updated = await api.updateClusterConfig(id, mins);
      setClusters((cs) => cs.map((c) => (c.id === id ? updated : c)));
    });
  }

  return (
    <Page
      title="Clusters"
      subtitle="Compute that runs your SQL, notebooks, and jobs."
      actions={
        <button type="button" className="weft-btn-primary" onClick={() => setShowForm((v) => !v)}>
          <PlusIcon width={16} height={16} />
          Create cluster
        </button>
      }
    >
      {showForm && <CreateClusterForm onSubmit={onCreate} onCancel={() => setShowForm(false)} />}

      {loading ? (
        <p className="text-sm text-muted">Loading clusters…</p>
      ) : clusters.length === 0 ? (
        <div className="weft-card grid place-items-center px-6 py-16 text-center">
          <p className="text-sm text-muted">No clusters yet. Create one to get started.</p>
        </div>
      ) : (
        <div className="flex flex-col gap-3">
          {clusters.map((c) => (
            <ClusterCard
              key={c.id}
              cluster={c}
              busy={busy === c.id}
              expanded={expanded === c.id}
              onToggle={() => setExpanded((e) => (e === c.id ? null : c.id))}
              onStart={() => onStart(c.id)}
              onStop={() => onStop(c.id)}
              onDelete={() => onDelete(c.id)}
              onSaveAutoTerminate={(mins) => onSaveAutoTerminate(c.id, mins)}
            />
          ))}
        </div>
      )}
    </Page>
  );
}

function ClusterCard({
  cluster,
  busy,
  expanded,
  onToggle,
  onStart,
  onStop,
  onDelete,
  onSaveAutoTerminate,
}: {
  cluster: Cluster;
  busy: boolean;
  expanded: boolean;
  onToggle: () => void;
  onStart: () => void;
  onStop: () => void;
  onDelete: () => void;
  onSaveAutoTerminate: (mins: number | null) => Promise<void>;
}) {
  const isRunning = cluster.state === "running";
  const isStopped = cluster.state === "stopped";
  const [editTerm, setEditTerm] = useState(false);
  const [termValue, setTermValue] = useState<string>(
    cluster.autoTerminateMinutes != null ? String(cluster.autoTerminateMinutes) : "",
  );
  const tagEntries = Object.entries(cluster.tags ?? {});
  return (
    <div className="weft-card overflow-hidden">
      <div className="flex flex-wrap items-center gap-4 px-5 py-4">
        <button
          type="button"
          className="grid h-7 w-7 shrink-0 place-items-center rounded-md text-muted transition hover:text-body"
          style={{ backgroundColor: "color-mix(in srgb, var(--weft-text-muted) 8%, transparent)" }}
          onClick={onToggle}
          aria-expanded={expanded}
          aria-label={expanded ? `Hide events for ${cluster.name}` : `Show events for ${cluster.name}`}
        >
          <ChevronRightIcon
            width={15}
            height={15}
            style={{
              transform: expanded ? "rotate(90deg)" : "none",
              transition: "transform 150ms ease",
            }}
          />
        </button>

        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-3">
            <button
              type="button"
              className="truncate text-left text-sm font-semibold text-body hover:underline"
              onClick={onToggle}
            >
              {cluster.name}
            </button>
            <StatusBadge tone={STATE_TONE[cluster.state]} label={cluster.state} />
          </div>
          <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
            <span className="font-mono">{cluster.id}</span>
            <span>{cluster.size}</span>
            <span>
              {cluster.activeWorkers}/{cluster.maxWorkers} workers
            </span>
            <span>{cluster.runtime}</span>
            <span>by {cluster.creator}</span>
          </div>
          {cluster.image && (
            <div className="mt-2.5 flex items-center gap-2 text-xs text-muted">
              <span className="shrink-0">image</span>
              <EndpointChip endpoint={cluster.image} />
            </div>
          )}
          {tagEntries.length > 0 && (
            <div className="mt-2.5 flex flex-wrap items-center gap-1.5 text-xs text-muted">
              <span className="shrink-0">tags</span>
              {tagEntries.map(([k, v]) => (
                <span key={k} className="rounded-full bg-bg-subtle px-2 py-0.5 font-mono text-[11px]">
                  {k}={v}
                </span>
              ))}
            </div>
          )}
          <div className="mt-2.5 flex items-center gap-2 text-xs text-muted">
            <span className="shrink-0">auto-terminate</span>
            {editTerm ? (
              <>
                <input
                  type="number"
                  min={1}
                  className="weft-input h-7 w-24 text-xs"
                  placeholder="never"
                  value={termValue}
                  onChange={(e) => setTermValue(e.target.value)}
                />
                <span>min</span>
                <button
                  type="button"
                  className="text-accent hover:underline"
                  disabled={busy}
                  onClick={async () => {
                    const mins = termValue.trim() === "" ? null : Math.max(1, Number(termValue));
                    await onSaveAutoTerminate(mins);
                    setEditTerm(false);
                  }}
                >
                  Save
                </button>
                <button type="button" className="hover:underline" onClick={() => setEditTerm(false)}>
                  Cancel
                </button>
              </>
            ) : (
              <>
                <span className="font-mono">
                  {cluster.autoTerminateMinutes != null
                    ? `${cluster.autoTerminateMinutes} min idle`
                    : "never"}
                </span>
                {isStopped && (
                  <button
                    type="button"
                    className="text-accent hover:underline"
                    onClick={() => setEditTerm(true)}
                  >
                    Edit
                  </button>
                )}
                {!isStopped && <span className="text-[11px]">(editable when stopped)</span>}
              </>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2">
          {isRunning ? (
            <button type="button" className="weft-btn-ghost" disabled={busy} onClick={onStop}>
              <StopIcon width={15} height={15} />
              Stop
            </button>
          ) : (
            <button
              type="button"
              className="weft-btn-ghost"
              disabled={busy || cluster.state === "pending"}
              onClick={onStart}
            >
              <PlayIcon width={15} height={15} />
              Start
            </button>
          )}
          <button
            type="button"
            className="weft-btn-ghost"
            style={{ color: "var(--weft-danger)" }}
            disabled={busy}
            onClick={onDelete}
            aria-label={`Delete ${cluster.name}`}
          >
            <TrashIcon width={15} height={15} />
          </button>
        </div>
      </div>

      {expanded && <ClusterEvents clusterId={cluster.id} />}
    </div>
  );
}

/** Monospace `sc://host:port` chip on the dark code surface with a copy button. */
function EndpointChip({ endpoint }: { endpoint: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(endpoint);
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
    } catch {
      /* clipboard blocked — ignore */
    }
  }

  return (
    <div
      className="inline-flex max-w-full items-center gap-2 rounded-md px-2.5 py-1"
      style={{ backgroundColor: "var(--weft-code-bg)" }}
    >
      <span
        className="truncate font-mono text-xs"
        style={{ color: "var(--weft-code-text)" }}
        title={endpoint}
      >
        {endpoint}
      </span>
      <button
        type="button"
        className="shrink-0 rounded px-1.5 py-0.5 text-xs font-medium transition"
        style={{
          color: copied ? "var(--weft-success)" : "var(--weft-code-text)",
          backgroundColor: "color-mix(in srgb, var(--weft-code-text) 12%, transparent)",
        }}
        onClick={copy}
        aria-label="Copy connect endpoint"
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

/** Lifecycle timeline for one cluster; polls every ~3s while expanded. */
function ClusterEvents({ clusterId }: { clusterId: string }) {
  const [events, setEvents] = useState<ClusterEvent[] | null>(null);
  const [error, setError] = useState(false);
  const loadedOnce = useRef(false);

  useEffect(() => {
    let alive = true;
    loadedOnce.current = false;
    setEvents(null);
    setError(false);

    const refresh = () =>
      api
        .clusterEvents(clusterId)
        .then((evs) => {
          if (!alive) return;
          setEvents(evs);
          setError(false);
          loadedOnce.current = true;
        })
        .catch(() => {
          if (!alive) return;
          if (!loadedOnce.current) setError(true);
        });

    refresh();
    const timer = setInterval(refresh, REFRESH_MS);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, [clusterId]);

  return (
    <div
      className="border-t px-5 py-4"
      style={{ borderColor: "var(--weft-border)", backgroundColor: "var(--weft-bg-subtle)" }}
    >
      <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-muted">Lifecycle events</div>
      {error ? (
        <p className="text-xs text-muted">Couldn’t load events.</p>
      ) : events === null ? (
        <p className="text-xs text-muted">Loading events…</p>
      ) : events.length === 0 ? (
        <p className="text-xs text-muted">No events yet.</p>
      ) : (
        <ol className="flex flex-col">
          {events.map((ev, i) => (
            <li key={`${ev.at}-${i}`} className="flex gap-3">
              <div className="flex flex-col items-center">
                <span
                  className="mt-1.5 h-2 w-2 shrink-0 rounded-full"
                  style={{ backgroundColor: "var(--weft-accent)" }}
                />
                {i < events.length - 1 && (
                  <span className="w-px flex-1" style={{ backgroundColor: "var(--weft-border)" }} />
                )}
              </div>
              <div className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 pb-3">
                <span className="font-mono text-xs text-muted">{formatTime(ev.at)}</span>
                <span className="text-sm text-body">{ev.message}</span>
              </div>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}

/** Format a unix-seconds timestamp as a readable local time. */
function formatTime(unixSeconds: number): string {
  const d = new Date(unixSeconds * 1000);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function CreateClusterForm({
  onSubmit,
  onCancel,
}: {
  onSubmit: (input: CreateClusterInput) => void;
  onCancel: () => void;
}) {
  const [name, setName] = useState("");
  const [size, setSize] = useState<ClusterSize>("medium");
  const [minWorkers, setMinWorkers] = useState(1);
  const [maxWorkers, setMaxWorkers] = useState(4);
  const [autoTerminate, setAutoTerminate] = useState<string>("");
  const [tagRows, setTagRows] = useState<{ key: string; value: string }[]>([
    { key: "", value: "" },
  ]);
  const [submitting, setSubmitting] = useState(false);

  const valid = name.trim().length > 0 && minWorkers >= 1 && maxWorkers >= minWorkers;

  function setTag(i: number, field: "key" | "value", v: string) {
    setTagRows((rows) => rows.map((r, idx) => (idx === i ? { ...r, [field]: v } : r)));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid) return;
    setSubmitting(true);
    try {
      const tags: Record<string, string> = {};
      for (const { key, value } of tagRows) {
        const k = key.trim();
        if (k) tags[k] = value.trim();
      }
      const mins = autoTerminate.trim() === "" ? null : Math.max(1, Number(autoTerminate));
      await onSubmit({ name: name.trim(), size, minWorkers, maxWorkers, tags, autoTerminateMinutes: mins });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={handleSubmit} className="weft-card mb-5 px-5 py-5">
      <h2 className="mb-4 text-sm font-semibold text-body">New cluster</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div className="sm:col-span-2">
          <label className="weft-label" htmlFor="cl-name">
            Name
          </label>
          <input
            id="cl-name"
            className="weft-input"
            placeholder="analytics-prod"
            value={name}
            onChange={(e) => setName(e.target.value)}
            autoFocus
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="cl-size">
            Size
          </label>
          <select
            id="cl-size"
            className="weft-input"
            value={size}
            onChange={(e) => setSize(e.target.value as ClusterSize)}
          >
            {SIZES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="weft-label" htmlFor="cl-min">
              Min workers
            </label>
            <input
              id="cl-min"
              type="number"
              min={1}
              className="weft-input"
              value={minWorkers}
              onChange={(e) => setMinWorkers(Math.max(1, Number(e.target.value)))}
            />
          </div>
          <div>
            <label className="weft-label" htmlFor="cl-max">
              Max workers
            </label>
            <input
              id="cl-max"
              type="number"
              min={1}
              className="weft-input"
              value={maxWorkers}
              onChange={(e) => setMaxWorkers(Math.max(1, Number(e.target.value)))}
            />
          </div>
        </div>
        <div>
          <label className="weft-label" htmlFor="cl-autoterm">
            Auto-terminate (idle minutes)
          </label>
          <input
            id="cl-autoterm"
            type="number"
            min={1}
            className="weft-input"
            placeholder="never"
            value={autoTerminate}
            onChange={(e) => setAutoTerminate(e.target.value)}
          />
          <p className="mt-1 text-xs text-muted">Leave blank to never auto-terminate.</p>
        </div>
        <div className="sm:col-span-2">
          <label className="weft-label">Tags (applied to cluster pods)</label>
          <div className="flex flex-col gap-2">
            {tagRows.map((row, i) => (
              <div key={i} className="flex items-center gap-2">
                <input
                  className="weft-input font-mono text-xs"
                  placeholder="key (e.g. team)"
                  value={row.key}
                  onChange={(e) => setTag(i, "key", e.target.value)}
                />
                <input
                  className="weft-input font-mono text-xs"
                  placeholder="value (e.g. analytics)"
                  value={row.value}
                  onChange={(e) => setTag(i, "value", e.target.value)}
                />
                <button
                  type="button"
                  className="shrink-0 rounded-weft-sm px-2 py-1 text-xs text-muted hover:bg-bg-subtle"
                  onClick={() => setTagRows((rows) => rows.filter((_, idx) => idx !== i))}
                  aria-label="Remove tag"
                >
                  ✕
                </button>
              </div>
            ))}
            <button
              type="button"
              className="self-start text-xs text-accent hover:underline"
              onClick={() => setTagRows((rows) => [...rows, { key: "", value: "" }])}
            >
              + Add tag
            </button>
          </div>
        </div>
      </div>
      {maxWorkers < minWorkers && (
        <p className="mt-2 text-xs" style={{ color: "var(--weft-danger)" }}>
          Max workers must be ≥ min workers.
        </p>
      )}
      <div className="mt-5 flex justify-end gap-2">
        <button type="button" className="weft-btn-ghost" onClick={onCancel}>
          Cancel
        </button>
        <button type="submit" className="weft-btn-primary" disabled={!valid || submitting}>
          {submitting ? "Creating…" : "Create cluster"}
        </button>
      </div>
    </form>
  );
}
