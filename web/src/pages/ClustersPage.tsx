import { useEffect, useState } from "react";
import { Page } from "../components/Layout";
import { StatusBadge } from "../components/StatusBadge";
import { PlayIcon, PlusIcon, StopIcon, TrashIcon } from "../components/icons";
import {
  api,
  type Cluster,
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

export function ClustersPage() {
  const [clusters, setClusters] = useState<Cluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);

  useEffect(() => {
    api
      .listClusters()
      .then(setClusters)
      .finally(() => setLoading(false));
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
    });
  }

  async function onCreate(input: CreateClusterInput) {
    const created = await api.createCluster(input);
    setClusters((cs) => [created, ...cs]);
    setShowForm(false);
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
              onStart={() => onStart(c.id)}
              onStop={() => onStop(c.id)}
              onDelete={() => onDelete(c.id)}
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
  onStart,
  onStop,
  onDelete,
}: {
  cluster: Cluster;
  busy: boolean;
  onStart: () => void;
  onStop: () => void;
  onDelete: () => void;
}) {
  const isRunning = cluster.state === "running";
  return (
    <div className="weft-card flex flex-wrap items-center gap-4 px-5 py-4">
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-3">
          <span className="truncate text-sm font-semibold text-body">{cluster.name}</span>
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
  );
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
  const [submitting, setSubmitting] = useState(false);

  const valid = name.trim().length > 0 && minWorkers >= 1 && maxWorkers >= minWorkers;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid) return;
    setSubmitting(true);
    try {
      await onSubmit({ name: name.trim(), size, minWorkers, maxWorkers });
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
