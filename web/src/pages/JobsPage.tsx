import { useEffect, useState } from "react";
import { Page } from "../components/Layout";
import { StatusBadge } from "../components/StatusBadge";
import { ChevronRightIcon, PlayIcon } from "../components/icons";
import {
  api,
  type Job,
  type JobDoc,
  type JobRun,
  type JobStatus,
  type JobTask,
  type RunStatus,
} from "../lib/api";

const JOB_TONE: Record<JobStatus, "success" | "warning" | "danger" | "muted"> = {
  succeeded: "success",
  running: "warning",
  failed: "danger",
  scheduled: "muted",
  paused: "muted",
};

const RUN_TONE: Record<RunStatus, "success" | "warning" | "danger" | "muted"> = {
  SUCCESS: "success",
  RUNNING: "warning",
  FAILED: "danger",
  PENDING: "muted",
};

export function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [openId, setOpenId] = useState<string | null>(null);

  useEffect(() => {
    api.listJobs().then(setJobs);
  }, []);

  if (openId) {
    return <JobView id={openId} onClose={() => setOpenId(null)} />;
  }

  return (
    <Page title="Jobs" subtitle="Scheduled task DAGs with run history.">
      {jobs.length === 0 ? (
        <p className="text-sm text-muted">Loading jobs…</p>
      ) : (
        <div className="flex flex-col gap-3">
          {jobs.map((job) => (
            <button
              key={job.id}
              type="button"
              onClick={() => setOpenId(job.id)}
              className="weft-card flex items-center gap-4 px-5 py-4 text-left transition-colors hover:bg-bg-subtle"
            >
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-3">
                  <span className="truncate text-sm font-semibold text-body">{job.name}</span>
                  <StatusBadge tone={JOB_TONE[job.status]} label={job.status} />
                </div>
                <div className="mt-1.5 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted">
                  <span className="font-mono">{job.id}</span>
                  <span className="font-mono">{job.schedule}</span>
                  <span>by {job.owner}</span>
                  <span>last run {new Date(job.lastRun).toLocaleString()}</span>
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

function JobView({ id, onClose }: { id: string; onClose: () => void }) {
  const [doc, setDoc] = useState<JobDoc | null>(null);
  const [runs, setRuns] = useState<JobRun[]>([]);
  const [selectedRun, setSelectedRun] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  useEffect(() => {
    api.getJob(id).then(setDoc);
    api.jobRuns(id).then((rs) => {
      setRuns(rs);
      setSelectedRun(rs[0]?.id ?? null);
    });
  }, [id]);

  async function onRunNow() {
    setRunning(true);
    try {
      // Live: POST /api/jobs/:id/run enqueues the DAG on a cluster.
      const run = await api.runJob(id);
      setRuns((rs) => [run, ...rs]);
      setSelectedRun(run.id);
    } finally {
      setRunning(false);
    }
  }

  if (!doc) {
    return (
      <Page title="Job" subtitle="Loading…">
        <p className="text-sm text-muted">Loading job…</p>
      </Page>
    );
  }

  const active = runs.find((r) => r.id === selectedRun) ?? null;
  const taskStatus = new Map(active?.taskRuns.map((t) => [t.taskId, t.status]));

  return (
    <Page
      title={doc.name}
      subtitle={`Schedule ${doc.schedule} · ${doc.tasks.length} tasks`}
      actions={
        <div className="flex items-center gap-2">
          <button type="button" className="weft-btn-primary" onClick={onRunNow} disabled={running}>
            <PlayIcon width={15} height={15} />
            {running ? "Starting…" : "Run now"}
          </button>
          <button type="button" className="weft-btn-ghost" onClick={onClose}>
            Back to jobs
          </button>
        </div>
      }
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
            Task DAG {active && <span className="normal-case">· run {active.id}</span>}
          </h2>
          <TaskDag tasks={doc.tasks} taskStatus={taskStatus} />
        </section>

        <section>
          <h2 className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted">
            Run history
          </h2>
          <RunHistory runs={runs} selectedId={selectedRun} onSelect={setSelectedRun} />
        </section>
      </div>
    </Page>
  );
}

/**
 * Task DAG as a topologically-layered SVG: tasks placed in dependency levels,
 * with arrows drawn from each dependency to its dependent. Per-task status (for
 * the selected run) tints the node.
 */
function TaskDag({
  tasks,
  taskStatus,
}: {
  tasks: JobTask[];
  taskStatus: Map<string, RunStatus>;
}) {
  // Assign each task a level = 1 + max(level of deps).
  const levelOf = new Map<string, number>();
  function level(id: string, seen: Set<string>): number {
    if (levelOf.has(id)) return levelOf.get(id)!;
    if (seen.has(id)) return 0; // cycle guard
    seen.add(id);
    const t = tasks.find((x) => x.id === id);
    const l = !t || t.dependsOn.length === 0 ? 0 : 1 + Math.max(...t.dependsOn.map((d) => level(d, seen)));
    levelOf.set(id, l);
    return l;
  }
  tasks.forEach((t) => level(t.id, new Set()));

  // Group by level, then lay out a grid.
  const byLevel = new Map<number, JobTask[]>();
  for (const t of tasks) {
    const l = levelOf.get(t.id) ?? 0;
    if (!byLevel.has(l)) byLevel.set(l, []);
    byLevel.get(l)!.push(t);
  }
  const levels = [...byLevel.keys()].sort((a, b) => a - b);

  const NODE_W = 150;
  const NODE_H = 38;
  const GAP_X = 40;
  const GAP_Y = 18;
  const colCount = levels.length;
  const maxRows = Math.max(...levels.map((l) => byLevel.get(l)!.length), 1);
  const width = colCount * NODE_W + (colCount - 1) * GAP_X + 8;
  const height = maxRows * NODE_H + (maxRows - 1) * GAP_Y + 8;

  // Position each node.
  const pos = new Map<string, { x: number; y: number }>();
  levels.forEach((l, ci) => {
    const col = byLevel.get(l)!;
    col.forEach((t, ri) => {
      pos.set(t.id, {
        x: 4 + ci * (NODE_W + GAP_X),
        y: 4 + ri * (NODE_H + GAP_Y),
      });
    });
  });

  const tone = (id: string): string => {
    const s = taskStatus.get(id);
    if (s === "SUCCESS") return "var(--weft-success)";
    if (s === "FAILED") return "var(--weft-danger)";
    if (s === "RUNNING") return "var(--weft-warning)";
    return "var(--weft-border)";
  };

  return (
    <div className="weft-card overflow-auto p-3">
      <svg viewBox={`0 0 ${width} ${height}`} width={width} height={height} role="img" aria-label="Task DAG">
        <defs>
          <marker
            id="dag-arrow"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="6"
            markerHeight="6"
            orient="auto-start-reverse"
          >
            <path d="M0,0 L10,5 L0,10 z" fill="var(--weft-text-muted)" />
          </marker>
        </defs>

        {/* Dependency edges. */}
        {tasks.flatMap((t) =>
          t.dependsOn.map((dep) => {
            const from = pos.get(dep);
            const to = pos.get(t.id);
            if (!from || !to) return null;
            const x1 = from.x + NODE_W;
            const y1 = from.y + NODE_H / 2;
            const x2 = to.x;
            const y2 = to.y + NODE_H / 2;
            const mx = (x1 + x2) / 2;
            return (
              <path
                key={`${dep}->${t.id}`}
                d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`}
                fill="none"
                stroke="var(--weft-text-muted)"
                strokeWidth={1.5}
                markerEnd="url(#dag-arrow)"
              />
            );
          }),
        )}

        {/* Task nodes. */}
        {tasks.map((t) => {
          const p = pos.get(t.id)!;
          const c = tone(t.id);
          const s = taskStatus.get(t.id);
          return (
            <g key={t.id}>
              <rect
                x={p.x}
                y={p.y}
                width={NODE_W}
                height={NODE_H}
                rx={6}
                fill="var(--weft-surface)"
                stroke={c}
                strokeWidth={1.75}
              />
              <circle cx={p.x + 12} cy={p.y + NODE_H / 2} r={4} fill={c} />
              <text
                x={p.x + 24}
                y={p.y + NODE_H / 2 + 4}
                fontSize={11}
                fontFamily="var(--weft-font-mono)"
                fill="var(--weft-text)"
              >
                {t.name.length > 18 ? `${t.name.slice(0, 17)}…` : t.name}
              </text>
              {s && (
                <text
                  x={p.x + NODE_W - 6}
                  y={p.y + NODE_H - 5}
                  textAnchor="end"
                  fontSize={8}
                  fill="var(--weft-text-muted)"
                >
                  {s}
                </text>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function RunHistory({
  runs,
  selectedId,
  onSelect,
}: {
  runs: JobRun[];
  selectedId: string | null;
  onSelect: (id: string) => void;
}) {
  if (runs.length === 0) {
    return (
      <div className="weft-card grid place-items-center px-6 py-12 text-center">
        <p className="text-sm text-muted">No runs yet. Trigger one with “Run now”.</p>
      </div>
    );
  }
  return (
    <div className="weft-card overflow-hidden">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr>
            {["Run", "Status", "Started", "Duration"].map((h) => (
              <th
                key={h}
                className="border-b border-hairline bg-bg-subtle px-4 py-2 text-left text-xs font-semibold text-muted"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr
              key={r.id}
              onClick={() => onSelect(r.id)}
              className={[
                "cursor-pointer",
                r.id === selectedId ? "bg-bg-subtle" : "hover:bg-bg-subtle",
              ].join(" ")}
            >
              <td className="border-b border-hairline px-4 py-2 font-mono text-xs text-body">{r.id}</td>
              <td className="border-b border-hairline px-4 py-2">
                <StatusBadge tone={RUN_TONE[r.status]} label={r.status} />
              </td>
              <td className="border-b border-hairline px-4 py-2 text-xs text-muted">
                {new Date(r.startedAt).toLocaleString()}
              </td>
              <td className="border-b border-hairline px-4 py-2 text-xs text-muted">
                {r.durationMs ? `${Math.round(r.durationMs / 1000)}s` : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
