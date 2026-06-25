import { useEffect, useMemo, useState } from "react";
import { Page } from "../components/Layout";
import {
  ChevronRightIcon,
  DatabaseIcon,
  PlugIcon,
  TableIcon,
} from "../components/icons";
import {
  api,
  type AttachCatalogInput,
  type CatalogObject,
  type ExternalCatalogType,
  type TableDetail,
} from "../lib/api";

/** fqn for a catalog object: parent path + name. */
function fqnOf(o: CatalogObject): string {
  return o.parent ? `${o.parent}.${o.name}` : o.name;
}

export function CatalogPage() {
  const [objects, setObjects] = useState<CatalogObject[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set(["main", "main.sales"]));
  const [selected, setSelected] = useState<string | null>(null);
  const [detail, setDetail] = useState<TableDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [showConnect, setShowConnect] = useState(false);

  useEffect(() => {
    api.listCatalog().then(setObjects);
  }, []);

  // Group children by parent fqn for tree rendering.
  const childrenOf = useMemo(() => {
    const map = new Map<string | null, CatalogObject[]>();
    for (const o of objects) {
      const key = o.parent;
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(o);
    }
    return map;
  }, [objects]);

  const roots = childrenOf.get(null) ?? [];

  function toggle(fqn: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(fqn) ? next.delete(fqn) : next.add(fqn);
      return next;
    });
  }

  function select(o: CatalogObject) {
    const fqn = fqnOf(o);
    if (o.kind === "table" || o.kind === "view") {
      setSelected(fqn);
      setDetailLoading(true);
      api
        .tableDetail(fqn)
        .then(setDetail)
        .catch(() => setDetail(null))
        .finally(() => setDetailLoading(false));
    } else {
      toggle(fqn);
    }
  }

  // Recursive tree node.
  function renderNode(o: CatalogObject, depth: number) {
    const fqn = fqnOf(o);
    const kids = childrenOf.get(fqn) ?? [];
    const hasKids = o.kind === "catalog" || o.kind === "schema";
    const isOpen = expanded.has(fqn);
    const isSelected = selected === fqn;
    return (
      <div key={o.id}>
        <button
          type="button"
          onClick={() => select(o)}
          className={[
            "flex w-full items-center gap-1.5 rounded-weft-sm px-2 py-1.5 text-left text-sm transition-colors",
            isSelected ? "bg-bg-subtle text-accent" : "text-body hover:bg-bg-subtle",
          ].join(" ")}
          style={{ paddingLeft: `${depth * 14 + 8}px` }}
        >
          {hasKids ? (
            <ChevronRightIcon
              width={14}
              height={14}
              style={{ transform: isOpen ? "rotate(90deg)" : "none", transition: "transform .12s" }}
            />
          ) : (
            <span className="inline-block w-3.5" />
          )}
          {o.kind === "catalog" || o.kind === "schema" ? (
            <DatabaseIcon width={14} height={14} className="shrink-0 text-muted" />
          ) : (
            <TableIcon width={14} height={14} className="shrink-0 text-muted" />
          )}
          <span className="truncate">{o.name}</span>
          <span className="ml-auto shrink-0 text-[10px] uppercase tracking-wide text-muted">
            {o.kind}
          </span>
        </button>
        {hasKids && isOpen && kids.map((k) => renderNode(k, depth + 1))}
      </div>
    );
  }

  return (
    <Page
      title="Catalog"
      subtitle="Browse governed catalogs, schemas, tables, and views."
      actions={
        <button type="button" className="weft-btn-ghost" onClick={() => setShowConnect(true)}>
          <PlugIcon width={15} height={15} />
          Connections
        </button>
      }
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[320px_1fr]">
        <div className="weft-card p-2">
          {roots.length === 0 ? (
            <p className="px-2 py-3 text-sm text-muted">Loading catalog…</p>
          ) : (
            roots.map((o) => renderNode(o, 0))
          )}
        </div>

        <DetailPanel detail={detail} loading={detailLoading} hasSelection={selected !== null} />
      </div>

      {showConnect && <ConnectModal onClose={() => setShowConnect(false)} />}
    </Page>
  );
}

function DetailPanel({
  detail,
  loading,
  hasSelection,
}: {
  detail: TableDetail | null;
  loading: boolean;
  hasSelection: boolean;
}) {
  if (!hasSelection) {
    return (
      <div className="weft-card grid place-items-center px-6 py-16 text-center">
        <p className="text-sm text-muted">Select a table or view to see its schema and metadata.</p>
      </div>
    );
  }
  if (loading) {
    return (
      <div className="weft-card grid place-items-center px-6 py-16 text-center">
        <p className="text-sm text-muted">Loading details…</p>
      </div>
    );
  }
  if (!detail) {
    return (
      <div className="weft-card grid place-items-center px-6 py-16 text-center">
        <p className="text-sm text-muted">No details available for this object.</p>
      </div>
    );
  }

  return (
    <div className="weft-card overflow-hidden">
      <div className="border-b border-hairline px-5 py-4">
        <div className="flex items-center gap-2">
          <TableIcon width={16} height={16} className="text-muted" />
          <span className="font-mono text-sm font-semibold text-body">{detail.fqn}</span>
          <span className="rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] uppercase tracking-wide text-muted">
            {detail.kind}
          </span>
        </div>
        <dl className="mt-3 grid grid-cols-2 gap-x-6 gap-y-1.5 text-xs sm:grid-cols-4">
          <Meta label="Owner" value={detail.owner} />
          <Meta label="Format" value={detail.format} />
          <Meta label="Rows" value={detail.rows != null ? detail.rows.toLocaleString() : "—"} />
          <Meta label="Size" value={detail.sizeBytes != null ? fmtBytes(detail.sizeBytes) : "—"} />
          <Meta label="Created" value={new Date(detail.createdAt).toLocaleDateString()} />
          <div className="col-span-2 sm:col-span-4">
            <dt className="text-muted">Location</dt>
            <dd className="mt-0.5 break-all font-mono text-[11px] text-body">{detail.location}</dd>
          </div>
        </dl>
      </div>

      <div className="px-5 py-2 text-xs font-semibold text-muted">Columns ({detail.columns.length})</div>
      <div className="overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr>
              {["Column", "Type", "Nullable", "Comment"].map((h) => (
                <th
                  key={h}
                  className="border-y border-hairline bg-bg-subtle px-4 py-2 text-left text-xs font-semibold text-muted"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {detail.columns.map((c) => (
              <tr key={c.name} className="hover:bg-bg-subtle">
                <td className="border-b border-hairline px-4 py-1.5 font-mono text-xs text-body">{c.name}</td>
                <td className="border-b border-hairline px-4 py-1.5 font-mono text-xs text-muted">{c.type}</td>
                <td className="border-b border-hairline px-4 py-1.5 text-xs text-muted">
                  {c.nullable ? "YES" : "NO"}
                </td>
                <td className="border-b border-hairline px-4 py-1.5 text-xs text-muted">
                  {c.comment ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function Meta({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <dt className="text-muted">{label}</dt>
      <dd className="mt-0.5 text-body">{value}</dd>
    </div>
  );
}

const CONNECTOR_TYPES: { value: ExternalCatalogType; label: string; uriHint: string }[] = [
  { value: "hms", label: "Hive Metastore (HMS)", uriHint: "thrift://metastore:9083" },
  { value: "glue", label: "AWS Glue", uriHint: "glue://us-east-1/123456789012" },
  { value: "unity", label: "Unity Catalog", uriHint: "https://dbc-xxxx.cloud.databricks.com" },
  { value: "local", label: "Local filesystem", uriHint: "file:///data/warehouse" },
];

function ConnectModal({ onClose }: { onClose: () => void }) {
  const [name, setName] = useState("");
  const [type, setType] = useState<ExternalCatalogType>("hms");
  const [uri, setUri] = useState("");
  const [comment, setComment] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [done, setDone] = useState<string | null>(null);

  const hint = CONNECTOR_TYPES.find((c) => c.value === type)?.uriHint ?? "";
  const valid = name.trim().length > 0 && uri.trim().length > 0;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      const input: AttachCatalogInput = {
        name: name.trim(),
        type,
        uri: uri.trim(),
        comment: comment.trim() || undefined,
      };
      const res = await api.attachCatalog(input);
      setDone(res.name);
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      className="fixed inset-0 z-40 grid place-items-center bg-black/40 p-4"
      onClick={onClose}
      role="presentation"
    >
      <div
        className="weft-card w-full max-w-md p-5"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-modal="true"
        aria-label="Attach external catalog"
      >
        {done ? (
          <div className="text-center">
            <h2 className="text-sm font-semibold text-body">Connection attached</h2>
            <p className="mt-2 text-sm text-muted">
              External catalog <span className="font-mono text-body">{done}</span> is now mounted and
              its objects will appear in the tree.
            </p>
            <button type="button" className="weft-btn-primary mt-5" onClick={onClose}>
              Done
            </button>
          </div>
        ) : (
          <form onSubmit={submit}>
            <h2 className="mb-1 text-sm font-semibold text-body">Attach external catalog</h2>
            <p className="mb-4 text-xs text-muted">
              Mount a Hive Metastore, AWS Glue, Unity Catalog, or local warehouse as a governed
              catalog.
            </p>
            <div className="flex flex-col gap-4">
              <div>
                <label className="weft-label" htmlFor="conn-type">
                  Type
                </label>
                <select
                  id="conn-type"
                  className="weft-input"
                  value={type}
                  onChange={(e) => setType(e.target.value as ExternalCatalogType)}
                >
                  {CONNECTOR_TYPES.map((c) => (
                    <option key={c.value} value={c.value}>
                      {c.label}
                    </option>
                  ))}
                </select>
              </div>
              <div>
                <label className="weft-label" htmlFor="conn-name">
                  Catalog name
                </label>
                <input
                  id="conn-name"
                  className="weft-input"
                  placeholder="legacy_hive"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  autoFocus
                />
              </div>
              <div>
                <label className="weft-label" htmlFor="conn-uri">
                  Connection URI
                </label>
                <input
                  id="conn-uri"
                  className="weft-input font-mono"
                  placeholder={hint}
                  value={uri}
                  onChange={(e) => setUri(e.target.value)}
                />
              </div>
              <div>
                <label className="weft-label" htmlFor="conn-comment">
                  Comment (optional)
                </label>
                <input
                  id="conn-comment"
                  className="weft-input"
                  placeholder="Read-only mirror of the legacy lake"
                  value={comment}
                  onChange={(e) => setComment(e.target.value)}
                />
              </div>
            </div>
            <div className="mt-5 flex justify-end gap-2">
              <button type="button" className="weft-btn-ghost" onClick={onClose}>
                Cancel
              </button>
              <button type="submit" className="weft-btn-primary" disabled={!valid || submitting}>
                {submitting ? "Attaching…" : "Attach catalog"}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

function fmtBytes(n: number): string {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 && i > 0 ? 1 : 0)} ${units[i]}`;
}
