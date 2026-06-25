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
  type CatalogNamespace,
  type CatalogTable,
  type ExternalCatalogType,
} from "../lib/api";

/** A flattened tree node built from the live catalog (catalog → schema → table). */
type TreeNode =
  | { kind: "catalog"; fqn: string; name: string }
  | { kind: "schema"; fqn: string; name: string }
  | { kind: "table"; fqn: string; name: string; table: CatalogTable };

export function CatalogPage() {
  const [catalog, setCatalog] = useState<CatalogNamespace[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [selected, setSelected] = useState<TreeNode | null>(null);
  const [showConnect, setShowConnect] = useState(false);

  useEffect(() => {
    api
      .getCatalog()
      .then((cat) => {
        setCatalog(cat);
        // Open the first catalog + its first schema by default.
        const first = cat[0];
        const firstSchema = first?.schemas[0];
        const init = new Set<string>();
        if (first) init.add(first.name);
        if (first && firstSchema) init.add(`${first.name}.${firstSchema.name}`);
        setExpanded(init);
      })
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load catalog"));
  }, []);

  // Map each parent fqn to its child nodes for tree rendering.
  const childrenOf = useMemo(() => {
    const map = new Map<string | null, TreeNode[]>();
    const push = (parent: string | null, node: TreeNode) => {
      if (!map.has(parent)) map.set(parent, []);
      map.get(parent)!.push(node);
    };
    for (const cat of catalog ?? []) {
      push(null, { kind: "catalog", fqn: cat.name, name: cat.name });
      for (const schema of cat.schemas) {
        const schemaFqn = `${cat.name}.${schema.name}`;
        push(cat.name, { kind: "schema", fqn: schemaFqn, name: schema.name });
        for (const table of schema.tables) {
          const tableFqn = `${schemaFqn}.${table.name}`;
          push(schemaFqn, { kind: "table", fqn: tableFqn, name: table.name, table });
        }
      }
    }
    return map;
  }, [catalog]);

  const roots = childrenOf.get(null) ?? [];

  function toggle(fqn: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(fqn) ? next.delete(fqn) : next.add(fqn);
      return next;
    });
  }

  function select(node: TreeNode) {
    if (node.kind === "table") {
      setSelected(node);
    } else {
      toggle(node.fqn);
    }
  }

  // Recursive tree node.
  function renderNode(node: TreeNode, depth: number) {
    const kids = childrenOf.get(node.fqn) ?? [];
    const hasKids = node.kind === "catalog" || node.kind === "schema";
    const isOpen = expanded.has(node.fqn);
    const isSelected = selected?.fqn === node.fqn;
    return (
      <div key={node.fqn}>
        <button
          type="button"
          onClick={() => select(node)}
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
          {node.kind === "catalog" || node.kind === "schema" ? (
            <DatabaseIcon width={14} height={14} className="shrink-0 text-muted" />
          ) : (
            <TableIcon width={14} height={14} className="shrink-0 text-muted" />
          )}
          <span className="truncate">{node.name}</span>
          <span className="ml-auto shrink-0 text-[10px] uppercase tracking-wide text-muted">
            {node.kind}
          </span>
        </button>
        {hasKids && isOpen && kids.map((k) => renderNode(k, depth + 1))}
      </div>
    );
  }

  return (
    <Page
      title="Catalog"
      subtitle="Browse governed catalogs, schemas, and tables."
      actions={
        <button type="button" className="weft-btn-ghost" onClick={() => setShowConnect(true)}>
          <PlugIcon width={15} height={15} />
          Connections
        </button>
      }
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[320px_1fr]">
        <div className="weft-card p-2">
          {error ? (
            <p className="px-2 py-3 text-sm" style={{ color: "var(--weft-danger)" }}>
              {error}
            </p>
          ) : catalog === null ? (
            <p className="px-2 py-3 text-sm text-muted">Loading catalog…</p>
          ) : roots.length === 0 ? (
            <p className="px-2 py-3 text-sm text-muted">No catalogs available.</p>
          ) : (
            roots.map((node) => renderNode(node, 0))
          )}
        </div>

        <DetailPanel node={selected} />
      </div>

      {showConnect && <ConnectModal onClose={() => setShowConnect(false)} />}
    </Page>
  );
}

function DetailPanel({ node }: { node: TreeNode | null }) {
  if (!node || node.kind !== "table") {
    return (
      <div className="weft-card grid place-items-center px-6 py-16 text-center">
        <p className="text-sm text-muted">Select a table to see its columns.</p>
      </div>
    );
  }

  const { table, fqn } = node;

  return (
    <div className="weft-card overflow-hidden">
      <div className="border-b border-hairline px-5 py-4">
        <div className="flex items-center gap-2">
          <TableIcon width={16} height={16} className="text-muted" />
          <span className="font-mono text-sm font-semibold text-body">{fqn}</span>
          <span className="rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] uppercase tracking-wide text-muted">
            table
          </span>
        </div>
      </div>

      <div className="px-5 py-2 text-xs font-semibold text-muted">
        Columns ({table.columns.length})
      </div>
      <div className="overflow-auto">
        <table className="w-full border-collapse text-sm">
          <thead>
            <tr>
              {["Column", "Type"].map((h) => (
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
            {table.columns.map((c) => (
              <tr key={c.name} className="hover:bg-bg-subtle">
                <td className="border-b border-hairline px-4 py-1.5 font-mono text-xs text-body">
                  {c.name}
                </td>
                <td className="border-b border-hairline px-4 py-1.5 font-mono text-xs text-muted">
                  {c.data_type}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
              Demo only — not wired to the gateway yet. Mount a Hive Metastore, AWS Glue, Unity
              Catalog, or local warehouse as a governed catalog.
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
