import { useEffect, useState } from "react";
import { Page } from "../components/Layout";
import { StatusBadge } from "../components/StatusBadge";
import { PermissionsIcon, PlusIcon, TrashIcon } from "../components/icons";
import {
  api,
  type Grant,
  type GrantEffect,
  type GrantInput,
  type PrincipalType,
  type Privilege,
  type Securable,
} from "../lib/api";

const PRIVILEGES: Privilege[] = [
  "SELECT",
  "MODIFY",
  "USE CATALOG",
  "USE SCHEMA",
  "ALL PRIVILEGES",
  "BROWSE",
];

export function PermissionsPage() {
  const [securables, setSecurables] = useState<Securable[]>([]);
  const [securable, setSecurable] = useState<string>("");
  const [grants, setGrants] = useState<Grant[]>([]);
  const [loading, setLoading] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);

  useEffect(() => {
    api.listSecurables().then((s) => {
      setSecurables(s);
      if (s[0]) setSecurable(s[0].fqn);
    });
  }, []);

  useEffect(() => {
    if (!securable) return;
    setLoading(true);
    api
      .grants(securable)
      .then(setGrants)
      .finally(() => setLoading(false));
  }, [securable]);

  async function onGrant(input: GrantInput) {
    const created = await api.grant(input);
    setGrants((g) => [...g, created]);
    setShowForm(false);
  }

  async function onRevoke(id: string) {
    setBusy(id);
    try {
      await api.revoke(securable, id);
      setGrants((g) => g.filter((x) => x.id !== id));
    } finally {
      setBusy(null);
    }
  }

  const current = securables.find((s) => s.fqn === securable);

  return (
    <Page
      title="Permissions"
      subtitle="Govern access with Unity-Catalog-style grants on each securable."
      actions={
        <button
          type="button"
          className="weft-btn-primary"
          onClick={() => setShowForm((v) => !v)}
          disabled={!securable}
        >
          <PlusIcon width={16} height={16} />
          Grant privilege
        </button>
      }
    >
      <div className="mb-5 flex flex-wrap items-end gap-3">
        <div className="min-w-[260px]">
          <label className="weft-label" htmlFor="securable">
            Securable
          </label>
          <select
            id="securable"
            className="weft-input"
            value={securable}
            onChange={(e) => setSecurable(e.target.value)}
          >
            {securables.map((s) => (
              <option key={s.fqn} value={s.fqn}>
                {s.label}
              </option>
            ))}
          </select>
        </div>
        {current && (
          <div className="flex items-center gap-1.5 pb-2 text-xs text-muted">
            <PermissionsIcon width={14} height={14} />
            Effective grants on <span className="font-mono text-body">{current.fqn}</span>
          </div>
        )}
      </div>

      {showForm && (
        <GrantForm securable={securable} onSubmit={onGrant} onCancel={() => setShowForm(false)} />
      )}

      {loading ? (
        <p className="text-sm text-muted">Loading grants…</p>
      ) : grants.length === 0 ? (
        <div className="weft-card grid place-items-center px-6 py-12 text-center">
          <p className="text-sm text-muted">No grants on this securable yet.</p>
        </div>
      ) : (
        <div className="weft-card overflow-hidden">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {["Principal", "Type", "Privilege", "Effect", ""].map((h) => (
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
              {grants.map((g) => (
                <tr key={g.id} className="hover:bg-bg-subtle">
                  <td className="border-b border-hairline px-4 py-2 font-mono text-xs text-body">
                    {g.principal}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs capitalize text-muted">
                    {g.principalType}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs font-medium text-body">
                    {g.privilege}
                  </td>
                  <td className="border-b border-hairline px-4 py-2">
                    <StatusBadge
                      tone={g.effect === "allow" ? "success" : "danger"}
                      label={g.effect}
                    />
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-right">
                    <button
                      type="button"
                      className="weft-btn-ghost"
                      style={{ color: "var(--weft-danger)" }}
                      disabled={busy === g.id}
                      onClick={() => onRevoke(g.id)}
                      aria-label={`Revoke ${g.privilege} from ${g.principal}`}
                    >
                      <TrashIcon width={14} height={14} />
                      Revoke
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Page>
  );
}

function GrantForm({
  securable,
  onSubmit,
  onCancel,
}: {
  securable: string;
  onSubmit: (input: GrantInput) => void;
  onCancel: () => void;
}) {
  const [principal, setPrincipal] = useState("");
  const [principalType, setPrincipalType] = useState<PrincipalType>("group");
  const [privilege, setPrivilege] = useState<Privilege>("SELECT");
  const [effect, setEffect] = useState<GrantEffect>("allow");
  const [submitting, setSubmitting] = useState(false);

  const valid = principal.trim().length > 0;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      await onSubmit({
        securable,
        principal: principal.trim(),
        principalType,
        privilege,
        effect,
      });
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={submit} className="weft-card mb-5 px-5 py-5">
      <h2 className="mb-4 text-sm font-semibold text-body">Grant / deny a privilege</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div className="sm:col-span-2">
          <label className="weft-label" htmlFor="g-principal">
            Principal
          </label>
          <input
            id="g-principal"
            className="weft-input"
            placeholder="analysts or user@weft.dev"
            value={principal}
            onChange={(e) => setPrincipal(e.target.value)}
            autoFocus
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="g-ptype">
            Principal type
          </label>
          <select
            id="g-ptype"
            className="weft-input"
            value={principalType}
            onChange={(e) => setPrincipalType(e.target.value as PrincipalType)}
          >
            <option value="group">group</option>
            <option value="user">user</option>
          </select>
        </div>
        <div>
          <label className="weft-label" htmlFor="g-priv">
            Privilege
          </label>
          <select
            id="g-priv"
            className="weft-input"
            value={privilege}
            onChange={(e) => setPrivilege(e.target.value as Privilege)}
          >
            {PRIVILEGES.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="weft-label" htmlFor="g-effect">
            Effect
          </label>
          <select
            id="g-effect"
            className="weft-input"
            value={effect}
            onChange={(e) => setEffect(e.target.value as GrantEffect)}
          >
            <option value="allow">allow (GRANT)</option>
            <option value="deny">deny (REVOKE)</option>
          </select>
        </div>
      </div>
      <div className="mt-5 flex justify-end gap-2">
        <button type="button" className="weft-btn-ghost" onClick={onCancel}>
          Cancel
        </button>
        <button type="submit" className="weft-btn-primary" disabled={!valid || submitting}>
          {submitting ? "Applying…" : effect === "allow" ? "Grant" : "Deny"}
        </button>
      </div>
    </form>
  );
}
