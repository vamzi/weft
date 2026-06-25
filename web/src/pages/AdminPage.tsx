import { useEffect, useState } from "react";
import { Page } from "../components/Layout";
import { StatusBadge } from "../components/StatusBadge";
import { PlusIcon, TrashIcon } from "../components/icons";
import {
  api,
  type AdminGroup,
  type AdminUser,
  type GrantDto,
  type GrantEffect,
  type PrincipalType,
  type Privilege,
  type SecurableType,
} from "../lib/api";

type Tab = "users" | "groups" | "permissions";

const TABS: { id: Tab; label: string }[] = [
  { id: "users", label: "Users" },
  { id: "groups", label: "Groups" },
  { id: "permissions", label: "Permissions" },
];

const PRIVILEGES: Privilege[] = [
  "SELECT",
  "MODIFY",
  "USE CATALOG",
  "USE SCHEMA",
  "ALL PRIVILEGES",
  "BROWSE",
  "CREATE TABLE",
  "MANAGE",
];

const SECURABLE_TYPES: SecurableType[] = [
  "catalog",
  "schema",
  "table",
  "view",
  "metastore",
  "connection",
];

/**
 * Databricks-style governance console. Three sections — Users, Groups, and
 * Permissions (grants) — each LIVE against the gateway's `/api/admin/*` and
 * `/api/grants` endpoints.
 */
export function AdminPage() {
  const [tab, setTab] = useState<Tab>("users");

  return (
    <Page
      title="Admin"
      subtitle="Manage users, groups, and access grants across the metastore."
    >
      <div className="mb-5 flex gap-1 border-b border-hairline">
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            onClick={() => setTab(t.id)}
            className={[
              "-mb-px border-b-2 px-4 py-2 text-sm font-medium transition-colors",
              tab === t.id
                ? "border-accent text-accent"
                : "border-transparent text-muted hover:text-body",
            ].join(" ")}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "users" && <UsersSection />}
      {tab === "groups" && <GroupsSection />}
      {tab === "permissions" && <PermissionsSection />}
    </Page>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div
      className="mb-4 rounded-weft-sm px-3 py-2 text-sm"
      role="alert"
      style={{
        color: "var(--weft-danger)",
        backgroundColor: "color-mix(in srgb, var(--weft-danger) 10%, transparent)",
      }}
    >
      {message}
    </div>
  );
}

// ─────────────────────────────────────── Users ───────────────────────────────────────────────────

function UsersSection() {
  const [users, setUsers] = useState<AdminUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function refresh() {
    setLoading(true);
    api
      .listUsers()
      .then(setUsers)
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load users"))
      .finally(() => setLoading(false));
  }

  useEffect(refresh, []);

  return (
    <div>
      <div className="mb-4 flex justify-end">
        <button type="button" className="weft-btn-primary" onClick={() => setShowForm((v) => !v)}>
          <PlusIcon width={16} height={16} />
          Create user
        </button>
      </div>

      {error && <ErrorBanner message={error} />}

      {showForm && (
        <CreateUserForm
          onCancel={() => setShowForm(false)}
          onDone={() => {
            setShowForm(false);
            refresh();
          }}
          onError={setError}
        />
      )}

      {loading ? (
        <p className="text-sm text-muted">Loading users…</p>
      ) : users.length === 0 ? (
        <Empty note="No users yet." />
      ) : (
        <div className="weft-card overflow-hidden">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {["Username", "Groups"].map((h) => (
                  <th key={h} className={thClass}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <tr key={u.username} className="hover:bg-bg-subtle">
                  <td className="border-b border-hairline px-4 py-2 font-mono text-xs text-body">
                    {u.username}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs text-muted">
                    {u.groups.length ? (
                      <span className="flex flex-wrap gap-1.5">
                        {u.groups.map((g) => (
                          <span key={g} className={pillClass}>
                            {g}
                          </span>
                        ))}
                      </span>
                    ) : (
                      "—"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function CreateUserForm({
  onCancel,
  onDone,
  onError,
}: {
  onCancel: () => void;
  onDone: () => void;
  onError: (msg: string) => void;
}) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [groups, setGroups] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const valid = username.trim().length > 0 && password.length > 0;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      await api.createUser({
        username: username.trim(),
        password,
        groups: splitList(groups),
      });
      onDone();
    } catch (err) {
      onError(err instanceof Error ? err.message : "Failed to create user");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={submit} className="weft-card mb-5 px-5 py-5">
      <h2 className="mb-4 text-sm font-semibold text-body">New user</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <div>
          <label className="weft-label" htmlFor="u-name">
            Username
          </label>
          <input
            id="u-name"
            className="weft-input"
            placeholder="jdoe"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoFocus
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="u-pass">
            Password
          </label>
          <input
            id="u-pass"
            type="password"
            className="weft-input"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="u-groups">
            Groups (comma-separated)
          </label>
          <input
            id="u-groups"
            className="weft-input"
            placeholder="analysts, admins"
            value={groups}
            onChange={(e) => setGroups(e.target.value)}
          />
        </div>
      </div>
      <FormActions submitting={submitting} valid={valid} onCancel={onCancel} label="Create user" />
    </form>
  );
}

// ─────────────────────────────────────── Groups ──────────────────────────────────────────────────

function GroupsSection() {
  const [groups, setGroups] = useState<AdminGroup[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function refresh() {
    setLoading(true);
    api
      .listGroups()
      .then(setGroups)
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load groups"))
      .finally(() => setLoading(false));
  }

  useEffect(refresh, []);

  return (
    <div>
      <div className="mb-4 flex justify-end">
        <button type="button" className="weft-btn-primary" onClick={() => setShowForm((v) => !v)}>
          <PlusIcon width={16} height={16} />
          Create group
        </button>
      </div>

      {error && <ErrorBanner message={error} />}

      {showForm && (
        <CreateGroupForm
          onCancel={() => setShowForm(false)}
          onDone={() => {
            setShowForm(false);
            refresh();
          }}
          onError={setError}
        />
      )}

      {loading ? (
        <p className="text-sm text-muted">Loading groups…</p>
      ) : groups.length === 0 ? (
        <Empty note="No groups yet." />
      ) : (
        <div className="weft-card overflow-hidden">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {["Group", "Members"].map((h) => (
                  <th key={h} className={thClass}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {groups.map((g) => (
                <tr key={g.name} className="hover:bg-bg-subtle">
                  <td className="border-b border-hairline px-4 py-2 font-mono text-xs text-body">
                    {g.name}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs text-muted">
                    {g.members.length ? (
                      <span className="flex flex-wrap gap-1.5">
                        {g.members.map((m) => (
                          <span key={m} className={pillClass}>
                            {m}
                          </span>
                        ))}
                      </span>
                    ) : (
                      "—"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function CreateGroupForm({
  onCancel,
  onDone,
  onError,
}: {
  onCancel: () => void;
  onDone: () => void;
  onError: (msg: string) => void;
}) {
  const [name, setName] = useState("");
  const [members, setMembers] = useState("");
  const [submitting, setSubmitting] = useState(false);

  const valid = name.trim().length > 0;

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      await api.createGroup({ name: name.trim(), members: splitList(members) });
      onDone();
    } catch (err) {
      onError(err instanceof Error ? err.message : "Failed to create group");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={submit} className="weft-card mb-5 px-5 py-5">
      <h2 className="mb-4 text-sm font-semibold text-body">New group</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <div>
          <label className="weft-label" htmlFor="g-name">
            Group name
          </label>
          <input
            id="g-name"
            className="weft-input"
            placeholder="analysts"
            value={name}
            onChange={(e) => setName(e.target.value)}
            autoFocus
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="g-members">
            Members (comma-separated)
          </label>
          <input
            id="g-members"
            className="weft-input"
            placeholder="jdoe, asmith"
            value={members}
            onChange={(e) => setMembers(e.target.value)}
          />
        </div>
      </div>
      <FormActions submitting={submitting} valid={valid} onCancel={onCancel} label="Create group" />
    </form>
  );
}

// ────────────────────────────────────── Permissions ──────────────────────────────────────────────

function PermissionsSection() {
  const [grants, setGrants] = useState<GrantDto[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  function refresh() {
    setLoading(true);
    api
      .listGrants()
      .then(setGrants)
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load grants"))
      .finally(() => setLoading(false));
  }

  useEffect(refresh, []);

  async function revoke(g: GrantDto) {
    const key = grantKey(g);
    setBusy(key);
    setError(null);
    try {
      await api.revokeGrant(g);
      setGrants((gs) => gs.filter((x) => grantKey(x) !== key));
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to revoke grant");
    } finally {
      setBusy(null);
    }
  }

  return (
    <div>
      <div className="mb-4 flex justify-end">
        <button type="button" className="weft-btn-primary" onClick={() => setShowForm((v) => !v)}>
          <PlusIcon width={16} height={16} />
          Grant privilege
        </button>
      </div>

      {error && <ErrorBanner message={error} />}

      {showForm && (
        <GrantForm
          onCancel={() => setShowForm(false)}
          onDone={() => {
            setShowForm(false);
            refresh();
          }}
          onError={setError}
        />
      )}

      {loading ? (
        <p className="text-sm text-muted">Loading grants…</p>
      ) : grants.length === 0 ? (
        <Empty note="No grants yet. Grant a privilege to get started." />
      ) : (
        <div className="weft-card overflow-hidden">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {["Securable", "Type", "Privilege", "Principal", "Effect", ""].map((h, i) => (
                  <th key={i} className={thClass}>
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {grants.map((g) => (
                <tr key={grantKey(g)} className="hover:bg-bg-subtle">
                  <td className="border-b border-hairline px-4 py-2 font-mono text-xs text-body">
                    {g.securable_name || "—"}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs capitalize text-muted">
                    {g.securable_type}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs font-medium text-body">
                    {g.privilege}
                  </td>
                  <td className="border-b border-hairline px-4 py-2 text-xs text-muted">
                    <span className="font-mono text-body">{g.principal_id}</span>
                    <span className="ml-1 capitalize">({g.principal_kind})</span>
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
                      disabled={busy === grantKey(g)}
                      onClick={() => revoke(g)}
                      aria-label={`Revoke ${g.privilege} from ${g.principal_id}`}
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
    </div>
  );
}

function GrantForm({
  onCancel,
  onDone,
  onError,
}: {
  onCancel: () => void;
  onDone: () => void;
  onError: (msg: string) => void;
}) {
  const [securableType, setSecurableType] = useState<SecurableType>("table");
  const [securableName, setSecurableName] = useState("");
  const [privilege, setPrivilege] = useState<Privilege>("SELECT");
  const [principalKind, setPrincipalKind] = useState<PrincipalType>("group");
  const [principalId, setPrincipalId] = useState("");
  const [effect, setEffect] = useState<GrantEffect>("allow");
  const [submitting, setSubmitting] = useState(false);

  // `metastore` carries no name; everything else needs one.
  const nameRequired = securableType !== "metastore";
  const valid =
    principalId.trim().length > 0 && (!nameRequired || securableName.trim().length > 0);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!valid || submitting) return;
    setSubmitting(true);
    try {
      await api.createGrant({
        securable_type: securableType,
        securable_name: securableName.trim(),
        privilege,
        principal_kind: principalKind,
        principal_id: principalId.trim(),
        effect,
      });
      onDone();
    } catch (err) {
      onError(err instanceof Error ? err.message : "Failed to create grant");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <form onSubmit={submit} className="weft-card mb-5 px-5 py-5">
      <h2 className="mb-4 text-sm font-semibold text-body">Grant / deny a privilege</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        <div>
          <label className="weft-label" htmlFor="gr-stype">
            Securable type
          </label>
          <select
            id="gr-stype"
            className="weft-input"
            value={securableType}
            onChange={(e) => setSecurableType(e.target.value as SecurableType)}
          >
            {SECURABLE_TYPES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
        <div className="lg:col-span-2">
          <label className="weft-label" htmlFor="gr-sname">
            Securable name {nameRequired ? "" : "(not required for metastore)"}
          </label>
          <input
            id="gr-sname"
            className="weft-input font-mono"
            placeholder="main.sales.orders"
            value={securableName}
            onChange={(e) => setSecurableName(e.target.value)}
            disabled={!nameRequired}
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="gr-priv">
            Privilege
          </label>
          <select
            id="gr-priv"
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
          <label className="weft-label" htmlFor="gr-pkind">
            Principal kind
          </label>
          <select
            id="gr-pkind"
            className="weft-input"
            value={principalKind}
            onChange={(e) => setPrincipalKind(e.target.value as PrincipalType)}
          >
            <option value="group">group</option>
            <option value="user">user</option>
          </select>
        </div>
        <div>
          <label className="weft-label" htmlFor="gr-pid">
            Principal id
          </label>
          <input
            id="gr-pid"
            className="weft-input"
            placeholder="analysts or jdoe"
            value={principalId}
            onChange={(e) => setPrincipalId(e.target.value)}
          />
        </div>
        <div>
          <label className="weft-label" htmlFor="gr-effect">
            Effect
          </label>
          <select
            id="gr-effect"
            className="weft-input"
            value={effect}
            onChange={(e) => setEffect(e.target.value as GrantEffect)}
          >
            <option value="allow">allow (GRANT)</option>
            <option value="deny">deny</option>
          </select>
        </div>
      </div>
      <FormActions
        submitting={submitting}
        valid={valid}
        onCancel={onCancel}
        label={effect === "allow" ? "Grant" : "Deny"}
      />
    </form>
  );
}

// ─────────────────────────────────────── Shared bits ─────────────────────────────────────────────

const thClass =
  "border-b border-hairline bg-bg-subtle px-4 py-2 text-left text-xs font-semibold text-muted";

const pillClass =
  "rounded-full bg-bg-subtle px-2 py-0.5 text-[10px] font-medium text-muted ring-1 ring-inset ring-[var(--weft-border)]";

function Empty({ note }: { note: string }) {
  return (
    <div className="weft-card grid place-items-center px-6 py-12 text-center">
      <p className="text-sm text-muted">{note}</p>
    </div>
  );
}

function FormActions({
  submitting,
  valid,
  onCancel,
  label,
}: {
  submitting: boolean;
  valid: boolean;
  onCancel: () => void;
  label: string;
}) {
  return (
    <div className="mt-5 flex justify-end gap-2">
      <button type="button" className="weft-btn-ghost" onClick={onCancel}>
        Cancel
      </button>
      <button type="submit" className="weft-btn-primary" disabled={!valid || submitting}>
        {submitting ? "Applying…" : label}
      </button>
    </div>
  );
}

function splitList(s: string): string[] {
  return s
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

/** Stable identity for a grant row (the gateway has no grant id). */
function grantKey(g: GrantDto): string {
  return [g.securable_type, g.securable_name, g.privilege, g.principal_kind, g.principal_id, g.effect].join(
    "|",
  );
}
