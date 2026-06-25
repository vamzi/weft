//! `weft-govern` — Weft's Unity-Catalog-parity governance plane.
//!
//! The catalog SPI ([`weft_catalog`](../weft_catalog/index.html)) is intentionally **auth-free**:
//! providers resolve metadata and are trusted to enforce nothing. Governance is a *separate*
//! plane that decides *who may see what*, layered on top via two seams (see the platform plan):
//!
//! 1. a `GovernedCatalog` decorator that filters `list_*` and authorizes `load_table`, and
//! 2. an authorizer hook in `weft-connect`/`weft-analyzer` that denies unauthorized references
//!    and injects row-filter / column-mask plan rewrites *before* execution.
//!
//! This crate owns the **model** and the **evaluator** — the parts that must be correct and that
//! every other team codes against, so they are frozen here, dependency-free and exhaustively
//! tested, before the Postgres-backed store (`weft-meta`) or the SQL surface
//! (`GRANT`/`REVOKE`/`SHOW GRANTS`) land.
//!
//! Faithful to the Databricks/Unity model:
//!
//! - **Securable tree** — `metastore → catalog → schema → {table, view, volume, function}`, plus
//!   metastore-level `external location`, `storage credential`, `connection`. Each securable has
//!   exactly one **owner**, who holds [`Privilege::AllPrivileges`] on it and its subtree.
//! - **Inheritance** — a grant on a parent flows to every current/future child.
//! - **Traversal gating** — reaching a table needs the data privilege **and**
//!   [`Privilege::UseCatalog`]/[`Privilege::UseSchema`] on every ancestor.
//! - **DENY overrides** — an explicit [`Effect::Deny`] anywhere on the path beats any allow.
//! - **Principals** — users, groups (transitive membership resolved by the caller into an
//!   [`Identity`]), and service principals.

use std::fmt;

pub mod governed;
pub mod resolve;
pub mod sql;

/// The kinds of securable objects Weft governs (Unity-Catalog parity, core set).
///
/// `Catalog`/`Schema`/`Table`/`View`/`Volume`/`Function` form the three-level
/// `catalog.schema.object` namespace; the rest are metastore-level securables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SecurableType {
    /// The root. A grant here does **not** inherit (matches UC: the metastore is special).
    Metastore,
    /// First level of the namespace.
    Catalog,
    /// Second level (a database).
    Schema,
    /// A table.
    Table,
    /// A view.
    View,
    /// A managed file volume.
    Volume,
    /// A registered function.
    Function,
    /// An external storage location (metastore-level).
    ExternalLocation,
    /// A storage credential (metastore-level).
    StorageCredential,
    /// A connection to an external system / catalog (metastore-level).
    Connection,
}

impl SecurableType {
    /// How many namespace parts a securable of this type carries
    /// (`Catalog` = 1, `Schema` = 2, leaf objects = 3). Metastore-level securables are named by a
    /// single identifier and treated as roots for inheritance.
    pub fn name_arity(&self) -> usize {
        match self {
            SecurableType::Metastore => 0,
            SecurableType::Catalog
            | SecurableType::ExternalLocation
            | SecurableType::StorageCredential
            | SecurableType::Connection => 1,
            SecurableType::Schema => 2,
            SecurableType::Table
            | SecurableType::View
            | SecurableType::Volume
            | SecurableType::Function => 3,
        }
    }

    /// Whether this securable participates in the `catalog → schema → object` namespace tree
    /// (and therefore inherits grants from ancestors). Metastore-level securables do not.
    pub fn is_namespaced(&self) -> bool {
        matches!(
            self,
            SecurableType::Catalog
                | SecurableType::Schema
                | SecurableType::Table
                | SecurableType::View
                | SecurableType::Volume
                | SecurableType::Function
        )
    }
}

/// A securable identified by its type and multi-part name (e.g. `Table` named `["main","sales","orders"]`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Securable {
    /// The securable kind.
    pub kind: SecurableType,
    /// The fully-qualified name parts. Length should match [`SecurableType::name_arity`].
    pub name: Vec<String>,
}

impl Securable {
    /// Construct a securable.
    pub fn new(kind: SecurableType, name: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            kind,
            name: name.into_iter().map(Into::into).collect(),
        }
    }

    /// The metastore root.
    pub fn metastore() -> Self {
        Self {
            kind: SecurableType::Metastore,
            name: Vec::new(),
        }
    }

    /// A catalog by name.
    pub fn catalog(catalog: impl Into<String>) -> Self {
        Self::new(SecurableType::Catalog, [catalog.into()])
    }

    /// A schema by `catalog.schema`.
    pub fn schema(catalog: impl Into<String>, schema: impl Into<String>) -> Self {
        Self::new(SecurableType::Schema, [catalog.into(), schema.into()])
    }

    /// A table by `catalog.schema.table`.
    pub fn table(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self::new(
            SecurableType::Table,
            [catalog.into(), schema.into(), table.into()],
        )
    }

    /// The chain of ancestors that grant traversal/inheritance for this securable, **closest
    /// first** (its schema, then catalog, then metastore). Empty for non-namespaced securables
    /// beyond the metastore.
    ///
    /// Example: for table `main.sales.orders` → `[schema main.sales, catalog main, metastore]`.
    pub fn ancestors(&self) -> Vec<Securable> {
        let mut out = Vec::new();
        if self.kind.is_namespaced() {
            // Walk down the namespace parts to the closest enclosing schema/catalog.
            if self.name.len() >= 3 {
                out.push(Securable::schema(
                    self.name[0].clone(),
                    self.name[1].clone(),
                ));
            }
            if self.name.len() >= 2 {
                out.push(Securable::catalog(self.name[0].clone()));
            }
        }
        out.push(Securable::metastore());
        out
    }

    /// Whether `other` is this securable or one of its ancestors (i.e. a grant on `other` reaches
    /// `self` by inheritance). The metastore is excluded from data-privilege inheritance (UC
    /// semantics) — callers gate that separately; see [`Evaluator`].
    fn covered_by(&self, other: &Securable) -> bool {
        self == other || self.ancestors().iter().any(|a| a == other)
    }
}

impl fmt::Display for Securable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.name.is_empty() {
            write!(f, "{:?}", self.kind)
        } else {
            write!(f, "{:?} {}", self.kind, self.name.join("."))
        }
    }
}

/// The privilege set (Unity-Catalog core). `UseCatalog`/`UseSchema` are mandatory traversal
/// gates; `AllPrivileges` expands at check time; `Manage` is the act-as-owner grant-management
/// privilege (not itself a data privilege).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Privilege {
    /// Traverse into a catalog. Required on the catalog for any access below it.
    UseCatalog,
    /// Traverse into a schema. Required on the schema for any access to its objects.
    UseSchema,
    /// Create a catalog (granted at the metastore).
    CreateCatalog,
    /// Create a schema (granted on a catalog).
    CreateSchema,
    /// Create a table (granted on a catalog/schema).
    CreateTable,
    /// Create a function.
    CreateFunction,
    /// Create a volume.
    CreateVolume,
    /// Read a table/view.
    Select,
    /// Write (insert/update/delete/merge) a table.
    Modify,
    /// Execute a function.
    Execute,
    /// Read a volume's files.
    ReadVolume,
    /// Write a volume's files.
    WriteVolume,
    /// Read files at an external location.
    ReadFiles,
    /// Write files at an external location.
    WriteFiles,
    /// Create an external table at a location.
    CreateExternalTable,
    /// See metadata (name/existence) without data access.
    Browse,
    /// Shorthand expanding to every data privilege on a securable + its subtree.
    AllPrivileges,
    /// Act as owner for grant management (grant/revoke) without being the owner.
    Manage,
}

impl Privilege {
    /// Whether holding [`Privilege::AllPrivileges`] implies this privilege. `AllPrivileges`
    /// covers every data/traversal privilege, but **not** the governance privileges `Manage`
    /// (and, in the wider UC model, `ExternalUse*`), matching Databricks semantics.
    pub fn covered_by_all(&self) -> bool {
        !matches!(self, Privilege::Manage)
    }
}

/// A principal a grant can target.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Principal {
    /// A user (by stable id / email).
    User(String),
    /// A group (by name). Membership is resolved transitively into an [`Identity`] by the caller.
    Group(String),
    /// A service principal (by client id).
    ServicePrincipal(String),
}

/// Allow or deny. An explicit deny overrides any allow on the same or an ancestor securable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Effect {
    /// Grant the privilege.
    Allow,
    /// Explicitly deny the privilege (wins over inherited/explicit allows).
    Deny,
}

/// A single grant: `effect privilege ON securable TO principal`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// The securable the grant is attached to.
    pub securable: Securable,
    /// The privilege granted/denied.
    pub privilege: Privilege,
    /// Who it applies to.
    pub principal: Principal,
    /// Allow or deny.
    pub effect: Effect,
}

impl Grant {
    /// `GRANT privilege ON securable TO principal`.
    pub fn allow(securable: Securable, privilege: Privilege, principal: Principal) -> Self {
        Self {
            securable,
            privilege,
            principal,
            effect: Effect::Allow,
        }
    }

    /// `DENY privilege ON securable TO principal`.
    pub fn deny(securable: Securable, privilege: Privilege, principal: Principal) -> Self {
        Self {
            securable,
            privilege,
            principal,
            effect: Effect::Deny,
        }
    }
}

/// An ownership record: `principal` owns `securable` (and, by extension, its subtree). An owner
/// holds every privilege on the owned securable and everything beneath it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ownership {
    /// The owned securable.
    pub securable: Securable,
    /// The owning principal (commonly a [`Principal::User`] or [`Principal::Group`]).
    pub principal: Principal,
}

/// A caller's resolved identity: the user plus the **transitively-expanded** set of groups they
/// belong to and any service principals acting on their behalf. The caller (gateway/SCIM layer)
/// is responsible for the transitive group expansion; the evaluator treats the set as ground truth.
#[derive(Debug, Clone, Default)]
pub struct Identity {
    /// The user id (`None` for a pure service-principal call).
    pub user: Option<String>,
    /// Transitively-resolved group names.
    pub groups: Vec<String>,
    /// Service principal client ids acting as this identity.
    pub service_principals: Vec<String>,
}

impl Identity {
    /// A user with no groups.
    pub fn user(id: impl Into<String>) -> Self {
        Self {
            user: Some(id.into()),
            groups: Vec::new(),
            service_principals: Vec::new(),
        }
    }

    /// Builder: add transitively-resolved groups.
    pub fn with_groups(mut self, groups: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.groups = groups.into_iter().map(Into::into).collect();
        self
    }

    /// Whether `principal` matches this identity (the user, any of its groups, or any of its SPs).
    fn matches(&self, principal: &Principal) -> bool {
        match principal {
            Principal::User(u) => self.user.as_deref() == Some(u.as_str()),
            Principal::Group(g) => self.groups.iter().any(|x| x == g),
            Principal::ServicePrincipal(s) => self.service_principals.iter().any(|x| x == s),
        }
    }
}

/// The outcome of an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// The action is permitted.
    Allow,
    /// The action is denied, with a human-readable reason (surfaced to the user / audit log).
    Deny(String),
}

impl Decision {
    /// Whether the decision permits the action.
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow)
    }
}

/// The authorization evaluator. Holds the grant + ownership set (typically loaded from `weft-meta`
/// for a metastore) and answers `can(identity, privilege, securable)` questions.
///
/// Semantics (Unity-Catalog parity):
/// 1. **Deny wins.** A matching [`Effect::Deny`] on the securable or any ancestor → denied.
/// 2. **Ownership.** If the identity owns the securable or any ancestor → allowed (owner holds
///    all privileges on the subtree).
/// 3. **Inherited allow.** A matching [`Effect::Allow`] (for the privilege or [`Privilege::AllPrivileges`])
///    on the securable or any ancestor → the privilege itself is held.
/// 4. **Traversal gate.** For a *data* action on a namespaced securable, holding the privilege is
///    not enough — the identity must also hold [`Privilege::UseCatalog`] on the catalog and, for
///    schema-or-deeper securables, [`Privilege::UseSchema`] on the schema. Missing traversal → denied.
pub struct Evaluator {
    grants: Vec<Grant>,
    owners: Vec<Ownership>,
}

impl Evaluator {
    /// Build an evaluator from a grant set (no ownership records).
    pub fn new(grants: Vec<Grant>) -> Self {
        Self {
            grants,
            owners: Vec::new(),
        }
    }

    /// Build an evaluator from grants and ownership records.
    pub fn with_owners(grants: Vec<Grant>, owners: Vec<Ownership>) -> Self {
        Self { grants, owners }
    }

    /// Decide whether `identity` may perform `privilege` on `securable`.
    pub fn authorize(
        &self,
        identity: &Identity,
        privilege: Privilege,
        securable: &Securable,
    ) -> Decision {
        // 1. Explicit deny anywhere on the path wins outright.
        if self.has_matching(identity, privilege, securable, Effect::Deny) {
            return Decision::Deny(format!("explicit DENY of {privilege:?} on {securable}"));
        }

        // 2. Ownership of the securable or an ancestor grants everything beneath.
        if self.is_owner_of_path(identity, securable) {
            return Decision::Allow;
        }

        // 3. Must hold the privilege itself (directly or inherited).
        if !self.has_matching(identity, privilege, securable, Effect::Allow) {
            return Decision::Deny(format!(
                "no grant of {privilege:?} on {securable} or an ancestor"
            ));
        }

        // 4. Traversal gating for data actions on namespaced securables.
        if Self::is_traversal_gated(privilege, securable) {
            if let Some(missing) = self.missing_traversal(identity, securable) {
                return Decision::Deny(format!(
                    "missing {missing:?} traversal grant for {securable}"
                ));
            }
        }

        Decision::Allow
    }

    /// Convenience boolean form of [`Evaluator::authorize`].
    pub fn can(&self, identity: &Identity, privilege: Privilege, securable: &Securable) -> bool {
        self.authorize(identity, privilege, securable).is_allowed()
    }

    /// Whether a grant of `(privilege | AllPrivileges)` with the given `effect`, targeting any of
    /// `identity`'s principals, exists on `securable` or any of its ancestors.
    fn has_matching(
        &self,
        identity: &Identity,
        privilege: Privilege,
        securable: &Securable,
        effect: Effect,
    ) -> bool {
        self.grants.iter().any(|g| {
            g.effect == effect
                && identity.matches(&g.principal)
                && securable.covered_by(&g.securable)
                && self.grant_covers_privilege(g, privilege)
                // The metastore root does not inherit *data* privileges down to namespaced
                // securables (UC: metastore grants don't inherit). It still covers metastore-level
                // securables and traversal is handled separately.
                && !(g.securable.kind == SecurableType::Metastore && securable.kind != SecurableType::Metastore)
        })
    }

    fn grant_covers_privilege(&self, grant: &Grant, requested: Privilege) -> bool {
        grant.privilege == requested
            || (grant.privilege == Privilege::AllPrivileges && requested.covered_by_all())
    }

    fn is_owner_of_path(&self, identity: &Identity, securable: &Securable) -> bool {
        self.owners
            .iter()
            .any(|o| identity.matches(&o.principal) && securable.covered_by(&o.securable))
    }

    /// Data privileges on namespaced securables require `USE` traversal; the `USE`/`CREATE`/`Browse`
    /// privileges and metastore-level securables do not gate on themselves.
    fn is_traversal_gated(privilege: Privilege, securable: &Securable) -> bool {
        if !securable.kind.is_namespaced() {
            return false;
        }
        !matches!(
            privilege,
            Privilege::UseCatalog | Privilege::UseSchema | Privilege::Browse
        )
    }

    /// Returns the first missing traversal privilege (`UseCatalog`, then `UseSchema`), or `None`
    /// if all required traversal grants are present.
    fn missing_traversal(&self, identity: &Identity, securable: &Securable) -> Option<Privilege> {
        // Owning the path also satisfies traversal.
        if self.is_owner_of_path(identity, securable) {
            return None;
        }
        let catalog = Securable::catalog(securable.name[0].clone());
        if !self.holds_use(identity, Privilege::UseCatalog, &catalog) {
            return Some(Privilege::UseCatalog);
        }
        // Schema-or-deeper securables also need USE SCHEMA on the enclosing schema.
        if securable.name.len() >= 2 {
            let schema = Securable::schema(securable.name[0].clone(), securable.name[1].clone());
            if !self.holds_use(identity, Privilege::UseSchema, &schema) {
                return Some(Privilege::UseSchema);
            }
        }
        None
    }

    /// Whether a `USE` privilege is held on `target` or an inheriting ancestor, without recursing
    /// into traversal (avoids infinite regress).
    fn holds_use(&self, identity: &Identity, use_priv: Privilege, target: &Securable) -> bool {
        if self.is_owner_of_path(identity, target) {
            return true;
        }
        self.grants.iter().any(|g| {
            g.effect == Effect::Allow
                && identity.matches(&g.principal)
                && target.covered_by(&g.securable)
                && self.grant_covers_privilege(g, use_priv)
                && !(g.securable.kind == SecurableType::Metastore
                    && target.kind != SecurableType::Metastore)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn orders() -> Securable {
        Securable::table("main", "sales", "orders")
    }

    /// SELECT requires the grant *and* USE traversal on catalog + schema.
    #[test]
    fn select_needs_grant_and_traversal() {
        let alice = Principal::User("alice".into());
        let id = Identity::user("alice");
        // Only the SELECT grant, no USE — denied on traversal.
        let ev = Evaluator::new(vec![Grant::allow(
            orders(),
            Privilege::Select,
            alice.clone(),
        )]);
        assert!(!ev.can(&id, Privilege::Select, &orders()));

        // Add USE CATALOG + USE SCHEMA — now allowed.
        let ev = Evaluator::new(vec![
            Grant::allow(orders(), Privilege::Select, alice.clone()),
            Grant::allow(
                Securable::catalog("main"),
                Privilege::UseCatalog,
                alice.clone(),
            ),
            Grant::allow(
                Securable::schema("main", "sales"),
                Privilege::UseSchema,
                alice.clone(),
            ),
        ]);
        assert!(ev.can(&id, Privilege::Select, &orders()));
    }

    /// A grant on the catalog inherits to every table beneath it.
    #[test]
    fn inherited_select_from_catalog() {
        let g = Principal::Group("analysts".into());
        let id = Identity::user("bob").with_groups(["analysts"]);
        let ev = Evaluator::new(vec![
            Grant::allow(Securable::catalog("main"), Privilege::Select, g.clone()),
            Grant::allow(Securable::catalog("main"), Privilege::UseCatalog, g.clone()),
            Grant::allow(Securable::catalog("main"), Privilege::UseSchema, g.clone()),
        ]);
        assert!(ev.can(&id, Privilege::Select, &orders()));
        // A different catalog is not covered.
        assert!(!ev.can(
            &id,
            Privilege::Select,
            &Securable::table("other", "sales", "orders")
        ));
    }

    /// DENY anywhere on the path beats an inherited allow.
    #[test]
    fn deny_overrides_inherited_allow() {
        let alice = Principal::User("alice".into());
        let id = Identity::user("alice");
        let ev = Evaluator::new(vec![
            Grant::allow(
                Securable::catalog("main"),
                Privilege::AllPrivileges,
                alice.clone(),
            ),
            Grant::allow(
                Securable::catalog("main"),
                Privilege::UseCatalog,
                alice.clone(),
            ),
            Grant::allow(
                Securable::catalog("main"),
                Privilege::UseSchema,
                alice.clone(),
            ),
            Grant::deny(orders(), Privilege::Select, alice.clone()),
        ]);
        assert!(!ev.can(&id, Privilege::Select, &orders()));
        // MODIFY still allowed (only SELECT was denied).
        assert!(ev.can(&id, Privilege::Modify, &orders()));
    }

    /// AllPrivileges expands to data privileges but not to Manage.
    #[test]
    fn all_privileges_expands_except_manage() {
        let alice = Principal::User("alice".into());
        let id = Identity::user("alice");
        // Ownership is the cleanest way to exercise AllPrivileges expansion (no USE-traversal
        // grants needed): an owner of the catalog holds every data privilege beneath it.
        let ev_owned = Evaluator::with_owners(
            vec![],
            vec![Ownership {
                securable: Securable::catalog("main"),
                principal: alice.clone(),
            }],
        );
        assert!(ev_owned.can(&id, Privilege::Select, &orders()));
        assert!(ev_owned.can(&id, Privilege::Modify, &orders()));
        // Manage is not implied by AllPrivileges (must be owner or explicit grant); owner has it.
        assert!(ev_owned.can(&id, Privilege::Manage, &orders()));
        // Plain AllPrivileges grant (non-owner) does NOT cover Manage.
        let ev_grant = Evaluator::new(vec![Grant::allow(
            Securable::metastore(),
            Privilege::AllPrivileges,
            alice.clone(),
        )]);
        assert!(!ev_grant.can(&id, Privilege::Manage, &Securable::metastore()));
    }

    /// Owning the catalog grants everything beneath, traversal included.
    #[test]
    fn ownership_grants_subtree() {
        let alice = Principal::User("alice".into());
        let id = Identity::user("alice");
        let ev = Evaluator::with_owners(
            vec![],
            vec![Ownership {
                securable: Securable::catalog("main"),
                principal: alice,
            }],
        );
        assert!(ev.can(&id, Privilege::Select, &orders()));
        assert!(ev.can(
            &id,
            Privilege::CreateTable,
            &Securable::schema("main", "sales")
        ));
    }

    /// Membership is by the resolved identity set; non-members are denied.
    #[test]
    fn group_membership_required() {
        let g = Principal::Group("analysts".into());
        let ev = Evaluator::new(vec![
            Grant::allow(Securable::catalog("main"), Privilege::Select, g.clone()),
            Grant::allow(Securable::catalog("main"), Privilege::UseCatalog, g.clone()),
            Grant::allow(Securable::catalog("main"), Privilege::UseSchema, g.clone()),
        ]);
        assert!(ev.can(
            &Identity::user("bob").with_groups(["analysts"]),
            Privilege::Select,
            &orders()
        ));
        assert!(!ev.can(
            &Identity::user("eve").with_groups(["interns"]),
            Privilege::Select,
            &orders()
        ));
    }

    /// Metastore grants don't inherit down to namespaced data securables.
    #[test]
    fn metastore_grant_does_not_inherit_data() {
        let alice = Principal::User("alice".into());
        let id = Identity::user("alice");
        let ev = Evaluator::new(vec![Grant::allow(
            Securable::metastore(),
            Privilege::Select,
            alice,
        )]);
        assert!(!ev.can(&id, Privilege::Select, &orders()));
    }

    #[test]
    fn ancestors_chain() {
        assert_eq!(
            orders().ancestors(),
            vec![
                Securable::schema("main", "sales"),
                Securable::catalog("main"),
                Securable::metastore(),
            ]
        );
    }
}
