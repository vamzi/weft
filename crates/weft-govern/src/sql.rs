//! The SQL governance surface: parse `GRANT` / `DENY` / `REVOKE` / `SHOW GRANTS` into the
//! [`crate`] model so `weft-connect` can route them to the store + evaluator.
//!
//! Supported (Unity-Catalog-style) forms:
//! ```sql
//! GRANT  <priv>[, <priv>...] ON <securable> TO <principal>
//! DENY   <priv>[, <priv>...] ON <securable> TO <principal>
//! REVOKE <priv>[, <priv>...] ON <securable> FROM <principal>
//! SHOW GRANTS [<principal>] [ON <securable>]
//! ```
//! `<securable>` is `TABLE main.sales.orders`, `SCHEMA main.sales`, `CATALOG main`, `METASTORE`,
//! `EXTERNAL LOCATION <name>`, etc. `<principal>` is a back-tick/quoted name, optionally prefixed
//! with `USER` / `GROUP` / `SERVICE_PRINCIPAL` (default: a group, the common grant target).

use weft_catalog::split_ident;

use crate::{Effect, Grant, Principal, Privilege, Securable, SecurableType};

/// A parsed governance statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// `GRANT`/`DENY`: one [`Grant`] per (privilege × principal). Effect distinguishes grant/deny.
    Grant(Vec<Grant>),
    /// `REVOKE`: the matching grants to remove (effect [`Effect::Allow`]).
    Revoke(Vec<Grant>),
    /// `SHOW GRANTS [principal] [ON securable]`.
    ShowGrants {
        /// Filter to a securable, if given.
        securable: Option<Securable>,
        /// Filter to a principal, if given.
        principal: Option<Principal>,
    },
}

/// A parse failure with a human-readable reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "governance parse error: {}", self.0)
    }
}

/// Whether `sql` looks like a governance statement (so the SQL layer routes it here).
pub fn is_governance_statement(sql: &str) -> bool {
    let head = sql.trim_start().to_ascii_uppercase();
    head.starts_with("GRANT ")
        || head.starts_with("DENY ")
        || head.starts_with("REVOKE ")
        || head.starts_with("SHOW GRANTS")
}

/// Parse a single governance statement.
pub fn parse(sql: &str) -> Result<Statement, ParseError> {
    let s = sql.trim().trim_end_matches(';').trim();
    let upper = s.to_ascii_uppercase();

    if upper.starts_with("SHOW GRANTS") {
        return parse_show(&s["SHOW GRANTS".len()..]);
    }

    let (effect, rest, sep) = if let Some(r) = strip_kw(s, "GRANT") {
        (Effect::Allow, r, " TO ")
    } else if let Some(r) = strip_kw(s, "DENY") {
        (Effect::Deny, r, " TO ")
    } else if let Some(r) = strip_kw(s, "REVOKE") {
        (Effect::Allow, r, " FROM ")
    } else {
        return Err(ParseError(format!("not a governance statement: {s}")));
    };
    let is_revoke = upper.starts_with("REVOKE");

    // <priv-list> ON <securable> {TO|FROM} <principal>
    let (privs_part, after_on) =
        split_once_ci(rest, " ON ").ok_or_else(|| ParseError("expected ` ON ` clause".into()))?;
    let (securable_part, principal_part) = split_once_ci(after_on, sep)
        .ok_or_else(|| ParseError(format!("expected `{}` clause", sep.trim())))?;

    let privileges = parse_privileges(privs_part)?;
    let securable = parse_securable(securable_part)?;
    let principal = parse_principal(principal_part)?;

    let grants = privileges
        .into_iter()
        .map(|p| Grant {
            securable: securable.clone(),
            privilege: p,
            principal: principal.clone(),
            effect,
        })
        .collect();
    Ok(if is_revoke {
        Statement::Revoke(grants)
    } else {
        Statement::Grant(grants)
    })
}

fn parse_show(rest: &str) -> Result<Statement, ParseError> {
    let rest = rest.trim();
    if rest.is_empty() {
        return Ok(Statement::ShowGrants {
            securable: None,
            principal: None,
        });
    }
    // `SHOW GRANTS ON <securable>` — securable only (no leading principal).
    if let Some(sec) = strip_kw(rest, "ON") {
        return Ok(Statement::ShowGrants {
            securable: Some(parse_securable(sec)?),
            principal: None,
        });
    }
    // `SHOW GRANTS <principal> [ON <securable>]`.
    let (principal_part, securable_part) = match split_once_ci(rest, " ON ") {
        Some((p, s)) => (p.trim(), Some(s)),
        None => (rest, None),
    };
    let principal = if principal_part.is_empty() {
        None
    } else {
        Some(parse_principal(principal_part)?)
    };
    let securable = match securable_part {
        Some(s) => Some(parse_securable(s)?),
        None => None,
    };
    Ok(Statement::ShowGrants {
        securable,
        principal,
    })
}

fn parse_privileges(part: &str) -> Result<Vec<Privilege>, ParseError> {
    part.split(',')
        .map(|p| {
            let key = normalize_ws(p).to_ascii_uppercase();
            privilege_from_str(&key)
                .ok_or_else(|| ParseError(format!("unknown privilege `{}`", p.trim())))
        })
        .collect()
}

fn privilege_from_str(p: &str) -> Option<Privilege> {
    Some(match p {
        "SELECT" => Privilege::Select,
        "MODIFY" => Privilege::Modify,
        "EXECUTE" => Privilege::Execute,
        "USE CATALOG" => Privilege::UseCatalog,
        "USE SCHEMA" => Privilege::UseSchema,
        "CREATE CATALOG" => Privilege::CreateCatalog,
        "CREATE SCHEMA" => Privilege::CreateSchema,
        "CREATE TABLE" => Privilege::CreateTable,
        "CREATE FUNCTION" => Privilege::CreateFunction,
        "CREATE VOLUME" => Privilege::CreateVolume,
        "READ VOLUME" => Privilege::ReadVolume,
        "WRITE VOLUME" => Privilege::WriteVolume,
        "READ FILES" => Privilege::ReadFiles,
        "WRITE FILES" => Privilege::WriteFiles,
        "CREATE EXTERNAL TABLE" => Privilege::CreateExternalTable,
        "BROWSE" => Privilege::Browse,
        "ALL PRIVILEGES" => Privilege::AllPrivileges,
        "MANAGE" => Privilege::Manage,
        _ => return None,
    })
}

fn parse_securable(part: &str) -> Result<Securable, ParseError> {
    let part = part.trim();
    let upper = normalize_ws(part).to_ascii_uppercase();
    if upper == "METASTORE" {
        return Ok(Securable::metastore());
    }
    // Match the longest type phrase first (two-word types before one-word).
    const TYPES: &[(&str, SecurableType)] = &[
        ("EXTERNAL LOCATION", SecurableType::ExternalLocation),
        ("STORAGE CREDENTIAL", SecurableType::StorageCredential),
        ("CATALOG", SecurableType::Catalog),
        ("SCHEMA", SecurableType::Schema),
        ("DATABASE", SecurableType::Schema), // Spark alias for schema
        ("TABLE", SecurableType::Table),
        ("VIEW", SecurableType::View),
        ("VOLUME", SecurableType::Volume),
        ("FUNCTION", SecurableType::Function),
        ("CONNECTION", SecurableType::Connection),
    ];
    for (phrase, kind) in TYPES {
        if let Some(name_part) = strip_kw(part, phrase) {
            let name = split_ident(name_part.trim());
            if name.is_empty() {
                return Err(ParseError(format!("securable `{phrase}` requires a name")));
            }
            return Ok(Securable { kind: *kind, name });
        }
    }
    Err(ParseError(format!("unknown securable: `{part}`")))
}

fn parse_principal(part: &str) -> Result<Principal, ParseError> {
    let part = part.trim();
    // Optional kind keyword.
    if let Some(rest) = strip_kw(part, "USER") {
        return Ok(Principal::User(unquote(rest)));
    }
    if let Some(rest) = strip_kw(part, "GROUP") {
        return Ok(Principal::Group(unquote(rest)));
    }
    if let Some(rest) = strip_kw(part, "SERVICE_PRINCIPAL") {
        return Ok(Principal::ServicePrincipal(unquote(rest)));
    }
    let name = unquote(part);
    if name.is_empty() {
        return Err(ParseError("expected a principal".into()));
    }
    // Bare name defaults to a group (the common grant target in Databricks/UC).
    Ok(Principal::Group(name))
}

// ── small parsing helpers ──────────────────────────────────────────────────────────────────────

/// If `s` (case-insensitively) starts with keyword `kw` followed by whitespace, return the rest.
fn strip_kw<'a>(s: &'a str, kw: &str) -> Option<&'a str> {
    let s = s.trim_start();
    if s.len() >= kw.len() && s[..kw.len()].eq_ignore_ascii_case(kw) {
        let rest = &s[kw.len()..];
        if rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace()) {
            return Some(rest.trim_start());
        }
    }
    None
}

/// Case-insensitive `split_once` on a separator (e.g. `" ON "`).
fn split_once_ci<'a>(s: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let up = s.to_ascii_uppercase();
    let pos = up.find(&sep.to_ascii_uppercase())?;
    Some((&s[..pos], &s[pos + sep.len()..]))
}

/// Collapse internal runs of whitespace to single spaces and trim.
fn normalize_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Strip surrounding back-ticks / single / double quotes.
fn unquote(s: &str) -> String {
    let s = s.trim();
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let (f, l) = (bytes[0], bytes[bytes.len() - 1]);
        if (f == b'`' && l == b'`') || (f == b'\'' && l == b'\'') || (f == b'"' && l == b'"') {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grant_select_to_group() {
        let st = parse("GRANT SELECT ON TABLE main.sales.orders TO `analysts`").unwrap();
        assert_eq!(
            st,
            Statement::Grant(vec![Grant::allow(
                Securable::table("main", "sales", "orders"),
                Privilege::Select,
                Principal::Group("analysts".into()),
            )])
        );
    }

    #[test]
    fn grant_multiple_privileges_with_use() {
        let st = parse("GRANT USE CATALOG, USE SCHEMA ON SCHEMA main.sales TO GROUP `analysts`")
            .unwrap();
        match st {
            Statement::Grant(grants) => {
                assert_eq!(grants.len(), 2);
                assert_eq!(grants[0].privilege, Privilege::UseCatalog);
                assert_eq!(grants[1].privilege, Privilege::UseSchema);
                assert_eq!(grants[0].securable, Securable::schema("main", "sales"));
            }
            _ => panic!("expected grant"),
        }
    }

    #[test]
    fn deny_modify_to_user() {
        let st = parse("DENY MODIFY ON TABLE main.sales.orders TO USER `alice`;").unwrap();
        assert_eq!(
            st,
            Statement::Grant(vec![Grant::deny(
                Securable::table("main", "sales", "orders"),
                Privilege::Modify,
                Principal::User("alice".into()),
            )])
        );
    }

    #[test]
    fn revoke_from_group() {
        let st = parse("REVOKE SELECT ON TABLE main.sales.orders FROM `analysts`").unwrap();
        match st {
            Statement::Revoke(grants) => {
                assert_eq!(grants.len(), 1);
                assert_eq!(grants[0].privilege, Privilege::Select);
            }
            _ => panic!("expected revoke"),
        }
    }

    #[test]
    fn all_privileges_and_metastore() {
        let st = parse("GRANT ALL PRIVILEGES ON METASTORE TO GROUP `admins`").unwrap();
        match st {
            Statement::Grant(grants) => {
                assert_eq!(grants[0].privilege, Privilege::AllPrivileges);
                assert_eq!(grants[0].securable, Securable::metastore());
            }
            _ => panic!("expected grant"),
        }
    }

    #[test]
    fn show_grants_forms() {
        assert_eq!(
            parse("SHOW GRANTS ON TABLE main.sales.orders").unwrap(),
            Statement::ShowGrants {
                securable: Some(Securable::table("main", "sales", "orders")),
                principal: None
            }
        );
        assert_eq!(
            parse("SHOW GRANTS `analysts`").unwrap(),
            Statement::ShowGrants {
                securable: None,
                principal: Some(Principal::Group("analysts".into()))
            }
        );
        assert_eq!(
            parse("SHOW GRANTS").unwrap(),
            Statement::ShowGrants {
                securable: None,
                principal: None
            }
        );
    }

    #[test]
    fn detection_and_errors() {
        assert!(is_governance_statement("grant select on table t to `g`"));
        assert!(is_governance_statement("SHOW GRANTS"));
        assert!(!is_governance_statement("SELECT 1"));
        assert!(parse("GRANT BOGUS ON TABLE t.a.b TO `g`").is_err());
        assert!(parse("GRANT SELECT ON WIDGET x TO `g`").is_err());
    }
}
