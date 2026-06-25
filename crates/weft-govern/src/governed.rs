//! `GovernedCatalog` — the enforcement decorator.
//!
//! The catalog SPI is auth-free; this wraps any [`weft_catalog::CatalogProvider`] and applies the
//! [`Evaluator`] so a session sees only what its [`Identity`] is granted:
//!
//! - `list_namespaces` / `list_tables` are **filtered** to the visible objects (the user can
//!   `BROWSE` or `SELECT` them).
//! - `load_table` is **authorized** for `SELECT` before the inner provider resolves it; a denied
//!   reference returns a not-found-style error (so existence isn't leaked).
//!
//! Per the plan this is one of the two enforcement seams (the other being the analyzer hook that
//! injects row-filter/column-mask rewrites). A securable's name is `[catalog, ..namespace, table]`
//! — the wrapped provider *is* the catalog, so its [`name`](weft_catalog::CatalogProvider::name)
//! is the first part.

use std::sync::Arc;

use async_trait::async_trait;
use weft_catalog::{CatalogProvider, Result, TableMetadata};
use weft_common::Error;

use crate::{Evaluator, Identity, Privilege, Securable, SecurableType};

/// A [`CatalogProvider`] wrapper that enforces governance for one [`Identity`].
pub struct GovernedCatalog {
    inner: Arc<dyn CatalogProvider>,
    evaluator: Arc<Evaluator>,
    identity: Identity,
}

impl GovernedCatalog {
    /// Wrap `inner`, enforcing `evaluator`'s grants for `identity`.
    pub fn new(
        inner: Arc<dyn CatalogProvider>,
        evaluator: Arc<Evaluator>,
        identity: Identity,
    ) -> Self {
        Self {
            inner,
            evaluator,
            identity,
        }
    }

    /// The securable for a table reached via `namespace.table` within this catalog.
    fn table_securable(&self, namespace: &[String], table: &str) -> Securable {
        let mut name = Vec::with_capacity(namespace.len() + 2);
        name.push(self.inner.name().to_string());
        name.extend(namespace.iter().cloned());
        name.push(table.to_string());
        Securable {
            kind: SecurableType::Table,
            name,
        }
    }

    /// The securable for a namespace (schema) within this catalog.
    fn schema_securable(&self, namespace: &[String]) -> Securable {
        let mut name = Vec::with_capacity(namespace.len() + 1);
        name.push(self.inner.name().to_string());
        name.extend(namespace.iter().cloned());
        Securable {
            kind: SecurableType::Schema,
            name,
        }
    }

    /// Whether the identity may even *see* a securable in a listing: it can `BROWSE` it (metadata
    /// only, not traversal-gated) or `SELECT` it (full access). Used to filter `list_*`.
    fn visible(&self, securable: &Securable) -> bool {
        self.evaluator
            .can(&self.identity, Privilege::Browse, securable)
            || self
                .evaluator
                .can(&self.identity, Privilege::Select, securable)
    }
}

#[async_trait]
impl CatalogProvider for GovernedCatalog {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
        let all = self.inner.list_namespaces(parent).await?;
        Ok(all
            .into_iter()
            .filter(|ns| self.visible(&self.schema_securable(ns)))
            .collect())
    }

    async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
        let all = self.inner.list_tables(namespace).await?;
        Ok(all
            .into_iter()
            .filter(|t| self.visible(&self.table_securable(namespace, t)))
            .collect())
    }

    async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
        let securable = self.table_securable(namespace, table);
        match self
            .evaluator
            .authorize(&self.identity, Privilege::Select, &securable)
        {
            crate::Decision::Allow => self.inner.load_table(namespace, table).await,
            // Not-found-style error so a denied reference doesn't leak that the table exists.
            crate::Decision::Deny(_) => Err(Error::Plan(format!(
                "no such table: {}.{table}",
                namespace.join(".")
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Grant, Principal, Securable};
    use std::collections::HashMap;
    use weft_catalog::TableFormat;

    /// A minimal in-memory provider for one catalog named `prod` with `sales.orders` + `sales.secret`.
    struct FakeCatalog;

    #[async_trait]
    impl CatalogProvider for FakeCatalog {
        fn name(&self) -> &str {
            "prod"
        }
        async fn list_namespaces(&self, parent: &[String]) -> Result<Vec<Vec<String>>> {
            Ok(if parent.is_empty() {
                vec![vec!["sales".into()]]
            } else {
                vec![]
            })
        }
        async fn list_tables(&self, namespace: &[String]) -> Result<Vec<String>> {
            Ok(if namespace == ["sales"] {
                vec!["orders".into(), "secret".into()]
            } else {
                vec![]
            })
        }
        async fn load_table(&self, namespace: &[String], table: &str) -> Result<TableMetadata> {
            let key = format!("{}.{table}", namespace.join("."));
            if key == "sales.orders" || key == "sales.secret" {
                Ok(TableMetadata {
                    name: format!("prod.{key}"),
                    location: format!("s3://prod/{table}"),
                    format: TableFormat::Parquet,
                    schema: None,
                    storage_options: HashMap::new(),
                    partition_columns: vec![],
                })
            } else {
                Err(Error::Plan(format!("no such table: {key}")))
            }
        }
    }

    /// Grants that let `analysts` SELECT `prod.sales.orders` only (with the USE traversal).
    fn evaluator() -> Arc<Evaluator> {
        let g = Principal::Group("analysts".into());
        Arc::new(Evaluator::new(vec![
            Grant::allow(Securable::catalog("prod"), Privilege::UseCatalog, g.clone()),
            Grant::allow(
                Securable::schema("prod", "sales"),
                Privilege::UseSchema,
                g.clone(),
            ),
            Grant::allow(
                Securable::table("prod", "sales", "orders"),
                Privilege::Select,
                g.clone(),
            ),
            // BROWSE on `orders` only (not the schema) so it lists, while `secret` stays hidden —
            // schema-level BROWSE would inherit to every table, which is the opposite of this test.
            Grant::allow(
                Securable::table("prod", "sales", "orders"),
                Privilege::Browse,
                g,
            ),
        ]))
    }

    fn analyst() -> Identity {
        Identity::user("bob").with_groups(["analysts"])
    }

    #[tokio::test]
    async fn authorized_table_loads() {
        let gc = GovernedCatalog::new(Arc::new(FakeCatalog), evaluator(), analyst());
        let md = gc.load_table(&["sales".into()], "orders").await.unwrap();
        assert_eq!(md.location, "s3://prod/orders");
    }

    #[tokio::test]
    async fn unauthorized_table_is_hidden_and_denied() {
        let gc = GovernedCatalog::new(Arc::new(FakeCatalog), evaluator(), analyst());
        // `secret` exists in the inner provider but the user has no grant → not-found error.
        let err = gc
            .load_table(&["sales".into()], "secret")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Plan(_)));
        // And it's filtered out of the listing (only `orders` is visible).
        let tables = gc.list_tables(&["sales".into()]).await.unwrap();
        assert_eq!(tables, vec!["orders".to_string()]);
    }

    #[tokio::test]
    async fn no_grants_sees_nothing() {
        let gc = GovernedCatalog::new(
            Arc::new(FakeCatalog),
            Arc::new(Evaluator::new(vec![])),
            Identity::user("eve"),
        );
        assert!(gc.list_namespaces(&[]).await.unwrap().is_empty());
        assert!(gc.load_table(&["sales".into()], "orders").await.is_err());
    }
}
