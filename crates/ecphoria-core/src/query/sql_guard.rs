//! Shared SQL guarding for the cross-store query path — table extraction (for routing a query to the
//! store that owns its tables) and a generic per-tenant view rewrite. The read-only validation
//! (SELECT-only + no filesystem/network functions) and the collision-resistant per-tenant view name
//! are reused from [`EpisodicStore`] so all SQL entry points share one security boundary.

use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations, visit_relations_mut, Ident, ObjectName};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use crate::memory::episodic::EpisodicStore;

/// Reject anything that isn't a read-only SELECT (or that calls a filesystem/network function).
pub(crate) fn validate_read_only(sql: &str) -> crate::Result<()> {
    EpisodicStore::validate_read_only(sql)
}

/// The lower-cased base table names a query reads from (last identifier of each relation), e.g.
/// `SELECT * FROM mem.memories m JOIN sessions s …` → `["memories", "sessions"]`. Used to route a
/// query to the store that owns its tables. Returns `Err` if the SQL doesn't parse.
pub(crate) fn referenced_tables(sql: &str) -> crate::Result<Vec<String>> {
    let statements = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|e| crate::Error::Query(format!("SQL parse error: {e}")))?;
    let mut tables = Vec::new();
    for stmt in &statements {
        let _ = visit_relations(stmt, |name: &ObjectName| {
            if let Some(ident) = name.0.last() {
                let t = ident.value.to_ascii_lowercase();
                if !tables.contains(&t) {
                    tables.push(t);
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }
    Ok(tables)
}

/// Rewrite each of `base_tables` in `sql` to its per-tenant view name, so a tenant only ever reads
/// its own rows (the same AST-rewrite technique `EpisodicStore` uses for `episodic`/`sessions`).
/// Rejects any direct reference to an internal `<table>__t_*` view. Fails **closed**: unparseable
/// SQL is rejected, never run unscoped.
pub(crate) fn scope_tables(sql: &str, tenant: &str, base_tables: &[&str]) -> crate::Result<String> {
    let mut statements = Parser::parse_sql(&DuckDbDialect {}, sql)
        .map_err(|e| crate::Error::Query(format!("SQL parse error (tenant scope): {e}")))?;
    if statements.is_empty() {
        return Err(crate::Error::Query("empty SQL statement".into()));
    }
    let mut forbidden = false;
    for stmt in statements.iter_mut() {
        let flow = visit_relations_mut(stmt, |name: &mut ObjectName| {
            let last = name
                .0
                .last()
                .map(|i| i.value.to_ascii_lowercase())
                .unwrap_or_default();
            if base_tables.contains(&last.as_str()) {
                let view = EpisodicStore::tenant_scoped_view_name(&last, tenant);
                *name = ObjectName(vec![Ident::new(view)]);
            } else if base_tables
                .iter()
                .any(|b| last.starts_with(&format!("{b}__t_")))
            {
                // A tenant must never address an internal per-tenant view by name.
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        });
        if flow.is_break() {
            forbidden = true;
            break;
        }
    }
    if forbidden {
        return Err(crate::Error::Query(
            "reference to an internal per-tenant view is not permitted".into(),
        ));
    }
    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_referenced_tables() {
        let t =
            referenced_tables("SELECT * FROM memories m JOIN memory_edges e ON e.src = m.subject")
                .unwrap();
        assert!(t.contains(&"memories".to_string()));
        assert!(t.contains(&"memory_edges".to_string()));
        assert!(referenced_tables("SELECT 1").unwrap().is_empty());
    }

    #[test]
    fn scopes_only_named_tables() {
        let out = scope_tables(
            "SELECT content FROM memories WHERE valid_to IS NULL",
            "acme",
            &["memories"],
        )
        .unwrap();
        // The bare `memories` reference is replaced by acme's per-tenant view (`memories__t_…`), so
        // the raw `FROM memories ` form is gone.
        let lower = out.to_lowercase();
        assert!(lower.contains("memories__t_"), "view not injected: {out}");
        assert!(
            !lower.contains("from memories "),
            "raw table still present: {out}"
        );
    }

    #[test]
    fn rejects_internal_view_reference() {
        let view = EpisodicStore::tenant_scoped_view_name("memories", "acme");
        let sql = format!("SELECT * FROM {view}");
        assert!(scope_tables(&sql, "acme", &["memories"]).is_err());
    }
}
