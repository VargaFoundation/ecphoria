//! Query planning — routes SQL to the appropriate execution backend.

use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

/// A planned query ready for execution.
#[derive(Debug)]
pub enum QueryPlan {
    /// Pure SQL query executed via DuckDB (SELECT, SHOW, DESCRIBE, etc.).
    Sql(String),
    /// DML statement (INSERT, UPDATE, DELETE).
    Dml(String),
    /// Vector similarity search (detected via strata_search function call).
    VectorSearch { query_text: String, k: usize },
}

/// Plans queries by analyzing SQL and routing appropriately.
pub struct QueryPlanner;

impl QueryPlanner {
    /// Analyze SQL and produce a query plan.
    pub fn plan(sql: &str) -> crate::Result<QueryPlan> {
        let trimmed = sql.trim();

        // Quick check for strata-specific functions
        let upper = trimmed.to_uppercase();
        if upper.contains("STRATA_SEARCH(") || upper.contains("STRATA_SEARCH (") {
            // Extract search arguments from the SQL
            // Pattern: strata_search('query text', k)
            if let Some(args) = extract_function_args(trimmed, "strata_search") {
                let parts: Vec<&str> = args.splitn(2, ',').collect();
                let query_text = parts
                    .first()
                    .unwrap_or(&"")
                    .trim()
                    .trim_matches('\'')
                    .trim_matches('"')
                    .to_string();
                let k = parts
                    .get(1)
                    .and_then(|s| s.trim().parse::<usize>().ok())
                    .unwrap_or(5);
                return Ok(QueryPlan::VectorSearch { query_text, k });
            }
        }

        // Parse with sqlparser to detect statement type
        let dialect = DuckDbDialect {};
        match Parser::parse_sql(&dialect, trimmed) {
            Ok(statements) => {
                if let Some(stmt) = statements.first() {
                    match stmt {
                        sqlparser::ast::Statement::Query(_) => Ok(QueryPlan::Sql(trimmed.into())),
                        sqlparser::ast::Statement::Insert(_)
                        | sqlparser::ast::Statement::Update { .. }
                        | sqlparser::ast::Statement::Delete(_) => {
                            Ok(QueryPlan::Dml(trimmed.into()))
                        }
                        // Everything else (CREATE, DROP, etc.) treated as SQL
                        _ => Ok(QueryPlan::Sql(trimmed.into())),
                    }
                } else {
                    Ok(QueryPlan::Sql(trimmed.into()))
                }
            }
            // If parser fails, pass through to DuckDB (it may support syntax we don't)
            Err(_) => Ok(QueryPlan::Sql(trimmed.into())),
        }
    }
}

/// Extract function arguments from SQL like `func_name('arg1', arg2)`.
fn extract_function_args<'a>(sql: &'a str, func_name: &str) -> Option<&'a str> {
    let lower = sql.to_lowercase();
    let func_lower = func_name.to_lowercase();
    let pos = lower.find(&func_lower)?;
    let after_name = &sql[pos + func_name.len()..];
    let open = after_name.find('(')?;
    let rest = &after_name[open + 1..];
    let close = rest.find(')')?;
    Some(&rest[..close])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_select() {
        let plan = QueryPlanner::plan("SELECT * FROM episodic").unwrap();
        assert!(matches!(plan, QueryPlan::Sql(_)));
    }

    #[test]
    fn plan_select_with_where() {
        let plan =
            QueryPlanner::plan("SELECT * FROM episodic WHERE source = 'app' LIMIT 10").unwrap();
        assert!(matches!(plan, QueryPlan::Sql(_)));
    }

    #[test]
    fn plan_insert() {
        let plan = QueryPlanner::plan(
            "INSERT INTO episodic (id, source, event_type, payload, ts) VALUES ('a','b','c','{}','2024-01-01')",
        )
        .unwrap();
        assert!(matches!(plan, QueryPlan::Dml(_)));
    }

    #[test]
    fn plan_strata_search() {
        let plan = QueryPlanner::plan("SELECT * FROM strata_search('billing issue', 5)").unwrap();
        match plan {
            QueryPlan::VectorSearch { query_text, k } => {
                assert_eq!(query_text, "billing issue");
                assert_eq!(k, 5);
            }
            other => panic!("expected VectorSearch, got {:?}", other),
        }
    }

    #[test]
    fn plan_strata_search_default_k() {
        let plan = QueryPlanner::plan("SELECT * FROM strata_search('test query')").unwrap();
        match plan {
            QueryPlan::VectorSearch { query_text, k } => {
                assert_eq!(query_text, "test query");
                assert_eq!(k, 5); // default
            }
            other => panic!("expected VectorSearch, got {:?}", other),
        }
    }

    #[test]
    fn plan_unparseable_sql_passes_through() {
        // DuckDB-specific syntax that sqlparser might not understand
        let plan = QueryPlanner::plan("PRAGMA database_list").unwrap();
        assert!(matches!(plan, QueryPlan::Sql(_)));
    }

    #[test]
    fn extract_args() {
        let args =
            extract_function_args("SELECT * FROM strata_search('hello', 3)", "strata_search");
        assert_eq!(args, Some("'hello', 3"));
    }

    #[test]
    fn extract_args_not_found() {
        let args = extract_function_args("SELECT 1", "strata_search");
        assert!(args.is_none());
    }
}
