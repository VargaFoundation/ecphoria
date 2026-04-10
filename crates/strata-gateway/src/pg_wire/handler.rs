//! PostgreSQL wire protocol handler — routes SQL to the Strata engine.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo, QueryResponse,
    Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};

use strata_core::StrataEngine;

/// No-auth startup handler.
pub struct StrataStartupHandler;
impl NoopStartupHandler for StrataStartupHandler {}

/// PG wire handler backed by the Strata engine.
pub struct PgWireHandler {
    engine: Arc<StrataEngine>,
    query_parser: Arc<NoopQueryParser>,
}

impl PgWireHandler {
    pub fn new(engine: Arc<StrataEngine>) -> Self {
        Self {
            engine,
            query_parser: Arc::new(NoopQueryParser::new()),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for PgWireHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // Route all queries through the engine's DuckDB
        match self.engine.query_sql(query).await {
            Ok(rows) => {
                if rows.is_empty() {
                    // Could be a DDL/DML statement
                    return Ok(vec![Response::Execution(Tag::new("OK"))]);
                }

                // Build field info from first row's keys
                let field_names: Vec<String> = if let Some(first) = rows.first() {
                    first
                        .as_object()
                        .map(|obj| obj.keys().cloned().collect())
                        .unwrap_or_default()
                } else {
                    vec![]
                };

                // Infer types from first row's values
                let field_types: Vec<Type> = if let Some(first) = rows.first() {
                    field_names
                        .iter()
                        .map(|name| {
                            first
                                .as_object()
                                .and_then(|obj| obj.get(name))
                                .map(infer_pg_type)
                                .unwrap_or(Type::VARCHAR)
                        })
                        .collect()
                } else {
                    field_names.iter().map(|_| Type::VARCHAR).collect()
                };

                let fields: Vec<FieldInfo> = field_names
                    .iter()
                    .zip(field_types.iter())
                    .enumerate()
                    .map(|(i, (name, pg_type))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            pg_type.clone(),
                            pgwire::api::portal::Format::UnifiedText.format_for(i),
                        )
                    })
                    .collect();

                let schema = Arc::new(fields);

                // Encode rows
                let mut data_rows = Vec::new();
                for row in &rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    if let Some(obj) = row.as_object() {
                        for name in &field_names {
                            let val = obj.get(name).map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            });
                            encoder
                                .encode_field(&val)
                                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                        }
                    }
                    data_rows.push(encoder.finish());
                }

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(data_rows),
                ))])
            }
            Err(e) => {
                // Try as a non-SELECT statement
                Err(PgWireError::UserError(Box::new(
                    pgwire::error::ErrorInfo::new(
                        "ERROR".to_owned(),
                        "42000".to_owned(),
                        e.to_string(),
                    ),
                )))
            }
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgWireHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let query = &portal.statement.statement;
        match self.engine.query_sql(query).await {
            Ok(rows) if rows.is_empty() => Ok(Response::Execution(Tag::new("OK"))),
            Ok(rows) => {
                let field_names: Vec<String> = rows
                    .first()
                    .and_then(|r| r.as_object())
                    .map(|obj| obj.keys().cloned().collect())
                    .unwrap_or_default();

                let field_types: Vec<Type> = if let Some(first) = rows.first() {
                    field_names
                        .iter()
                        .map(|name| {
                            first
                                .as_object()
                                .and_then(|obj| obj.get(name))
                                .map(infer_pg_type)
                                .unwrap_or(Type::VARCHAR)
                        })
                        .collect()
                } else {
                    field_names.iter().map(|_| Type::VARCHAR).collect()
                };

                let fields: Vec<FieldInfo> = field_names
                    .iter()
                    .zip(field_types.iter())
                    .enumerate()
                    .map(|(i, (name, pg_type))| {
                        FieldInfo::new(
                            name.clone(),
                            None,
                            None,
                            pg_type.clone(),
                            portal.result_column_format.format_for(i),
                        )
                    })
                    .collect();
                let schema = Arc::new(fields);

                let mut data_rows = Vec::new();
                for row in &rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    if let Some(obj) = row.as_object() {
                        for name in &field_names {
                            let val = obj.get(name).map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            });
                            encoder
                                .encode_field(&val)
                                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
                        }
                    }
                    data_rows.push(encoder.finish());
                }

                Ok(Response::Query(QueryResponse::new(
                    schema,
                    stream::iter(data_rows),
                )))
            }
            Err(e) => Err(PgWireError::UserError(Box::new(
                pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42000".to_owned(),
                    e.to_string(),
                ),
            ))),
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribeStatementResponse::new(vec![], vec![]))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(DescribePortalResponse::new(vec![]))
    }
}

/// Factory that creates PG wire server handlers.
pub struct PgWireFactory {
    handler: Arc<PgWireHandler>,
}

impl PgWireFactory {
    pub fn new(engine: Arc<StrataEngine>) -> Self {
        Self {
            handler: Arc::new(PgWireHandler::new(engine)),
        }
    }
}

impl PgWireServerHandlers for PgWireFactory {
    type StartupHandler = StrataStartupHandler;
    type SimpleQueryHandler = PgWireHandler;
    type ExtendedQueryHandler = PgWireHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(StrataStartupHandler)
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

/// Start the PG wire server on the given address.
///
/// `max_connections` limits concurrent PG wire connections via a semaphore.
pub async fn start_pg_wire(
    addr: &str,
    engine: Arc<StrataEngine>,
    max_connections: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let factory = Arc::new(PgWireFactory::new(engine));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_connections));

    tracing::info!(addr, max_connections, "PG wire server listening");

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    let permit = match semaphore.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tracing::warn!(
                                %peer_addr,
                                "PG wire connection rejected: max connections reached"
                            );
                            drop(socket);
                            continue;
                        }
                    };
                    let factory_ref = factory.clone();
                    tokio::spawn(async move {
                        let _ = pgwire::tokio::process_socket(socket, None, factory_ref).await;
                        drop(permit);
                    });
                }
                Err(e) => {
                    tracing::error!("PG wire accept error: {e}");
                    break;
                }
            }
        }
    });

    Ok(())
}

/// Infer a PostgreSQL type from a JSON value.
fn infer_pg_type(value: &serde_json::Value) -> Type {
    match value {
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                Type::INT8
            } else {
                Type::FLOAT8
            }
        }
        serde_json::Value::Bool(_) => Type::BOOL,
        serde_json::Value::Null => Type::VARCHAR,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Type::JSON,
        serde_json::Value::String(s) => {
            // Try to detect timestamps
            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                Type::TIMESTAMPTZ
            } else {
                Type::VARCHAR
            }
        }
    }
}
