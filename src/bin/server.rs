//! Snowflake-compatible HTTP server backed by local-db.
//!
//! This server implements the Snowflake wire protocol endpoints:
//! - POST /session/v1/login-request — authentication (always succeeds)
//! - POST /queries/v1/query-request — execute SQL and return results
//! - POST /session?delete=true — close session
//!
//! Usage:
//!   cargo run --features server --bin local-db-server -- [--port PORT]
//!
//! Then point any Snowflake connector at it:
//!   conn = snowflake.connector.connect(
//!       host='localhost', port=8765, protocol='http',
//!       user='test', password='test', account='test'
//!   )

use std::collections::HashMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value as JsonValue};

// ── Types ────────────────────────────────────────────────────────────────────

/// Commands sent to a connection's dedicated thread.
#[allow(dead_code)]
enum ConnCmd {
    /// Execute a statement (DDL/DML) and return affected row count.
    Execute {
        sql: String,
        bindings: Vec<local_db::Value>,
        reply: mpsc::Sender<ConnResult>,
    },
    /// Execute a query (SELECT) and return column metadata + rows.
    Query {
        sql: String,
        bindings: Vec<local_db::Value>,
        reply: mpsc::Sender<ConnResult>,
    },
    /// Execute a batch of semicolon-separated statements.
    Batch {
        sql: String,
        reply: mpsc::Sender<ConnResult>,
    },
    /// Shut down the connection thread.
    Close,
}

/// Results sent back from the connection thread.
enum ConnResult {
    Execute {
        affected_rows: usize,
    },
    Query {
        columns: Vec<ColumnMeta>,
        rows: Vec<Vec<local_db::Value>>,
    },
    Batch,
    Error(String),
}

#[derive(Clone)]
#[allow(dead_code)]
struct ColumnMeta {
    name: String,
    type_name: String,
}

/// Handle to a connection running on a dedicated thread.
struct ConnHandle {
    sender: mpsc::Sender<ConnCmd>,
}

impl ConnHandle {
    fn send_cmd(&self, cmd: ConnCmd) -> Result<(), String> {
        self.sender
            .send(cmd)
            .map_err(|_| "connection thread terminated".to_string())
    }
}

type Sessions = Arc<Mutex<HashMap<String, ConnHandle>>>;

#[derive(Clone)]
struct AppState {
    sessions: Sessions,
}

// ── Connection thread ────────────────────────────────────────────────────────

fn spawn_connection_thread(rx: mpsc::Receiver<ConnCmd>) {
    thread::spawn(move || {
        let conn = match local_db::Connection::open_in_memory() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to open in-memory database: {e}");
                return;
            }
        };

        for cmd in rx {
            match cmd {
                ConnCmd::Execute {
                    sql,
                    bindings,
                    reply,
                } => {
                    let params: Vec<&dyn rusqlite::types::ToSql> =
                        bindings.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
                    let result = match conn.execute(&sql, &params) {
                        Ok(n) => ConnResult::Execute { affected_rows: n },
                        Err(e) => ConnResult::Error(e.to_string()),
                    };
                    let _ = reply.send(result);
                }
                ConnCmd::Query {
                    sql,
                    bindings,
                    reply,
                } => {
                    let params: Vec<&dyn rusqlite::types::ToSql> =
                        bindings.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
                    let result = match conn.query(&sql, &params) {
                        Ok(rows) => {
                            let columns: Vec<ColumnMeta> = if rows.is_empty() {
                                vec![]
                            } else {
                                rows[0]
                                    .columns()
                                    .iter()
                                    .map(|name| ColumnMeta {
                                        name: name.clone(),
                                        type_name: "TEXT".to_string(),
                                    })
                                    .collect()
                            };
                            let row_data: Vec<Vec<local_db::Value>> = rows
                                .iter()
                                .map(|row| {
                                    (0..row.column_count())
                                        .map(|i| row.value(i).cloned().unwrap_or(local_db::Value::Null))
                                        .collect()
                                })
                                .collect();
                            ConnResult::Query {
                                columns,
                                rows: row_data,
                            }
                        }
                        Err(e) => ConnResult::Error(e.to_string()),
                    };
                    let _ = reply.send(result);
                }
                ConnCmd::Batch { sql, reply } => {
                    let result = match conn.execute_batch(&sql) {
                        Ok(()) => ConnResult::Batch,
                        Err(e) => ConnResult::Error(e.to_string()),
                    };
                    let _ = reply.send(result);
                }
                ConnCmd::Close => break,
            }
        }
    });
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn new_session_id() -> String {
    uuid::Uuid::new_v4().to_string().replace('-', "")
}

fn new_query_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Convert JSON bindings from Snowflake format to local_db::Value vec.
///
/// Snowflake sends bindings as: {"1": {"type": "...", "value": "..."}, "2": ...}
fn parse_bindings(bindings: &Option<JsonValue>) -> Vec<local_db::Value> {
    let Some(bindings) = bindings else {
        return vec![];
    };
    let Some(obj) = bindings.as_object() else {
        return vec![];
    };

    let mut indexed: Vec<(usize, local_db::Value)> = obj
        .iter()
        .filter_map(|(key, val)| {
            let idx: usize = key.parse().ok()?;
            let value = binding_to_value(val);
            Some((idx, value))
        })
        .collect();
    indexed.sort_by_key(|(i, _)| *i);
    indexed.into_iter().map(|(_, v)| v).collect()
}

fn binding_to_value(val: &JsonValue) -> local_db::Value {
    // Snowflake format: {"type": "TEXT", "value": "hello"}
    if let Some(obj) = val.as_object() {
        if let Some(v) = obj.get("value") {
            let type_hint = obj
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("TEXT");
            return json_to_value_with_type(v, type_hint);
        }
    }
    // Plain JSON value
    json_to_value(val)
}

fn json_to_value_with_type(v: &JsonValue, type_hint: &str) -> local_db::Value {
    match v {
        JsonValue::Null => local_db::Value::Null,
        JsonValue::String(s) => match type_hint.to_uppercase().as_str() {
            "FIXED" | "INTEGER" | "NUMBER" => s
                .parse::<i64>()
                .map(local_db::Value::Integer)
                .unwrap_or_else(|_| local_db::Value::Text(s.clone())),
            "REAL" | "FLOAT" | "DOUBLE" => s
                .parse::<f64>()
                .map(local_db::Value::Real)
                .unwrap_or_else(|_| local_db::Value::Text(s.clone())),
            "BOOLEAN" => match s.to_lowercase().as_str() {
                "true" | "1" => local_db::Value::Boolean(true),
                "false" | "0" => local_db::Value::Boolean(false),
                _ => local_db::Value::Text(s.clone()),
            },
            _ => local_db::Value::Text(s.clone()),
        },
        _ => json_to_value(v),
    }
}

fn json_to_value(v: &JsonValue) -> local_db::Value {
    match v {
        JsonValue::Null => local_db::Value::Null,
        JsonValue::Bool(b) => local_db::Value::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                local_db::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                local_db::Value::Real(f)
            } else {
                local_db::Value::Text(n.to_string())
            }
        }
        JsonValue::String(s) => local_db::Value::Text(s.clone()),
        _ => local_db::Value::Text(v.to_string()),
    }
}

fn value_to_json_string(v: &local_db::Value) -> JsonValue {
    // Snowflake returns all values as strings in rowset
    match v {
        local_db::Value::Null => JsonValue::Null,
        local_db::Value::Integer(i) => JsonValue::String(i.to_string()),
        local_db::Value::Real(r) => JsonValue::String(r.to_string()),
        local_db::Value::Text(s) => JsonValue::String(s.clone()),
        local_db::Value::Blob(b) => JsonValue::String(hex::encode(b)),
        local_db::Value::Boolean(b) => JsonValue::String(if *b { "1" } else { "0" }.to_string()),
    }
}

/// Simple hex encoding for blobs (avoids adding a dependency).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

fn is_query(sql: &str) -> bool {
    let trimmed = sql.trim_start().to_uppercase();
    trimmed.starts_with("SELECT")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("DESC ")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("VALUES")
        || trimmed.starts_with("EXPLAIN")
        || trimmed.starts_with("LIST")
        || trimmed.starts_with("LS ")
}

fn snowflake_type_for_value(v: &local_db::Value) -> &'static str {
    match v {
        local_db::Value::Null => "TEXT",
        local_db::Value::Integer(_) => "FIXED",
        local_db::Value::Real(_) => "REAL",
        local_db::Value::Text(_) => "TEXT",
        local_db::Value::Blob(_) => "BINARY",
        local_db::Value::Boolean(_) => "BOOLEAN",
    }
}

// ── Route handlers ───────────────────────────────────────────────────────────

async fn health() -> Json<JsonValue> {
    Json(json!({"status": "ok"}))
}

/// POST /session/v1/login-request
///
/// Creates a new session (in-memory SQLite database) and returns a token.
async fn login(State(state): State<AppState>, Json(_body): Json<JsonValue>) -> Json<JsonValue> {
    let session_id = new_session_id();
    let token = format!("local-db-token-{session_id}");

    // Spawn a dedicated thread for this connection
    let (tx, rx) = mpsc::channel();
    spawn_connection_thread(rx);

    let handle = ConnHandle { sender: tx };
    state
        .sessions
        .lock()
        .unwrap()
        .insert(session_id.clone(), handle);

    Json(json!({
        "success": true,
        "code": null,
        "message": null,
        "data": {
            "token": token,
            "masterToken": token,
            "masterValidityInSeconds": 86400,
            "sessionId": session_id,
            "parameters": [
                {"name": "AUTOCOMMIT", "value": true},
                {"name": "CLIENT_SESSION_KEEP_ALIVE", "value": false},
                {"name": "QUERY_RESULT_FORMAT", "value": "JSON"},
            ],
            "sessionInfo": {
                "databaseName": "LOCAL_DB",
                "schemaName": "PUBLIC",
                "warehouseName": "LOCAL_WH",
                "roleName": "SYSADMIN"
            }
        }
    }))
}

/// POST /session — handle session close (delete=true) and heartbeat
async fn session_action(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> StatusCode {
    if params.get("delete").map(|v| v == "true").unwrap_or(false) {
        // Close session
        if let Some(token) = extract_session_id(&headers) {
            if let Some(handle) = state.sessions.lock().unwrap().remove(&token) {
                let _ = handle.send_cmd(ConnCmd::Close);
            }
        }
    }
    StatusCode::OK
}

/// POST /queries/v1/query-request
async fn query_request(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<JsonValue>,
) -> (StatusCode, Json<JsonValue>) {
    let session_id = match extract_session_id(&headers) {
        Some(id) => id,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "success": false,
                    "code": "390100",
                    "message": "Missing or invalid session token",
                    "data": null
                })),
            );
        }
    };

    let sessions = state.sessions.lock().unwrap();
    let handle = match sessions.get(&session_id) {
        Some(h) => h,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "success": false,
                    "code": "390104",
                    "message": "Session does not exist",
                    "data": null
                })),
            );
        }
    };

    let sql = body
        .get("sqlText")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if sql.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "success": false,
                "code": "000900",
                "message": "Empty SQL text",
                "data": null
            })),
        );
    }

    let bindings = parse_bindings(&body.get("bindings").cloned());
    let query_id = new_query_id();

    let (reply_tx, reply_rx) = mpsc::channel();

    let cmd = if is_query(&sql) {
        ConnCmd::Query {
            sql,
            bindings,
            reply: reply_tx,
        }
    } else {
        ConnCmd::Execute {
            sql,
            bindings,
            reply: reply_tx,
        }
    };

    if let Err(e) = handle.send_cmd(cmd) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "success": false,
                "code": "000001",
                "message": e,
                "data": null
            })),
        );
    }

    // Wait for result (with timeout)
    let result = match reply_rx.recv_timeout(std::time::Duration::from_secs(30)) {
        Ok(r) => r,
        Err(_) => {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({
                    "success": false,
                    "code": "000002",
                    "message": "Query timed out",
                    "data": null
                })),
            );
        }
    };

    match result {
        ConnResult::Query { columns, rows } => {
            let rowtype: Vec<JsonValue> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    // Determine type from first row if available
                    let sf_type = rows
                        .first()
                        .and_then(|row| row.get(i))
                        .map(snowflake_type_for_value)
                        .unwrap_or("TEXT");
                    json!({
                        "name": col.name.to_uppercase(),
                        "database": "LOCAL_DB",
                        "schema": "PUBLIC",
                        "table": "",
                        "nullable": true,
                        "type": sf_type,
                        "byteLength": null,
                        "length": null,
                        "scale": 0,
                        "precision": null
                    })
                })
                .collect();

            let rowset: Vec<JsonValue> = rows
                .iter()
                .map(|row| {
                    JsonValue::Array(row.iter().map(value_to_json_string).collect())
                })
                .collect();

            let total = rowset.len();

            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "code": null,
                    "message": null,
                    "data": {
                        "rowtype": rowtype,
                        "rowset": rowset,
                        "total": total,
                        "returned": total,
                        "queryId": query_id,
                        "queryResultFormat": "json",
                        "parameters": []
                    }
                })),
            )
        }
        ConnResult::Execute { affected_rows } => {
            (
                StatusCode::OK,
                Json(json!({
                    "success": true,
                    "code": null,
                    "message": null,
                    "data": {
                        "rowtype": [],
                        "rowset": [],
                        "total": 0,
                        "returned": 0,
                        "queryId": query_id,
                        "queryResultFormat": "json",
                        "parameters": [],
                        "numberOfRows": affected_rows
                    }
                })),
            )
        }
        ConnResult::Batch => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "code": null,
                "message": null,
                "data": {
                    "rowtype": [],
                    "rowset": [],
                    "total": 0,
                    "returned": 0,
                    "queryId": query_id,
                    "queryResultFormat": "json",
                    "parameters": []
                }
            })),
        ),
        ConnResult::Error(e) => (
            StatusCode::OK,
            Json(json!({
                "success": false,
                "code": "002003",
                "message": e,
                "data": {
                    "queryId": query_id
                }
            })),
        ),
    }
}

/// Extract session ID from the Authorization header.
///
/// The Snowflake connector sends: `Snowflake Token="local-db-token-<session_id>"`
fn extract_session_id(headers: &axum::http::HeaderMap) -> Option<String> {
    let auth = headers.get("Authorization")?.to_str().ok()?;
    // Format: Snowflake Token="local-db-token-<session_id>"
    if let Some(rest) = auth.strip_prefix("Snowflake Token=\"") {
        let token = rest.trim_end_matches('"');
        token.strip_prefix("local-db-token-").map(|s| s.to_string())
    } else {
        None
    }
}

// ── Telemetry stub ───────────────────────────────────────────────────────────

/// POST /telemetry/send — accept and discard telemetry data
async fn telemetry_send() -> Json<JsonValue> {
    Json(json!({"success": true}))
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let port: u16 = std::env::args()
        .skip_while(|a| a != "--port")
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(8765);

    let state = AppState {
        sessions: Arc::new(Mutex::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/session/v1/login-request", post(login))
        .route("/session", post(session_action))
        .route("/queries/v1/query-request", post(query_request))
        .route("/telemetry/send", post(telemetry_send))
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    eprintln!("local-db-server listening on {addr}");
    eprintln!("Connect with: snowflake.connector.connect(host='localhost', port={port}, protocol='http', user='test', password='test', account='test')");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
