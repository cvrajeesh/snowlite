//! Snowflake-compatible HTTP server backed by snowlite.
//!
//! This server implements the Snowflake wire protocol endpoints:
//! - POST /session/v1/login-request — authentication (always succeeds)
//! - POST /queries/v1/query-request — execute SQL and return results
//! - POST /session?delete=true — close session
//!
//! Usage:
//!   cargo run --features server --bin snowlite-server -- [--port PORT]
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
use tower_http::decompression::RequestDecompressionLayer;

// ── Types ────────────────────────────────────────────────────────────────────

/// Commands sent to a connection's dedicated thread.
#[allow(dead_code)]
enum ConnCmd {
    /// Execute a statement (DDL/DML) and return affected row count.
    Execute {
        sql: String,
        bindings: Vec<snowlite::Value>,
        reply: mpsc::Sender<ConnResult>,
    },
    /// Execute a query (SELECT) and return column metadata + rows.
    Query {
        sql: String,
        bindings: Vec<snowlite::Value>,
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
        rows: Vec<Vec<snowlite::Value>>,
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
pub struct AppState {
    sessions: Sessions,
}

// ── Connection thread ────────────────────────────────────────────────────────

fn spawn_connection_thread(rx: mpsc::Receiver<ConnCmd>) {
    thread::spawn(move || {
        let conn = match snowlite::Connection::open_in_memory() {
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
                        Err(e) => {
                            // COMMIT/ROLLBACK fail when no transaction is active (SQLite
                            // autocommit mode).  Treat this as a silent no-op so that
                            // `conn.commit()` / `conn.rollback()` never raise on the client.
                            let msg = e.to_string().to_lowercase();
                            let sql_up = sql.trim().to_uppercase();
                            if (sql_up.starts_with("COMMIT") || sql_up.starts_with("ROLLBACK"))
                                && (msg.contains("no transaction") || msg.contains("cannot commit") || msg.contains("cannot rollback"))
                            {
                                ConnResult::Execute { affected_rows: 0 }
                            } else {
                                ConnResult::Error(e.to_string())
                            }
                        }
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
                            let row_data: Vec<Vec<snowlite::Value>> = rows
                                .iter()
                                .map(|row| {
                                    (0..row.column_count())
                                        .map(|i| row.value(i).cloned().unwrap_or(snowlite::Value::Null))
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

/// Convert JSON bindings from Snowflake format to snowlite::Value vec.
///
/// Snowflake sends bindings as: {"1": {"type": "...", "value": "..."}, "2": ...}
fn parse_bindings(bindings: &Option<JsonValue>) -> Vec<snowlite::Value> {
    let Some(bindings) = bindings else {
        return vec![];
    };
    let Some(obj) = bindings.as_object() else {
        return vec![];
    };

    let mut indexed: Vec<(usize, snowlite::Value)> = obj
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

fn binding_to_value(val: &JsonValue) -> snowlite::Value {
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

fn json_to_value_with_type(v: &JsonValue, type_hint: &str) -> snowlite::Value {
    match v {
        JsonValue::Null => snowlite::Value::Null,
        JsonValue::String(s) => match type_hint.to_uppercase().as_str() {
            "FIXED" | "INTEGER" | "NUMBER" => s
                .parse::<i64>()
                .map(snowlite::Value::Integer)
                .unwrap_or_else(|_| snowlite::Value::Text(s.clone())),
            "REAL" | "FLOAT" | "DOUBLE" => s
                .parse::<f64>()
                .map(snowlite::Value::Real)
                .unwrap_or_else(|_| snowlite::Value::Text(s.clone())),
            "BOOLEAN" => match s.to_lowercase().as_str() {
                "true" | "1" => snowlite::Value::Boolean(true),
                "false" | "0" => snowlite::Value::Boolean(false),
                _ => snowlite::Value::Text(s.clone()),
            },
            _ => snowlite::Value::Text(s.clone()),
        },
        _ => json_to_value(v),
    }
}

fn json_to_value(v: &JsonValue) -> snowlite::Value {
    match v {
        JsonValue::Null => snowlite::Value::Null,
        JsonValue::Bool(b) => snowlite::Value::Boolean(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                snowlite::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                snowlite::Value::Real(f)
            } else {
                snowlite::Value::Text(n.to_string())
            }
        }
        JsonValue::String(s) => snowlite::Value::Text(s.clone()),
        _ => snowlite::Value::Text(v.to_string()),
    }
}

fn value_to_json_string(v: &snowlite::Value) -> JsonValue {
    // Snowflake returns all values as strings in rowset
    match v {
        snowlite::Value::Null => JsonValue::Null,
        snowlite::Value::Integer(i) => JsonValue::String(i.to_string()),
        snowlite::Value::Real(r) => JsonValue::String(r.to_string()),
        snowlite::Value::Text(s) => JsonValue::String(s.clone()),
        snowlite::Value::Blob(b) => JsonValue::String(hex::encode(b)),
        snowlite::Value::Boolean(b) => JsonValue::String(if *b { "1" } else { "0" }.to_string()),
    }
}

/// Simple hex encoding for blobs (avoids adding a dependency).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

/// Normalize C-style backslash escapes in SQL string literals to forms SQLite understands.
///
/// The Snowflake Python connector's pyformat paramstyle uses C-style escapes when
/// interpolating bound values into SQL strings:
///   - `\'` → `''`  (SQL standard quote doubling)
///   - `\n` → actual newline
///   - `\r` → actual carriage return
///   - `\t` → actual tab
///   - `\\` → `\`
///
/// This function processes only content inside single-quoted string literals, leaving
/// the rest of the SQL untouched.
fn normalize_sql_string_escapes(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_string = false;

    while let Some(c) = chars.next() {
        if in_string {
            if c == '\\' {
                match chars.peek() {
                    Some(&'\'') => {
                        chars.next();
                        result.push_str("''");
                    }
                    Some(&'n') => {
                        chars.next();
                        result.push('\n');
                    }
                    Some(&'r') => {
                        chars.next();
                        result.push('\r');
                    }
                    Some(&'t') => {
                        chars.next();
                        result.push('\t');
                    }
                    Some(&'\\') => {
                        chars.next();
                        result.push('\\');
                    }
                    _ => {
                        result.push(c);
                    }
                }
            } else if c == '\'' {
                result.push(c);
                in_string = false;
            } else {
                result.push(c);
            }
        } else {
            if c == '\'' {
                in_string = true;
            }
            result.push(c);
        }
    }
    result
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

fn snowflake_type_for_value(v: &snowlite::Value) -> &'static str {
    match v {
        snowlite::Value::Null => "TEXT",
        snowlite::Value::Integer(_) => "FIXED",
        snowlite::Value::Real(_) => "REAL",
        snowlite::Value::Text(_) => "TEXT",
        snowlite::Value::Blob(_) => "BINARY",
        snowlite::Value::Boolean(_) => "BOOLEAN",
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
    let token = format!("snowlite-token-{session_id}");

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

    let sql = normalize_sql_string_escapes(&sql);
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
                        "total": affected_rows,
                        "returned": 0,
                        "queryId": query_id,
                        "queryResultFormat": "json",
                        "parameters": []
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
/// The Snowflake connector sends: `Snowflake Token="snowlite-token-<session_id>"`
fn extract_session_id(headers: &axum::http::HeaderMap) -> Option<String> {
    let auth = headers.get("Authorization")?.to_str().ok()?;
    // Format: Snowflake Token="snowlite-token-<session_id>"
    if let Some(rest) = auth.strip_prefix("Snowflake Token=\"") {
        let token = rest.trim_end_matches('"');
        token.strip_prefix("snowlite-token-").map(|s| s.to_string())
    } else {
        None
    }
}

// ── Telemetry stub ───────────────────────────────────────────────────────────

/// POST /telemetry/send — accept and discard telemetry data
async fn telemetry_send() -> Json<JsonValue> {
    Json(json!({"success": true}))
}

// ── App builder ───────────────────────────────────────────────────────────────

impl AppState {
    fn new() -> Self {
        AppState {
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Build the axum router with all routes wired up.
///
/// Extracted from `main` so tests can construct the app in-process without
/// binding a real TCP port.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/session/v1/login-request", post(login))
        .route("/session", post(session_action))
        .route("/queries/v1/query-request", post(query_request))
        .route("/telemetry/send", post(telemetry_send))
        .layer(RequestDecompressionLayer::new())
        .with_state(state)
}

// ── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let port: u16 = std::env::args()
        .skip_while(|a| a != "--port")
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(8765);

    let app = build_router(AppState::new());

    let addr = format!("0.0.0.0:{port}");
    eprintln!("snowlite-server listening on {addr}");
    eprintln!("Connect with: snowflake.connector.connect(host='localhost', port={port}, protocol='http', user='test', password='test', account='test')");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
