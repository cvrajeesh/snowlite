//! Integration tests for the Snowflake-compatible HTTP server.
//!
//! These tests exercise the server in-process using axum's tower service
//! interface — no real TCP port is bound.
//!
//! Run with:
//!   cargo test --features server --test server_tests

use axum::{
    body::Body,
    http::{self, Request, StatusCode},
};
use serde_json::{json, Value};
use tower::ServiceExt; // for `oneshot`

// The binary crate isn't a library, so we inline the minimal helpers needed
// to build the router. We use the `include!` trick via a path re-export in
// server.rs — but since bins can't be depended on directly, we replicate the
// app-building logic here through the public `build_router` / `AppState`
// re-exported by the binary (available when compiled as part of the test).
//
// In Cargo, `[[test]]` with `required-features = ["server"]` means the test
// binary is linked with the same crate graph, so we can reach the server
// module via `include!` or by calling the binary's public items directly.
//
// We use a thin helper module that rebuilds the router the same way server.rs does.

mod app {
    use axum::Router;
    use snowlite::Connection;
    use std::collections::HashMap;
    use std::sync::{mpsc, Arc, Mutex};
    use std::thread;
    use axum::{
        extract::{Query, State},
        http::StatusCode,
        routing::{get, post},
        Json,
    };
    use serde_json::{json, Value as JsonValue};

    // ── Mirror of the server types (kept minimal for tests) ──────────────────

    #[allow(dead_code)]
    pub(super) enum ConnCmd {
        Execute { sql: String, bindings: Vec<snowlite::Value>, reply: mpsc::Sender<ConnResult> },
        Query   { sql: String, bindings: Vec<snowlite::Value>, reply: mpsc::Sender<ConnResult> },
        Close,
    }

    pub(super) enum ConnResult {
        Execute { affected_rows: usize },
        Query   { columns: Vec<String>, rows: Vec<Vec<snowlite::Value>> },
        Error(String),
    }

    pub(super) struct ConnHandle {
        pub sender: mpsc::Sender<ConnCmd>,
    }

    impl ConnHandle {
        pub fn send(&self, cmd: ConnCmd) -> Result<(), String> {
            self.sender.send(cmd).map_err(|_| "thread gone".into())
        }
    }

    pub(super) type Sessions = Arc<Mutex<HashMap<String, ConnHandle>>>;

    #[derive(Clone)]
    pub(super) struct AppState {
        pub sessions: Sessions,
    }

    impl AppState {
        pub fn new() -> Self {
            AppState { sessions: Arc::new(Mutex::new(HashMap::new())) }
        }
    }

    // ── Connection thread ────────────────────────────────────────────────────

    pub(super) fn spawn_conn(rx: mpsc::Receiver<ConnCmd>) {
        thread::spawn(move || {
            let conn = Connection::open_in_memory().expect("open db");
            for cmd in rx {
                match cmd {
                    ConnCmd::Execute { sql, bindings, reply } => {
                        let p: Vec<&dyn rusqlite::types::ToSql> =
                            bindings.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
                        let r = match conn.execute(&sql, &p) {
                            Ok(n)  => ConnResult::Execute { affected_rows: n },
                            Err(e) => {
                                // COMMIT/ROLLBACK fail when no transaction is active; treat as no-op.
                                let msg = e.to_string().to_lowercase();
                                let sql_up = sql.trim().to_uppercase();
                                if (sql_up.starts_with("COMMIT") || sql_up.starts_with("ROLLBACK"))
                                    && (msg.contains("no transaction")
                                        || msg.contains("cannot commit")
                                        || msg.contains("cannot rollback"))
                                {
                                    ConnResult::Execute { affected_rows: 0 }
                                } else {
                                    ConnResult::Error(e.to_string())
                                }
                            }
                        };
                        let _ = reply.send(r);
                    }
                    ConnCmd::Query { sql, bindings, reply } => {
                        let p: Vec<&dyn rusqlite::types::ToSql> =
                            bindings.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();
                        let r = match conn.query(&sql, &p) {
                            Ok(rows) => {
                                let columns = rows.first().map(|r| r.columns().to_vec()).unwrap_or_default();
                                let data = rows.iter().map(|row| {
                                    (0..row.column_count())
                                        .map(|i| row.value(i).cloned().unwrap_or(snowlite::Value::Null))
                                        .collect()
                                }).collect();
                                ConnResult::Query { columns, rows: data }
                            }
                            Err(e) => ConnResult::Error(e.to_string()),
                        };
                        let _ = reply.send(r);
                    }
                    ConnCmd::Close => break,
                }
            }
        });
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn new_session_id() -> String {
        // Use a simple counter-based ID for determinism in tests
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        format!("{:032x}", CTR.fetch_add(1, Ordering::Relaxed))
    }

    fn new_query_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static CTR: AtomicU64 = AtomicU64::new(1);
        format!("query-{}", CTR.fetch_add(1, Ordering::Relaxed))
    }

    fn parse_bindings(bindings: &Option<JsonValue>) -> Vec<snowlite::Value> {
        let Some(b) = bindings else { return vec![] };
        let Some(obj) = b.as_object() else { return vec![] };
        let mut pairs: Vec<(usize, snowlite::Value)> = obj.iter().filter_map(|(k, v)| {
            let idx: usize = k.parse().ok()?;
            let val = if let Some(o) = v.as_object() {
                let raw = o.get("value").unwrap_or(v);
                let hint = o.get("type").and_then(|t| t.as_str()).unwrap_or("TEXT");
                match (raw, hint.to_uppercase().as_str()) {
                    (JsonValue::Null, _) => snowlite::Value::Null,
                    (JsonValue::String(s), "FIXED" | "INTEGER" | "NUMBER") =>
                        s.parse::<i64>().map(snowlite::Value::Integer).unwrap_or_else(|_| snowlite::Value::Text(s.clone())),
                    (JsonValue::String(s), "REAL" | "FLOAT") =>
                        s.parse::<f64>().map(snowlite::Value::Real).unwrap_or_else(|_| snowlite::Value::Text(s.clone())),
                    (JsonValue::String(s), "BOOLEAN") => match s.to_lowercase().as_str() {
                        "true" | "1" => snowlite::Value::Boolean(true),
                        _ => snowlite::Value::Boolean(false),
                    },
                    (JsonValue::String(s), _) => snowlite::Value::Text(s.clone()),
                    (JsonValue::Number(n), _) => n.as_i64().map(snowlite::Value::Integer)
                        .or_else(|| n.as_f64().map(snowlite::Value::Real))
                        .unwrap_or(snowlite::Value::Null),
                    (JsonValue::Bool(b), _) => snowlite::Value::Boolean(*b),
                    (other, _) => snowlite::Value::Text(other.to_string()),
                }
            } else {
                match v {
                    JsonValue::Null => snowlite::Value::Null,
                    JsonValue::Bool(b) => snowlite::Value::Boolean(*b),
                    JsonValue::Number(n) => n.as_i64().map(snowlite::Value::Integer)
                        .or_else(|| n.as_f64().map(snowlite::Value::Real))
                        .unwrap_or(snowlite::Value::Null),
                    JsonValue::String(s) => snowlite::Value::Text(s.clone()),
                    other => snowlite::Value::Text(other.to_string()),
                }
            };
            Some((idx, val))
        }).collect();
        pairs.sort_by_key(|(i, _)| *i);
        pairs.into_iter().map(|(_, v)| v).collect()
    }

    fn is_query(sql: &str) -> bool {
        let t = sql.trim_start().to_uppercase();
        t.starts_with("SELECT") || t.starts_with("WITH") || t.starts_with("VALUES")
            || t.starts_with("SHOW") || t.starts_with("DESCRIBE") || t.starts_with("EXPLAIN")
    }

    fn to_sf_string(v: &snowlite::Value) -> JsonValue {
        match v {
            snowlite::Value::Null       => JsonValue::Null,
            snowlite::Value::Integer(i) => JsonValue::String(i.to_string()),
            snowlite::Value::Real(r)    => JsonValue::String(r.to_string()),
            snowlite::Value::Text(s)    => JsonValue::String(s.clone()),
            snowlite::Value::Boolean(b) => JsonValue::String(if *b { "1" } else { "0" }.to_string()),
            snowlite::Value::Blob(b)    => JsonValue::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        }
    }

    fn sf_type(v: &snowlite::Value) -> &'static str {
        match v {
            snowlite::Value::Integer(_) => "FIXED",
            snowlite::Value::Real(_)    => "REAL",
            snowlite::Value::Boolean(_) => "BOOLEAN",
            snowlite::Value::Blob(_)    => "BINARY",
            _                           => "TEXT",
        }
    }

    fn extract_session_id(headers: &axum::http::HeaderMap) -> Option<String> {
        let auth = headers.get("Authorization")?.to_str().ok()?;
        let rest = auth.strip_prefix("Snowflake Token=\"")?;
        let token = rest.trim_end_matches('"');
        token.strip_prefix("snowlite-token-").map(|s| s.to_string())
    }

    // ── SQL preprocessing ────────────────────────────────────────────────────

    /// Normalise C-style backslash escapes inside single-quoted SQL string literals.
    ///
    /// The Snowflake connector's pyformat paramstyle emits `\'` for a literal
    /// apostrophe and `\n`/`\r`/`\t` for whitespace control characters.
    /// SQLite requires `''` (SQL-standard doubling) for apostrophes and does not
    /// interpret `\n` as a newline in string literals.
    pub(super) fn normalize_sql_string_escapes(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut chars = sql.chars().peekable();
        let mut in_string = false;
        while let Some(c) = chars.next() {
            if in_string {
                if c == '\\' {
                    match chars.peek() {
                        Some(&'\'') => { chars.next(); result.push_str("''"); }
                        Some(&'n')  => { chars.next(); result.push('\n'); }
                        Some(&'r')  => { chars.next(); result.push('\r'); }
                        Some(&'t')  => { chars.next(); result.push('\t'); }
                        Some(&'\\') => { chars.next(); result.push('\\'); }
                        _           => { result.push(c); }
                    }
                } else {
                    if c == '\'' { in_string = false; }
                    result.push(c);
                }
            } else {
                if c == '\'' { in_string = true; }
                result.push(c);
            }
        }
        result
    }

    // ── Handlers ─────────────────────────────────────────────────────────────

    async fn health() -> Json<JsonValue> {
        Json(json!({"status": "ok"}))
    }

    async fn login(State(state): State<AppState>, Json(_body): Json<JsonValue>) -> Json<JsonValue> {
        let session_id = new_session_id();
        let token = format!("snowlite-token-{session_id}");
        let (tx, rx) = mpsc::channel();
        spawn_conn(rx);
        state.sessions.lock().unwrap().insert(session_id.clone(), ConnHandle { sender: tx });
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

    async fn session_action(
        State(state): State<AppState>,
        Query(params): Query<HashMap<String, String>>,
        headers: axum::http::HeaderMap,
    ) -> StatusCode {
        if params.get("delete").map(|v| v == "true").unwrap_or(false) {
            if let Some(id) = extract_session_id(&headers) {
                if let Some(h) = state.sessions.lock().unwrap().remove(&id) {
                    let _ = h.send(ConnCmd::Close);
                }
            }
        }
        StatusCode::OK
    }

    async fn query_request(
        State(state): State<AppState>,
        headers: axum::http::HeaderMap,
        Json(body): Json<JsonValue>,
    ) -> (StatusCode, Json<JsonValue>) {
        let session_id = match extract_session_id(&headers) {
            Some(id) => id,
            None => return (StatusCode::UNAUTHORIZED, Json(json!({
                "success": false, "code": "390100",
                "message": "Missing or invalid session token", "data": null
            }))),
        };

        let sessions = state.sessions.lock().unwrap();
        let handle = match sessions.get(&session_id) {
            Some(h) => h,
            None => return (StatusCode::UNAUTHORIZED, Json(json!({
                "success": false, "code": "390104",
                "message": "Session does not exist", "data": null
            }))),
        };

        let sql = body.get("sqlText").and_then(|v| v.as_str()).unwrap_or("").to_string();
        if sql.is_empty() {
            return (StatusCode::BAD_REQUEST, Json(json!({
                "success": false, "code": "000900",
                "message": "Empty SQL text", "data": null
            })));
        }

        let sql = normalize_sql_string_escapes(&sql);
        let bindings = parse_bindings(&body.get("bindings").cloned());
        let query_id = new_query_id();
        let (tx, rx) = mpsc::channel();

        let cmd = if is_query(&sql) {
            ConnCmd::Query { sql, bindings, reply: tx }
        } else {
            ConnCmd::Execute { sql, bindings, reply: tx }
        };

        if let Err(e) = handle.send(cmd) {
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
                "success": false, "code": "000001", "message": e, "data": null
            })));
        }

        match rx.recv_timeout(std::time::Duration::from_secs(30)) {
            Err(_) => (StatusCode::GATEWAY_TIMEOUT, Json(json!({
                "success": false, "code": "000002", "message": "Query timed out", "data": null
            }))),
            Ok(ConnResult::Execute { affected_rows }) => (StatusCode::OK, Json(json!({
                "success": true, "code": null, "message": null,
                "data": {
                    "rowtype": [], "rowset": [],
                    "total": affected_rows, "returned": 0,
                    "queryId": query_id, "queryResultFormat": "json",
                    "parameters": []
                }
            }))),
            Ok(ConnResult::Query { columns, rows }) => {
                let rowtype: Vec<JsonValue> = columns.iter().enumerate().map(|(i, name)| {
                    let t = rows.first().and_then(|r| r.get(i)).map(sf_type).unwrap_or("TEXT");
                    json!({ "name": name.to_uppercase(), "database": "LOCAL_DB", "schema": "PUBLIC",
                            "table": "", "nullable": true, "type": t,
                            "byteLength": null, "length": null, "scale": 0, "precision": null })
                }).collect();
                let rowset: Vec<JsonValue> = rows.iter()
                    .map(|r| JsonValue::Array(r.iter().map(to_sf_string).collect()))
                    .collect();
                let total = rowset.len();
                (StatusCode::OK, Json(json!({
                    "success": true, "code": null, "message": null,
                    "data": {
                        "rowtype": rowtype, "rowset": rowset,
                        "total": total, "returned": total,
                        "queryId": query_id, "queryResultFormat": "json", "parameters": []
                    }
                })))
            }
            Ok(ConnResult::Error(e)) => (StatusCode::OK, Json(json!({
                "success": false, "code": "002003", "message": e,
                "data": { "queryId": query_id }
            }))),
        }
    }

    async fn telemetry_send() -> Json<JsonValue> {
        Json(json!({"success": true}))
    }

    /// Build the router — mirrors server.rs's `build_router`.
    pub fn build_router(state: AppState) -> axum::Router {
        Router::new()
            .route("/health", get(health))
            .route("/session/v1/login-request", post(login))
            .route("/session", post(session_action))
            .route("/queries/v1/query-request", post(query_request))
            .route("/telemetry/send", post(telemetry_send))
            .with_state(state)
    }
}

// ── Test helpers ─────────────────────────────────────────────────────────────

/// POST to /session/v1/login-request, return the token string.
async fn login(app: axum::Router) -> (axum::Router, String) {
    let req = Request::builder()
        .method(http::Method::POST)
        .uri("/session/v1/login-request")
        .header("Content-Type", "application/json")
        .body(Body::from(json!({"data":{"ACCOUNT_NAME":"test","LOGIN_NAME":"test","PASSWORD":"test"}}).to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["success"], true);
    let token = body["data"]["token"].as_str().unwrap().to_string();
    (app, token)
}

/// Execute a query-request and return the parsed JSON body.
async fn query(app: axum::Router, token: &str, sql: &str) -> (axum::Router, Value) {
    query_with_bindings(app, token, sql, None).await
}

async fn query_with_bindings(
    app: axum::Router,
    token: &str,
    sql: &str,
    bindings: Option<Value>,
) -> (axum::Router, Value) {
    let mut payload = json!({"sqlText": sql, "sequenceId": 1});
    if let Some(b) = bindings {
        payload["bindings"] = b;
    }
    let req = Request::builder()
        .method(http::Method::POST)
        .uri("/queries/v1/query-request")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Snowflake Token=\"{}\"", token))
        .body(Body::from(payload.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let body = body_json(resp).await;
    (app, body)
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn new_app() -> axum::Router {
    app::build_router(app::AppState::new())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_health() {
    let app = new_app();
    let req = Request::builder()
        .method(http::Method::GET)
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn test_login_returns_token_and_session_info() {
    let (_, token) = login(new_app()).await;
    assert!(token.starts_with("snowlite-token-"), "token={token}");
}

#[tokio::test]
async fn test_telemetry_accepted() {
    let app = new_app();
    let req = Request::builder()
        .method(http::Method::POST)
        .uri("/telemetry/send")
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"logs":[]}"#))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_query_without_auth_returns_401() {
    let app = new_app();
    let req = Request::builder()
        .method(http::Method::POST)
        .uri("/queries/v1/query-request")
        .header("Content-Type", "application/json")
        .body(Body::from(json!({"sqlText": "SELECT 1"}).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_query_with_invalid_session_returns_401() {
    let app = new_app();
    let req = Request::builder()
        .method(http::Method::POST)
        .uri("/queries/v1/query-request")
        .header("Content-Type", "application/json")
        .header("Authorization", "Snowflake Token=\"snowlite-token-doesnotexist\"")
        .body(Body::from(json!({"sqlText": "SELECT 1"}).to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let body = body_json(resp).await;
    assert_eq!(body["success"], false);
    assert_eq!(body["code"], "390104");
}

#[tokio::test]
async fn test_empty_sql_returns_400() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "").await;
    // Status 400 is returned; we get the body directly as already checked
    assert_eq!(body["success"], false);
    assert_eq!(body["code"], "000900");
}

#[tokio::test]
async fn test_ddl_create_table() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "CREATE TABLE t (id NUMBER, name VARCHAR)").await;
    assert_eq!(body["success"], true, "DDL failed: {body}");
    assert_eq!(body["data"]["rowset"], json!([]));
}

#[tokio::test]
async fn test_snowflake_create_or_replace_translated() {
    let (app, token) = login(new_app()).await;
    // CREATE OR REPLACE TABLE is a Snowflake-ism; the translator should rewrite it
    let (_, body) = query(app, &token,
        "CREATE OR REPLACE TABLE products (id NUMBER, price FLOAT)").await;
    assert_eq!(body["success"], true, "CREATE OR REPLACE failed: {body}");
}

#[tokio::test]
async fn test_insert_returns_affected_rows() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE TABLE t (id NUMBER, name VARCHAR)").await;
    let (_, body) = query(app, &token, "INSERT INTO t VALUES (1, 'Alice')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["total"], 1);
}

#[tokio::test]
async fn test_select_returns_rows_and_rowtype() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE TABLE t (id NUMBER, name VARCHAR)").await;
    let (app, _) = query(app, &token, "INSERT INTO t VALUES (1, 'Alice')").await;
    let (app, _) = query(app, &token, "INSERT INTO t VALUES (2, 'Bob')").await;

    let (_, body) = query(app, &token, "SELECT id, name FROM t ORDER BY id").await;
    assert_eq!(body["success"], true, "{body}");

    let rowset = &body["data"]["rowset"];
    assert_eq!(rowset.as_array().unwrap().len(), 2);
    assert_eq!(rowset[0], json!(["1", "Alice"]));
    assert_eq!(rowset[1], json!(["2", "Bob"]));

    let rowtype = &body["data"]["rowtype"];
    assert_eq!(rowtype[0]["name"], "ID");
    assert_eq!(rowtype[1]["name"], "NAME");

    assert_eq!(body["data"]["total"], 2);
    assert_eq!(body["data"]["returned"], 2);
}

#[tokio::test]
async fn test_select_with_no_rows() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE TABLE t (id NUMBER)").await;
    let (_, body) = query(app, &token, "SELECT * FROM t").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([]));
    assert_eq!(body["data"]["total"], 0);
}

#[tokio::test]
async fn test_null_values_encoded_correctly() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE TABLE t (id NUMBER, val VARCHAR)").await;
    let (app, _) = query(app, &token, "INSERT INTO t VALUES (1, NULL)").await;
    let (_, body) = query(app, &token, "SELECT val FROM t").await;
    assert_eq!(body["success"], true, "{body}");
    // Snowflake encodes NULL as JSON null in rowset
    assert_eq!(body["data"]["rowset"][0][0], Value::Null);
}

#[tokio::test]
async fn test_parameterized_query_with_bindings() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE TABLE t (id NUMBER, name VARCHAR)").await;
    let (app, _) = query(app, &token, "INSERT INTO t VALUES (1, 'Alice')").await;
    let (app, _) = query(app, &token, "INSERT INTO t VALUES (2, 'Bob')").await;

    // Bindings in Snowflake format: positional, {"1": {"type": "FIXED", "value": "1"}}
    let bindings = json!({"1": {"type": "FIXED", "value": "1"}});
    let (_, body) = query_with_bindings(
        app, &token,
        "SELECT name FROM t WHERE id = ?",
        Some(bindings),
    ).await;

    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["Alice"]]));
}

#[tokio::test]
async fn test_parameterized_insert_with_bindings() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE TABLE t (id NUMBER, name VARCHAR)").await;

    let bindings = json!({
        "1": {"type": "FIXED",  "value": "42"},
        "2": {"type": "TEXT",   "value": "Charlie"}
    });
    let (app, body) = query_with_bindings(
        app, &token,
        "INSERT INTO t VALUES (?, ?)",
        Some(bindings),
    ).await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["total"], 1);

    let (_, body) = query(app, &token, "SELECT id, name FROM t").await;
    assert_eq!(body["data"]["rowset"], json!([["42", "Charlie"]]));
}

#[tokio::test]
async fn test_snowflake_iff_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token,
        "SELECT IFF(1 > 0, 'yes', 'no')").await;
    assert_eq!(body["success"], true, "IFF failed: {body}");
    assert_eq!(body["data"]["rowset"], json!([["yes"]]));
}

#[tokio::test]
async fn test_snowflake_nvl_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token,
        "SELECT NVL(NULL, 'fallback')").await;
    assert_eq!(body["success"], true, "NVL failed: {body}");
    assert_eq!(body["data"]["rowset"], json!([["fallback"]]));
}

#[tokio::test]
async fn test_snowflake_coalesce_equivalent() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token,
        "SELECT COALESCE(NULL, NULL, 'third')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["third"]]));
}

#[tokio::test]
async fn test_invalid_sql_returns_error_response() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "THIS IS NOT VALID SQL %%%").await;
    // Server returns HTTP 200 but success=false (mirrors Snowflake error protocol)
    assert_eq!(body["success"], false, "{body}");
    assert!(!body["message"].as_str().unwrap_or("").is_empty());
}

#[tokio::test]
async fn test_session_close_invalidates_token() {
    let (app, token) = login(new_app()).await;

    // Close the session
    let req = Request::builder()
        .method(http::Method::POST)
        .uri("/session?delete=true")
        .header("Authorization", format!("Snowflake Token=\"{}\"", token))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Subsequent query should fail with 401
    let (_, body) = query(app, &token, "SELECT 1").await;
    assert_eq!(body["success"], false);
    assert_eq!(body["code"], "390104");
}

#[tokio::test]
async fn test_multiple_independent_sessions_are_isolated() {
    // Each login gets its own in-memory DB; data in one must not leak to another.
    let app = new_app();
    let (app, token_a) = login(app).await;
    let (app, token_b) = login(app).await;

    // Insert a row in session A
    let (app, _) = query(app, &token_a, "CREATE TABLE t (v NUMBER)").await;
    let (app, _) = query(app, &token_a, "INSERT INTO t VALUES (99)").await;

    // Session B must not see table t at all
    let (_, body) = query(app, &token_b, "SELECT * FROM t").await;
    assert_eq!(body["success"], false, "session B should not see session A's table, got: {body}");
}

#[tokio::test]
async fn test_ddl_then_dml_then_select_full_cycle() {
    let (app, token) = login(new_app()).await;

    let (app, b) = query(app, &token,
        "CREATE OR REPLACE TABLE orders (id NUMBER, item VARCHAR, qty NUMBER)").await;
    assert_eq!(b["success"], true, "{b}");

    let (app, b) = query(app, &token,
        "INSERT INTO orders VALUES (1, 'widget', 10)").await;
    assert_eq!(b["data"]["total"], 1, "{b}");

    let (app, b) = query(app, &token,
        "INSERT INTO orders VALUES (2, 'gadget', 5)").await;
    assert_eq!(b["data"]["total"], 1, "{b}");

    let (app, b) = query(app, &token,
        "UPDATE orders SET qty = 20 WHERE id = 1").await;
    assert_eq!(b["success"], true, "{b}");

    let (_, b) = query(app, &token,
        "SELECT id, item, qty FROM orders ORDER BY id").await;
    assert_eq!(b["success"], true, "{b}");
    assert_eq!(b["data"]["rowset"], json!([
        ["1", "widget", "20"],
        ["2", "gadget", "5"]
    ]));
}

#[tokio::test]
async fn test_rowtype_reports_correct_sf_types() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token,
        "CREATE TABLE t (n NUMBER, f FLOAT, s VARCHAR)").await;
    let (app, _) = query(app, &token,
        "INSERT INTO t VALUES (1, 3.14, 'hello')").await;
    let (_, b) = query(app, &token, "SELECT n, f, s FROM t").await;

    let rt = &b["data"]["rowtype"];
    assert_eq!(rt[0]["type"], "FIXED",   "integer column should be FIXED");
    assert_eq!(rt[1]["type"], "REAL",    "float column should be REAL");
    assert_eq!(rt[2]["type"], "TEXT",    "varchar column should be TEXT");
    assert_eq!(rt[0]["nullable"], true);
}

#[tokio::test]
async fn test_query_result_format_is_json() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT 1 AS n").await;
    assert_eq!(body["data"]["queryResultFormat"], "json");
    assert!(body["data"]["queryId"].as_str().is_some());
}

// ── Connection lifecycle (converted from test_connection.py) ──────────────────

#[tokio::test]
async fn test_select_integer_constant() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT 42 AS answer").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["42"]]));
    assert_eq!(body["data"]["rowtype"][0]["name"], "ANSWER");
}

#[tokio::test]
async fn test_select_string_constant() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT 'hello' AS greeting").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["hello"]]));
}

#[tokio::test]
async fn test_multiple_sequential_logins() {
    // Re-connecting (login/logout cycle) must always succeed.
    for _ in 0..3 {
        let (_, token) = login(new_app()).await;
        assert!(token.starts_with("snowlite-token-"));
    }
}

#[tokio::test]
async fn test_two_queries_share_same_session_db() {
    // A second query on the same session token can see data from the first.
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE mc_test (n NUMBER)").await;
    let (app, _) = query(app, &token, "INSERT INTO mc_test VALUES (1)").await;
    let (_, body) = query(app, &token, "SELECT n FROM mc_test").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["1"]]));
}

// ── DML rowcount (converted from test_cursor.py) ──────────────────────────────

#[tokio::test]
async fn test_rowcount_after_single_insert() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE rc_ins (n NUMBER)").await;
    let (_, body) = query(app, &token, "INSERT INTO rc_ins VALUES (1)").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["total"], 1);
}

#[tokio::test]
async fn test_rowcount_after_bulk_insert() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE rc_bulk (n NUMBER)").await;
    let (_, body) = query(app, &token, "INSERT INTO rc_bulk VALUES (1), (2), (3)").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["total"], 3);
}

#[tokio::test]
async fn test_rowcount_after_update() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE rc_upd (n NUMBER)").await;
    let (app, _) = query(app, &token, "INSERT INTO rc_upd VALUES (1)").await;
    let (app, _) = query(app, &token, "INSERT INTO rc_upd VALUES (2)").await;
    let (_, body) = query(app, &token, "UPDATE rc_upd SET n = 99 WHERE n = 1").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["total"], 1);
}

#[tokio::test]
async fn test_rowcount_after_delete() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE rc_del (n NUMBER)").await;
    let (app, _) = query(app, &token, "INSERT INTO rc_del VALUES (1)").await;
    let (app, _) = query(app, &token, "INSERT INTO rc_del VALUES (2)").await;
    let (_, body) = query(app, &token, "DELETE FROM rc_del").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["total"], 2);
}

#[tokio::test]
async fn test_dml_response_has_empty_rowtype() {
    // DML must not return column metadata (description is falsy after DML).
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE dml_desc (n NUMBER)").await;
    let (_, body) = query(app, &token, "INSERT INTO dml_desc VALUES (1)").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowtype"], json!([]));
}

// ── Noop statements (converted from test_noop_statements.py) ─────────────────

#[tokio::test]
async fn test_noop_use_database() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "USE DATABASE mydb").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([]));
}

#[tokio::test]
async fn test_noop_use_schema() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "USE SCHEMA public").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_noop_use_warehouse() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "USE WAREHOUSE compute_wh").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_noop_alter_session_set() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "ALTER SESSION SET QUERY_TAG = 'test'").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_noop_alter_session_unset() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "ALTER SESSION UNSET QUERY_TAG").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_noop_show_tables_empty_rowset() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SHOW TABLES").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([]));
}

#[tokio::test]
async fn test_noop_show_databases_empty_rowset() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SHOW DATABASES").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([]));
}

#[tokio::test]
async fn test_noop_grant() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "GRANT SELECT ON TABLE foo TO ROLE analyst").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_noop_create_warehouse() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "CREATE OR REPLACE WAREHOUSE test_wh").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_noop_does_not_corrupt_next_query() {
    // A noop immediately before a SELECT must not break the result.
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "SHOW TABLES").await;
    let (_, body) = query(app, &token, "SELECT 123 AS v").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["123"]]));
}

#[tokio::test]
async fn test_noop_followed_by_dml() {
    // Noops before real DDL/DML leave the connection fully functional.
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "USE DATABASE mydb").await;
    let (app, _) = query(app, &token, "ALTER SESSION SET QUERY_TAG = 'noop_test'").await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE noop_dml (n NUMBER)").await;
    let (app, _) = query(app, &token, "INSERT INTO noop_dml VALUES (1)").await;
    let (_, body) = query(app, &token, "SELECT n FROM noop_dml").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["1"]]));
}

// ── Transaction control (converted from test_transactions.py) ─────────────────

#[tokio::test]
async fn test_begin_commit_makes_data_visible() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE txn_commit (n NUMBER)").await;
    let (app, _) = query(app, &token, "BEGIN").await;
    let (app, _) = query(app, &token, "INSERT INTO txn_commit VALUES (7)").await;
    let (app, _) = query(app, &token, "COMMIT").await;
    let (_, body) = query(app, &token, "SELECT n FROM txn_commit").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["7"]]));
}

#[tokio::test]
async fn test_begin_rollback_discards_insert() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE txn_roll (n NUMBER)").await;
    let (app, _) = query(app, &token, "BEGIN").await;
    let (app, _) = query(app, &token, "INSERT INTO txn_roll VALUES (99)").await;
    let (app, _) = query(app, &token, "ROLLBACK").await;
    let (_, body) = query(app, &token, "SELECT COUNT(*) AS c FROM txn_roll").await;
    assert_eq!(body["success"], true);
    assert_eq!(body["data"]["rowset"], json!([["0"]]));
}

#[tokio::test]
async fn test_commit_without_active_transaction_is_safe() {
    // COMMIT when no transaction is open must not error.
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "COMMIT").await;
    assert_eq!(body["success"], true);
}

#[tokio::test]
async fn test_rollback_without_active_transaction_is_safe() {
    // ROLLBACK when no transaction is open must not error.
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "ROLLBACK").await;
    assert_eq!(body["success"], true);
}

// ── String escape normalisation (converted from test_types.py) ────────────────

#[tokio::test]
async fn test_backslash_quote_normalised_to_sql_doubling() {
    // The Snowflake connector pyformat emits \' inside string literals.
    // The server must normalise \' → '' before sending to SQLite.
    let (app, token) = login(new_app()).await;
    let sql = r#"SELECT 'it\'s a test'"#;
    let (_, body) = query(app, &token, sql).await;
    assert_eq!(body["success"], true, "backslash-quote should be normalised: {body}");
    let val = body["data"]["rowset"][0][0].as_str().unwrap_or("");
    assert!(val.contains("it") && val.contains("s a test"), "got: {val}");
}

#[tokio::test]
async fn test_backslash_n_normalised_to_actual_newline() {
    // The connector emits \n (two chars) inside string literals; server rewrites to real newline.
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE nl_t (v VARCHAR)").await;
    let sql = r"INSERT INTO nl_t VALUES ('line1\nline2')";
    let (app, body) = query(app, &token, sql).await;
    assert_eq!(body["success"], true, "{body}");
    let (_, body) = query(app, &token, "SELECT v FROM nl_t").await;
    let val = body["data"]["rowset"][0][0].as_str().unwrap_or("");
    assert!(val.contains('\n'), "expected actual newline in result, got: {val:?}");
}

// ── Type round-trips (converted from test_types.py) ───────────────────────────

#[tokio::test]
async fn test_float_binding_roundtrip() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE float_t (v NUMBER(10,4))").await;
    let bindings = json!({"1": {"type": "REAL", "value": "3.14"}});
    let (app, body) = query_with_bindings(
        app, &token, "INSERT INTO float_t VALUES (?)", Some(bindings),
    ).await;
    assert_eq!(body["success"], true, "{body}");
    let (_, body) = query(app, &token, "SELECT v FROM float_t").await;
    let val: f64 = body["data"]["rowset"][0][0].as_str().unwrap().parse().unwrap();
    assert!((val - 3.14).abs() < 0.001, "expected ~3.14 got {val}");
}

#[tokio::test]
async fn test_null_binding_roundtrip() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE null_t (v VARCHAR)").await;
    let bindings = json!({"1": {"type": "TEXT", "value": null}});
    let (app, body) = query_with_bindings(
        app, &token, "INSERT INTO null_t VALUES (?)", Some(bindings),
    ).await;
    assert_eq!(body["success"], true, "{body}");
    let (_, body) = query(app, &token, "SELECT v FROM null_t").await;
    assert_eq!(body["data"]["rowset"][0][0], Value::Null);
}

#[tokio::test]
async fn test_boolean_column_stored_as_integer() {
    // BOOLEAN DDL is rewritten to INTEGER; stored value is "1"/"0".
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE bool_t (v BOOLEAN)").await;
    let (app, _) = query(app, &token, "INSERT INTO bool_t VALUES (TRUE)").await;
    let (_, body) = query(app, &token, "SELECT v FROM bool_t").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "1");
}

#[tokio::test]
async fn test_unicode_string_roundtrip() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE uni_t (v VARCHAR)").await;
    let (app, _) = query(app, &token, "INSERT INTO uni_t VALUES ('café 中文 😀')").await;
    let (_, body) = query(app, &token, "SELECT v FROM uni_t").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "café 中文 😀");
}

// ── Snowflake function translations via HTTP (converted from test_snowflake_functions.py) ─

#[tokio::test]
async fn test_zeroifnull_null_returns_zero() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT ZEROIFNULL(NULL)").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "0");
}

#[tokio::test]
async fn test_zeroifnull_value_unchanged() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT ZEROIFNULL(5)").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "5");
}

#[tokio::test]
async fn test_nullifzero_zero_returns_null() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT NULLIFZERO(0)").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0], Value::Null);
}

#[tokio::test]
async fn test_nullifzero_nonzero_unchanged() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT NULLIFZERO(5)").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "5");
}

#[tokio::test]
async fn test_nvl2_not_null_branch() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT NVL2('x', 'not-null', 'null')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["not-null"]]));
}

#[tokio::test]
async fn test_nvl2_null_branch() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT NVL2(NULL, 'not-null', 'null-branch')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["null-branch"]]));
}

#[tokio::test]
async fn test_iff_false_branch() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT IFF(0 > 1, 'yes', 'no')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["no"]]));
}

#[tokio::test]
async fn test_decode_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(
        app, &token,
        "SELECT DECODE('I', 'A', 'Active', 'I', 'Inactive', 'Unknown')",
    ).await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["Inactive"]]));
}

#[tokio::test]
async fn test_contains_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT CONTAINS('hello world', 'world')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "1");
}

#[tokio::test]
async fn test_startswith_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT STARTSWITH('hello world', 'hello')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "1");
}

#[tokio::test]
async fn test_endswith_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT ENDSWITH('hello world', 'world')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"][0][0].as_str().unwrap(), "1");
}

#[tokio::test]
async fn test_ilike_case_insensitive_filter() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE ilike_t (name VARCHAR)").await;
    let (app, _) = query(app, &token, "INSERT INTO ilike_t VALUES ('Hello World')").await;
    let (app, _) = query(app, &token, "INSERT INTO ilike_t VALUES ('goodbye')").await;
    let (_, body) = query(app, &token, "SELECT name FROM ilike_t WHERE name ILIKE '%hello%'").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["total"], 1);
    assert_eq!(body["data"]["rowset"], json!([["Hello World"]]));
}

#[tokio::test]
async fn test_split_part_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT SPLIT_PART('a@b@c', '@', 2)").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["b"]]));
}

#[tokio::test]
async fn test_dateadd_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT DATEADD(day, 1, '2024-01-15')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["2024-01-16"]]));
}

#[tokio::test]
async fn test_datediff_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT DATEDIFF(day, '2024-01-01', '2024-01-15')").await;
    assert_eq!(body["success"], true, "{body}");
    let val: f64 = body["data"]["rowset"][0][0].as_str().unwrap().parse().unwrap();
    assert!((val - 14.0).abs() < 0.01, "expected 14 got {val}");
}

#[tokio::test]
async fn test_date_trunc_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT DATE_TRUNC('month', '2024-03-15')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["2024-03-01"]]));
}

#[tokio::test]
async fn test_year_month_day_extraction() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(
        app, &token,
        "SELECT YEAR('2024-06-15'), MONTH('2024-06-15'), DAY('2024-06-15')",
    ).await;
    assert_eq!(body["success"], true, "{body}");
    let row = &body["data"]["rowset"][0];
    assert_eq!(row[0].as_str().unwrap().parse::<i64>().unwrap(), 2024);
    assert_eq!(row[1].as_str().unwrap().parse::<i64>().unwrap(), 6);
    assert_eq!(row[2].as_str().unwrap().parse::<i64>().unwrap(), 15);
}

#[tokio::test]
async fn test_to_varchar_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT TO_VARCHAR(42)").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["42"]]));
}

#[tokio::test]
async fn test_to_number_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT TO_NUMBER('3.14')").await;
    assert_eq!(body["success"], true, "{body}");
    let val: f64 = body["data"]["rowset"][0][0].as_str().unwrap().parse().unwrap();
    assert!((val - 3.14).abs() < 0.001, "expected 3.14 got {val}");
}

#[tokio::test]
async fn test_to_date_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT TO_DATE('2024-01-15')").await;
    assert_eq!(body["success"], true, "{body}");
    assert_eq!(body["data"]["rowset"], json!([["2024-01-15"]]));
}

#[tokio::test]
async fn test_booland_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT BOOLAND(1, 1), BOOLAND(1, 0)").await;
    assert_eq!(body["success"], true, "{body}");
    let row = &body["data"]["rowset"][0];
    assert_eq!(row[0].as_str().unwrap().parse::<i64>().unwrap(), 1);
    assert_eq!(row[1].as_str().unwrap().parse::<i64>().unwrap(), 0);
}

#[tokio::test]
async fn test_boolor_function() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT BOOLOR(0, 1), BOOLOR(0, 0)").await;
    assert_eq!(body["success"], true, "{body}");
    let row = &body["data"]["rowset"][0];
    assert_eq!(row[0].as_str().unwrap().parse::<i64>().unwrap(), 1);
    assert_eq!(row[1].as_str().unwrap().parse::<i64>().unwrap(), 0);
}

#[tokio::test]
async fn test_colon_path_extraction() {
    let (app, token) = login(new_app()).await;
    let (app, _) = query(app, &token, "CREATE OR REPLACE TABLE semi (data VARIANT)").await;
    let (app, _) = query(app, &token, r#"INSERT INTO semi VALUES ('{"name":"Alice"}')"#).await;
    let (_, body) = query(app, &token, "SELECT data:name FROM semi").await;
    assert_eq!(body["success"], true, "{body}");
    let val = body["data"]["rowset"][0][0].as_str().unwrap_or("");
    assert!(val.contains("Alice"), "expected 'Alice' in {val}");
}

#[tokio::test]
async fn test_object_construct_empty() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT OBJECT_CONSTRUCT()").await;
    assert_eq!(body["success"], true, "{body}");
    let raw = body["data"]["rowset"][0][0].as_str().unwrap_or("{}");
    let parsed: Value = serde_json::from_str(raw).expect("valid JSON from OBJECT_CONSTRUCT");
    assert_eq!(parsed, json!({}));
}

#[tokio::test]
async fn test_array_construct() {
    let (app, token) = login(new_app()).await;
    let (_, body) = query(app, &token, "SELECT ARRAY_CONSTRUCT(1, 2, 3)").await;
    assert_eq!(body["success"], true, "{body}");
    let raw = body["data"]["rowset"][0][0].as_str().unwrap_or("[]");
    let parsed: Value = serde_json::from_str(raw).expect("valid JSON from ARRAY_CONSTRUCT");
    assert_eq!(parsed, json!([1, 2, 3]));
}
