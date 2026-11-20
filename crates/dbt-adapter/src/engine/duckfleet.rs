//! DuckFleet execution engine.
//!
//! DuckFleet is a DuckDB-flavored adapter whose statements do NOT run against a
//! local ADBC driver; they are routed over HTTP to a remote DuckFleet gateway,
//! which pins each session to a DuckDB worker sharing one DuckLake. A model's
//! `meta.platform` (`cpu`/`memory`/`disk`) sizes a dedicated pod; a model that
//! names none runs on the operator's default persistent DuckDB.
//!
//! This module provides [`DuckFleetSidecarClient`], a blocking HTTP client that
//! implements [`SidecarClient`]. `AdapterType::DuckFleet` is wired (in
//! `dbt-compilation`) to a [`crate::engine::SidecarEngine`] holding this client,
//! so the engine's `execute`/introspection calls become gateway calls.
//!
//! ## Session-lifecycle design (approach "a": session-per-node registry)
//!
//! We considered two ways to keep per-model compute correct while keeping a
//! model's whole multi-statement materialization inside ONE gateway
//! session/transaction:
//!
//!   (a) Keep a registry inside the client keyed by `ctx.node_id()`, opening a
//!       gateway session lazily and sizing it from the node's `meta.platform`.
//!   (b) Fold `meta.platform` into the engine connection fingerprint so a
//!       compute change invalidates the thread-local cached connection (the way
//!       the old Python adapter closed-and-reopened on a model/compute change).
//!
//! We chose (a). The sidecar engine's `execute_with_options` ignores the passed
//! `&dyn Connection` and calls `client.execute(ctx, sql, fetch)` — so the engine
//! layer has no per-node connection object to hang a session off of; the client
//! is the only place that sees every `ctx`. Approach (b) would require the
//! sidecar engine to participate in `resolve_connection_config`/`fingerprint`,
//! which it does not. Keying the registry by `node_id` is also a natural fit for
//! how a materialization runs: dbt materializes one model per worker thread, so
//! every `execute(ctx)` for that model carries the same `node_id` and lands on
//! the same gateway session; `COMMIT`/`ROLLBACK` (routed through the
//! [`DuckFleetConnection`]) hit that same session; and when the thread's cached
//! connection is reused for the next model, [`DuckFleetConnection::update_node_id`]
//! closes the previous node's session (freeing its worker/isolated pod).
//!
//! The compute for a node is captured in [`SidecarClient::new_connection`], which
//! receives the minijinja `State` (and thus the model's `config.meta.platform`);
//! it is stashed by `node_id` and consumed when the session is first opened.
//!
//! NOTE (to validate against a live gateway): if dbt ever acquires the execution
//! connection for a node WITHOUT a populated `State` (e.g. the parallel
//! `AdapterConnectionFactory` path, which passes `state = None`), no compute is
//! captured and that node falls back to the persistent tier. The materialization
//! macro path does carry `State`, so sized models work; this fallback only ever
//! degrades toward persistent, never mis-sizes.

use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use arrow_array::{Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use minijinja::State;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use ureq::{Agent, Body, Error as UreqError, http};
use uuid::Uuid;

use adbc_core::PartitionedResult;
use adbc_core::options::{OptionStatement, OptionValue};
use dbt_adbc::{Connection, QueryCtx, Statement};
use dbt_adapter_engine::sidecar_client::{ColumnInfo, SidecarClient};
use dbt_common::error::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;

/// How long we retry a "placing"/transport refusal on open before giving up.
/// Isolated pods provision on demand, so an open may legitimately wait this long.
const OPEN_RETRY_SECS: u64 = 90;
/// Global per-request timeout for the shared agent. The upper bound is set by the
/// open path (a pod may take several minutes to become ready), matching the
/// Python client's 450s open ceiling with headroom.
const AGENT_TIMEOUT_SECS: u64 = 600;

/// A blocking HTTP client that routes dbt execution to a DuckFleet gateway.
pub struct DuckFleetSidecarClient {
    /// Base URL of the gateway, trailing slash trimmed (e.g. `http://duckfleet:8080`).
    endpoint: String,
    /// Bearer credential; never logged or surfaced by `dbt debug`.
    auth_header: String,
    agent: Agent,
    /// node_id -> open gateway `session_id`. `""` is the metadata/introspection
    /// session (always persistent; it never carries a compute size).
    sessions: Mutex<HashMap<String, String>>,
    /// node_id -> the `compute` JSON resolved from that node's `meta.platform`,
    /// captured in `new_connection` and consumed the first time the session opens.
    pending_compute: Mutex<HashMap<String, JsonValue>>,
    /// A weak handle to self, so `new_connection` can hand an owned `Arc` to the
    /// returned [`DuckFleetConnection`] (which needs to call back for
    /// commit/rollback/cancel/close).
    me: Weak<DuckFleetSidecarClient>,
}

impl std::fmt::Debug for DuckFleetSidecarClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omits the bearer token.
        f.debug_struct("DuckFleetSidecarClient")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl DuckFleetSidecarClient {
    /// Build the client from an `endpoint` + bearer `token`.
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Arc<Self> {
        let endpoint = endpoint.into().trim_end_matches('/').to_string();
        let auth_header = format!("Bearer {}", token.into());
        let agent: Agent = Agent::config_builder()
            .tls_config(dbt_adbc::install::build_tls_config())
            // We read status codes ourselves to recover the gateway's JSON error body.
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(AGENT_TIMEOUT_SECS)))
            .build()
            .into();
        Arc::new_cyclic(|me| Self {
            endpoint,
            auth_header,
            agent,
            sessions: Mutex::new(HashMap::new()),
            pending_compute: Mutex::new(HashMap::new()),
            me: me.clone(),
        })
    }

    fn url(&self, suffix: &str) -> String {
        format!("{}{}", self.endpoint, suffix)
    }

    /// Open a gateway session with the given `compute` (empty object = persistent)
    /// and return its `session_id`, retrying transient placement/transport
    /// refusals until [`OPEN_RETRY_SECS`] elapses.
    fn open_session(&self, connection_name: &str, compute: JsonValue) -> AdapterResult<String> {
        let body = json!({
            "compute": compute,
            "context": { "connection_name": connection_name, "dbt": { "node_id": connection_name } },
        });
        let started = Instant::now();
        loop {
            match self
                .agent
                .post(self.url("/v1/sessions"))
                .header("Authorization", &self.auth_header)
                .header("Content-Type", "application/json")
                .send_json(body.clone())
            {
                Ok(mut resp) => {
                    let status = resp.status().as_u16();
                    if (200..300).contains(&status) {
                        let parsed = resp.body_mut().read_json::<JsonValue>().map_err(|e| {
                            AdapterError::new(
                                AdapterErrorKind::Driver,
                                format!("DuckFleet: unreadable open response: {e}"),
                            )
                        })?;
                        let session_id = parsed
                            .get("session_id")
                            .and_then(JsonValue::as_str)
                            .ok_or_else(|| {
                                AdapterError::new(
                                    AdapterErrorKind::UnexpectedResult,
                                    "DuckFleet: open response had no session_id",
                                )
                            })?;
                        return Ok(session_id.to_string());
                    }
                    let failure = read_failure(status, resp);
                    if failure.waitable_on_open() && started.elapsed().as_secs() < OPEN_RETRY_SECS {
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    return Err(failure.into_adapter_error());
                }
                Err(err) => {
                    // Transport failure: unknown outcome. Keep waiting out a placing
                    // session (the pod may still be coming up) until the budget is spent.
                    if started.elapsed().as_secs() < OPEN_RETRY_SECS {
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                    return Err(transport_error(err));
                }
            }
        }
    }

    fn close_session_id(&self, session_id: &str) {
        // Best-effort: a close failure only means the gateway will reap it on idle.
        let _ = self
            .agent
            .delete(self.url(&format!("/v1/sessions/{session_id}")))
            .header("Authorization", &self.auth_header)
            .call();
    }

    /// Get the open session for `key`, opening one (sized from any pending
    /// compute, else persistent) if absent.
    fn session_for(&self, key: &str) -> AdapterResult<String> {
        if let Some(id) = self.sessions.lock().unwrap().get(key) {
            return Ok(id.clone());
        }
        let compute = self
            .pending_compute
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_else(|| json!({}));
        let name = if key.is_empty() { "dbt" } else { key };
        let session_id = self.open_session(name, compute)?;
        let mut sessions = self.sessions.lock().unwrap();
        // A concurrent opener for the same key is not expected (one model = one
        // thread), but if it happened, keep the first and close ours.
        if let Some(existing) = sessions.get(key) {
            let existing = existing.clone();
            drop(sessions);
            self.close_session_id(&session_id);
            return Ok(existing);
        }
        sessions.insert(key.to_string(), session_id.clone());
        Ok(session_id)
    }

    /// Run one statement on `session_id`. `fetch = false` still executes the
    /// statement (DDL/DML) but discards the result stream.
    fn run_query(
        &self,
        session_id: &str,
        sql: &str,
        fetch: bool,
    ) -> AdapterResult<Option<RecordBatch>> {
        let body = json!({
            "request_id": Uuid::new_v4().to_string(),
            "sql": sql,
            "parameters": [],
            "context": {},
        });
        let resp = self
            .agent
            .post(self.url(&format!("/v1/sessions/{session_id}/queries")))
            .header("Authorization", &self.auth_header)
            .header("Content-Type", "application/json")
            .send_json(body)
            .map_err(transport_error)?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(read_failure(status, resp).into_adapter_error());
        }
        if !fetch {
            // Drain and drop the body so the connection is reusable.
            let mut reader = resp.into_body().into_reader();
            let mut sink = Vec::new();
            let _ = reader.read_to_end(&mut sink);
            return Ok(None);
        }
        let reader = resp.into_body().into_reader();
        let stream = arrow_ipc::reader::StreamReader::try_new(reader, None).map_err(|e| {
            AdapterError::new(
                AdapterErrorKind::Arrow,
                format!("DuckFleet: bad Arrow stream: {e}"),
            )
        })?;
        let schema: SchemaRef = stream.schema();
        let mut batches = Vec::new();
        for batch in stream {
            batches.push(batch.map_err(|e| {
                AdapterError::new(
                    AdapterErrorKind::Arrow,
                    format!("DuckFleet: Arrow decode error: {e}"),
                )
            })?);
        }
        let combined = arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
            AdapterError::new(
                AdapterErrorKind::Arrow,
                format!("DuckFleet: could not concatenate result batches: {e}"),
            )
        })?;
        Ok(Some(combined))
    }

    /// Run a statement on the persistent metadata session (used by introspection).
    fn run_metadata(&self, sql: &str) -> AdapterResult<RecordBatch> {
        let session_id = self.session_for("")?;
        match self.run_query(&session_id, sql, true) {
            Ok(Some(batch)) => Ok(batch),
            Ok(None) => Err(AdapterError::new(
                AdapterErrorKind::UnexpectedResult,
                "DuckFleet: metadata query returned no result set",
            )),
            Err(err) => {
                // A dead session (gateway restart / idle expiry) should not stick.
                if err.sqlstate().starts_with("08") {
                    self.sessions.lock().unwrap().remove("");
                }
                Err(err)
            }
        }
    }

    /// Route `COMMIT`/`ROLLBACK` (or a cancel/close) for a node's session, if open.
    fn control(&self, node_id: &Option<String>, statement: &str) -> AdapterResult<()> {
        let key = node_id.clone().unwrap_or_default();
        let session_id = { self.sessions.lock().unwrap().get(&key).cloned() };
        if let Some(session_id) = session_id {
            self.run_query(&session_id, statement, false)?;
        }
        Ok(())
    }

    fn cancel_node(&self, node_id: &Option<String>) {
        let key = node_id.clone().unwrap_or_default();
        let session_id = { self.sessions.lock().unwrap().get(&key).cloned() };
        if let Some(session_id) = session_id {
            let _ = self
                .agent
                .post(self.url(&format!("/v1/sessions/{session_id}/cancel")))
                .header("Authorization", &self.auth_header)
                .send_empty();
        }
    }

    /// Close a node's gateway session (freeing its worker/isolated pod), if open.
    fn close_node(&self, node_id: &Option<String>) {
        let key = node_id.clone().unwrap_or_default();
        let session_id = { self.sessions.lock().unwrap().remove(&key) };
        if let Some(session_id) = session_id {
            self.close_session_id(&session_id);
        }
    }
}

impl SidecarClient for DuckFleetSidecarClient {
    fn execute(
        &self,
        ctx: &QueryCtx,
        sql: &str,
        fetch: bool,
    ) -> AdapterResult<Option<RecordBatch>> {
        let key = ctx.node_id().cloned().unwrap_or_default();
        let session_id = self.session_for(&key)?;
        match self.run_query(&session_id, sql, fetch) {
            Ok(result) => Ok(result),
            Err(err) => {
                if err.sqlstate().starts_with("08") {
                    // Session is gone; drop it so a retry reopens a fresh one.
                    self.sessions.lock().unwrap().remove(&key);
                }
                Err(err)
            }
        }
    }

    fn new_connection(
        &self,
        state: Option<&State>,
        node_id: Option<String>,
    ) -> AdapterResult<Box<dyn Connection>> {
        let key = node_id.clone().unwrap_or_default();
        let compute = compute_from_state(state);
        self.pending_compute.lock().unwrap().insert(key, compute);
        let client = self.me.upgrade().ok_or_else(|| {
            AdapterError::new(
                AdapterErrorKind::Internal,
                "DuckFleet client was dropped before a connection could be made",
            )
        })?;
        Ok(Box::new(DuckFleetConnection { client, node_id }))
    }

    fn note_compute(&self, node_id: Option<&str>, state: Option<&State>) {
        let key = node_id.unwrap_or_default().to_string();
        // Learn the size only until the node's session is open; once it is, the pod is
        // fixed for the node's lifetime, and a later statement whose state carries a
        // thinner config must not be read as a downgrade.
        if self.sessions.lock().unwrap().contains_key(&key) {
            return;
        }
        let compute = compute_from_state(state);
        self.pending_compute.lock().unwrap().insert(key, compute);
    }

    fn shutdown(&self) -> AdapterResult<()> {
        let sessions: Vec<String> = {
            let mut map = self.sessions.lock().unwrap();
            map.drain().map(|(_, v)| v).collect()
        };
        for session_id in sessions {
            self.close_session_id(&session_id);
        }
        Ok(())
    }

    fn get_relation_type(
        &self,
        _quoting: &ResolvedQuoting,
        database: &str,
        schema: &str,
        table: &str,
    ) -> AdapterResult<Option<RelationType>> {
        let sql = format!(
            "SELECT table_type FROM information_schema.tables \
             WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}' LIMIT 1",
            sql_lit(database),
            sql_lit(schema),
            sql_lit(table),
        );
        let batch = self.run_metadata(&sql)?;
        Ok(utf8_col(&batch, 0)
            .into_iter()
            .flatten()
            .next()
            .map(|t| relation_type_of(&t)))
    }

    fn get_columns(&self, relation_name: &str) -> AdapterResult<Vec<ColumnInfo>> {
        // `relation_name` is the already-quoted `db.schema.identifier`.
        let batch = self.run_metadata(&format!("DESCRIBE {relation_name}"))?;
        let names = utf8_col(&batch, 0);
        let types = utf8_col(&batch, 1);
        Ok(names
            .into_iter()
            .zip(types)
            .filter_map(|(name, data_type)| {
                name.map(|name| ColumnInfo {
                    name,
                    data_type: data_type.unwrap_or_default(),
                })
            })
            .collect())
    }

    fn list_relations(
        &self,
        _quoting: &ResolvedQuoting,
        database: &str,
        schema: &str,
    ) -> AdapterResult<Vec<(String, String, String, RelationType)>> {
        let sql = format!(
            "SELECT table_name, table_type FROM information_schema.tables \
             WHERE table_catalog = '{}' AND table_schema = '{}'",
            sql_lit(database),
            sql_lit(schema),
        );
        let batch = self.run_metadata(&sql)?;
        let names = utf8_col(&batch, 0);
        let types = utf8_col(&batch, 1);
        Ok(names
            .into_iter()
            .zip(types)
            .filter_map(|(name, table_type)| {
                name.map(|name| {
                    (
                        database.to_string(),
                        schema.to_string(),
                        name,
                        relation_type_of(table_type.as_deref().unwrap_or("")),
                    )
                })
            })
            .collect())
    }
}

/// The thin connection handle dbt caches thread-local. Execution and
/// introspection go through the engine → [`DuckFleetSidecarClient`]; this type
/// exists to carry transaction control (`commit`/`rollback`), cancellation, and
/// — crucially — to close a node's gateway session when the thread moves on to
/// the next node or the connection is dropped.
struct DuckFleetConnection {
    client: Arc<DuckFleetSidecarClient>,
    node_id: Option<String>,
}

impl std::fmt::Debug for DuckFleetConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckFleetConnection")
            .field("node_id", &self.node_id)
            .finish()
    }
}

impl Connection for DuckFleetConnection {
    fn new_statement(&mut self) -> adbc_core::error::Result<Box<dyn Statement>> {
        // A gateway-backed statement bound to this connection's node session, so
        // the `conn.new_statement()` paths dbt uses (notably `dbt show` and
        // ad-hoc queries) run on the same gateway session as the node's work.
        Ok(Box::new(DuckFleetStatement {
            client: Arc::clone(&self.client),
            key: self.node_id.clone().unwrap_or_default(),
            sql: None,
        }))
    }

    fn update_node_id(&mut self, node_id: Option<String>) {
        if node_id != self.node_id {
            // The thread's cached connection is being reused for a different node;
            // close the previous node's session so its worker/isolated pod is freed.
            self.client.close_node(&self.node_id);
            self.node_id = node_id;
        }
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        self.client.cancel_node(&self.node_id);
        Ok(())
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        self.client
            .control(&self.node_id, "COMMIT")
            .map_err(adapter_to_adbc)
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        self.client
            .control(&self.node_id, "ROLLBACK")
            .map_err(adapter_to_adbc)
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> adbc_core::error::Result<Schema> {
        // Resolve the relation's Arrow schema by asking the gateway for a
        // zero-row result set. Seed loading uses this to learn column types.
        let quote = |ident: &str| format!("\"{}\"", ident.replace('"', "\"\""));
        let mut parts: Vec<String> = Vec::new();
        if let Some(catalog) = catalog {
            parts.push(quote(catalog));
        }
        if let Some(db_schema) = db_schema {
            parts.push(quote(db_schema));
        }
        parts.push(quote(table_name));
        let sql = format!("SELECT * FROM {} LIMIT 0", parts.join("."));
        let batch = self.client.run_metadata(&sql).map_err(adapter_to_adbc)?;
        Ok(batch.schema().as_ref().clone())
    }

    fn get_objects<'a>(
        &'a self,
        _depth: adbc_core::options::ObjectDepth,
        _catalog: Option<&'a str>,
        _db_schema: Option<&'a str>,
        _table_name: Option<&'a str>,
        _table_type: Option<Vec<&'a str>>,
        _column_name: Option<&'a str>,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'a>> {
        Err(adbc_core::error::Error::with_message_and_status(
            "DuckFleet: use the sidecar client's introspection, not get_objects",
            adbc_core::error::Status::NotImplemented,
        ))
    }
}

fn not_supported(what: &str) -> adbc_core::error::Error {
    adbc_core::error::Error::with_message_and_status(
        format!("DuckFleet: {what} on ADBC statements is not supported"),
        adbc_core::error::Status::NotImplemented,
    )
}

/// A gateway-backed ADBC statement. dbt's DuckDB-family `conn.new_statement()`
/// paths set SQL on it and execute; we route that to the node's gateway session
/// via the shared client, so it shares the session (and transaction) with the
/// node's materialization. Parameter binding is not wired to the gateway (the
/// paths that reach here do not bind; seeds load via the adapter's bound-INSERT
/// path, not this).
struct DuckFleetStatement {
    client: Arc<DuckFleetSidecarClient>,
    key: String,
    sql: Option<String>,
}

impl DuckFleetStatement {
    fn run(&self, fetch: bool) -> adbc_core::error::Result<Option<RecordBatch>> {
        let sql = self.sql.clone().ok_or_else(|| {
            adbc_core::error::Error::with_message_and_status(
                "DuckFleet: no SQL set on statement",
                adbc_core::error::Status::InvalidState,
            )
        })?;
        let session_id = self.client.session_for(&self.key).map_err(adapter_to_adbc)?;
        self.client
            .run_query(&session_id, &sql, fetch)
            .map_err(adapter_to_adbc)
    }
}

impl Statement for DuckFleetStatement {
    fn set_sql_query(&mut self, sql: &str) -> adbc_core::error::Result<()> {
        self.sql = Some(sql.to_string());
        Ok(())
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        Ok(())
    }

    fn execute<'a>(
        &'a mut self,
    ) -> adbc_core::error::Result<Box<dyn RecordBatchReader + Send + 'a>> {
        let (schema, batches): (SchemaRef, Vec<Result<RecordBatch, ArrowError>>) =
            match self.run(true)? {
                Some(batch) => (batch.schema(), vec![Ok(batch)]),
                None => (Arc::new(Schema::empty()), Vec::new()),
            };
        Ok(Box::new(RecordBatchIterator::new(batches, schema)))
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        self.run(false)?;
        // The gateway does not report an affected-row count.
        Ok(None)
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<Schema> {
        Ok(self
            .run(true)?
            .map(|batch| batch.schema().as_ref().clone())
            .unwrap_or_else(Schema::empty))
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<PartitionedResult> {
        Err(not_supported("execute_partitions"))
    }

    fn bind(&mut self, _batch: RecordBatch) -> adbc_core::error::Result<()> {
        Err(not_supported("parameter binding"))
    }

    fn bind_stream(
        &mut self,
        _reader: Box<dyn RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        Err(not_supported("parameter binding"))
    }

    fn set_substrait_plan(&mut self, _plan: &[u8]) -> adbc_core::error::Result<()> {
        Err(not_supported("Substrait plans"))
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<Schema> {
        Ok(Schema::empty())
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        self.client.cancel_node(&Some(self.key.clone()));
        Ok(())
    }

    // dbt sets context options (node id, phase, metadata, fetch, telemetry) on the
    // statement and occasionally reads options back. The gateway path needs none of
    // them (the session is keyed by node already), so these are benign rather than
    // the trait defaults, which `unimplemented!()`-panic.
    fn set_option(
        &mut self,
        _key: OptionStatement,
        _value: OptionValue,
    ) -> adbc_core::error::Result<()> {
        Ok(())
    }

    fn get_option_string(&self, _key: OptionStatement) -> adbc_core::error::Result<String> {
        Ok(String::new())
    }

    fn get_option_int(&self, _key: OptionStatement) -> adbc_core::error::Result<i64> {
        Ok(0)
    }

    fn get_option_bytes(&self, _key: OptionStatement) -> adbc_core::error::Result<Vec<u8>> {
        Ok(Vec::new())
    }

    fn get_option_double(&self, _key: OptionStatement) -> adbc_core::error::Result<f64> {
        Ok(0.0)
    }
}

impl Drop for DuckFleetConnection {
    fn drop(&mut self) {
        // Closing the connection closes its gateway session, which rolls back any
        // uncommitted work — matching the gateway's own close semantics.
        self.client.close_node(&self.node_id);
    }
}

/// Reads a model's `config.meta.platform` (`cpu`/`memory`/`disk`) out of the
/// Jinja `State` and builds the gateway `compute` object. Any of the three keys
/// present → an isolated pod of that size; none present → `{}` → the persistent
/// tier. This mirrors the server's own presence rule exactly; non-resource keys
/// under `platform` (e.g. `tags`) are dropped, as the Python adapter did.
fn compute_from_state(state: Option<&State>) -> JsonValue {
    let Some(state) = state else {
        return json!({});
    };
    let platform = state
        .lookup("model", &[])
        .and_then(|m| m.get_attr("config").ok())
        .and_then(|c| c.get_attr("meta").ok())
        .and_then(|meta| meta.get_attr("platform").ok());
    let Some(platform) = platform else {
        return json!({});
    };
    if platform.is_undefined() || platform.is_none() {
        return json!({});
    }
    let Ok(platform_json) = serde_json::to_value(&platform) else {
        return json!({});
    };
    let mut compute = JsonMap::new();
    for key in ["cpu", "memory", "disk"] {
        if let Some(value) = platform_json.get(key)
            && !value.is_null()
        {
            compute.insert(key.to_string(), value.clone());
        }
    }
    JsonValue::Object(compute)
}

/// A single `VARCHAR` column read out as `Option<String>`s. Returns empty if the
/// column is not a UTF-8 array (information_schema/DESCRIBE always yield VARCHAR).
fn utf8_col(batch: &RecordBatch, idx: usize) -> Vec<Option<String>> {
    if idx >= batch.num_columns() {
        return Vec::new();
    }
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .map(|array| {
            (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some(array.value(i).to_string())
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn relation_type_of(table_type: &str) -> RelationType {
    if table_type.to_ascii_uppercase().contains("VIEW") {
        RelationType::View
    } else {
        RelationType::Table
    }
}

/// Escape a value for embedding in a single-quoted SQL literal.
fn sql_lit(value: &str) -> String {
    value.replace('\'', "''")
}

/// The DuckFleet gateway's JSON error envelope: `{error, failure_kind, retry_safe}`.
struct GatewayFailure {
    status: u16,
    kind: Option<String>,
    message: String,
}

impl GatewayFailure {
    /// Whether an open should keep waiting rather than fail: the gateway is
    /// draining/starting or a proxy returned 502/503/504.
    fn waitable_on_open(&self) -> bool {
        matches!(self.kind.as_deref(), Some("draining") | Some("starting"))
            || matches!(self.status, 502..=504)
    }

    fn into_adapter_error(self) -> AdapterError {
        let (kind, sqlstate) = match self.kind.as_deref() {
            Some("sql_error") | Some("transaction_conflict") | Some("duplicate_request") => {
                (AdapterErrorKind::SqlExecution, None)
            }
            Some("timeout") => (AdapterErrorKind::SqlExecution, None),
            Some("canceled") | Some("interrupted") => (AdapterErrorKind::Cancelled, None),
            Some("unauthorized") | Some("forbidden") => (AdapterErrorKind::Authentication, None),
            Some("not_found") | Some("unknown_session") => {
                // A gone session is a dead connection (SQLSTATE class 08) so the
                // engine's thread-local cache discards it.
                (AdapterErrorKind::Driver, Some(*b"08003"))
            }
            Some("compute_unavailable") | Some("budget_exhausted") | Some("budget_job_cost")
            | Some("budget_exceeded") | Some("invalid_request") => {
                (AdapterErrorKind::Configuration, None)
            }
            Some("capacity") | Some("draining") | Some("starting") | Some("unavailable")
            | Some("not_placeable") => (AdapterErrorKind::Driver, Some(*b"08006")),
            _ => (AdapterErrorKind::Driver, None),
        };
        let message = format!(
            "DuckFleet gateway: {} (HTTP {}{})",
            self.message,
            self.status,
            self.kind
                .as_deref()
                .map(|k| format!(", {k}"))
                .unwrap_or_default(),
        );
        match sqlstate {
            Some(state) => {
                AdapterError::new_with_sqlstate_and_vendor_code(kind, message, state, None)
            }
            None => AdapterError::new(kind, message),
        }
    }
}

/// Read the gateway's JSON error envelope from a non-2xx response.
fn read_failure(status: u16, resp: http::Response<Body>) -> GatewayFailure {
    let mut reader = resp.into_body().into_reader();
    let mut buffer = Vec::new();
    let _ = reader.read_to_end(&mut buffer);
    let body = String::from_utf8_lossy(&buffer).to_string();
    match serde_json::from_str::<JsonValue>(&body) {
        Ok(json) => GatewayFailure {
            status,
            kind: json
                .get("failure_kind")
                .and_then(JsonValue::as_str)
                .map(str::to_string),
            message: json
                .get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or(&body)
                .to_string(),
        },
        Err(_) => GatewayFailure {
            status,
            kind: None,
            message: if body.is_empty() {
                format!("HTTP {status}")
            } else {
                body
            },
        },
    }
}

/// A transport failure (no HTTP response): the outcome is unknown, so the engine
/// must treat the connection as dead (SQLSTATE class 08) and never silently replay.
fn transport_error(err: UreqError) -> AdapterError {
    AdapterError::new_with_sqlstate_and_vendor_code(
        AdapterErrorKind::Driver,
        format!("DuckFleet gateway transport failure: {err}"),
        *b"08000",
        None,
    )
}

fn adapter_to_adbc(err: AdapterError) -> adbc_core::error::Error {
    adbc_core::error::Error::with_message_and_status(
        err.to_string(),
        adbc_core::error::Status::Internal,
    )
}
