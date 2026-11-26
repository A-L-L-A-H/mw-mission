use crate::error::MissionError;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use std::path::Path;
use std::sync::{Arc, Mutex};

const SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS missions (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    mission_id TEXT REFERENCES missions(id) ON DELETE CASCADE,
    node_key TEXT,
    queue TEXT NOT NULL DEFAULT 'default',
    status TEXT NOT NULL CHECK(status IN ('waiting_deps', 'queued', 'leased', 'completed', 'failed', 'dlq')),
    priority INTEGER NOT NULL DEFAULT 0,
    capability_tags_json TEXT NOT NULL DEFAULT '[]',
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    last_error TEXT,
    idempotency_key TEXT UNIQUE,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    backoff_base_ms INTEGER NOT NULL DEFAULT 1000,
    run_after_ms INTEGER NOT NULL,
    leased_by TEXT,
    lease_expires_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    completed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS job_dependencies (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    depends_on_job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (job_id, depends_on_job_id),
    CHECK (job_id <> depends_on_job_id)
);

CREATE TABLE IF NOT EXISTS cron_schedules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    cron_expr TEXT NOT NULL,
    queue TEXT NOT NULL DEFAULT 'default',
    capability_tags_json TEXT NOT NULL DEFAULT '[]',
    payload_json TEXT NOT NULL DEFAULT '{}',
    max_retries INTEGER NOT NULL DEFAULT 3,
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run_at_ms INTEGER NOT NULL,
    last_run_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dlq_entries (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id) ON DELETE CASCADE,
    queue TEXT NOT NULL,
    mission_id TEXT,
    error_message TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    failed_at_ms INTEGER NOT NULL,
    replayed_at_ms INTEGER,
    replay_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS outbox_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    published INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL,
    published_at_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_lease_scan
    ON jobs(status, queue, run_after_ms, priority DESC, created_at_ms ASC);

CREATE INDEX IF NOT EXISTS idx_jobs_lease_expiry
    ON jobs(status, lease_expires_at_ms);

CREATE INDEX IF NOT EXISTS idx_jobs_mission
    ON jobs(mission_id, status);

CREATE INDEX IF NOT EXISTS idx_job_deps_parent
    ON job_dependencies(depends_on_job_id, job_id);

CREATE INDEX IF NOT EXISTS idx_cron_due
    ON cron_schedules(enabled, next_run_at_ms);

CREATE INDEX IF NOT EXISTS idx_dlq_active
    ON dlq_entries(replayed_at_ms, failed_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON outbox_events(published, seq ASC);
"#;

#[derive(Clone)]
pub struct Database {
    connection: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, MissionError> {
        let path_ref = path.as_ref();
        if let Some(parent_dir) = path_ref.parent()
            && !parent_dir.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent_dir).map_err(|io_err| {
                MissionError::Internal(format!(
                    "failed to create database directory {}: {io_err}",
                    parent_dir.display()
                ))
            })?;
        }

        let conn = Connection::open_with_flags(
            path_ref,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )?;
        Self::initialize(conn)
    }

    pub fn open_in_memory() -> Result<Self, MissionError> {
        let conn = Connection::open_in_memory()?;
        Self::initialize(conn)
    }

    fn initialize(conn: Connection) -> Result<Self, MissionError> {
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;
             PRAGMA busy_timeout = 5000;
             PRAGMA temp_store = MEMORY;",
        )?;
        conn.execute_batch(SCHEMA_SQL)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn with_write_tx<F, T>(&self, operation: F) -> Result<T, MissionError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<T, MissionError>,
    {
        let mut conn = self
            .connection
            .lock()
            .map_err(|_| MissionError::Internal("database mutex poisoned".to_string()))?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let output = operation(&tx)?;
        tx.commit()?;
        Ok(output)
    }

    pub fn with_read<F, T>(&self, operation: F) -> Result<T, MissionError>
    where
        F: FnOnce(&Connection) -> Result<T, MissionError>,
    {
        let conn = self
            .connection
            .lock()
            .map_err(|_| MissionError::Internal("database mutex poisoned".to_string()))?;
        operation(&conn)
    }
}

pub fn millis_to_utc(ms: i64) -> Result<DateTime<Utc>, MissionError> {
    DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| MissionError::Internal(format!("timestamp out of range: {ms}")))
}

pub fn opt_millis_to_utc(opt_ms: Option<i64>) -> Result<Option<DateTime<Utc>>, MissionError> {
    match opt_ms {
        Some(ms) => Ok(Some(millis_to_utc(ms)?)),
        None => Ok(None),
    }
}
