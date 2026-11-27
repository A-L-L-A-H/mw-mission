use crate::cron::CronExpression;
use crate::db::{Database, millis_to_utc, opt_millis_to_utc};
use crate::engine::MissionEngine;
use crate::error::MissionError;
use crate::models::{EnqueueJobRequest, JobRecord, JobStatus};
use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, Row, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use uuid::Uuid;

const DEFAULT_FLEET_QUEUE: &str = "fleet";
const DEFAULT_MAX_RETRIES: u32 = 3;

const FLEET_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS fleet_jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    target_service TEXT NOT NULL,
    description TEXT,
    cron_expr TEXT NOT NULL,
    queue TEXT NOT NULL DEFAULT 'fleet',
    capability_tags_json TEXT NOT NULL DEFAULT '[]',
    payload_json TEXT NOT NULL DEFAULT '{}',
    max_retries INTEGER NOT NULL DEFAULT 3,
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run_at_ms INTEGER NOT NULL,
    last_run_at_ms INTEGER,
    last_job_id TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fleet_jobs_due
    ON fleet_jobs(enabled, next_run_at_ms);

CREATE INDEX IF NOT EXISTS idx_fleet_jobs_service
    ON fleet_jobs(target_service, enabled);
"#;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegisterFleetJobRequest {
    pub name: String,
    pub target_service: String,
    pub description: Option<String>,
    pub cron_expr: String,
    pub queue: Option<String>,
    #[serde(default)]
    pub capability_tags: Vec<String>,
    #[serde(default = "default_json_object")]
    pub payload_json: Value,
    pub max_retries: Option<u32>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct UpdateFleetJobRequest {
    pub target_service: Option<String>,
    pub description: Option<String>,
    pub cron_expr: Option<String>,
    pub queue: Option<String>,
    pub capability_tags: Option<Vec<String>>,
    pub payload_json: Option<Value>,
    pub max_retries: Option<u32>,
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetJobRecord {
    pub id: String,
    pub name: String,
    pub target_service: String,
    pub description: Option<String>,
    pub cron_expr: String,
    pub queue: String,
    pub capability_tags: Vec<String>,
    pub payload_json: Value,
    pub max_retries: u32,
    pub enabled: bool,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub last_job_id: Option<String>,
    pub last_job_status: Option<JobStatus>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetTriggerOutcome {
    pub fleet_job: FleetJobRecord,
    pub dispatched_job: JobRecord,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetDispatchResult {
    pub dispatched_count: usize,
    pub dispatched_job_ids: Vec<String>,
    pub skipped_fleet_job_ids: Vec<String>,
}

#[derive(Clone)]
pub struct FleetJobManager {
    database: Database,
}

impl FleetJobManager {
    pub fn new(database: Database) -> Result<Self, MissionError> {
        database.with_write_tx(|tx| {
            tx.execute_batch(FLEET_SCHEMA_SQL)?;
            Ok(())
        })?;
        Ok(Self { database })
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn register_job(
        &self,
        req: RegisterFleetJobRequest,
    ) -> Result<FleetJobRecord, MissionError> {
        self.register_job_at(req, Utc::now().timestamp_millis())
    }

    pub fn register_job_at(
        &self,
        req: RegisterFleetJobRequest,
        now_ms: i64,
    ) -> Result<FleetJobRecord, MissionError> {
        let name = req.name.trim().to_string();
        if name.is_empty() {
            return Err(MissionError::Validation(
                "fleet job name must not be empty".to_string(),
            ));
        }

        let target_service = req.target_service.trim().to_string();
        if target_service.is_empty() {
            return Err(MissionError::Validation(
                "target_service must not be empty".to_string(),
            ));
        }

        let cron_expr = req.cron_expr.trim().to_string();
        let parsed_cron = CronExpression::parse(&cron_expr)?;
        let now_dt = millis_to_utc(now_ms)?;
        let next_run_at = parsed_cron.next_after(now_dt)?;

        let queue = normalize_fleet_queue(req.queue.as_deref())?;
        let capability_tags = normalize_tags(&req.capability_tags);
        let tags_serialized = serde_json::to_string(&capability_tags)?;
        let payload_serialized = serde_json::to_string(&req.payload_json)?;
        let max_retries = req.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let description = req
            .description
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);
        let id = Uuid::new_v4().to_string();

        self.database.with_write_tx(|tx| {
            let duplicate: Option<String> = tx
                .query_row(
                    "SELECT id FROM fleet_jobs WHERE name = ?1",
                    params![name],
                    |row| row.get(0),
                )
                .optional()?;
            if duplicate.is_some() {
                return Err(MissionError::Conflict(format!(
                    "fleet job already exists with name: {name}"
                )));
            }

            tx.execute(
                "INSERT INTO fleet_jobs (
                    id, name, target_service, description, cron_expr, queue,
                    capability_tags_json, payload_json, max_retries, enabled,
                    next_run_at_ms, last_run_at_ms, last_job_id,
                    created_at_ms, updated_at_ms
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9, ?10,
                    ?11, NULL, NULL,
                    ?12, ?12
                 )",
                params![
                    id,
                    name,
                    target_service,
                    description,
                    cron_expr,
                    queue,
                    tags_serialized,
                    payload_serialized,
                    max_retries,
                    i32::from(req.enabled),
                    next_run_at.timestamp_millis(),
                    now_ms,
                ],
            )?;

            load_fleet_job_by_key(tx, &id)
        })
    }

    pub fn list_jobs(
        &self,
        target_service: Option<&str>,
        enabled: Option<bool>,
    ) -> Result<Vec<FleetJobRecord>, MissionError> {
        let mut clauses: Vec<String> = Vec::new();
        let mut bind_values: Vec<rusqlite::types::Value> = Vec::new();

        if let Some(service) = target_service.map(str::trim).filter(|s| !s.is_empty()) {
            bind_values.push(rusqlite::types::Value::Text(service.to_string()));
            clauses.push(format!("fj.target_service = ?{}", bind_values.len()));
        }

        if let Some(flag) = enabled {
            bind_values.push(rusqlite::types::Value::Integer(i64::from(flag)));
            clauses.push(format!("fj.enabled = ?{}", bind_values.len()));
        }

        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };

        let sql = format!(
            "SELECT fj.id, fj.name, fj.target_service, fj.description, fj.cron_expr,
                    fj.queue, fj.capability_tags_json, fj.payload_json, fj.max_retries,
                    fj.enabled, fj.next_run_at_ms, fj.last_run_at_ms, fj.last_job_id,
                    j.status, fj.created_at_ms, fj.updated_at_ms
             FROM fleet_jobs fj
             LEFT JOIN jobs j ON j.id = fj.last_job_id
             {where_sql}
             ORDER BY fj.next_run_at_ms ASC, fj.name ASC"
        );

        self.database.with_read(|conn| {
            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(bind_values))?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next()? {
                jobs.push(map_fleet_job_row(row)?);
            }
            Ok(jobs)
        })
    }

    pub fn get_job(&self, id_or_name: &str) -> Result<FleetJobRecord, MissionError> {
        let key = id_or_name.trim();
        if key.is_empty() {
            return Err(MissionError::Validation(
                "fleet job identifier must not be empty".to_string(),
            ));
        }
        self.database
            .with_read(|conn| load_fleet_job_by_key(conn, key))
    }

    pub fn update_job(
        &self,
        id_or_name: &str,
        req: UpdateFleetJobRequest,
    ) -> Result<FleetJobRecord, MissionError> {
        self.update_job_at(id_or_name, req, Utc::now().timestamp_millis())
    }

    pub fn update_job_at(
        &self,
        id_or_name: &str,
        req: UpdateFleetJobRequest,
        now_ms: i64,
    ) -> Result<FleetJobRecord, MissionError> {
        let key = id_or_name.trim();
        if key.is_empty() {
            return Err(MissionError::Validation(
                "fleet job identifier must not be empty".to_string(),
            ));
        }

        let now_dt = millis_to_utc(now_ms)?;

        self.database.with_write_tx(|tx| {
            let existing = load_fleet_job_by_key(tx, key)?;

            let target_service = match req.target_service {
                Some(raw) => {
                    let trimmed = raw.trim().to_string();
                    if trimmed.is_empty() {
                        return Err(MissionError::Validation(
                            "target_service must not be empty".to_string(),
                        ));
                    }
                    trimmed
                }
                None => existing.target_service.clone(),
            };

            let description = match req.description {
                Some(raw) => {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_string())
                    }
                }
                None => existing.description.clone(),
            };

            let (cron_expr, cron_changed) = match req.cron_expr {
                Some(raw) => {
                    let trimmed = raw.trim().to_string();
                    CronExpression::parse(&trimmed)?;
                    let changed = trimmed != existing.cron_expr;
                    (trimmed, changed)
                }
                None => (existing.cron_expr.clone(), false),
            };

            let queue = match req.queue {
                Some(raw) => normalize_fleet_queue(Some(&raw))?,
                None => existing.queue.clone(),
            };

            let capability_tags = match req.capability_tags {
                Some(tags) => normalize_tags(&tags),
                None => existing.capability_tags.clone(),
            };
            let tags_serialized = serde_json::to_string(&capability_tags)?;

            let payload_json = req.payload_json.unwrap_or_else(|| existing.payload_json.clone());
            let payload_serialized = serde_json::to_string(&payload_json)?;
            let max_retries = req.max_retries.unwrap_or(existing.max_retries);
            let enabled = req.enabled.unwrap_or(existing.enabled);

            let next_run_at_ms = if cron_changed || (!existing.enabled && enabled) {
                let parsed = CronExpression::parse(&cron_expr)?;
                parsed.next_after(now_dt)?.timestamp_millis()
            } else {
                existing.next_run_at.timestamp_millis()
            };

            tx.execute(
                "UPDATE fleet_jobs
                 SET target_service = ?1,
                     description = ?2,
                     cron_expr = ?3,
                     queue = ?4,
                     capability_tags_json = ?5,
                     payload_json = ?6,
                     max_retries = ?7,
                     enabled = ?8,
                     next_run_at_ms = ?9,
                     updated_at_ms = ?10
                 WHERE id = ?11",
                params![
                    target_service,
                    description,
                    cron_expr,
                    queue,
                    tags_serialized,
                    payload_serialized,
                    max_retries,
                    i32::from(enabled),
                    next_run_at_ms,
                    now_ms,
                    existing.id,
                ],
            )?;

            load_fleet_job_by_key(tx, &existing.id)
        })
    }

    pub fn delete_job(&self, id_or_name: &str) -> Result<(), MissionError> {
        let key = id_or_name.trim();
        if key.is_empty() {
            return Err(MissionError::Validation(
                "fleet job identifier must not be empty".to_string(),
            ));
        }

        self.database.with_write_tx(|tx| {
            let deleted = tx.execute(
                "DELETE FROM fleet_jobs WHERE id = ?1 OR name = ?1",
                params![key],
            )?;
            if deleted == 0 {
                return Err(MissionError::CronNotFound(format!(
                    "fleet job not found: {key}"
                )));
            }
            Ok(())
        })
    }

    pub fn trigger_job(
        &self,
        engine: &MissionEngine,
        id_or_name: &str,
        reason: Option<&str>,
    ) -> Result<FleetTriggerOutcome, MissionError> {
        self.trigger_job_at(engine, id_or_name, reason, Utc::now().timestamp_millis())
    }

    pub fn trigger_job_at(
        &self,
        engine: &MissionEngine,
        id_or_name: &str,
        reason: Option<&str>,
        now_ms: i64,
    ) -> Result<FleetTriggerOutcome, MissionError> {
        let fleet_job = self.get_job(id_or_name)?;
        let merged_payload = merge_fleet_metadata(
            &fleet_job.payload_json,
            &fleet_job.id,
            &fleet_job.name,
            &fleet_job.target_service,
            "manual",
            reason,
            None,
        );

        let enqueue_outcome = engine.enqueue_job_at(
            EnqueueJobRequest {
                queue: Some(fleet_job.queue.clone()),
                capability_tags: fleet_job.capability_tags.clone(),
                payload_json: merged_payload,
                idempotency_key: None,
                max_retries: Some(fleet_job.max_retries),
                backoff_base_ms: None,
                priority: None,
                run_after: None,
                mission_id: None,
                node_key: Some(fleet_job.name.clone()),
                depends_on: Vec::new(),
            },
            now_ms,
        )?;

        let dispatched_job_id = enqueue_outcome.job.id.clone();
        let updated_fleet_job = self.database.with_write_tx(|tx| {
            tx.execute(
                "UPDATE fleet_jobs
                 SET last_run_at_ms = ?1,
                     last_job_id = ?2,
                     updated_at_ms = ?1
                 WHERE id = ?3",
                params![now_ms, dispatched_job_id, fleet_job.id],
            )?;
            load_fleet_job_by_key(tx, &fleet_job.id)
        })?;

        Ok(FleetTriggerOutcome {
            fleet_job: updated_fleet_job,
            dispatched_job: enqueue_outcome.job,
            deduplicated: enqueue_outcome.deduplicated,
        })
    }

    pub fn dispatch_due_jobs(
        &self,
        engine: &MissionEngine,
    ) -> Result<FleetDispatchResult, MissionError> {
        self.dispatch_due_jobs_at(engine, Utc::now().timestamp_millis())
    }

    pub fn dispatch_due_jobs_at(
        &self,
        engine: &MissionEngine,
        now_ms: i64,
    ) -> Result<FleetDispatchResult, MissionError> {
        let now_dt = millis_to_utc(now_ms)?;

        let due_jobs = self.database.with_read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT fj.id, fj.name, fj.target_service, fj.description, fj.cron_expr,
                        fj.queue, fj.capability_tags_json, fj.payload_json, fj.max_retries,
                        fj.enabled, fj.next_run_at_ms, fj.last_run_at_ms, fj.last_job_id,
                        j.status, fj.created_at_ms, fj.updated_at_ms
                 FROM fleet_jobs fj
                 LEFT JOIN jobs j ON j.id = fj.last_job_id
                 WHERE fj.enabled = 1 AND fj.next_run_at_ms <= ?1
                 ORDER BY fj.next_run_at_ms ASC, fj.id ASC",
            )?;
            let mut rows = stmt.query(params![now_ms])?;
            let mut collected = Vec::new();
            while let Some(row) = rows.next()? {
                collected.push(map_fleet_job_row(row)?);
            }
            Ok(collected)
        })?;

        let mut dispatched_job_ids = Vec::new();
        let mut skipped_fleet_job_ids = Vec::new();

        for fleet_job in due_jobs {
            let parsed_cron = CronExpression::parse(&fleet_job.cron_expr)?;
            let scheduled_slot_ms = fleet_job.next_run_at.timestamp_millis();
            let next_run_after = parsed_cron.next_after(now_dt)?;
            let idem_key = format!("fleet:{}:{scheduled_slot_ms}", fleet_job.id);

            let merged_payload = merge_fleet_metadata(
                &fleet_job.payload_json,
                &fleet_job.id,
                &fleet_job.name,
                &fleet_job.target_service,
                "cron",
                None,
                Some(scheduled_slot_ms),
            );

            let enqueue_outcome = engine.enqueue_job_at(
                EnqueueJobRequest {
                    queue: Some(fleet_job.queue.clone()),
                    capability_tags: fleet_job.capability_tags.clone(),
                    payload_json: merged_payload,
                    idempotency_key: Some(idem_key),
                    max_retries: Some(fleet_job.max_retries),
                    backoff_base_ms: None,
                    priority: None,
                    run_after: None,
                    mission_id: None,
                    node_key: Some(fleet_job.name.clone()),
                    depends_on: Vec::new(),
                },
                now_ms,
            )?;

            if enqueue_outcome.deduplicated {
                skipped_fleet_job_ids.push(fleet_job.id.clone());
            } else {
                dispatched_job_ids.push(enqueue_outcome.job.id.clone());
            }

            let job_id = enqueue_outcome.job.id;
            self.database.with_write_tx(|tx| {
                tx.execute(
                    "UPDATE fleet_jobs
                     SET last_run_at_ms = ?1,
                         last_job_id = ?2,
                         next_run_at_ms = ?3,
                         updated_at_ms = ?1
                     WHERE id = ?4",
                    params![
                        now_ms,
                        job_id,
                        next_run_after.timestamp_millis(),
                        fleet_job.id
                    ],
                )?;
                Ok(())
            })?;
        }

        Ok(FleetDispatchResult {
            dispatched_count: dispatched_job_ids.len(),
            dispatched_job_ids,
            skipped_fleet_job_ids,
        })
    }

    pub fn seed_default_presets(&self) -> Result<Vec<FleetJobRecord>, MissionError> {
        let mut created = Vec::new();
        for preset in default_fleet_presets() {
            match self.register_job(preset) {
                Ok(record) => created.push(record),
                Err(MissionError::Conflict(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(created)
    }
}

pub fn default_fleet_presets() -> Vec<RegisterFleetJobRequest> {
    vec![
        RegisterFleetJobRequest {
            name: "mw-stack-pg-backup".to_string(),
            target_service: "mw-stack".to_string(),
            description: Some("Nightly PostgreSQL & SQLite WAL fleet backup snapshot".to_string()),
            cron_expr: "0 2 * * *".to_string(),
            queue: Some(DEFAULT_FLEET_QUEUE.to_string()),
            capability_tags: vec!["ops".to_string(), "backup".to_string()],
            payload_json: json!({
                "action": "pg_backup",
                "retention_days": 14,
            }),
            max_retries: Some(3),
            enabled: true,
        },
        RegisterFleetJobRequest {
            name: "mw-lab-mirror-sync".to_string(),
            target_service: "mw-lab".to_string(),
            description: Some("Periodic local CI mirror synchronization".to_string()),
            cron_expr: "*/15 * * * *".to_string(),
            queue: Some(DEFAULT_FLEET_QUEUE.to_string()),
            capability_tags: vec!["ci".to_string(), "git".to_string()],
            payload_json: json!({
                "action": "mirror_sync",
            }),
            max_retries: Some(2),
            enabled: true,
        },
        RegisterFleetJobRequest {
            name: "mw-isp-voucher-prune".to_string(),
            target_service: "mw-isp".to_string(),
            description: Some("Prune expired WiFi hotspot vouchers and stale CoA sessions".to_string()),
            cron_expr: "0 3 * * *".to_string(),
            queue: Some(DEFAULT_FLEET_QUEUE.to_string()),
            capability_tags: vec!["isp".to_string(), "cleanup".to_string()],
            payload_json: json!({
                "action": "prune_expired_vouchers",
            }),
            max_retries: Some(3),
            enabled: true,
        },
        RegisterFleetJobRequest {
            name: "mw-cp-usage-rollup".to_string(),
            target_service: "mw-cp".to_string(),
            description: Some("Hourly PaaS tenant usage metering rollup".to_string()),
            cron_expr: "0 * * * *".to_string(),
            queue: Some(DEFAULT_FLEET_QUEUE.to_string()),
            capability_tags: vec!["paas".to_string(), "metering".to_string()],
            payload_json: json!({
                "action": "rollup_usage_meters",
            }),
            max_retries: Some(3),
            enabled: true,
        },
    ]
}

fn load_fleet_job_by_key(
    conn: &rusqlite::Connection,
    key: &str,
) -> Result<FleetJobRecord, MissionError> {
    conn.query_row(
        "SELECT fj.id, fj.name, fj.target_service, fj.description, fj.cron_expr,
                fj.queue, fj.capability_tags_json, fj.payload_json, fj.max_retries,
                fj.enabled, fj.next_run_at_ms, fj.last_run_at_ms, fj.last_job_id,
                j.status, fj.created_at_ms, fj.updated_at_ms
         FROM fleet_jobs fj
         LEFT JOIN jobs j ON j.id = fj.last_job_id
         WHERE fj.id = ?1 OR fj.name = ?1",
        params![key],
        map_fleet_job_row_boxed,
    )
    .optional()?
    .ok_or_else(|| MissionError::CronNotFound(format!("fleet job not found: {key}")))?
}

fn map_fleet_job_row_boxed(
    row: &Row<'_>,
) -> rusqlite::Result<Result<FleetJobRecord, MissionError>> {
    Ok(map_fleet_job_row(row))
}

fn map_fleet_job_row(row: &Row<'_>) -> Result<FleetJobRecord, MissionError> {
    let tags_raw: String = row.get(6)?;
    let payload_raw: String = row.get(7)?;
    let enabled_int: i32 = row.get(9)?;
    let last_status_raw: Option<String> = row.get(13)?;
    let last_job_status = match last_status_raw {
        Some(raw) => Some(JobStatus::parse(&raw).ok_or_else(|| {
            MissionError::Internal(format!("invalid job status in db: {raw}"))
        })?),
        None => None,
    };

    Ok(FleetJobRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        target_service: row.get(2)?,
        description: row.get(3)?,
        cron_expr: row.get(4)?,
        queue: row.get(5)?,
        capability_tags: serde_json::from_str(&tags_raw)?,
        payload_json: serde_json::from_str(&payload_raw)?,
        max_retries: row.get(8)?,
        enabled: enabled_int != 0,
        next_run_at: millis_to_utc(row.get(10)?)?,
        last_run_at: opt_millis_to_utc(row.get(11)?)?,
        last_job_id: row.get(12)?,
        last_job_status,
        created_at: millis_to_utc(row.get(14)?)?,
        updated_at: millis_to_utc(row.get(15)?)?,
    })
}

fn merge_fleet_metadata(
    payload: &Value,
    fleet_job_id: &str,
    fleet_job_name: &str,
    target_service: &str,
    trigger: &str,
    reason: Option<&str>,
    scheduled_slot_ms: Option<i64>,
) -> Value {
    let mut meta = serde_json::Map::new();
    meta.insert("id".to_string(), Value::String(fleet_job_id.to_string()));
    meta.insert("name".to_string(), Value::String(fleet_job_name.to_string()));
    meta.insert(
        "target_service".to_string(),
        Value::String(target_service.to_string()),
    );
    meta.insert("trigger".to_string(), Value::String(trigger.to_string()));
    if let Some(reason_str) = reason.map(str::trim).filter(|s| !s.is_empty()) {
        meta.insert("reason".to_string(), Value::String(reason_str.to_string()));
    }
    if let Some(slot_ms) = scheduled_slot_ms {
        meta.insert("scheduled_slot_ms".to_string(), json!(slot_ms));
    }

    match payload {
        Value::Object(obj) => {
            let mut cloned = obj.clone();
            cloned.insert("_fleet_job".to_string(), Value::Object(meta));
            Value::Object(cloned)
        }
        other => json!({
            "data": other,
            "_fleet_job": Value::Object(meta),
        }),
    }
}

fn normalize_fleet_queue(raw: Option<&str>) -> Result<String, MissionError> {
    let chosen = raw.unwrap_or(DEFAULT_FLEET_QUEUE).trim();
    if chosen.is_empty() {
        return Err(MissionError::Validation(
            "queue name must not be empty".to_string(),
        ));
    }
    Ok(chosen.to_string())
}

fn normalize_tags(tags: &[String]) -> Vec<String> {
    let unique: BTreeSet<String> = tags
        .iter()
        .map(|tag| tag.trim())
        .filter(|tag| !tag.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    unique.into_iter().collect()
}

fn default_json_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CompleteJobRequest, LeaseJobRequest};
    use chrono::TimeZone;
    use tempfile::TempDir;

    fn setup_manager_and_engine() -> (FleetJobManager, MissionEngine) {
        let db = Database::open_in_memory().expect("open in-memory db");
        let manager = FleetJobManager::new(db.clone()).expect("init fleet job manager");
        let engine = MissionEngine::new(db);
        (manager, engine)
    }

    #[test]
    fn test_fleet_job_validation_and_conflict() {
        let (manager, _) = setup_manager_and_engine();
        let now_ms = Utc
            .with_ymd_and_hms(2026, 11, 27, 0, 0, 0)
            .unwrap()
            .timestamp_millis();

        let err_empty_name = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "   ".to_string(),
                    target_service: "mw-stack".to_string(),
                    description: None,
                    cron_expr: "*/5 * * * *".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    max_retries: None,
                    enabled: true,
                },
                now_ms,
            )
            .expect_err("empty name should fail");
        assert!(matches!(err_empty_name, MissionError::Validation(_)));

        let err_empty_svc = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "valid-name".to_string(),
                    target_service: "".to_string(),
                    description: None,
                    cron_expr: "*/5 * * * *".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    max_retries: None,
                    enabled: true,
                },
                now_ms,
            )
            .expect_err("empty target_service should fail");
        assert!(matches!(err_empty_svc, MissionError::Validation(_)));

        let err_bad_cron = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "bad-cron".to_string(),
                    target_service: "mw-lab".to_string(),
                    description: None,
                    cron_expr: "* * *".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    max_retries: None,
                    enabled: true,
                },
                now_ms,
            )
            .expect_err("invalid cron should fail");
        assert!(matches!(err_bad_cron, MissionError::Validation(_)));

        let created = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "dup-job".to_string(),
                    target_service: "mw-lab".to_string(),
                    description: Some("first".to_string()),
                    cron_expr: "*/5 * * * *".to_string(),
                    queue: None,
                    capability_tags: vec!["ci".to_string(), "ci".to_string(), " git ".to_string()],
                    payload_json: json!({"ok": true}),
                    max_retries: Some(2),
                    enabled: true,
                },
                now_ms,
            )
            .expect("register valid job");
        assert_eq!(created.queue, "fleet");
        assert_eq!(created.capability_tags, vec!["ci", "git"]);

        let dup_err = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "dup-job".to_string(),
                    target_service: "mw-lab".to_string(),
                    description: None,
                    cron_expr: "*/5 * * * *".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    max_retries: None,
                    enabled: true,
                },
                now_ms,
            )
            .expect_err("duplicate name should conflict");
        assert!(matches!(dup_err, MissionError::Conflict(_)));
    }

    #[test]
    fn test_fleet_job_crud_and_filtering() {
        let (manager, _) = setup_manager_and_engine();
        let now_ms = Utc
            .with_ymd_and_hms(2026, 11, 27, 1, 2, 0)
            .unwrap()
            .timestamp_millis();

        let job_a = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "isp-prune".to_string(),
                    target_service: "mw-isp".to_string(),
                    description: Some("Prune expired vouchers".to_string()),
                    cron_expr: "0 3 * * *".to_string(),
                    queue: Some("isp-cron".to_string()),
                    capability_tags: vec!["isp".to_string()],
                    payload_json: json!({"dry_run": false}),
                    max_retries: Some(4),
                    enabled: true,
                },
                now_ms,
            )
            .expect("register job_a");

        let job_b = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "cp-rollup".to_string(),
                    target_service: "mw-cp".to_string(),
                    description: None,
                    cron_expr: "*/10 * * * *".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    max_retries: None,
                    enabled: false,
                },
                now_ms,
            )
            .expect("register job_b");

        let all = manager.list_jobs(None, None).expect("list all");
        assert_eq!(all.len(), 2);

        let isp_only = manager
            .list_jobs(Some("mw-isp"), Some(true))
            .expect("filter by service and enabled");
        assert_eq!(isp_only.len(), 1);
        assert_eq!(isp_only[0].id, job_a.id);

        let fetched_by_name = manager.get_job("cp-rollup").expect("get by name");
        assert_eq!(fetched_by_name.id, job_b.id);
        assert!(!fetched_by_name.enabled);

        let updated = manager
            .update_job_at(
                &job_b.id,
                UpdateFleetJobRequest {
                    enabled: Some(true),
                    cron_expr: Some("*/15 * * * *".to_string()),
                    payload_json: Some(json!({"window": "15m"})),
                    max_retries: Some(5),
                    ..Default::default()
                },
                now_ms + 60_000,
            )
            .expect("update job_b");
        assert!(updated.enabled);
        assert_eq!(updated.cron_expr, "*/15 * * * *");
        assert_eq!(updated.max_retries, 5);
        assert_eq!(updated.payload_json["window"], json!("15m"));
        assert_eq!(
            updated.next_run_at,
            Utc.with_ymd_and_hms(2026, 11, 27, 1, 15, 0).unwrap()
        );

        manager.delete_job("isp-prune").expect("delete by name");
        let missing = manager.get_job(&job_a.id).expect_err("deleted job gone");
        assert!(matches!(missing, MissionError::CronNotFound(_)));
    }

    #[test]
    fn test_fleet_job_dispatch_due_and_idempotency() {
        let (manager, engine) = setup_manager_and_engine();
        let t0 = Utc.with_ymd_and_hms(2026, 11, 27, 10, 2, 0).unwrap();

        let active = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "lab-sync".to_string(),
                    target_service: "mw-lab".to_string(),
                    description: None,
                    cron_expr: "*/5 * * * *".to_string(),
                    queue: Some("fleet".to_string()),
                    capability_tags: vec!["ci".to_string()],
                    payload_json: json!({"action": "sync"}),
                    max_retries: Some(2),
                    enabled: true,
                },
                t0.timestamp_millis(),
            )
            .expect("register active");

        assert_eq!(
            active.next_run_at,
            Utc.with_ymd_and_hms(2026, 11, 27, 10, 5, 0).unwrap()
        );

        let _disabled = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "disabled-sync".to_string(),
                    target_service: "mw-lab".to_string(),
                    description: None,
                    cron_expr: "*/5 * * * *".to_string(),
                    queue: Some("fleet".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({}),
                    max_retries: None,
                    enabled: false,
                },
                t0.timestamp_millis(),
            )
            .expect("register disabled");

        // Before 10:05:00 -> nothing dispatched
        let before = Utc.with_ymd_and_hms(2026, 11, 27, 10, 4, 59).unwrap();
        let early = manager
            .dispatch_due_jobs_at(&engine, before.timestamp_millis())
            .expect("early tick");
        assert_eq!(early.dispatched_count, 0);

        // At 10:05:00 -> 1 dispatched, disabled skipped
        let due_at = Utc.with_ymd_and_hms(2026, 11, 27, 10, 5, 0).unwrap();
        let due = manager
            .dispatch_due_jobs_at(&engine, due_at.timestamp_millis())
            .expect("due tick");
        assert_eq!(due.dispatched_count, 1);
        assert_eq!(due.dispatched_job_ids.len(), 1);

        let job = engine
            .get_job(&due.dispatched_job_ids[0])
            .expect("load dispatched job");
        assert_eq!(job.queue, "fleet");
        assert_eq!(job.payload_json["_fleet_job"]["name"], json!("lab-sync"));
        assert_eq!(job.payload_json["_fleet_job"]["trigger"], json!("cron"));

        // Tick again at 10:05:30 -> next_run_at already moved to 10:10:00, zero duplicates
        let after = Utc.with_ymd_and_hms(2026, 11, 27, 10, 5, 30).unwrap();
        let repeat = manager
            .dispatch_due_jobs_at(&engine, after.timestamp_millis())
            .expect("repeat tick");
        assert_eq!(repeat.dispatched_count, 0);

        let reloaded = manager.get_job(&active.id).expect("reload fleet job");
        assert_eq!(
            reloaded.next_run_at,
            Utc.with_ymd_and_hms(2026, 11, 27, 10, 10, 0).unwrap()
        );
        assert_eq!(reloaded.last_job_id.as_deref(), Some(job.id.as_str()));
        assert_eq!(reloaded.last_job_status, Some(JobStatus::Queued));
    }

    #[test]
    fn test_fleet_job_manual_trigger_and_status_reflection() {
        let (manager, engine) = setup_manager_and_engine();
        let now_ms = Utc
            .with_ymd_and_hms(2026, 11, 27, 12, 0, 0)
            .unwrap()
            .timestamp_millis();

        let registered = manager
            .register_job_at(
                RegisterFleetJobRequest {
                    name: "manual-backup".to_string(),
                    target_service: "mw-stack".to_string(),
                    description: None,
                    cron_expr: "0 2 * * *".to_string(),
                    queue: Some("ops".to_string()),
                    capability_tags: vec!["backup".to_string()],
                    payload_json: json!({"target": "postgres"}),
                    max_retries: Some(3),
                    enabled: true,
                },
                now_ms,
            )
            .expect("register");

        let triggered = manager
            .trigger_job_at(&engine, "manual-backup", Some("pre-deploy"), now_ms)
            .expect("manual trigger");
        assert!(!triggered.deduplicated);
        assert_eq!(
            triggered.dispatched_job.payload_json["_fleet_job"]["reason"],
            json!("pre-deploy")
        );
        assert_eq!(
            triggered.fleet_job.last_job_id.as_deref(),
            Some(triggered.dispatched_job.id.as_str())
        );
        assert_eq!(triggered.fleet_job.last_job_status, Some(JobStatus::Queued));

        let leased = engine
            .lease_next_job_at(
                LeaseJobRequest {
                    worker_id: "ops-worker".to_string(),
                    queues: vec!["ops".to_string()],
                    capability_tags: vec!["backup".to_string()],
                    lease_ttl_secs: 30,
                },
                now_ms + 1_000,
            )
            .expect("lease")
            .expect("job present");
        assert_eq!(leased.id, triggered.dispatched_job.id);

        engine
            .complete_job_at(
                &leased.id,
                CompleteJobRequest {
                    worker_id: "ops-worker".to_string(),
                    result_json: json!({"snapshot": "ok"}),
                }
                .worker_id
                .as_str(),
                json!({"snapshot": "ok"}),
                now_ms + 2_000,
            )
            .expect("complete");

        let after_complete = manager.get_job(&registered.id).expect("reload");
        assert_eq!(after_complete.last_job_status, Some(JobStatus::Completed));
    }

    #[test]
    fn test_fleet_presets_and_wal_persistence() {
        let temp_dir = TempDir::new().expect("tempdir");
        let db_path = temp_dir.path().join("fleet-wal.db");

        {
            let db = Database::open(&db_path).expect("open db");
            let manager = FleetJobManager::new(db).expect("create manager");
            let seeded = manager.seed_default_presets().expect("seed presets");
            assert_eq!(seeded.len(), default_fleet_presets().len());
            let second_seed = manager.seed_default_presets().expect("idempotent seed");
            assert!(second_seed.is_empty());
        }

        let reopened = Database::open(&db_path).expect("reopen db");
        let manager = FleetJobManager::new(reopened).expect("reinit manager");
        let all = manager.list_jobs(None, None).expect("list persisted");
        assert_eq!(all.len(), default_fleet_presets().len());
        assert!(all.iter().any(|j| j.name == "mw-stack-pg-backup"));
    }
}
