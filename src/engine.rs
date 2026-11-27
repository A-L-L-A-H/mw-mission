use crate::cron::CronExpression;
use crate::db::{Database, millis_to_utc, opt_millis_to_utc};
use crate::error::MissionError;
use crate::models::{
    BrokerStats, CompleteJobOutcome, CreateCronScheduleRequest, CreateDagMissionRequest,
    CronDispatchResult, CronScheduleRecord, DlqEntryRecord, EnqueueJobOutcome, EnqueueJobRequest,
    FailJobOutcome, JobRecord, JobStatus, LeaseJobRequest, ListJobsFilter, MissionDetail,
    MissionRecord, MissionStatus, MissionSummary, OutboxEventRecord, QueueDepthStats, ReapResult,
};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params, params_from_iter};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use uuid::Uuid;

const DEFAULT_QUEUE: &str = "default";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_BACKOFF_BASE_MS: i64 = 1_000;
const MAX_BACKOFF_MS: i64 = 3_600_000;
const MAX_LEASE_TTL_SECS: u64 = 86_400;
const DEFAULT_LIST_LIMIT: usize = 100;
const MAX_LIST_LIMIT: usize = 1_000;

#[derive(Clone)]
pub struct MissionEngine {
    database: Database,
}

impl MissionEngine {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn enqueue_job(
        &self,
        request: EnqueueJobRequest,
    ) -> Result<EnqueueJobOutcome, MissionError> {
        self.enqueue_job_at(request, Utc::now().timestamp_millis())
    }

    pub fn enqueue_job_at(
        &self,
        request: EnqueueJobRequest,
        now_ms: i64,
    ) -> Result<EnqueueJobOutcome, MissionError> {
        let queue = normalize_queue(request.queue.as_deref())?;
        let capability_tags = normalize_tags(&request.capability_tags);
        let max_retries = request.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let backoff_base_ms = request.backoff_base_ms.unwrap_or(DEFAULT_BACKOFF_BASE_MS);
        if backoff_base_ms < 0 {
            return Err(MissionError::Validation(
                "backoff_base_ms must be non-negative".to_string(),
            ));
        }

        let idempotency_key = normalize_optional_nonempty(request.idempotency_key)?;
        let priority = request.priority.unwrap_or(0);
        let run_after_ms = request
            .run_after
            .map(|ts| ts.timestamp_millis())
            .unwrap_or(now_ms);
        let payload_serialized = serde_json::to_string(&request.payload_json)?;
        let tags_serialized = serde_json::to_string(&capability_tags)?;
        let unique_deps = dedup_preserve_order(&request.depends_on);

        self.database.with_write_tx(|tx| {
            if let Some(ref idem_key) = idempotency_key
                && let Some(existing_job) = load_job_by_idempotency_key(tx, idem_key)?
            {
                return Ok(EnqueueJobOutcome {
                    job: existing_job,
                    deduplicated: true,
                });
            }

            if let Some(ref mission_id) = request.mission_id {
                let mission_exists: Option<String> = tx
                    .query_row(
                        "SELECT id FROM missions WHERE id = ?1",
                        params![mission_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if mission_exists.is_none() {
                    return Err(MissionError::MissionNotFound(mission_id.clone()));
                }
            }

            let mut unsatisfied_deps = 0usize;
            for dep_id in &unique_deps {
                let dep_status: Option<String> = tx
                    .query_row(
                        "SELECT status FROM jobs WHERE id = ?1",
                        params![dep_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                match dep_status {
                    None => return Err(MissionError::DependencyNotFound(dep_id.clone())),
                    Some(status_str) if status_str != JobStatus::Completed.as_str() => {
                        unsatisfied_deps += 1;
                    }
                    Some(_) => {}
                }
            }

            let initial_status = if unsatisfied_deps > 0 {
                JobStatus::WaitingDeps
            } else {
                JobStatus::Queued
            };

            let job_id = Uuid::new_v4().to_string();
            tx.execute(
                "INSERT INTO jobs (
                    id, mission_id, node_key, queue, status, priority,
                    capability_tags_json, payload_json, result_json, last_error,
                    idempotency_key, attempts, max_retries, backoff_base_ms,
                    run_after_ms, leased_by, lease_expires_at_ms,
                    created_at_ms, updated_at_ms, completed_at_ms
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, NULL, NULL,
                    ?9, 0, ?10, ?11,
                    ?12, NULL, NULL,
                    ?13, ?13, NULL
                 )",
                params![
                    job_id,
                    request.mission_id,
                    request.node_key,
                    queue,
                    initial_status.as_str(),
                    priority,
                    tags_serialized,
                    payload_serialized,
                    idempotency_key,
                    max_retries,
                    backoff_base_ms,
                    run_after_ms,
                    now_ms,
                ],
            )?;

            for dep_id in &unique_deps {
                tx.execute(
                    "INSERT INTO job_dependencies (job_id, depends_on_job_id, created_at_ms)
                     VALUES (?1, ?2, ?3)",
                    params![job_id, dep_id, now_ms],
                )?;
            }

            append_outbox_event(
                tx,
                "job",
                &job_id,
                "job.enqueued",
                &json!({
                    "job_id": job_id,
                    "queue": queue,
                    "status": initial_status.as_str(),
                    "mission_id": request.mission_id,
                    "depends_on_count": unique_deps.len(),
                }),
                now_ms,
            )?;

            let inserted_job = load_job_by_id(tx, &job_id)?;
            Ok(EnqueueJobOutcome {
                job: inserted_job,
                deduplicated: false,
            })
        })
    }

    pub fn create_dag_mission(
        &self,
        request: CreateDagMissionRequest,
    ) -> Result<MissionDetail, MissionError> {
        self.create_dag_mission_at(request, Utc::now().timestamp_millis())
    }

    pub fn create_dag_mission_at(
        &self,
        request: CreateDagMissionRequest,
        now_ms: i64,
    ) -> Result<MissionDetail, MissionError> {
        let mission_name = request.name.trim().to_string();
        if mission_name.is_empty() {
            return Err(MissionError::Validation(
                "mission name must not be empty".to_string(),
            ));
        }
        if request.jobs.is_empty() {
            return Err(MissionError::Validation(
                "mission must contain at least one job".to_string(),
            ));
        }

        let mut key_to_job_id: HashMap<String, String> = HashMap::with_capacity(request.jobs.len());
        let mut seen_idem_keys: HashSet<String> = HashSet::new();

        for spec in &request.jobs {
            let trimmed_key = spec.key.trim();
            if trimmed_key.is_empty() {
                return Err(MissionError::Validation(
                    "DAG job key must not be empty".to_string(),
                ));
            }
            if key_to_job_id.contains_key(trimmed_key) {
                return Err(MissionError::Validation(format!(
                    "duplicate DAG job key: {trimmed_key}"
                )));
            }
            if let Some(backoff) = spec.backoff_base_ms
                && backoff < 0
            {
                return Err(MissionError::Validation(format!(
                    "job '{trimmed_key}' has negative backoff_base_ms"
                )));
            }
            let _ = normalize_queue(spec.queue.as_deref())?;
            if let Some(ref raw_idem) = spec.idempotency_key {
                let trimmed_idem = raw_idem.trim();
                if !trimmed_idem.is_empty() && !seen_idem_keys.insert(trimmed_idem.to_string()) {
                    return Err(MissionError::Validation(format!(
                        "duplicate idempotency_key within mission: {trimmed_idem}"
                    )));
                }
            }
            key_to_job_id.insert(trimmed_key.to_string(), Uuid::new_v4().to_string());
        }

        detect_dag_cycles(&request.jobs)?;

        let mission_id = Uuid::new_v4().to_string();
        let metadata_serialized = serde_json::to_string(&request.metadata_json)?;

        self.database.with_write_tx(|tx| {
            tx.execute(
                "INSERT INTO missions (id, name, status, metadata_json, created_at_ms, updated_at_ms, completed_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5, NULL)",
                params![
                    mission_id,
                    mission_name,
                    MissionStatus::Pending.as_str(),
                    metadata_serialized,
                    now_ms,
                ],
            )?;

            for spec in &request.jobs {
                let node_key = spec.key.trim();
                let job_id = key_to_job_id
                    .get(node_key)
                    .ok_or_else(|| MissionError::Internal("missing node key mapping".to_string()))?;
                let queue = normalize_queue(spec.queue.as_deref())?;
                let capability_tags = normalize_tags(&spec.capability_tags);
                let tags_serialized = serde_json::to_string(&capability_tags)?;
                let payload_serialized = serde_json::to_string(&spec.payload_json)?;
                let idempotency_key = normalize_optional_nonempty(spec.idempotency_key.clone())?;
                if let Some(ref idem_key) = idempotency_key {
                    let exists: Option<String> = tx
                        .query_row(
                            "SELECT id FROM jobs WHERE idempotency_key = ?1",
                            params![idem_key],
                            |row| row.get(0),
                        )
                        .optional()?;
                    if exists.is_some() {
                        return Err(MissionError::Conflict(format!(
                            "idempotency_key already exists: {idem_key}"
                        )));
                    }
                }

                let deps = dedup_preserve_order(&spec.depends_on);
                let initial_status = if deps.is_empty() {
                    JobStatus::Queued
                } else {
                    JobStatus::WaitingDeps
                };
                let max_retries = spec.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
                let backoff_base_ms = spec.backoff_base_ms.unwrap_or(DEFAULT_BACKOFF_BASE_MS);
                let priority = spec.priority.unwrap_or(0);
                let run_after_ms = spec
                    .run_after
                    .map(|ts| ts.timestamp_millis())
                    .unwrap_or(now_ms);

                tx.execute(
                    "INSERT INTO jobs (
                        id, mission_id, node_key, queue, status, priority,
                        capability_tags_json, payload_json, result_json, last_error,
                        idempotency_key, attempts, max_retries, backoff_base_ms,
                        run_after_ms, leased_by, lease_expires_at_ms,
                        created_at_ms, updated_at_ms, completed_at_ms
                     ) VALUES (
                        ?1, ?2, ?3, ?4, ?5, ?6,
                        ?7, ?8, NULL, NULL,
                        ?9, 0, ?10, ?11,
                        ?12, NULL, NULL,
                        ?13, ?13, NULL
                     )",
                    params![
                        job_id,
                        mission_id,
                        node_key,
                        queue,
                        initial_status.as_str(),
                        priority,
                        tags_serialized,
                        payload_serialized,
                        idempotency_key,
                        max_retries,
                        backoff_base_ms,
                        run_after_ms,
                        now_ms,
                    ],
                )?;
            }

            let mut edge_count = 0usize;
            for spec in &request.jobs {
                let node_key = spec.key.trim();
                let child_job_id = key_to_job_id
                    .get(node_key)
                    .ok_or_else(|| MissionError::Internal("missing child key mapping".to_string()))?;
                for dep_key in dedup_preserve_order(&spec.depends_on) {
                    let parent_job_id = key_to_job_id.get(dep_key.trim()).ok_or_else(|| {
                        MissionError::Validation(format!(
                            "job '{node_key}' references unknown dependency key '{dep_key}'"
                        ))
                    })?;
                    tx.execute(
                        "INSERT INTO job_dependencies (job_id, depends_on_job_id, created_at_ms)
                         VALUES (?1, ?2, ?3)",
                        params![child_job_id, parent_job_id, now_ms],
                    )?;
                    edge_count += 1;
                }
            }

            append_outbox_event(
                tx,
                "mission",
                &mission_id,
                "mission.created",
                &json!({
                    "mission_id": mission_id,
                    "name": mission_name,
                    "job_count": request.jobs.len(),
                    "edge_count": edge_count,
                }),
                now_ms,
            )?;

            load_mission_detail(tx, &mission_id)
        })
    }

    pub fn get_mission(&self, mission_id: &str) -> Result<MissionDetail, MissionError> {
        self.database
            .with_read(|conn| load_mission_detail(conn, mission_id))
    }

    pub fn get_job(&self, job_id: &str) -> Result<JobRecord, MissionError> {
        self.database
            .with_read(|conn| load_job_by_id(conn, job_id))
    }

    pub fn list_jobs(&self, filter: ListJobsFilter) -> Result<Vec<JobRecord>, MissionError> {
        let limit = filter
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT) as i64;

        self.database.with_read(|conn| {
            let mut sql = String::from(
                "SELECT id, mission_id, node_key, queue, status, priority,
                        capability_tags_json, payload_json, result_json, last_error,
                        idempotency_key, attempts, max_retries, backoff_base_ms,
                        run_after_ms, leased_by, lease_expires_at_ms,
                        created_at_ms, updated_at_ms, completed_at_ms
                 FROM jobs WHERE 1 = 1",
            );
            let mut bound: Vec<rusqlite::types::Value> = Vec::new();

            if let Some(ref queue) = filter.queue {
                sql.push_str(" AND queue = ?");
                bound.push(rusqlite::types::Value::Text(queue.clone()));
            }
            if let Some(status) = filter.status {
                sql.push_str(" AND status = ?");
                bound.push(rusqlite::types::Value::Text(status.as_str().to_string()));
            }
            if let Some(ref mission_id) = filter.mission_id {
                sql.push_str(" AND mission_id = ?");
                bound.push(rusqlite::types::Value::Text(mission_id.clone()));
            }

            sql.push_str(" ORDER BY priority DESC, created_at_ms ASC, id ASC LIMIT ?");
            bound.push(rusqlite::types::Value::Integer(limit));

            let mut stmt = conn.prepare(&sql)?;
            let mut rows = stmt.query(params_from_iter(bound))?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next()? {
                let mut record = map_job_row(row)?;
                record.depends_on = load_job_dependencies(conn, &record.id)?;
                jobs.push(record);
            }
            Ok(jobs)
        })
    }

    pub fn lease_next_job(
        &self,
        request: LeaseJobRequest,
    ) -> Result<Option<JobRecord>, MissionError> {
        self.lease_next_job_at(request, Utc::now().timestamp_millis())
    }

    pub fn lease_next_job_at(
        &self,
        request: LeaseJobRequest,
        now_ms: i64,
    ) -> Result<Option<JobRecord>, MissionError> {
        let worker_id = request.worker_id.trim().to_string();
        if worker_id.is_empty() {
            return Err(MissionError::Validation(
                "worker_id must not be empty".to_string(),
            ));
        }
        if request.lease_ttl_secs == 0 || request.lease_ttl_secs > MAX_LEASE_TTL_SECS {
            return Err(MissionError::Validation(format!(
                "lease_ttl_secs must be between 1 and {MAX_LEASE_TTL_SECS}"
            )));
        }

        let normalized_queues: Vec<String> = request
            .queues
            .iter()
            .map(|q| q.trim())
            .filter(|q| !q.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        let worker_tags: HashSet<String> = normalize_tags(&request.capability_tags)
            .into_iter()
            .collect();
        let lease_expires_at_ms = now_ms + (request.lease_ttl_secs as i64) * 1_000;

        self.database.with_write_tx(|tx| {
            let mut sql = String::from(
                "SELECT id, mission_id, capability_tags_json
                 FROM jobs
                 WHERE status = 'queued' AND run_after_ms <= ?",
            );
            let mut bound: Vec<rusqlite::types::Value> =
                vec![rusqlite::types::Value::Integer(now_ms)];

            if !normalized_queues.is_empty() {
                let placeholders = vec!["?"; normalized_queues.len()].join(", ");
                sql.push_str(&format!(" AND queue IN ({placeholders})"));
                for queue_name in &normalized_queues {
                    bound.push(rusqlite::types::Value::Text(queue_name.clone()));
                }
            }

            sql.push_str(" ORDER BY priority DESC, run_after_ms ASC, created_at_ms ASC, id ASC");

            let selected: Option<(String, Option<String>)> = {
                let mut stmt = tx.prepare(&sql)?;
                let mut rows = stmt.query(params_from_iter(bound))?;
                let mut matched = None;

                while let Some(row) = rows.next()? {
                    let candidate_id: String = row.get(0)?;
                    let mission_id: Option<String> = row.get(1)?;
                    let tags_raw: String = row.get(2)?;
                    let required_tags: Vec<String> = serde_json::from_str(&tags_raw)?;

                    if required_tags.iter().all(|tag| worker_tags.contains(tag)) {
                        matched = Some((candidate_id, mission_id));
                        break;
                    }
                }
                matched
            };

            let Some((job_id, mission_id)) = selected else {
                return Ok(None);
            };

            let updated_rows = tx.execute(
                "UPDATE jobs
                 SET status = 'leased',
                     leased_by = ?1,
                     lease_expires_at_ms = ?2,
                     attempts = attempts + 1,
                     updated_at_ms = ?3
                 WHERE id = ?4 AND status = 'queued'",
                params![worker_id, lease_expires_at_ms, now_ms, job_id],
            )?;

            if updated_rows == 0 {
                return Ok(None);
            }

            if let Some(ref mid) = mission_id {
                tx.execute(
                    "UPDATE missions
                     SET status = 'running', updated_at_ms = ?1
                     WHERE id = ?2 AND status = 'pending'",
                    params![now_ms, mid],
                )?;
            }

            append_outbox_event(
                tx,
                "job",
                &job_id,
                "job.leased",
                &json!({
                    "job_id": job_id,
                    "worker_id": worker_id,
                    "lease_expires_at_ms": lease_expires_at_ms,
                }),
                now_ms,
            )?;

            let leased_job = load_job_by_id(tx, &job_id)?;
            Ok(Some(leased_job))
        })
    }

    pub fn heartbeat_job(
        &self,
        job_id: &str,
        worker_id: &str,
        extend_secs: u64,
    ) -> Result<JobRecord, MissionError> {
        self.heartbeat_job_at(job_id, worker_id, extend_secs, Utc::now().timestamp_millis())
    }

    pub fn heartbeat_job_at(
        &self,
        job_id: &str,
        worker_id: &str,
        extend_secs: u64,
        now_ms: i64,
    ) -> Result<JobRecord, MissionError> {
        let trimmed_worker = worker_id.trim();
        if trimmed_worker.is_empty() {
            return Err(MissionError::Validation(
                "worker_id must not be empty".to_string(),
            ));
        }
        if extend_secs == 0 || extend_secs > MAX_LEASE_TTL_SECS {
            return Err(MissionError::Validation(format!(
                "extend_secs must be between 1 and {MAX_LEASE_TTL_SECS}"
            )));
        }

        self.database.with_write_tx(|tx| {
            let existing = load_job_by_id(tx, job_id)?;
            ensure_active_lease(&existing, trimmed_worker)?;

            let current_expiry_ms = existing
                .lease_expires_at
                .map(|ts| ts.timestamp_millis())
                .unwrap_or(now_ms);
            let max_allowed_expiry_ms = now_ms + (MAX_LEASE_TTL_SECS as i64) * 1_000;
            let extended_expiry_ms = (current_expiry_ms.max(now_ms)
                + (extend_secs as i64) * 1_000)
                .min(max_allowed_expiry_ms);

            tx.execute(
                "UPDATE jobs
                 SET lease_expires_at_ms = ?1, updated_at_ms = ?2
                 WHERE id = ?3 AND status = 'leased' AND leased_by = ?4",
                params![extended_expiry_ms, now_ms, job_id, trimmed_worker],
            )?;

            load_job_by_id(tx, job_id)
        })
    }

    pub fn complete_job(
        &self,
        job_id: &str,
        worker_id: &str,
        result_json: Value,
    ) -> Result<CompleteJobOutcome, MissionError> {
        self.complete_job_at(job_id, worker_id, result_json, Utc::now().timestamp_millis())
    }

    pub fn complete_job_at(
        &self,
        job_id: &str,
        worker_id: &str,
        result_json: Value,
        now_ms: i64,
    ) -> Result<CompleteJobOutcome, MissionError> {
        let trimmed_worker = worker_id.trim();
        if trimmed_worker.is_empty() {
            return Err(MissionError::Validation(
                "worker_id must not be empty".to_string(),
            ));
        }
        let result_serialized = serde_json::to_string(&result_json)?;

        self.database.with_write_tx(|tx| {
            let existing = load_job_by_id(tx, job_id)?;
            ensure_active_lease(&existing, trimmed_worker)?;

            tx.execute(
                "UPDATE jobs
                 SET status = 'completed',
                     result_json = ?1,
                     leased_by = NULL,
                     lease_expires_at_ms = NULL,
                     completed_at_ms = ?2,
                     updated_at_ms = ?2
                 WHERE id = ?3 AND status = 'leased' AND leased_by = ?4",
                params![result_serialized, now_ms, job_id, trimmed_worker],
            )?;

            append_outbox_event(
                tx,
                "job",
                job_id,
                "job.completed",
                &json!({
                    "job_id": job_id,
                    "worker_id": trimmed_worker,
                    "mission_id": existing.mission_id,
                }),
                now_ms,
            )?;

            let ready_children: Vec<String> = {
                let mut stmt = tx.prepare(
                    "SELECT c.id
                     FROM job_dependencies jd
                     INNER JOIN jobs c ON c.id = jd.job_id
                     WHERE jd.depends_on_job_id = ?1
                       AND c.status = 'waiting_deps'
                       AND NOT EXISTS (
                           SELECT 1
                           FROM job_dependencies parent_edge
                           INNER JOIN jobs parent_job
                             ON parent_job.id = parent_edge.depends_on_job_id
                           WHERE parent_edge.job_id = c.id
                             AND parent_job.status <> 'completed'
                       )
                     ORDER BY c.priority DESC, c.created_at_ms ASC, c.id ASC",
                )?;
                let mut rows = stmt.query(params![job_id])?;
                let mut children = Vec::new();
                while let Some(row) = rows.next()? {
                    children.push(row.get(0)?);
                }
                children
            };

            let mut promoted_job_ids = Vec::with_capacity(ready_children.len());
            for child_id in ready_children {
                let changed = tx.execute(
                    "UPDATE jobs
                     SET status = 'queued', updated_at_ms = ?1
                     WHERE id = ?2 AND status = 'waiting_deps'",
                    params![now_ms, child_id],
                )?;
                if changed > 0 {
                    append_outbox_event(
                        tx,
                        "job",
                        &child_id,
                        "job.unblocked",
                        &json!({
                            "job_id": child_id,
                            "unblocked_by": job_id,
                        }),
                        now_ms,
                    )?;
                    promoted_job_ids.push(child_id);
                }
            }

            if let Some(ref mission_id) = existing.mission_id {
                let remaining_incomplete: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM jobs WHERE mission_id = ?1 AND status <> 'completed'",
                    params![mission_id],
                    |row| row.get(0),
                )?;
                if remaining_incomplete == 0 {
                    tx.execute(
                        "UPDATE missions
                         SET status = 'completed', completed_at_ms = ?1, updated_at_ms = ?1
                         WHERE id = ?2",
                        params![now_ms, mission_id],
                    )?;
                    append_outbox_event(
                        tx,
                        "mission",
                        mission_id,
                        "mission.completed",
                        &json!({ "mission_id": mission_id }),
                        now_ms,
                    )?;
                }
            }

            let completed_job = load_job_by_id(tx, job_id)?;
            Ok(CompleteJobOutcome {
                job: completed_job,
                promoted_job_ids,
            })
        })
    }

    pub fn fail_job(
        &self,
        job_id: &str,
        worker_id: &str,
        error_msg: &str,
    ) -> Result<FailJobOutcome, MissionError> {
        self.fail_job_at(job_id, worker_id, error_msg, Utc::now().timestamp_millis())
    }

    pub fn fail_job_at(
        &self,
        job_id: &str,
        worker_id: &str,
        error_msg: &str,
        now_ms: i64,
    ) -> Result<FailJobOutcome, MissionError> {
        let trimmed_worker = worker_id.trim();
        if trimmed_worker.is_empty() {
            return Err(MissionError::Validation(
                "worker_id must not be empty".to_string(),
            ));
        }
        let clean_error = error_msg.trim();
        let persisted_error = if clean_error.is_empty() {
            "worker reported unspecified failure"
        } else {
            clean_error
        };

        self.database.with_write_tx(|tx| {
            let existing = load_job_by_id(tx, job_id)?;
            ensure_active_lease(&existing, trimmed_worker)?;

            if existing.attempts >= existing.max_retries {
                move_job_to_dlq(
                    tx,
                    &existing.id,
                    existing.mission_id.as_deref(),
                    &existing.queue,
                    &serde_json::to_string(&existing.payload_json)?,
                    existing.attempts,
                    persisted_error,
                    now_ms,
                )?;
                let dlq_job = load_job_by_id(tx, job_id)?;
                return Ok(FailJobOutcome {
                    job: dlq_job,
                    moved_to_dlq: true,
                    retry_delay_ms: None,
                });
            }

            let exponent = existing.attempts.saturating_sub(1).min(20);
            let delay_ms = existing
                .backoff_base_ms
                .saturating_mul(1_i64 << exponent)
                .min(MAX_BACKOFF_MS);
            let next_run_after_ms = now_ms + delay_ms;

            tx.execute(
                "UPDATE jobs
                 SET status = 'queued',
                     last_error = ?1,
                     leased_by = NULL,
                     lease_expires_at_ms = NULL,
                     run_after_ms = ?2,
                     updated_at_ms = ?3
                 WHERE id = ?4 AND status = 'leased' AND leased_by = ?5",
                params![
                    persisted_error,
                    next_run_after_ms,
                    now_ms,
                    job_id,
                    trimmed_worker
                ],
            )?;

            append_outbox_event(
                tx,
                "job",
                job_id,
                "job.retry_scheduled",
                &json!({
                    "job_id": job_id,
                    "worker_id": trimmed_worker,
                    "attempts": existing.attempts,
                    "max_retries": existing.max_retries,
                    "retry_delay_ms": delay_ms,
                    "run_after_ms": next_run_after_ms,
                    "error": persisted_error,
                }),
                now_ms,
            )?;

            let requeued_job = load_job_by_id(tx, job_id)?;
            Ok(FailJobOutcome {
                job: requeued_job,
                moved_to_dlq: false,
                retry_delay_ms: Some(delay_ms),
            })
        })
    }

    pub fn list_dlq(
        &self,
        include_replayed: bool,
        limit: Option<usize>,
    ) -> Result<Vec<DlqEntryRecord>, MissionError> {
        let bounded_limit = limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT) as i64;

        self.database.with_read(|conn| {
            let sql = if include_replayed {
                "SELECT id, job_id, queue, mission_id, error_message, attempts,
                        payload_json, failed_at_ms, replayed_at_ms, replay_count
                 FROM dlq_entries
                 ORDER BY failed_at_ms DESC, id ASC
                 LIMIT ?1"
            } else {
                "SELECT id, job_id, queue, mission_id, error_message, attempts,
                        payload_json, failed_at_ms, replayed_at_ms, replay_count
                 FROM dlq_entries
                 WHERE replayed_at_ms IS NULL
                 ORDER BY failed_at_ms DESC, id ASC
                 LIMIT ?1"
            };

            let mut stmt = conn.prepare(sql)?;
            let mut rows = stmt.query(params![bounded_limit])?;
            let mut entries = Vec::new();
            while let Some(row) = rows.next()? {
                entries.push(map_dlq_row(row)?);
            }
            Ok(entries)
        })
    }

    pub fn replay_dlq(&self, job_or_dlq_id: &str) -> Result<JobRecord, MissionError> {
        self.replay_dlq_at(job_or_dlq_id, Utc::now().timestamp_millis())
    }

    pub fn replay_dlq_at(
        &self,
        job_or_dlq_id: &str,
        now_ms: i64,
    ) -> Result<JobRecord, MissionError> {
        self.database.with_write_tx(|tx| {
            let resolved_job_id: Option<String> = tx
                .query_row(
                    "SELECT id FROM jobs WHERE id = ?1",
                    params![job_or_dlq_id],
                    |row| row.get(0),
                )
                .optional()?;

            let target_job_id = match resolved_job_id {
                Some(found_id) => found_id,
                None => tx
                    .query_row(
                        "SELECT job_id FROM dlq_entries WHERE id = ?1",
                        params![job_or_dlq_id],
                        |row| row.get(0),
                    )
                    .optional()?
                    .ok_or_else(|| MissionError::JobNotFound(job_or_dlq_id.to_string()))?,
            };

            let existing = load_job_by_id(tx, &target_job_id)?;
            if existing.status != JobStatus::Dlq {
                return Err(MissionError::NotInDlq(target_job_id));
            }

            let unsatisfied_deps: i64 = tx.query_row(
                "SELECT COUNT(*)
                 FROM job_dependencies jd
                 INNER JOIN jobs parent ON parent.id = jd.depends_on_job_id
                 WHERE jd.job_id = ?1 AND parent.status <> 'completed'",
                params![target_job_id],
                |row| row.get(0),
            )?;
            let restored_status = if unsatisfied_deps > 0 {
                JobStatus::WaitingDeps
            } else {
                JobStatus::Queued
            };

            tx.execute(
                "UPDATE jobs
                 SET status = ?1,
                     attempts = 0,
                     last_error = NULL,
                     leased_by = NULL,
                     lease_expires_at_ms = NULL,
                     run_after_ms = ?2,
                     updated_at_ms = ?2
                 WHERE id = ?3 AND status = 'dlq'",
                params![restored_status.as_str(), now_ms, target_job_id],
            )?;

            tx.execute(
                "UPDATE dlq_entries
                 SET replayed_at_ms = ?1, replay_count = replay_count + 1
                 WHERE job_id = ?2",
                params![now_ms, target_job_id],
            )?;

            if let Some(ref mission_id) = existing.mission_id {
                let remaining_dlq_in_mission: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM jobs WHERE mission_id = ?1 AND status = 'dlq'",
                    params![mission_id],
                    |row| row.get(0),
                )?;
                if remaining_dlq_in_mission == 0 {
                    tx.execute(
                        "UPDATE missions
                         SET status = 'running', updated_at_ms = ?1
                         WHERE id = ?2 AND status = 'failed'",
                        params![now_ms, mission_id],
                    )?;
                }
            }

            append_outbox_event(
                tx,
                "dlq",
                &target_job_id,
                "job.replayed",
                &json!({
                    "job_id": target_job_id,
                    "restored_status": restored_status.as_str(),
                }),
                now_ms,
            )?;

            load_job_by_id(tx, &target_job_id)
        })
    }

    pub fn reap_expired_leases(&self) -> Result<ReapResult, MissionError> {
        self.reap_expired_leases_at(Utc::now().timestamp_millis())
    }

    pub fn reap_expired_leases_at(&self, now_ms: i64) -> Result<ReapResult, MissionError> {
        self.database.with_write_tx(|tx| {
            let expired: Vec<(String, Option<String>, String, String, u32, u32)> = {
                let mut stmt = tx.prepare(
                    "SELECT id, mission_id, queue, payload_json, attempts, max_retries
                     FROM jobs
                     WHERE status = 'leased'
                       AND lease_expires_at_ms IS NOT NULL
                       AND lease_expires_at_ms < ?1
                     ORDER BY lease_expires_at_ms ASC, id ASC",
                )?;
                let mut rows = stmt.query(params![now_ms])?;
                let mut collected = Vec::new();
                while let Some(row) = rows.next()? {
                    collected.push((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ));
                }
                collected
            };

            let mut requeued_job_ids = Vec::new();
            let mut dlq_job_ids = Vec::new();

            for (job_id, mission_id, queue, payload_serialized, attempts, max_retries) in expired {
                if attempts >= max_retries {
                    move_job_to_dlq(
                        tx,
                        &job_id,
                        mission_id.as_deref(),
                        &queue,
                        &payload_serialized,
                        attempts,
                        "lease expired after exhausting max retries",
                        now_ms,
                    )?;
                    dlq_job_ids.push(job_id);
                } else {
                    tx.execute(
                        "UPDATE jobs
                         SET status = 'queued',
                             leased_by = NULL,
                             lease_expires_at_ms = NULL,
                             run_after_ms = ?1,
                             last_error = 'lease expired; re-queued by zombie reaper',
                             updated_at_ms = ?1
                         WHERE id = ?2 AND status = 'leased'",
                        params![now_ms, job_id],
                    )?;

                    append_outbox_event(
                        tx,
                        "job",
                        &job_id,
                        "job.lease_reaped",
                        &json!({
                            "job_id": job_id,
                            "attempts": attempts,
                            "max_retries": max_retries,
                        }),
                        now_ms,
                    )?;
                    requeued_job_ids.push(job_id);
                }
            }

            let reaped_count = requeued_job_ids.len() + dlq_job_ids.len();
            Ok(ReapResult {
                reaped_count,
                requeued_job_ids,
                dlq_job_ids,
            })
        })
    }

    pub fn create_cron_schedule(
        &self,
        request: CreateCronScheduleRequest,
    ) -> Result<CronScheduleRecord, MissionError> {
        self.create_cron_schedule_at(request, Utc::now().timestamp_millis())
    }

    pub fn create_cron_schedule_at(
        &self,
        request: CreateCronScheduleRequest,
        now_ms: i64,
    ) -> Result<CronScheduleRecord, MissionError> {
        let schedule_name = request.name.trim().to_string();
        if schedule_name.is_empty() {
            return Err(MissionError::Validation(
                "cron schedule name must not be empty".to_string(),
            ));
        }

        let parsed_cron = CronExpression::parse(&request.cron_expr)?;
        let now_dt = millis_to_utc(now_ms)?;
        let next_run_at = parsed_cron.next_after(now_dt)?;
        let queue = normalize_queue(request.queue.as_deref())?;
        let capability_tags = normalize_tags(&request.capability_tags);
        let tags_serialized = serde_json::to_string(&capability_tags)?;
        let payload_serialized = serde_json::to_string(&request.payload_json)?;
        let max_retries = request.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let schedule_id = Uuid::new_v4().to_string();

        self.database.with_write_tx(|tx| {
            let duplicate: Option<String> = tx
                .query_row(
                    "SELECT id FROM cron_schedules WHERE name = ?1",
                    params![schedule_name],
                    |row| row.get(0),
                )
                .optional()?;
            if duplicate.is_some() {
                return Err(MissionError::Conflict(format!(
                    "cron schedule already exists with name: {schedule_name}"
                )));
            }

            tx.execute(
                "INSERT INTO cron_schedules (
                    id, name, cron_expr, queue, capability_tags_json,
                    payload_json, max_retries, enabled, next_run_at_ms,
                    last_run_at_ms, created_at_ms, updated_at_ms
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5,
                    ?6, ?7, ?8, ?9,
                    NULL, ?10, ?10
                 )",
                params![
                    schedule_id,
                    schedule_name,
                    request.cron_expr.trim(),
                    queue,
                    tags_serialized,
                    payload_serialized,
                    max_retries,
                    i32::from(request.enabled),
                    next_run_at.timestamp_millis(),
                    now_ms,
                ],
            )?;

            load_cron_by_id(tx, &schedule_id)
        })
    }

    pub fn list_cron_schedules(&self) -> Result<Vec<CronScheduleRecord>, MissionError> {
        self.database.with_read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT id, name, cron_expr, queue, capability_tags_json,
                        payload_json, max_retries, enabled, next_run_at_ms,
                        last_run_at_ms, created_at_ms, updated_at_ms
                 FROM cron_schedules
                 ORDER BY next_run_at_ms ASC, name ASC",
            )?;
            let mut rows = stmt.query([])?;
            let mut schedules = Vec::new();
            while let Some(row) = rows.next()? {
                schedules.push(map_cron_row(row)?);
            }
            Ok(schedules)
        })
    }

    pub fn delete_cron_schedule(&self, schedule_id: &str) -> Result<(), MissionError> {
        self.database.with_write_tx(|tx| {
            let deleted = tx.execute(
                "DELETE FROM cron_schedules WHERE id = ?1",
                params![schedule_id],
            )?;
            if deleted == 0 {
                return Err(MissionError::CronNotFound(schedule_id.to_string()));
            }
            Ok(())
        })
    }

    pub fn dispatch_due_crons(&self) -> Result<CronDispatchResult, MissionError> {
        self.dispatch_due_crons_at(Utc::now().timestamp_millis())
    }

    pub fn dispatch_due_crons_at(&self, now_ms: i64) -> Result<CronDispatchResult, MissionError> {
        let now_dt = millis_to_utc(now_ms)?;

        self.database.with_write_tx(|tx| {
            let due_schedules: Vec<CronScheduleRecord> = {
                let mut stmt = tx.prepare(
                    "SELECT id, name, cron_expr, queue, capability_tags_json,
                            payload_json, max_retries, enabled, next_run_at_ms,
                            last_run_at_ms, created_at_ms, updated_at_ms
                     FROM cron_schedules
                     WHERE enabled = 1 AND next_run_at_ms <= ?1
                     ORDER BY next_run_at_ms ASC, id ASC",
                )?;
                let mut rows = stmt.query(params![now_ms])?;
                let mut due = Vec::new();
                while let Some(row) = rows.next()? {
                    due.push(map_cron_row(row)?);
                }
                due
            };

            let mut dispatched_job_ids = Vec::new();
            for schedule in due_schedules {
                let parsed = CronExpression::parse(&schedule.cron_expr)?;
                let scheduled_slot_ms = schedule.next_run_at.timestamp_millis();
                let next_run_after = parsed.next_after(now_dt)?;
                let idem_key = format!("cron:{}:{scheduled_slot_ms}", schedule.id);

                let already_created: Option<String> = tx
                    .query_row(
                        "SELECT id FROM jobs WHERE idempotency_key = ?1",
                        params![idem_key],
                        |row| row.get(0),
                    )
                    .optional()?;

                if already_created.is_none() {
                    let job_id = Uuid::new_v4().to_string();
                    let tags_serialized = serde_json::to_string(&schedule.capability_tags)?;
                    let payload_serialized = serde_json::to_string(&schedule.payload_json)?;

                    tx.execute(
                        "INSERT INTO jobs (
                            id, mission_id, node_key, queue, status, priority,
                            capability_tags_json, payload_json, result_json, last_error,
                            idempotency_key, attempts, max_retries, backoff_base_ms,
                            run_after_ms, leased_by, lease_expires_at_ms,
                            created_at_ms, updated_at_ms, completed_at_ms
                         ) VALUES (
                            ?1, NULL, ?2, ?3, 'queued', 0,
                            ?4, ?5, NULL, NULL,
                            ?6, 0, ?7, ?8,
                            ?9, NULL, NULL,
                            ?9, ?9, NULL
                         )",
                        params![
                            job_id,
                            schedule.name,
                            schedule.queue,
                            tags_serialized,
                            payload_serialized,
                            idem_key,
                            schedule.max_retries,
                            DEFAULT_BACKOFF_BASE_MS,
                            now_ms,
                        ],
                    )?;

                    append_outbox_event(
                        tx,
                        "cron",
                        &schedule.id,
                        "cron.dispatched",
                        &json!({
                            "schedule_id": schedule.id,
                            "schedule_name": schedule.name,
                            "job_id": job_id,
                            "scheduled_slot_ms": scheduled_slot_ms,
                        }),
                        now_ms,
                    )?;
                    dispatched_job_ids.push(job_id);
                }

                tx.execute(
                    "UPDATE cron_schedules
                     SET last_run_at_ms = ?1,
                         next_run_at_ms = ?2,
                         updated_at_ms = ?1
                     WHERE id = ?3",
                    params![now_ms, next_run_after.timestamp_millis(), schedule.id],
                )?;
            }

            Ok(CronDispatchResult { dispatched_job_ids })
        })
    }

    pub fn list_outbox_events(
        &self,
        only_unpublished: bool,
        limit: Option<usize>,
    ) -> Result<Vec<OutboxEventRecord>, MissionError> {
        let bounded_limit = limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT) as i64;

        self.database.with_read(|conn| {
            let sql = if only_unpublished {
                "SELECT seq, event_id, aggregate_type, aggregate_id, event_type,
                        payload_json, published, created_at_ms, published_at_ms
                 FROM outbox_events
                 WHERE published = 0
                 ORDER BY seq ASC
                 LIMIT ?1"
            } else {
                "SELECT seq, event_id, aggregate_type, aggregate_id, event_type,
                        payload_json, published, created_at_ms, published_at_ms
                 FROM outbox_events
                 ORDER BY seq ASC
                 LIMIT ?1"
            };

            let mut stmt = conn.prepare(sql)?;
            let mut rows = stmt.query(params![bounded_limit])?;
            let mut events = Vec::new();
            while let Some(row) = rows.next()? {
                events.push(map_outbox_row(row)?);
            }
            Ok(events)
        })
    }

    pub fn ack_outbox_event(&self, event_id: &str) -> Result<OutboxEventRecord, MissionError> {
        let now_ms = Utc::now().timestamp_millis();
        self.database.with_write_tx(|tx| {
            let changed = tx.execute(
                "UPDATE outbox_events
                 SET published = 1, published_at_ms = COALESCE(published_at_ms, ?1)
                 WHERE event_id = ?2",
                params![now_ms, event_id],
            )?;
            if changed == 0 {
                return Err(MissionError::OutboxEventNotFound(event_id.to_string()));
            }
            tx.query_row(
                "SELECT seq, event_id, aggregate_type, aggregate_id, event_type,
                        payload_json, published, created_at_ms, published_at_ms
                 FROM outbox_events
                 WHERE event_id = ?1",
                params![event_id],
                map_outbox_row_boxed,
            )
            .map_err(MissionError::Database)?
        })
    }

    pub fn get_stats(&self) -> Result<BrokerStats, MissionError> {
        self.database.with_read(|conn| {
            let mut stats = BrokerStats {
                total_jobs: 0,
                jobs_waiting_deps: 0,
                jobs_queued: 0,
                jobs_leased: 0,
                jobs_completed: 0,
                jobs_dlq: 0,
                total_missions: 0,
                missions_pending: 0,
                missions_running: 0,
                missions_completed: 0,
                missions_failed: 0,
                active_dlq_entries: 0,
                enabled_cron_schedules: 0,
                unpublished_outbox_events: 0,
                queues: BTreeMap::new(),
            };

            {
                let mut stmt =
                    conn.prepare("SELECT queue, status, COUNT(*) FROM jobs GROUP BY queue, status")?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let queue_name: String = row.get(0)?;
                    let status_raw: String = row.get(1)?;
                    let count: i64 = row.get(2)?;
                    let count_u64 = count.max(0) as u64;

                    let queue_entry =
                        stats
                            .queues
                            .entry(queue_name)
                            .or_insert_with(|| QueueDepthStats {
                                waiting_deps: 0,
                                queued: 0,
                                leased: 0,
                                completed: 0,
                                dlq: 0,
                            });

                    stats.total_jobs += count_u64;
                    match JobStatus::parse(&status_raw) {
                        Some(JobStatus::WaitingDeps) => {
                            stats.jobs_waiting_deps += count_u64;
                            queue_entry.waiting_deps += count_u64;
                        }
                        Some(JobStatus::Queued) => {
                            stats.jobs_queued += count_u64;
                            queue_entry.queued += count_u64;
                        }
                        Some(JobStatus::Leased) => {
                            stats.jobs_leased += count_u64;
                            queue_entry.leased += count_u64;
                        }
                        Some(JobStatus::Completed) => {
                            stats.jobs_completed += count_u64;
                            queue_entry.completed += count_u64;
                        }
                        Some(JobStatus::Dlq) => {
                            stats.jobs_dlq += count_u64;
                            queue_entry.dlq += count_u64;
                        }
                        Some(JobStatus::Failed) | None => {}
                    }
                }
            }

            {
                let mut stmt =
                    conn.prepare("SELECT status, COUNT(*) FROM missions GROUP BY status")?;
                let mut rows = stmt.query([])?;
                while let Some(row) = rows.next()? {
                    let status_raw: String = row.get(0)?;
                    let count: i64 = row.get(1)?;
                    let count_u64 = count.max(0) as u64;
                    stats.total_missions += count_u64;
                    match MissionStatus::parse(&status_raw) {
                        Some(MissionStatus::Pending) => stats.missions_pending += count_u64,
                        Some(MissionStatus::Running) => stats.missions_running += count_u64,
                        Some(MissionStatus::Completed) => stats.missions_completed += count_u64,
                        Some(MissionStatus::Failed) => stats.missions_failed += count_u64,
                        Some(MissionStatus::Cancelled) | None => {}
                    }
                }
            }

            let active_dlq: i64 = conn.query_row(
                "SELECT COUNT(*) FROM dlq_entries WHERE replayed_at_ms IS NULL",
                [],
                |row| row.get(0),
            )?;
            stats.active_dlq_entries = active_dlq.max(0) as u64;

            let enabled_crons: i64 = conn.query_row(
                "SELECT COUNT(*) FROM cron_schedules WHERE enabled = 1",
                [],
                |row| row.get(0),
            )?;
            stats.enabled_cron_schedules = enabled_crons.max(0) as u64;

            let unpublished_outbox: i64 = conn.query_row(
                "SELECT COUNT(*) FROM outbox_events WHERE published = 0",
                [],
                |row| row.get(0),
            )?;
            stats.unpublished_outbox_events = unpublished_outbox.max(0) as u64;

            Ok(stats)
        })
    }
}

fn ensure_active_lease(job: &JobRecord, worker_id: &str) -> Result<(), MissionError> {
    if job.status != JobStatus::Leased {
        return Err(MissionError::LeaseMismatch(format!(
            "job {} is in '{}' status, not 'leased'",
            job.id,
            job.status.as_str()
        )));
    }
    if job.leased_by.as_deref() != Some(worker_id) {
        return Err(MissionError::LeaseMismatch(format!(
            "job {} is leased by '{:?}', not '{worker_id}'",
            job.id, job.leased_by
        )));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn move_job_to_dlq(
    tx: &Transaction<'_>,
    job_id: &str,
    mission_id: Option<&str>,
    queue: &str,
    payload_serialized: &str,
    attempts: u32,
    error_message: &str,
    now_ms: i64,
) -> Result<(), MissionError> {
    tx.execute(
        "UPDATE jobs
         SET status = 'dlq',
             last_error = ?1,
             leased_by = NULL,
             lease_expires_at_ms = NULL,
             updated_at_ms = ?2
         WHERE id = ?3",
        params![error_message, now_ms, job_id],
    )?;

    let dlq_entry_id = Uuid::new_v4().to_string();
    tx.execute(
        "INSERT INTO dlq_entries (
            id, job_id, queue, mission_id, error_message, attempts,
            payload_json, failed_at_ms, replayed_at_ms, replay_count
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, 0)
         ON CONFLICT(job_id) DO UPDATE SET
            error_message = excluded.error_message,
            attempts = excluded.attempts,
            payload_json = excluded.payload_json,
            failed_at_ms = excluded.failed_at_ms,
            replayed_at_ms = NULL",
        params![
            dlq_entry_id,
            job_id,
            queue,
            mission_id,
            error_message,
            attempts,
            payload_serialized,
            now_ms,
        ],
    )?;

    if let Some(mid) = mission_id {
        tx.execute(
            "UPDATE missions
             SET status = 'failed', updated_at_ms = ?1
             WHERE id = ?2 AND status NOT IN ('completed', 'cancelled')",
            params![now_ms, mid],
        )?;
        append_outbox_event(
            tx,
            "mission",
            mid,
            "mission.failed",
            &json!({
                "mission_id": mid,
                "failed_job_id": job_id,
                "error": error_message,
            }),
            now_ms,
        )?;
    }

    append_outbox_event(
        tx,
        "dlq",
        job_id,
        "job.dlq",
        &json!({
            "job_id": job_id,
            "queue": queue,
            "attempts": attempts,
            "error": error_message,
        }),
        now_ms,
    )?;

    Ok(())
}

fn detect_dag_cycles(specs: &[crate::models::DagJobSpec]) -> Result<(), MissionError> {
    let mut in_degree: BTreeMap<String, usize> = BTreeMap::new();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();

    for spec in specs {
        let node_key = spec.key.trim().to_string();
        in_degree.insert(node_key.clone(), 0);
        dependents.entry(node_key).or_default();
    }

    for spec in specs {
        let node_key = spec.key.trim().to_string();
        let unique_deps = dedup_preserve_order(&spec.depends_on);
        for raw_dep in unique_deps {
            let dep_key = raw_dep.trim().to_string();
            if dep_key == node_key {
                return Err(MissionError::CycleDetected(vec![node_key]));
            }
            if !in_degree.contains_key(&dep_key) {
                return Err(MissionError::Validation(format!(
                    "job '{node_key}' depends on unknown DAG node '{dep_key}'"
                )));
            }
            if let Some(count) = in_degree.get_mut(&node_key) {
                *count += 1;
            }
            dependents.entry(dep_key).or_default().push(node_key.clone());
        }
    }

    let mut ready_queue: VecDeque<String> = in_degree
        .iter()
        .filter_map(|(key, &degree)| if degree == 0 { Some(key.clone()) } else { None })
        .collect();

    let mut visited_count = 0usize;
    while let Some(current_key) = ready_queue.pop_front() {
        visited_count += 1;
        if let Some(children) = dependents.get(&current_key) {
            for child_key in children {
                if let Some(degree) = in_degree.get_mut(child_key) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready_queue.push_back(child_key.clone());
                    }
                }
            }
        }
    }

    if visited_count != specs.len() {
        let cycle_nodes: Vec<String> = in_degree
            .into_iter()
            .filter_map(|(key, degree)| if degree > 0 { Some(key) } else { None })
            .collect();
        return Err(MissionError::CycleDetected(cycle_nodes));
    }

    Ok(())
}

fn append_outbox_event(
    tx: &Transaction<'_>,
    aggregate_type: &str,
    aggregate_id: &str,
    event_type: &str,
    payload: &Value,
    now_ms: i64,
) -> Result<(), MissionError> {
    let event_id = Uuid::new_v4().to_string();
    let payload_serialized = serde_json::to_string(payload)?;
    tx.execute(
        "INSERT INTO outbox_events (
            event_id, aggregate_type, aggregate_id, event_type,
            payload_json, published, created_at_ms, published_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, NULL)",
        params![
            event_id,
            aggregate_type,
            aggregate_id,
            event_type,
            payload_serialized,
            now_ms,
        ],
    )?;
    Ok(())
}

fn load_mission_detail(conn: &Connection, mission_id: &str) -> Result<MissionDetail, MissionError> {
    let mission = conn
        .query_row(
            "SELECT id, name, status, metadata_json, created_at_ms, updated_at_ms, completed_at_ms
             FROM missions WHERE id = ?1",
            params![mission_id],
            map_mission_row_boxed,
        )
        .optional()?
        .ok_or_else(|| MissionError::MissionNotFound(mission_id.to_string()))??;

    let mut stmt = conn.prepare(
        "SELECT id, mission_id, node_key, queue, status, priority,
                capability_tags_json, payload_json, result_json, last_error,
                idempotency_key, attempts, max_retries, backoff_base_ms,
                run_after_ms, leased_by, lease_expires_at_ms,
                created_at_ms, updated_at_ms, completed_at_ms
         FROM jobs
         WHERE mission_id = ?1
         ORDER BY created_at_ms ASC, id ASC",
    )?;
    let mut rows = stmt.query(params![mission_id])?;
    let mut jobs = Vec::new();
    let mut summary = MissionSummary {
        total_jobs: 0,
        waiting_deps: 0,
        queued: 0,
        leased: 0,
        completed: 0,
        dlq: 0,
    };

    while let Some(row) = rows.next()? {
        let mut record = map_job_row(row)?;
        record.depends_on = load_job_dependencies(conn, &record.id)?;
        summary.total_jobs += 1;
        match record.status {
            JobStatus::WaitingDeps => summary.waiting_deps += 1,
            JobStatus::Queued => summary.queued += 1,
            JobStatus::Leased => summary.leased += 1,
            JobStatus::Completed => summary.completed += 1,
            JobStatus::Dlq => summary.dlq += 1,
            JobStatus::Failed => {}
        }
        jobs.push(record);
    }

    Ok(MissionDetail {
        mission,
        jobs,
        summary,
    })
}

fn load_job_by_id(conn: &Connection, job_id: &str) -> Result<JobRecord, MissionError> {
    let maybe_job = conn
        .query_row(
            "SELECT id, mission_id, node_key, queue, status, priority,
                    capability_tags_json, payload_json, result_json, last_error,
                    idempotency_key, attempts, max_retries, backoff_base_ms,
                    run_after_ms, leased_by, lease_expires_at_ms,
                    created_at_ms, updated_at_ms, completed_at_ms
             FROM jobs WHERE id = ?1",
            params![job_id],
            map_job_row_boxed,
        )
        .optional()?;

    let mut job = match maybe_job {
        Some(mapped) => mapped?,
        None => return Err(MissionError::JobNotFound(job_id.to_string())),
    };
    job.depends_on = load_job_dependencies(conn, &job.id)?;
    Ok(job)
}

fn load_job_by_idempotency_key(
    conn: &Connection,
    idempotency_key: &str,
) -> Result<Option<JobRecord>, MissionError> {
    let maybe_job = conn
        .query_row(
            "SELECT id, mission_id, node_key, queue, status, priority,
                    capability_tags_json, payload_json, result_json, last_error,
                    idempotency_key, attempts, max_retries, backoff_base_ms,
                    run_after_ms, leased_by, lease_expires_at_ms,
                    created_at_ms, updated_at_ms, completed_at_ms
             FROM jobs WHERE idempotency_key = ?1",
            params![idempotency_key],
            map_job_row_boxed,
        )
        .optional()?;

    match maybe_job {
        Some(mapped) => {
            let mut job = mapped?;
            job.depends_on = load_job_dependencies(conn, &job.id)?;
            Ok(Some(job))
        }
        None => Ok(None),
    }
}

fn load_job_dependencies(conn: &Connection, job_id: &str) -> Result<Vec<String>, MissionError> {
    let mut stmt = conn.prepare(
        "SELECT depends_on_job_id
         FROM job_dependencies
         WHERE job_id = ?1
         ORDER BY created_at_ms ASC, depends_on_job_id ASC",
    )?;
    let mut rows = stmt.query(params![job_id])?;
    let mut deps = Vec::new();
    while let Some(row) = rows.next()? {
        deps.push(row.get(0)?);
    }
    Ok(deps)
}

fn load_cron_by_id(
    conn: &Connection,
    schedule_id: &str,
) -> Result<CronScheduleRecord, MissionError> {
    conn.query_row(
        "SELECT id, name, cron_expr, queue, capability_tags_json,
                payload_json, max_retries, enabled, next_run_at_ms,
                last_run_at_ms, created_at_ms, updated_at_ms
         FROM cron_schedules WHERE id = ?1",
        params![schedule_id],
        map_cron_row_boxed,
    )
    .optional()?
    .ok_or_else(|| MissionError::CronNotFound(schedule_id.to_string()))?
}

fn map_mission_row_boxed(row: &Row<'_>) -> rusqlite::Result<Result<MissionRecord, MissionError>> {
    Ok(map_mission_row(row))
}

fn map_mission_row(row: &Row<'_>) -> Result<MissionRecord, MissionError> {
    let status_raw: String = row.get(2)?;
    let status = MissionStatus::parse(&status_raw).ok_or_else(|| {
        MissionError::Internal(format!("invalid mission status in db: {status_raw}"))
    })?;
    let metadata_raw: String = row.get(3)?;
    Ok(MissionRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        status,
        metadata_json: serde_json::from_str(&metadata_raw)?,
        created_at: millis_to_utc(row.get(4)?)?,
        updated_at: millis_to_utc(row.get(5)?)?,
        completed_at: opt_millis_to_utc(row.get(6)?)?,
    })
}

fn map_job_row_boxed(row: &Row<'_>) -> rusqlite::Result<Result<JobRecord, MissionError>> {
    Ok(map_job_row(row))
}

fn map_job_row(row: &Row<'_>) -> Result<JobRecord, MissionError> {
    let status_raw: String = row.get(4)?;
    let status = JobStatus::parse(&status_raw)
        .ok_or_else(|| MissionError::Internal(format!("invalid job status in db: {status_raw}")))?;
    let tags_raw: String = row.get(6)?;
    let payload_raw: String = row.get(7)?;
    let result_raw: Option<String> = row.get(8)?;
    let result_json = match result_raw {
        Some(raw) => Some(serde_json::from_str(&raw)?),
        None => None,
    };

    Ok(JobRecord {
        id: row.get(0)?,
        mission_id: row.get(1)?,
        node_key: row.get(2)?,
        queue: row.get(3)?,
        status,
        priority: row.get(5)?,
        capability_tags: serde_json::from_str(&tags_raw)?,
        payload_json: serde_json::from_str(&payload_raw)?,
        result_json,
        last_error: row.get(9)?,
        idempotency_key: row.get(10)?,
        attempts: row.get(11)?,
        max_retries: row.get(12)?,
        backoff_base_ms: row.get(13)?,
        run_after: millis_to_utc(row.get(14)?)?,
        leased_by: row.get(15)?,
        lease_expires_at: opt_millis_to_utc(row.get(16)?)?,
        depends_on: Vec::new(),
        created_at: millis_to_utc(row.get(17)?)?,
        updated_at: millis_to_utc(row.get(18)?)?,
        completed_at: opt_millis_to_utc(row.get(19)?)?,
    })
}

fn map_dlq_row(row: &Row<'_>) -> Result<DlqEntryRecord, MissionError> {
    let payload_raw: String = row.get(6)?;
    Ok(DlqEntryRecord {
        id: row.get(0)?,
        job_id: row.get(1)?,
        queue: row.get(2)?,
        mission_id: row.get(3)?,
        error_message: row.get(4)?,
        attempts: row.get(5)?,
        payload_json: serde_json::from_str(&payload_raw)?,
        failed_at: millis_to_utc(row.get(7)?)?,
        replayed_at: opt_millis_to_utc(row.get(8)?)?,
        replay_count: row.get(9)?,
    })
}

fn map_cron_row_boxed(
    row: &Row<'_>,
) -> rusqlite::Result<Result<CronScheduleRecord, MissionError>> {
    Ok(map_cron_row(row))
}

fn map_cron_row(row: &Row<'_>) -> Result<CronScheduleRecord, MissionError> {
    let tags_raw: String = row.get(4)?;
    let payload_raw: String = row.get(5)?;
    let enabled_int: i32 = row.get(7)?;
    Ok(CronScheduleRecord {
        id: row.get(0)?,
        name: row.get(1)?,
        cron_expr: row.get(2)?,
        queue: row.get(3)?,
        capability_tags: serde_json::from_str(&tags_raw)?,
        payload_json: serde_json::from_str(&payload_raw)?,
        max_retries: row.get(6)?,
        enabled: enabled_int != 0,
        next_run_at: millis_to_utc(row.get(8)?)?,
        last_run_at: opt_millis_to_utc(row.get(9)?)?,
        created_at: millis_to_utc(row.get(10)?)?,
        updated_at: millis_to_utc(row.get(11)?)?,
    })
}

fn map_outbox_row_boxed(
    row: &Row<'_>,
) -> rusqlite::Result<Result<OutboxEventRecord, MissionError>> {
    Ok(map_outbox_row(row))
}

fn map_outbox_row(row: &Row<'_>) -> Result<OutboxEventRecord, MissionError> {
    let payload_raw: String = row.get(5)?;
    let published_int: i32 = row.get(6)?;
    Ok(OutboxEventRecord {
        seq: row.get(0)?,
        event_id: row.get(1)?,
        aggregate_type: row.get(2)?,
        aggregate_id: row.get(3)?,
        event_type: row.get(4)?,
        payload_json: serde_json::from_str(&payload_raw)?,
        published: published_int != 0,
        created_at: millis_to_utc(row.get(7)?)?,
        published_at: opt_millis_to_utc(row.get(8)?)?,
    })
}

fn normalize_queue(raw: Option<&str>) -> Result<String, MissionError> {
    let chosen = raw.unwrap_or(DEFAULT_QUEUE).trim();
    if chosen.is_empty() {
        return Err(MissionError::Validation(
            "queue name must not be empty".to_string(),
        ));
    }
    Ok(chosen.to_string())
}

fn normalize_optional_nonempty(raw: Option<String>) -> Result<Option<String>, MissionError> {
    match raw {
        None => Ok(None),
        Some(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
    }
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

fn dedup_preserve_order(entries: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for entry in entries {
        let trimmed = entry.trim();
        if !trimmed.is_empty() && seen.insert(trimmed.to_string()) {
            unique.push(trimmed.to_string());
        }
    }
    unique
}
