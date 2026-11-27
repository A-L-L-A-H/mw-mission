use crate::db::{Database, millis_to_utc, opt_millis_to_utc};
use crate::error::MissionError;
use crate::models::DlqEntryRecord;
use rusqlite::{OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::warn;

pub const DEFAULT_NTFY_URL: &str = "https://ntfy.sh";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqAlertOutcome {
    pub dlq_entry_id: String,
    pub job_id: String,
    pub queue: String,
    pub attempts: u32,
    pub escalated: bool,
    pub ntfy_status: Option<u16>,
}

#[derive(Clone)]
pub struct DeadLetterQueueManager {
    database: Database,
    ntfy_url: Option<String>,
    ntfy_topic: Option<String>,
    http_client: reqwest::Client,
}

impl DeadLetterQueueManager {
    pub fn new(database: Database) -> Self {
        let ntfy_url = std::env::var("MW_MISSION_NTFY_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let ntfy_topic = std::env::var("MW_MISSION_NTFY_TOPIC")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            database,
            ntfy_url,
            ntfy_topic,
            http_client,
        }
    }

    pub fn with_ntfy(database: Database, ntfy_url: Option<String>, ntfy_topic: Option<String>) -> Self {
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Self {
            database,
            ntfy_url: ntfy_url.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
            ntfy_topic: ntfy_topic.map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
            http_client,
        }
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn list_entries(
        &self,
        include_replayed: bool,
        limit: Option<usize>,
    ) -> Result<Vec<DlqEntryRecord>, MissionError> {
        let max_rows = limit.unwrap_or(100).clamp(1, 1000) as i64;
        self.database.with_read(|conn| {
            let mut stmt = if include_replayed {
                conn.prepare(
                    "SELECT id, job_id, queue, mission_id, error_message, attempts,
                            payload_json, failed_at_ms, replayed_at_ms, replay_count
                     FROM dlq_entries
                     ORDER BY failed_at_ms DESC
                     LIMIT ?1",
                )?
            } else {
                conn.prepare(
                    "SELECT id, job_id, queue, mission_id, error_message, attempts,
                            payload_json, failed_at_ms, replayed_at_ms, replay_count
                     FROM dlq_entries
                     WHERE replayed_at_ms IS NULL
                     ORDER BY failed_at_ms DESC
                     LIMIT ?1",
                )?
            };

            let mut rows = stmt.query(params![max_rows])?;
            let mut entries = Vec::new();
            while let Some(row) = rows.next()? {
                entries.push(map_dlq_row(row)?);
            }
            Ok(entries)
        })
    }

    pub fn get_entry(&self, job_or_dlq_id: &str) -> Result<DlqEntryRecord, MissionError> {
        let key = job_or_dlq_id.trim();
        if key.is_empty() {
            return Err(MissionError::Validation("DLQ identifier must not be empty".to_string()));
        }

        self.database.with_read(|conn| {
            conn.query_row(
                "SELECT id, job_id, queue, mission_id, error_message, attempts,
                        payload_json, failed_at_ms, replayed_at_ms, replay_count
                 FROM dlq_entries
                 WHERE id = ?1 OR job_id = ?1",
                params![key],
                |row| Ok(map_dlq_row(row)),
            )
            .optional()?
            .transpose()?
            .ok_or_else(|| MissionError::JobNotFound(format!("dlq entry not found: {key}")))
        })
    }

    pub async fn escalate_p0(&self, entry: &DlqEntryRecord) -> Result<DlqAlertOutcome, MissionError> {
        let target_topic = match &self.ntfy_topic {
            Some(t) => t.clone(),
            None => {
                return Ok(DlqAlertOutcome {
                    dlq_entry_id: entry.id.clone(),
                    job_id: entry.job_id.clone(),
                    queue: entry.queue.clone(),
                    attempts: entry.attempts,
                    escalated: false,
                    ntfy_status: None,
                });
            }
        };

        let base_url = self.ntfy_url.as_deref().unwrap_or(DEFAULT_NTFY_URL).trim_end_matches('/');
        let endpoint = format!("{base_url}/{target_topic}");

        let mission_tag = entry.mission_id.as_deref().unwrap_or("none");
        let body = format!(
            "P0 DLQ Escalation: Job {} in queue '{}' exhausted {} retries.\nMission: {}\nError: {}\nFailed at: {}",
            entry.job_id,
            entry.queue,
            entry.attempts,
            mission_tag,
            entry.error_message,
            entry.failed_at.to_rfc3339()
        );

        let response = self
            .http_client
            .post(&endpoint)
            .header("Title", format!("[mw-mission] P0 DLQ Alert: {}", entry.job_id))
            .header("Priority", "urgent")
            .header("Tags", "rotating_light,fire,dlq")
            .body(body)
            .send()
            .await;

        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                Ok(DlqAlertOutcome {
                    dlq_entry_id: entry.id.clone(),
                    job_id: entry.job_id.clone(),
                    queue: entry.queue.clone(),
                    attempts: entry.attempts,
                    escalated: resp.status().is_success(),
                    ntfy_status: Some(status),
                })
            }
            Err(err) => {
                warn!("failed to deliver P0 ntfy escalation: {err}");
                Ok(DlqAlertOutcome {
                    dlq_entry_id: entry.id.clone(),
                    job_id: entry.job_id.clone(),
                    queue: entry.queue.clone(),
                    attempts: entry.attempts,
                    escalated: false,
                    ntfy_status: None,
                })
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MissionEngine;
    use crate::models::{EnqueueJobRequest, LeaseJobRequest};
    use serde_json::json;

    #[tokio::test]
    async fn test_dead_letter_manager_list_and_escalate() {
        let db = Database::open_in_memory().expect("open db");
        let engine = MissionEngine::new(db.clone());
        let dlq_mgr = DeadLetterQueueManager::new(db);

        // Enqueue job with max_retries = 1
        let outcome = engine
            .enqueue_job(EnqueueJobRequest {
                queue: Some("alerts".to_string()),
                capability_tags: vec![],
                payload_json: json!({"critical": true}),
                idempotency_key: None,
                max_retries: Some(1),
                backoff_base_ms: None,
                priority: None,
                run_after: None,
                mission_id: None,
                node_key: None,
                depends_on: vec![],
            })
            .expect("enqueue");

        let leased = engine
            .lease_next_job(LeaseJobRequest {
                worker_id: "w1".to_string(),
                queues: vec!["alerts".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 10,
            })
            .expect("lease")
            .expect("leased job");

        let fail_res = engine
            .fail_job(&leased.id, "w1", "fatal database crash")
            .expect("fail job");
        assert!(fail_res.moved_to_dlq);

        let active_entries = dlq_mgr.list_entries(false, None).expect("list active");
        assert_eq!(active_entries.len(), 1);
        assert_eq!(active_entries[0].job_id, outcome.job.id);
        assert_eq!(active_entries[0].error_message, "fatal database crash");

        let fetched = dlq_mgr.get_entry(&outcome.job.id).expect("get by job id");
        assert_eq!(fetched.id, active_entries[0].id);

        // Escalate with unconfigured topic returns unescalated outcome safely
        let alert_outcome = dlq_mgr.escalate_p0(&fetched).await.expect("escalate");
        assert!(!alert_outcome.escalated);
        assert_eq!(alert_outcome.job_id, outcome.job.id);
    }
}
