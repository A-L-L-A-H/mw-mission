use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl MissionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    WaitingDeps,
    Queued,
    Leased,
    Completed,
    Failed,
    Dlq,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WaitingDeps => "waiting_deps",
            Self::Queued => "queued",
            Self::Leased => "leased",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Dlq => "dlq",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "waiting_deps" => Some(Self::WaitingDeps),
            "queued" => Some(Self::Queued),
            "leased" => Some(Self::Leased),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "dlq" => Some(Self::Dlq),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissionRecord {
    pub id: String,
    pub name: String,
    pub status: MissionStatus,
    pub metadata_json: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissionSummary {
    pub total_jobs: usize,
    pub waiting_deps: usize,
    pub queued: usize,
    pub leased: usize,
    pub completed: usize,
    pub dlq: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MissionDetail {
    pub mission: MissionRecord,
    pub jobs: Vec<JobRecord>,
    pub summary: MissionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: String,
    pub mission_id: Option<String>,
    pub node_key: Option<String>,
    pub queue: String,
    pub status: JobStatus,
    pub priority: i32,
    pub capability_tags: Vec<String>,
    pub payload_json: Value,
    pub result_json: Option<Value>,
    pub last_error: Option<String>,
    pub idempotency_key: Option<String>,
    pub attempts: u32,
    pub max_retries: u32,
    pub backoff_base_ms: i64,
    pub run_after: DateTime<Utc>,
    pub leased_by: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub depends_on: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnqueueJobRequest {
    pub queue: Option<String>,
    #[serde(default)]
    pub capability_tags: Vec<String>,
    #[serde(default = "default_json_object")]
    pub payload_json: Value,
    pub idempotency_key: Option<String>,
    pub max_retries: Option<u32>,
    pub backoff_base_ms: Option<i64>,
    pub priority: Option<i32>,
    pub run_after: Option<DateTime<Utc>>,
    pub mission_id: Option<String>,
    pub node_key: Option<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueJobOutcome {
    pub job: JobRecord,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DagJobSpec {
    pub key: String,
    pub queue: Option<String>,
    #[serde(default)]
    pub capability_tags: Vec<String>,
    #[serde(default = "default_json_object")]
    pub payload_json: Value,
    pub idempotency_key: Option<String>,
    pub max_retries: Option<u32>,
    pub backoff_base_ms: Option<i64>,
    pub priority: Option<i32>,
    pub run_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateDagMissionRequest {
    pub name: String,
    #[serde(default = "default_json_object")]
    pub metadata_json: Value,
    pub jobs: Vec<DagJobSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LeaseJobRequest {
    pub worker_id: String,
    #[serde(default)]
    pub queues: Vec<String>,
    #[serde(default)]
    pub capability_tags: Vec<String>,
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatJobRequest {
    pub worker_id: String,
    #[serde(default = "default_lease_ttl_secs")]
    pub extend_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CompleteJobRequest {
    pub worker_id: String,
    #[serde(default = "default_json_object")]
    pub result_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteJobOutcome {
    pub job: JobRecord,
    pub promoted_job_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FailJobRequest {
    pub worker_id: String,
    pub error_msg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailJobOutcome {
    pub job: JobRecord,
    pub moved_to_dlq: bool,
    pub retry_delay_ms: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ListJobsFilter {
    pub queue: Option<String>,
    pub status: Option<JobStatus>,
    pub mission_id: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntryRecord {
    pub id: String,
    pub job_id: String,
    pub queue: String,
    pub mission_id: Option<String>,
    pub error_message: String,
    pub attempts: u32,
    pub payload_json: Value,
    pub failed_at: DateTime<Utc>,
    pub replayed_at: Option<DateTime<Utc>>,
    pub replay_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReapResult {
    pub reaped_count: usize,
    pub requeued_job_ids: Vec<String>,
    pub dlq_job_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateCronScheduleRequest {
    pub name: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronScheduleRecord {
    pub id: String,
    pub name: String,
    pub cron_expr: String,
    pub queue: String,
    pub capability_tags: Vec<String>,
    pub payload_json: Value,
    pub max_retries: u32,
    pub enabled: bool,
    pub next_run_at: DateTime<Utc>,
    pub last_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronDispatchResult {
    pub dispatched_job_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxEventRecord {
    pub seq: i64,
    pub event_id: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload_json: Value,
    pub published: bool,
    pub created_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDepthStats {
    pub waiting_deps: u64,
    pub queued: u64,
    pub leased: u64,
    pub completed: u64,
    pub dlq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerStats {
    pub total_jobs: u64,
    pub jobs_waiting_deps: u64,
    pub jobs_queued: u64,
    pub jobs_leased: u64,
    pub jobs_completed: u64,
    pub jobs_dlq: u64,
    pub total_missions: u64,
    pub missions_pending: u64,
    pub missions_running: u64,
    pub missions_completed: u64,
    pub missions_failed: u64,
    pub active_dlq_entries: u64,
    pub enabled_cron_schedules: u64,
    pub unpublished_outbox_events: u64,
    pub queues: BTreeMap<String, QueueDepthStats>,
}

fn default_json_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn default_lease_ttl_secs() -> u64 {
    30
}

fn default_true() -> bool {
    true
}
