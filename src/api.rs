use crate::engine::MissionEngine;
use crate::error::MissionError;
use crate::models::{
    BrokerStats, CompleteJobOutcome, CompleteJobRequest, CreateCronScheduleRequest,
    CreateDagMissionRequest, CronDispatchResult, CronScheduleRecord, DlqEntryRecord,
    EnqueueJobOutcome, EnqueueJobRequest, FailJobOutcome, FailJobRequest, HeartbeatJobRequest,
    JobRecord, LeaseJobRequest, ListJobsFilter, MissionDetail, OutboxEventRecord, ReapResult,
};
use crate::slo_burn::{
    RecordSloOutcomeRequest, SloAlertDeliveryOutcome, SloBurnDetector, SloEvaluationReport,
};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::convert::Infallible;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Clone)]
pub struct AppState {
    pub engine: MissionEngine,
    pub slo_detector: SloBurnDetector,
}

#[derive(Debug, Serialize)]
struct JobsListResponse {
    jobs: Vec<JobRecord>,
}

#[derive(Debug, Serialize)]
struct LeaseJobResponse {
    job: Option<JobRecord>,
}

#[derive(Debug, Serialize)]
struct DlqListResponse {
    entries: Vec<DlqEntryRecord>,
}

#[derive(Debug, Serialize)]
struct CronListResponse {
    schedules: Vec<CronScheduleRecord>,
}

#[derive(Debug, Serialize)]
struct OutboxListResponse {
    events: Vec<OutboxEventRecord>,
}

#[derive(Debug, Deserialize)]
struct DlqQueryParams {
    #[serde(default)]
    include_replayed: bool,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct OutboxQueryParams {
    #[serde(default = "default_true")]
    only_unpublished: bool,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct SloQueryParams {
    queue: Option<String>,
}

fn default_true() -> bool {
    true
}

pub fn build_router(engine: MissionEngine) -> Router {
    let slo_detector =
        SloBurnDetector::new(engine.database().clone()).expect("init slo burn detector");
    build_router_with_slo(engine, slo_detector)
}

pub fn build_router_with_slo(engine: MissionEngine, slo_detector: SloBurnDetector) -> Router {
    let state = AppState {
        engine,
        slo_detector,
    };

    Router::new()
        .route("/health", get(health_handler))
        .route("/api/stats", get(stats_handler))
        .route("/api/events/stream", get(events_stream_handler))
        .route("/api/missions", post(create_mission_handler))
        .route("/api/missions/{id}", get(get_mission_handler))
        .route("/api/jobs", post(enqueue_job_handler).get(list_jobs_handler))
        .route("/api/jobs/{id}", get(get_job_handler))
        .route("/api/workers/lease", post(lease_job_handler))
        .route("/api/jobs/{id}/heartbeat", post(heartbeat_job_handler))
        .route("/api/jobs/{id}/complete", post(complete_job_handler))
        .route("/api/jobs/{id}/fail", post(fail_job_handler))
        .route("/api/dlq", get(list_dlq_handler))
        .route("/api/dlq/{id}/replay", post(replay_dlq_handler))
        .route(
            "/api/mission/jobs",
            post(enqueue_job_handler).get(list_jobs_handler),
        )
        .route("/api/mission/dlq", get(list_dlq_handler))
        .route("/api/mission/dlq/{id}/replay", post(replay_dlq_handler))
        .route(
            "/api/crons",
            post(create_cron_handler).get(list_crons_handler),
        )
        .route("/api/crons/{id}", delete(delete_cron_handler))
        .route("/api/crons/tick", post(tick_crons_handler))
        .route("/api/outbox", get(list_outbox_handler))
        .route("/api/outbox/{id}/ack", post(ack_outbox_handler))
        .route("/api/reaper/run", post(run_reaper_handler))
        .route("/api/slo/record", post(record_slo_outcome_handler))
        .route("/api/slo/evaluate", get(evaluate_slo_handler))
        .route("/api/slo/alert", post(alert_slo_handler))
        .with_state(state)
}

async fn run_blocking<F, T>(operation: F) -> Result<T, MissionError>
where
    F: FnOnce() -> Result<T, MissionError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|join_err| MissionError::Internal(format!("blocking task failed: {join_err}")))?
}

async fn health_handler() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "service": "mw-mission",
        "version": env!("CARGO_PKG_VERSION"),
        "timestamp": Utc::now(),
    }))
}

#[derive(Debug, Serialize)]
struct TelemetryStats {
    queued: u64,
    leased: u64,
    completed: u64,
    dlq: u64,
    outbox: u64,
}

#[derive(Debug, Serialize)]
struct TelemetrySnapshot {
    timestamp: DateTime<Utc>,
    stats: TelemetryStats,
}

async fn stats_handler(State(state): State<AppState>) -> Result<Json<BrokerStats>, MissionError> {
    let engine = state.engine;
    let stats = run_blocking(move || engine.get_stats()).await?;
    Ok(Json(stats))
}

async fn events_stream_handler(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(16);
    let engine = state.engine;

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        loop {
            ticker.tick().await;
            let engine_clone = engine.clone();
            let stats = match run_blocking(move || engine_clone.get_stats()).await {
                Ok(stats) => stats,
                Err(_) => break,
            };

            let snapshot = TelemetrySnapshot {
                timestamp: Utc::now(),
                stats: TelemetryStats {
                    queued: stats.jobs_queued,
                    leased: stats.jobs_leased,
                    completed: stats.jobs_completed,
                    dlq: stats.jobs_dlq,
                    outbox: stats.unpublished_outbox_events,
                },
            };

            let event = match Event::default().event("telemetry").json_data(snapshot) {
                Ok(event) => event,
                Err(_) => break,
            };

            if tx.send(Ok(event)).await.is_err() {
                break;
            }
        }
    });

    Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::default())
}

async fn create_mission_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateDagMissionRequest>,
) -> Result<(StatusCode, Json<MissionDetail>), MissionError> {
    let engine = state.engine;
    let detail = run_blocking(move || engine.create_dag_mission(payload)).await?;
    Ok((StatusCode::CREATED, Json(detail)))
}

async fn get_mission_handler(
    State(state): State<AppState>,
    Path(mission_id): Path<String>,
) -> Result<Json<MissionDetail>, MissionError> {
    let engine = state.engine;
    let detail = run_blocking(move || engine.get_mission(&mission_id)).await?;
    Ok(Json(detail))
}

async fn enqueue_job_handler(
    State(state): State<AppState>,
    Json(payload): Json<EnqueueJobRequest>,
) -> Result<(StatusCode, Json<EnqueueJobOutcome>), MissionError> {
    let engine = state.engine;
    let outcome = run_blocking(move || engine.enqueue_job(payload)).await?;
    let status = if outcome.deduplicated {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(outcome)))
}

async fn list_jobs_handler(
    State(state): State<AppState>,
    Query(filter): Query<ListJobsFilter>,
) -> Result<Json<JobsListResponse>, MissionError> {
    let engine = state.engine;
    let jobs = run_blocking(move || engine.list_jobs(filter)).await?;
    Ok(Json(JobsListResponse { jobs }))
}

async fn get_job_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<JobRecord>, MissionError> {
    let engine = state.engine;
    let job = run_blocking(move || engine.get_job(&job_id)).await?;
    Ok(Json(job))
}

async fn lease_job_handler(
    State(state): State<AppState>,
    Json(payload): Json<LeaseJobRequest>,
) -> Result<Json<LeaseJobResponse>, MissionError> {
    let engine = state.engine;
    let leased = run_blocking(move || engine.lease_next_job(payload)).await?;
    Ok(Json(LeaseJobResponse { job: leased }))
}

async fn heartbeat_job_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(payload): Json<HeartbeatJobRequest>,
) -> Result<Json<JobRecord>, MissionError> {
    let engine = state.engine;
    let updated =
        run_blocking(move || engine.heartbeat_job(&job_id, &payload.worker_id, payload.extend_secs))
            .await?;
    Ok(Json(updated))
}

async fn complete_job_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(payload): Json<CompleteJobRequest>,
) -> Result<Json<CompleteJobOutcome>, MissionError> {
    let engine = state.engine;
    let outcome = run_blocking(move || {
        engine.complete_job(&job_id, &payload.worker_id, payload.result_json)
    })
    .await?;
    Ok(Json(outcome))
}

async fn fail_job_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(payload): Json<FailJobRequest>,
) -> Result<Json<FailJobOutcome>, MissionError> {
    let engine = state.engine;
    let outcome =
        run_blocking(move || engine.fail_job(&job_id, &payload.worker_id, &payload.error_msg))
            .await?;
    Ok(Json(outcome))
}

async fn list_dlq_handler(
    State(state): State<AppState>,
    Query(query): Query<DlqQueryParams>,
) -> Result<Json<DlqListResponse>, MissionError> {
    let engine = state.engine;
    let entries =
        run_blocking(move || engine.list_dlq(query.include_replayed, query.limit)).await?;
    Ok(Json(DlqListResponse { entries }))
}

async fn replay_dlq_handler(
    State(state): State<AppState>,
    Path(job_or_dlq_id): Path<String>,
) -> Result<Json<JobRecord>, MissionError> {
    let engine = state.engine;
    let replayed = run_blocking(move || engine.replay_dlq(&job_or_dlq_id)).await?;
    Ok(Json(replayed))
}

async fn create_cron_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateCronScheduleRequest>,
) -> Result<(StatusCode, Json<CronScheduleRecord>), MissionError> {
    let engine = state.engine;
    let schedule = run_blocking(move || engine.create_cron_schedule(payload)).await?;
    Ok((StatusCode::CREATED, Json(schedule)))
}

async fn list_crons_handler(
    State(state): State<AppState>,
) -> Result<Json<CronListResponse>, MissionError> {
    let engine = state.engine;
    let schedules = run_blocking(move || engine.list_cron_schedules()).await?;
    Ok(Json(CronListResponse { schedules }))
}

async fn delete_cron_handler(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
) -> Result<Json<Value>, MissionError> {
    let engine = state.engine;
    let id_copy = schedule_id.clone();
    run_blocking(move || engine.delete_cron_schedule(&schedule_id)).await?;
    Ok(Json(json!({ "deleted": true, "id": id_copy })))
}

async fn tick_crons_handler(
    State(state): State<AppState>,
) -> Result<Json<CronDispatchResult>, MissionError> {
    let engine = state.engine;
    let dispatch = run_blocking(move || engine.dispatch_due_crons()).await?;
    Ok(Json(dispatch))
}

async fn list_outbox_handler(
    State(state): State<AppState>,
    Query(query): Query<OutboxQueryParams>,
) -> Result<Json<OutboxListResponse>, MissionError> {
    let engine = state.engine;
    let events =
        run_blocking(move || engine.list_outbox_events(query.only_unpublished, query.limit))
            .await?;
    Ok(Json(OutboxListResponse { events }))
}

async fn ack_outbox_handler(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<Json<OutboxEventRecord>, MissionError> {
    let engine = state.engine;
    let acknowledged = run_blocking(move || engine.ack_outbox_event(&event_id)).await?;
    Ok(Json(acknowledged))
}

async fn run_reaper_handler(
    State(state): State<AppState>,
) -> Result<Json<ReapResult>, MissionError> {
    let engine = state.engine;
    let result = run_blocking(move || engine.reap_expired_leases()).await?;
    Ok(Json(result))
}

async fn record_slo_outcome_handler(
    State(state): State<AppState>,
    Json(payload): Json<RecordSloOutcomeRequest>,
) -> Result<(StatusCode, Json<Value>), MissionError> {
    let detector = state.slo_detector;
    let queue = payload.queue.unwrap_or_else(|| "default".to_string());
    let job_id = payload.job_id;
    let is_error = payload.is_error;
    let now_ms = payload
        .recorded_at_ms
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let event_id = run_blocking(move || {
        detector.record_outcome_at(&queue, job_id.as_deref(), is_error, now_ms)
    })
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "event_id": event_id, "recorded": true })),
    ))
}

async fn evaluate_slo_handler(
    State(state): State<AppState>,
    Query(query): Query<SloQueryParams>,
) -> Result<Json<SloEvaluationReport>, MissionError> {
    let detector = state.slo_detector;
    let queue = query.queue;
    let report = run_blocking(move || detector.evaluate(queue.as_deref())).await?;
    Ok(Json(report))
}

async fn alert_slo_handler(
    State(state): State<AppState>,
    Query(query): Query<SloQueryParams>,
) -> Result<Json<SloAlertDeliveryOutcome>, MissionError> {
    let outcome = state
        .slo_detector
        .evaluate_and_alert(query.queue.as_deref())
        .await?;
    Ok(Json(outcome))
}
