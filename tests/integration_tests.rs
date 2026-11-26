use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use chrono::{TimeZone, Utc};
use http_body_util::BodyExt;
use mw_mission::api::build_router;
use mw_mission::db::Database;
use mw_mission::engine::MissionEngine;
use mw_mission::error::MissionError;
use mw_mission::models::{
    BrokerStats, CompleteJobOutcome, CreateCronScheduleRequest, CreateDagMissionRequest,
    DagJobSpec, EnqueueJobRequest, FailJobOutcome, JobRecord, JobStatus, LeaseJobRequest,
    ListJobsFilter, MissionDetail, MissionStatus,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use tempfile::TempDir;
use tower::ServiceExt;

fn create_memory_engine() -> MissionEngine {
    let database = Database::open_in_memory().expect("open in-memory sqlite");
    MissionEngine::new(database)
}

async fn send_json_request(
    router: &axum::Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method(method).uri(uri);
    let request = match body {
        Some(payload) => {
            builder = builder.header("content-type", "application/json");
            builder
                .body(Body::from(payload.to_string()))
                .expect("build json request")
        }
        None => builder.body(Body::empty()).expect("build empty request"),
    };

    let response = router
        .clone()
        .oneshot(request)
        .await
        .expect("execute router request");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("read body bytes")
        .to_bytes();
    let parsed: Value = serde_json::from_slice(&bytes).expect("parse response json");
    (status, parsed)
}

#[test]
fn test_dag_fan_out_fan_in_unblocking() {
    let engine = create_memory_engine();

    let mission_detail = engine
        .create_dag_mission(CreateDagMissionRequest {
            name: "etl-diamond-pipeline".to_string(),
            metadata_json: json!({ "env": "prod" }),
            jobs: vec![
                DagJobSpec {
                    key: "ingest".to_string(),
                    queue: Some("etl".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({ "stage": "ingest" }),
                    idempotency_key: None,
                    max_retries: Some(3),
                    backoff_base_ms: None,
                    priority: Some(10),
                    run_after: None,
                    depends_on: vec![],
                },
                DagJobSpec {
                    key: "transform_a".to_string(),
                    queue: Some("etl".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({ "stage": "transform_a" }),
                    idempotency_key: None,
                    max_retries: Some(3),
                    backoff_base_ms: None,
                    priority: Some(5),
                    run_after: None,
                    depends_on: vec!["ingest".to_string()],
                },
                DagJobSpec {
                    key: "transform_b".to_string(),
                    queue: Some("etl".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({ "stage": "transform_b" }),
                    idempotency_key: None,
                    max_retries: Some(3),
                    backoff_base_ms: None,
                    priority: Some(5),
                    run_after: None,
                    depends_on: vec!["ingest".to_string()],
                },
                DagJobSpec {
                    key: "transform_c".to_string(),
                    queue: Some("etl".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({ "stage": "transform_c" }),
                    idempotency_key: None,
                    max_retries: Some(3),
                    backoff_base_ms: None,
                    priority: Some(5),
                    run_after: None,
                    depends_on: vec!["ingest".to_string()],
                },
                DagJobSpec {
                    key: "aggregate".to_string(),
                    queue: Some("etl".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({ "stage": "aggregate" }),
                    idempotency_key: None,
                    max_retries: Some(3),
                    backoff_base_ms: None,
                    priority: Some(1),
                    run_after: None,
                    depends_on: vec![
                        "transform_a".to_string(),
                        "transform_b".to_string(),
                        "transform_c".to_string(),
                    ],
                },
                DagJobSpec {
                    key: "publish".to_string(),
                    queue: Some("etl".to_string()),
                    capability_tags: vec![],
                    payload_json: json!({ "stage": "publish" }),
                    idempotency_key: None,
                    max_retries: Some(3),
                    backoff_base_ms: None,
                    priority: Some(0),
                    run_after: None,
                    depends_on: vec!["aggregate".to_string()],
                },
            ],
        })
        .expect("create diamond DAG mission");

    assert_eq!(mission_detail.mission.status, MissionStatus::Pending);
    assert_eq!(mission_detail.summary.total_jobs, 6);
    assert_eq!(mission_detail.summary.queued, 1);
    assert_eq!(mission_detail.summary.waiting_deps, 5);

    let find_id = |node_key: &str| -> String {
        mission_detail
            .jobs
            .iter()
            .find(|job| job.node_key.as_deref() == Some(node_key))
            .expect("node exists")
            .id
            .clone()
    };

    let ingest_id = find_id("ingest");
    let transform_a_id = find_id("transform_a");
    let transform_b_id = find_id("transform_b");
    let transform_c_id = find_id("transform_c");
    let aggregate_id = find_id("aggregate");
    let publish_id = find_id("publish");

    // Lease ingest (only queued job)
    let leased_ingest = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "worker-1".to_string(),
            queues: vec!["etl".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 30,
        })
        .expect("lease ingest")
        .expect("ingest job available");
    assert_eq!(leased_ingest.id, ingest_id);

    // No second job should be leasable before ingest completes
    let empty_lease = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "worker-2".to_string(),
            queues: vec!["etl".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 30,
        })
        .expect("lease check");
    assert!(empty_lease.is_none());

    // Complete ingest -> fan-out unblocks transform_a, transform_b, transform_c
    let ingest_done = engine
        .complete_job(&ingest_id, "worker-1", json!({ "rows": 1000 }))
        .expect("complete ingest");
    let promoted_set: HashSet<String> = ingest_done.promoted_job_ids.into_iter().collect();
    assert_eq!(promoted_set.len(), 3);
    assert!(promoted_set.contains(&transform_a_id));
    assert!(promoted_set.contains(&transform_b_id));
    assert!(promoted_set.contains(&transform_c_id));

    // Lease all 3 fan-out branches in parallel
    let mut branch_jobs = Vec::new();
    for worker_idx in 1..=3 {
        let leased = engine
            .lease_next_job(LeaseJobRequest {
                worker_id: format!("worker-branch-{worker_idx}"),
                queues: vec!["etl".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            })
            .expect("lease branch")
            .expect("branch available");
        branch_jobs.push(leased);
    }

    // Complete first two branches -> aggregate MUST stay waiting_deps
    for (idx, branch_job) in branch_jobs.iter().take(2).enumerate() {
        let outcome = engine
            .complete_job(
                &branch_job.id,
                &format!("worker-branch-{}", idx + 1),
                json!({ "ok": true }),
            )
            .expect("complete branch");
        assert!(outcome.promoted_job_ids.is_empty());
        let agg = engine.get_job(&aggregate_id).expect("get aggregate");
        assert_eq!(agg.status, JobStatus::WaitingDeps);
    }

    // Complete third branch -> fan-in unblocks aggregate!
    let third_outcome = engine
        .complete_job(&branch_jobs[2].id, "worker-branch-3", json!({ "ok": true }))
        .expect("complete third branch");
    assert_eq!(third_outcome.promoted_job_ids, vec![aggregate_id.clone()]);

    // Lease and complete aggregate -> unblocks publish
    let leased_agg = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "worker-agg".to_string(),
            queues: vec!["etl".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 30,
        })
        .expect("lease agg")
        .expect("agg available");
    assert_eq!(leased_agg.id, aggregate_id);
    let agg_done = engine
        .complete_job(&aggregate_id, "worker-agg", json!({ "merged": 3 }))
        .expect("complete agg");
    assert_eq!(agg_done.promoted_job_ids, vec![publish_id.clone()]);

    // Lease and complete publish -> entire mission rolls up to completed
    let leased_pub = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "worker-pub".to_string(),
            queues: vec!["etl".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 30,
        })
        .expect("lease publish")
        .expect("publish available");
    assert_eq!(leased_pub.id, publish_id);
    let pub_done = engine
        .complete_job(&publish_id, "worker-pub", json!({ "published": true }))
        .expect("complete publish");
    assert!(pub_done.promoted_job_ids.is_empty());

    let final_mission = engine
        .get_mission(&mission_detail.mission.id)
        .expect("fetch completed mission");
    assert_eq!(final_mission.mission.status, MissionStatus::Completed);
    assert!(final_mission.mission.completed_at.is_some());
    assert_eq!(final_mission.summary.completed, 6);
}

#[test]
fn test_dag_cycle_detection_and_atomicity() {
    let engine = create_memory_engine();

    let cyclic_err = engine
        .create_dag_mission(CreateDagMissionRequest {
            name: "cyclic-pipeline".to_string(),
            metadata_json: json!({}),
            jobs: vec![
                DagJobSpec {
                    key: "step_a".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    idempotency_key: None,
                    max_retries: None,
                    backoff_base_ms: None,
                    priority: None,
                    run_after: None,
                    depends_on: vec!["step_c".to_string()],
                },
                DagJobSpec {
                    key: "step_b".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    idempotency_key: None,
                    max_retries: None,
                    backoff_base_ms: None,
                    priority: None,
                    run_after: None,
                    depends_on: vec!["step_a".to_string()],
                },
                DagJobSpec {
                    key: "step_c".to_string(),
                    queue: None,
                    capability_tags: vec![],
                    payload_json: json!({}),
                    idempotency_key: None,
                    max_retries: None,
                    backoff_base_ms: None,
                    priority: None,
                    run_after: None,
                    depends_on: vec!["step_b".to_string()],
                },
            ],
        })
        .expect_err("3-node cycle must be rejected");

    match cyclic_err {
        MissionError::CycleDetected(nodes) => {
            assert_eq!(nodes.len(), 3);
        }
        other => panic!("expected CycleDetected, got {other:?}"),
    }

    let self_cycle_err = engine
        .create_dag_mission(CreateDagMissionRequest {
            name: "self-cycle".to_string(),
            metadata_json: json!({}),
            jobs: vec![DagJobSpec {
                key: "solo".to_string(),
                queue: None,
                capability_tags: vec![],
                payload_json: json!({}),
                idempotency_key: None,
                max_retries: None,
                backoff_base_ms: None,
                priority: None,
                run_after: None,
                depends_on: vec!["solo".to_string()],
            }],
        })
        .expect_err("self-cycle must be rejected");
    assert!(matches!(self_cycle_err, MissionError::CycleDetected(_)));

    // Verify zero partial records leaked into DB
    let stats = engine.get_stats().expect("stats");
    assert_eq!(stats.total_missions, 0);
    assert_eq!(stats.total_jobs, 0);
}

#[test]
fn test_enqueue_job_depends_on_unblocking() {
    let engine = create_memory_engine();

    let parent = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("build".to_string()),
            capability_tags: vec![],
            payload_json: json!({ "step": "compile" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: None,
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("enqueue parent")
        .job;
    assert_eq!(parent.status, JobStatus::Queued);

    let child = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("build".to_string()),
            capability_tags: vec![],
            payload_json: json!({ "step": "package" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: None,
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![parent.id.clone()],
        })
        .expect("enqueue child")
        .job;
    assert_eq!(child.status, JobStatus::WaitingDeps);
    assert_eq!(child.depends_on, vec![parent.id.clone()]);

    let leased_parent = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "builder-1".to_string(),
            queues: vec!["build".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 30,
        })
        .expect("lease parent")
        .expect("parent available");
    assert_eq!(leased_parent.id, parent.id);

    let complete_outcome = engine
        .complete_job(&parent.id, "builder-1", json!({ "artifact": "bin" }))
        .expect("complete parent");
    assert_eq!(complete_outcome.promoted_job_ids, vec![child.id.clone()]);

    let updated_child = engine.get_job(&child.id).expect("reload child");
    assert_eq!(updated_child.status, JobStatus::Queued);

    // Enqueueing a child whose dependency is ALREADY completed starts queued immediately
    let immediate_child = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("build".to_string()),
            capability_tags: vec![],
            payload_json: json!({ "step": "post-check" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: None,
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![parent.id],
        })
        .expect("enqueue immediate child")
        .job;
    assert_eq!(immediate_child.status, JobStatus::Queued);
}

#[test]
fn test_idempotency_deduplication() {
    let engine = create_memory_engine();

    let first = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("billing".to_string()),
            capability_tags: vec!["pci".to_string()],
            payload_json: json!({ "invoice": "inv-2026-001", "amount_cents": 4200 }),
            idempotency_key: Some("charge:inv-2026-001".to_string()),
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: Some(5),
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("first enqueue");
    assert!(!first.deduplicated);

    let duplicate = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("billing".to_string()),
            capability_tags: vec!["pci".to_string()],
            payload_json: json!({ "invoice": "inv-2026-001", "amount_cents": 9999 }),
            idempotency_key: Some("charge:inv-2026-001".to_string()),
            max_retries: Some(1),
            backoff_base_ms: None,
            priority: Some(1),
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("duplicate enqueue");
    assert!(duplicate.deduplicated);
    assert_eq!(duplicate.job.id, first.job.id);
    assert_eq!(duplicate.job.payload_json["amount_cents"], json!(4200));

    let all_jobs = engine
        .list_jobs(ListJobsFilter::default())
        .expect("list all jobs");
    assert_eq!(all_jobs.len(), 1);
}

#[test]
fn test_capability_tag_routing_and_queue_isolation() {
    let engine = create_memory_engine();

    let gpu_job = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("compute".to_string()),
            capability_tags: vec!["gpu".to_string(), "cuda-12".to_string()],
            payload_json: json!({ "model": "whisper-v3" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: Some(10),
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("enqueue gpu job")
        .job;

    let arm_job = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("compute".to_string()),
            capability_tags: vec!["arm64".to_string(), "nvme".to_string()],
            payload_json: json!({ "task": "edge-compile" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: Some(5),
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("enqueue arm job")
        .job;

    let cpu_job = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("compute".to_string()),
            capability_tags: vec![],
            payload_json: json!({ "task": "general" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: Some(1),
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("enqueue general job")
        .job;

    let other_queue_job = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("emails".to_string()),
            capability_tags: vec![],
            payload_json: json!({ "to": "ops@moonwitness.local" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: Some(100),
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("enqueue email job")
        .job;

    // Worker with partial GPU capability (missing cuda-12) skips gpu_job and arm_job, gets cpu_job
    let first_lease = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "partial-gpu-worker".to_string(),
            queues: vec!["compute".to_string()],
            capability_tags: vec!["gpu".to_string()],
            lease_ttl_secs: 30,
        })
        .expect("partial lease")
        .expect("cpu job matched");
    assert_eq!(first_lease.id, cpu_job.id);

    // Same worker cannot lease gpu_job or arm_job, nor emails queue job
    let blocked_lease = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "partial-gpu-worker".to_string(),
            queues: vec!["compute".to_string()],
            capability_tags: vec!["gpu".to_string()],
            lease_ttl_secs: 30,
        })
        .expect("blocked lease");
    assert!(blocked_lease.is_none());

    // Superset GPU worker gets gpu_job
    let gpu_lease = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "cuda-worker".to_string(),
            queues: vec!["compute".to_string()],
            capability_tags: vec!["gpu".to_string(), "cuda-12".to_string(), "avx512".to_string()],
            lease_ttl_secs: 30,
        })
        .expect("gpu lease")
        .expect("gpu job matched");
    assert_eq!(gpu_lease.id, gpu_job.id);

    // ARM+NVMe worker gets arm_job
    let arm_lease = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "arm-worker".to_string(),
            queues: vec!["compute".to_string()],
            capability_tags: vec!["arm64".to_string(), "nvme".to_string()],
            lease_ttl_secs: 30,
        })
        .expect("arm lease")
        .expect("arm job matched");
    assert_eq!(arm_lease.id, arm_job.id);

    // Mail worker gets emails queue job
    let email_lease = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "mailer-1".to_string(),
            queues: vec!["emails".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 30,
        })
        .expect("email lease")
        .expect("email job matched");
    assert_eq!(email_lease.id, other_queue_job.id);
}

#[test]
fn test_exponential_retry_dlq_and_replay() {
    let engine = create_memory_engine();
    let t0: i64 = 1_760_000_000_000;

    let job = engine
        .enqueue_job_at(
            EnqueueJobRequest {
                queue: Some("webhooks".to_string()),
                capability_tags: vec![],
                payload_json: json!({ "url": "https://example.invalid/hook" }),
                idempotency_key: None,
                max_retries: Some(3),
                backoff_base_ms: Some(1_000),
                priority: None,
                run_after: None,
                mission_id: None,
                node_key: None,
                depends_on: vec![],
            },
            t0,
        )
        .expect("enqueue job")
        .job;

    // Attempt 1 -> fail -> 1x backoff (1000ms)
    let attempt1 = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-a".to_string(),
                queues: vec!["webhooks".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            },
            t0,
        )
        .expect("lease 1")
        .expect("job available");
    assert_eq!(attempt1.attempts, 1);

    let fail1 = engine
        .fail_job_at(&job.id, "worker-a", "502 bad gateway", t0 + 100)
        .expect("fail 1");
    assert!(!fail1.moved_to_dlq);
    assert_eq!(fail1.retry_delay_ms, Some(1_000));
    assert_eq!(fail1.job.status, JobStatus::Queued);
    assert_eq!(fail1.job.run_after.timestamp_millis(), t0 + 1_100);

    // Must NOT be leasable before run_after
    let too_early = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-b".to_string(),
                queues: vec!["webhooks".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            },
            t0 + 1_099,
        )
        .expect("early lease check");
    assert!(too_early.is_none());

    // Attempt 2 -> fail -> 2x backoff (2000ms)
    let attempt2 = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-b".to_string(),
                queues: vec!["webhooks".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            },
            t0 + 1_100,
        )
        .expect("lease 2")
        .expect("job ready for attempt 2");
    assert_eq!(attempt2.attempts, 2);

    let fail2 = engine
        .fail_job_at(&job.id, "worker-b", "503 service unavailable", t0 + 1_200)
        .expect("fail 2");
    assert!(!fail2.moved_to_dlq);
    assert_eq!(fail2.retry_delay_ms, Some(2_000));
    assert_eq!(fail2.job.status, JobStatus::Queued);
    assert_eq!(fail2.job.run_after.timestamp_millis(), t0 + 3_200);

    // Attempt 3 -> fail -> attempts (3) >= max_retries (3) -> moves to DLQ!
    let attempt3 = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-c".to_string(),
                queues: vec!["webhooks".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            },
            t0 + 3_200,
        )
        .expect("lease 3")
        .expect("job ready for attempt 3");
    assert_eq!(attempt3.attempts, 3);

    let fail3 = engine
        .fail_job_at(&job.id, "worker-c", "connection refused", t0 + 3_300)
        .expect("fail 3");
    assert!(fail3.moved_to_dlq);
    assert_eq!(fail3.retry_delay_ms, None);
    assert_eq!(fail3.job.status, JobStatus::Dlq);

    let active_dlq = engine.list_dlq(false, None).expect("list active dlq");
    assert_eq!(active_dlq.len(), 1);
    assert_eq!(active_dlq[0].job_id, job.id);
    assert_eq!(active_dlq[0].attempts, 3);
    assert_eq!(active_dlq[0].error_message, "connection refused");

    // Replay DLQ job back to queued
    let replayed = engine
        .replay_dlq_at(&job.id, t0 + 5_000)
        .expect("replay dlq job");
    assert_eq!(replayed.status, JobStatus::Queued);
    assert_eq!(replayed.attempts, 0);
    assert!(replayed.last_error.is_none());

    assert!(engine.list_dlq(false, None).expect("active dlq empty").is_empty());
    let dlq_history = engine.list_dlq(true, None).expect("dlq history");
    assert_eq!(dlq_history.len(), 1);
    assert_eq!(dlq_history[0].replay_count, 1);
    assert!(dlq_history[0].replayed_at.is_some());

    // Replayed job can now be leased and completed cleanly
    let re_leased = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-recovery".to_string(),
                queues: vec!["webhooks".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            },
            t0 + 5_000,
        )
        .expect("lease replayed job")
        .expect("replayed job available");
    assert_eq!(re_leased.id, job.id);
    assert_eq!(re_leased.attempts, 1);

    let recovered = engine
        .complete_job_at(
            &job.id,
            "worker-recovery",
            json!({ "status_code": 200 }),
            t0 + 5_500,
        )
        .expect("complete replayed job");
    assert_eq!(recovered.job.status, JobStatus::Completed);
}

#[test]
fn test_heartbeat_and_expired_lease_reaping() {
    let engine = create_memory_engine();
    let t0: i64 = 1_760_100_000_000;

    let alive_job = engine
        .enqueue_job_at(
            EnqueueJobRequest {
                queue: Some("jobs".to_string()),
                capability_tags: vec![],
                payload_json: json!({ "kind": "long_running" }),
                idempotency_key: None,
                max_retries: Some(3),
                backoff_base_ms: None,
                priority: Some(10),
                run_after: None,
                mission_id: None,
                node_key: None,
                depends_on: vec![],
            },
            t0,
        )
        .expect("enqueue alive")
        .job;

    let zombie_job = engine
        .enqueue_job_at(
            EnqueueJobRequest {
                queue: Some("jobs".to_string()),
                capability_tags: vec![],
                payload_json: json!({ "kind": "crashed_worker" }),
                idempotency_key: None,
                max_retries: Some(3),
                backoff_base_ms: None,
                priority: Some(5),
                run_after: None,
                mission_id: None,
                node_key: None,
                depends_on: vec![],
            },
            t0,
        )
        .expect("enqueue zombie")
        .job;

    let leased_alive = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-alive".to_string(),
                queues: vec!["jobs".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 10,
            },
            t0,
        )
        .expect("lease alive")
        .expect("alive available");
    assert_eq!(leased_alive.id, alive_job.id);

    let leased_zombie = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-zombie".to_string(),
                queues: vec!["jobs".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 10,
            },
            t0,
        )
        .expect("lease zombie")
        .expect("zombie available");
    assert_eq!(leased_zombie.id, zombie_job.id);

    // Wrong worker cannot heartbeat alive_job
    let hijack_err = engine
        .heartbeat_job_at(&alive_job.id, "imposter-worker", 20, t0 + 5_000)
        .expect_err("wrong worker heartbeat must fail");
    assert!(matches!(hijack_err, MissionError::LeaseMismatch(_)));

    // Alive worker extends lease by 20s -> expires at t0 + 30s
    let renewed = engine
        .heartbeat_job_at(&alive_job.id, "worker-alive", 20, t0 + 5_000)
        .expect("heartbeat alive");
    assert_eq!(
        renewed
            .lease_expires_at
            .expect("expiry present")
            .timestamp_millis(),
        t0 + 30_000
    );

    // At t0 + 15s, zombie_job is expired (< t0 + 15s), alive_job is still valid
    let reap_report = engine
        .reap_expired_leases_at(t0 + 15_000)
        .expect("reap expired leases");
    assert_eq!(reap_report.reaped_count, 1);
    assert_eq!(reap_report.requeued_job_ids, vec![zombie_job.id.clone()]);
    assert!(reap_report.dlq_job_ids.is_empty());

    let still_alive = engine.get_job(&alive_job.id).expect("reload alive");
    assert_eq!(still_alive.status, JobStatus::Leased);
    assert_eq!(still_alive.leased_by.as_deref(), Some("worker-alive"));

    // Alive worker finishes its extended job cleanly
    let finished_alive = engine
        .complete_job_at(
            &alive_job.id,
            "worker-alive",
            json!({ "completed": true }),
            t0 + 16_000,
        )
        .expect("complete alive job");
    assert_eq!(finished_alive.job.status, JobStatus::Completed);

    // Healthy worker can now pick up re-queued zombie_job
    let rescued = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-rescuer".to_string(),
                queues: vec!["jobs".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 30,
            },
            t0 + 15_000,
        )
        .expect("rescue lease")
        .expect("zombie job re-queued");
    assert_eq!(rescued.id, zombie_job.id);
    assert_eq!(rescued.attempts, 2);

    // If a poison-pill job expires again on its final attempt (attempt 3 of 3), reaper sends it to DLQ
    let _ = engine
        .reap_expired_leases_at(t0 + 50_000)
        .expect("second expiry");
    let final_lease = engine
        .lease_next_job_at(
            LeaseJobRequest {
                worker_id: "worker-crash-again".to_string(),
                queues: vec!["jobs".to_string()],
                capability_tags: vec![],
                lease_ttl_secs: 5,
            },
            t0 + 50_000,
        )
        .expect("lease attempt 3")
        .expect("available for attempt 3");
    assert_eq!(final_lease.id, zombie_job.id);
    assert_eq!(final_lease.attempts, 3);

    let poison_reap = engine
        .reap_expired_leases_at(t0 + 60_000)
        .expect("reap poison pill");
    assert_eq!(poison_reap.dlq_job_ids, vec![zombie_job.id.clone()]);
    assert_eq!(
        engine.get_job(&zombie_job.id).expect("reload dlq").status,
        JobStatus::Dlq
    );
}

#[tokio::test]
async fn test_wall_clock_expired_lease_reaping() {
    let engine = create_memory_engine();
    let enqueued = engine
        .enqueue_job(EnqueueJobRequest {
            queue: Some("realtime".to_string()),
            capability_tags: vec![],
            payload_json: json!({ "kind": "ttl-test" }),
            idempotency_key: None,
            max_retries: Some(3),
            backoff_base_ms: None,
            priority: None,
            run_after: None,
            mission_id: None,
            node_key: None,
            depends_on: vec![],
        })
        .expect("enqueue realtime job")
        .job;

    let leased = engine
        .lease_next_job(LeaseJobRequest {
            worker_id: "short-lived-worker".to_string(),
            queues: vec!["realtime".to_string()],
            capability_tags: vec![],
            lease_ttl_secs: 1,
        })
        .expect("lease realtime job")
        .expect("job leased");
    assert_eq!(leased.id, enqueued.id);

    tokio::time::sleep(std::time::Duration::from_millis(1_150)).await;

    let reap_report = engine.reap_expired_leases().expect("wall-clock reap");
    assert_eq!(reap_report.reaped_count, 1);
    assert_eq!(reap_report.requeued_job_ids, vec![enqueued.id.clone()]);

    let reloaded = engine.get_job(&enqueued.id).expect("reload reaped job");
    assert_eq!(reloaded.status, JobStatus::Queued);
    assert!(reloaded.leased_by.is_none());
}

#[test]
fn test_cron_schedule_dispatch_and_outbox_ack() {
    let engine = create_memory_engine();
    let created_at = Utc.with_ymd_and_hms(2026, 11, 26, 10, 2, 30).unwrap();

    let schedule = engine
        .create_cron_schedule_at(
            CreateCronScheduleRequest {
                name: "five-min-heartbeat".to_string(),
                cron_expr: "*/5 * * * *".to_string(),
                queue: Some("cron".to_string()),
                capability_tags: vec!["scheduler".to_string()],
                payload_json: json!({ "sweep": true }),
                max_retries: Some(2),
                enabled: true,
            },
            created_at.timestamp_millis(),
        )
        .expect("create cron schedule");

    assert_eq!(
        schedule.next_run_at,
        Utc.with_ymd_and_hms(2026, 11, 26, 10, 5, 0).unwrap()
    );

    // Before due time -> nothing dispatched
    let before_due = Utc.with_ymd_and_hms(2026, 11, 26, 10, 4, 59).unwrap();
    let early = engine
        .dispatch_due_crons_at(before_due.timestamp_millis())
        .expect("early cron tick");
    assert!(early.dispatched_job_ids.is_empty());

    // At 10:05:00 -> 1 job dispatched
    let due_time = Utc.with_ymd_and_hms(2026, 11, 26, 10, 5, 0).unwrap();
    let due = engine
        .dispatch_due_crons_at(due_time.timestamp_millis())
        .expect("due cron tick");
    assert_eq!(due.dispatched_job_ids.len(), 1);

    // Subsequent tick at 10:05:10 -> no duplicate dispatch; next_run_at moved to 10:10:00
    let after_due = Utc.with_ymd_and_hms(2026, 11, 26, 10, 5, 10).unwrap();
    let repeat = engine
        .dispatch_due_crons_at(after_due.timestamp_millis())
        .expect("repeat cron tick");
    assert!(repeat.dispatched_job_ids.is_empty());

    let schedules = engine.list_cron_schedules().expect("list crons");
    assert_eq!(
        schedules[0].next_run_at,
        Utc.with_ymd_and_hms(2026, 11, 26, 10, 10, 0).unwrap()
    );

    // Verify transactional outbox recorded cron.dispatched and can be acknowledged
    let unpublished = engine
        .list_outbox_events(true, None)
        .expect("list unpublished outbox");
    assert!(
        unpublished
            .iter()
            .any(|evt| evt.event_type == "cron.dispatched")
    );
    let first_evt_id = unpublished[0].event_id.clone();
    let acked = engine.ack_outbox_event(&first_evt_id).expect("ack outbox");
    assert!(acked.published);
    assert!(acked.published_at.is_some());
}

#[test]
fn test_sqlite_wal_file_persistence() {
    let temp_dir = TempDir::new().expect("create tempdir");
    let db_path = temp_dir.path().join("mission-wal.db");

    let job_id = {
        let db = Database::open(&db_path).expect("open file db");
        let journal_mode: String = db
            .with_read(|conn| {
                conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))
                    .map_err(MissionError::Database)
            })
            .expect("read journal_mode");
        assert_eq!(journal_mode.to_lowercase(), "wal");

        let engine = MissionEngine::new(db);
        engine
            .enqueue_job(EnqueueJobRequest {
                queue: Some("durable".to_string()),
                capability_tags: vec!["ssd".to_string()],
                payload_json: json!({ "persisted": true }),
                idempotency_key: Some("durable-key-1".to_string()),
                max_retries: Some(3),
                backoff_base_ms: None,
                priority: None,
                run_after: None,
                mission_id: None,
                node_key: None,
                depends_on: vec![],
            })
            .expect("enqueue durable job")
            .job
            .id
    };

    // Reopen database and confirm durable state
    let reopened = Database::open(&db_path).expect("reopen file db");
    let engine = MissionEngine::new(reopened);
    let loaded = engine.get_job(&job_id).expect("reload persisted job");
    assert_eq!(loaded.queue, "durable");
    assert_eq!(loaded.payload_json["persisted"], json!(true));
}

#[tokio::test]
async fn test_http_api_end_to_end() {
    let engine = create_memory_engine();
    let router = build_router(engine);

    // 1. Health endpoint
    let (status, health_body) = send_json_request(&router, Method::GET, "/health", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health_body["status"], json!("ok"));
    assert_eq!(health_body["service"], json!("mw-mission"));

    // 2. Reject cyclic mission with 400
    let (cycle_status, cycle_body) = send_json_request(
        &router,
        Method::POST,
        "/api/missions",
        Some(json!({
            "name": "bad-dag",
            "jobs": [
                { "key": "x", "depends_on": ["y"] },
                { "key": "y", "depends_on": ["x"] }
            ]
        })),
    )
    .await;
    assert_eq!(cycle_status, StatusCode::BAD_REQUEST);
    assert_eq!(cycle_body["error"], json!("cycle_detected"));

    // 3. Create valid DAG mission
    let (mission_status, mission_json) = send_json_request(
        &router,
        Method::POST,
        "/api/missions",
        Some(json!({
            "name": "http-dag",
            "jobs": [
                { "key": "fetch", "queue": "http-q", "capability_tags": ["net"] },
                { "key": "store", "queue": "http-q", "capability_tags": ["db"], "depends_on": ["fetch"] }
            ]
        })),
    )
    .await;
    assert_eq!(mission_status, StatusCode::CREATED);
    let created_mission: MissionDetail =
        serde_json::from_value(mission_json).expect("deserialize mission detail");
    assert_eq!(created_mission.summary.queued, 1);
    assert_eq!(created_mission.summary.waiting_deps, 1);

    // 4. GET /api/missions/{id}
    let (get_mission_status, fetched_json) = send_json_request(
        &router,
        Method::GET,
        &format!("/api/missions/{}", created_mission.mission.id),
        None,
    )
    .await;
    assert_eq!(get_mission_status, StatusCode::OK);
    assert_eq!(fetched_json["mission"]["name"], json!("http-dag"));

    // 5. Lease, heartbeat, complete fetch -> unblocks store
    let (lease_status, lease_json) = send_json_request(
        &router,
        Method::POST,
        "/api/workers/lease",
        Some(json!({
            "worker_id": "http-worker-1",
            "queues": ["http-q"],
            "capability_tags": ["net", "db"],
            "lease_ttl_secs": 30
        })),
    )
    .await;
    assert_eq!(lease_status, StatusCode::OK);
    let fetch_job: JobRecord =
        serde_json::from_value(lease_json["job"].clone()).expect("deserialize leased job");
    assert_eq!(fetch_job.node_key.as_deref(), Some("fetch"));

    let (hb_status, _) = send_json_request(
        &router,
        Method::POST,
        &format!("/api/jobs/{}/heartbeat", fetch_job.id),
        Some(json!({ "worker_id": "http-worker-1", "extend_secs": 45 })),
    )
    .await;
    assert_eq!(hb_status, StatusCode::OK);

    let (complete_status, complete_json) = send_json_request(
        &router,
        Method::POST,
        &format!("/api/jobs/{}/complete", fetch_job.id),
        Some(json!({ "worker_id": "http-worker-1", "result_json": { "bytes": 512 } })),
    )
    .await;
    assert_eq!(complete_status, StatusCode::OK);
    let complete_outcome: CompleteJobOutcome =
        serde_json::from_value(complete_json).expect("deserialize complete outcome");
    assert_eq!(complete_outcome.promoted_job_ids.len(), 1);

    // 6. Enqueue standalone job with idempotency_key + verify dedup status codes
    let (enq_status_1, enq_body_1) = send_json_request(
        &router,
        Method::POST,
        "/api/jobs",
        Some(json!({
            "queue": "dlq-test",
            "idempotency_key": "http-idem-1",
            "max_retries": 1,
            "payload_json": { "op": "flaky" }
        })),
    )
    .await;
    assert_eq!(enq_status_1, StatusCode::CREATED);
    assert_eq!(enq_body_1["deduplicated"], json!(false));

    let (enq_status_2, enq_body_2) = send_json_request(
        &router,
        Method::POST,
        "/api/jobs",
        Some(json!({
            "queue": "dlq-test",
            "idempotency_key": "http-idem-1",
            "max_retries": 1,
            "payload_json": { "op": "flaky" }
        })),
    )
    .await;
    assert_eq!(enq_status_2, StatusCode::OK);
    assert_eq!(enq_body_2["deduplicated"], json!(true));
    assert_eq!(enq_body_2["job"]["id"], enq_body_1["job"]["id"]);

    // 7. Lease flaky job, fail into DLQ, inspect GET /api/dlq, replay via POST /api/dlq/{id}/replay
    let (_, flaky_lease) = send_json_request(
        &router,
        Method::POST,
        "/api/workers/lease",
        Some(json!({
            "worker_id": "flaky-worker",
            "queues": ["dlq-test"],
            "capability_tags": [],
            "lease_ttl_secs": 30
        })),
    )
    .await;
    let flaky_id = flaky_lease["job"]["id"].as_str().expect("flaky id").to_string();

    let (fail_status, fail_json) = send_json_request(
        &router,
        Method::POST,
        &format!("/api/jobs/{flaky_id}/fail"),
        Some(json!({ "worker_id": "flaky-worker", "error_msg": "boom" })),
    )
    .await;
    assert_eq!(fail_status, StatusCode::OK);
    let fail_outcome: FailJobOutcome =
        serde_json::from_value(fail_json).expect("deserialize fail outcome");
    assert!(fail_outcome.moved_to_dlq);

    let (dlq_status, dlq_json) = send_json_request(&router, Method::GET, "/api/dlq", None).await;
    assert_eq!(dlq_status, StatusCode::OK);
    assert_eq!(dlq_json["entries"].as_array().expect("entries").len(), 1);

    let (replay_status, replay_json) = send_json_request(
        &router,
        Method::POST,
        &format!("/api/dlq/{flaky_id}/replay"),
        None,
    )
    .await;
    assert_eq!(replay_status, StatusCode::OK);
    assert_eq!(replay_json["status"], json!("queued"));

    // 8. GET /api/jobs and GET /api/stats
    let (list_status, list_json) =
        send_json_request(&router, Method::GET, "/api/jobs?queue=http-q", None).await;
    assert_eq!(list_status, StatusCode::OK);
    assert_eq!(list_json["jobs"].as_array().expect("jobs array").len(), 2);

    let (stats_status, stats_json) =
        send_json_request(&router, Method::GET, "/api/stats", None).await;
    assert_eq!(stats_status, StatusCode::OK);
    let stats: BrokerStats = serde_json::from_value(stats_json).expect("deserialize stats");
    assert_eq!(stats.total_jobs, 3);
    assert_eq!(stats.jobs_completed, 1);
    assert_eq!(stats.jobs_queued, 2);
    assert_eq!(stats.active_dlq_entries, 0);

    // 9. Cron CRUD + outbox + manual reaper endpoints
    let (cron_status, cron_json) = send_json_request(
        &router,
        Method::POST,
        "/api/crons",
        Some(json!({
            "name": "nightly-cleanup",
            "cron_expr": "0 3 * * *",
            "queue": "maintenance"
        })),
    )
    .await;
    assert_eq!(cron_status, StatusCode::CREATED);
    let cron_id = cron_json["id"].as_str().expect("cron id");

    let (list_crons_status, list_crons_json) =
        send_json_request(&router, Method::GET, "/api/crons", None).await;
    assert_eq!(list_crons_status, StatusCode::OK);
    assert_eq!(
        list_crons_json["schedules"]
            .as_array()
            .expect("schedules")
            .len(),
        1
    );

    let (del_cron_status, _) = send_json_request(
        &router,
        Method::DELETE,
        &format!("/api/crons/{cron_id}"),
        None,
    )
    .await;
    assert_eq!(del_cron_status, StatusCode::OK);

    let (reap_status, reap_json) =
        send_json_request(&router, Method::POST, "/api/reaper/run", None).await;
    assert_eq!(reap_status, StatusCode::OK);
    assert_eq!(reap_json["reaped_count"], json!(0));

    let (outbox_status, outbox_json) =
        send_json_request(&router, Method::GET, "/api/outbox", None).await;
    assert_eq!(outbox_status, StatusCode::OK);
    let events = outbox_json["events"].as_array().expect("outbox events");
    assert!(!events.is_empty());
    let evt_id = events[0]["event_id"].as_str().expect("event_id");

    let (ack_status, ack_json) = send_json_request(
        &router,
        Method::POST,
        &format!("/api/outbox/{evt_id}/ack"),
        None,
    )
    .await;
    assert_eq!(ack_status, StatusCode::OK);
    assert_eq!(ack_json["published"], json!(true));
}
