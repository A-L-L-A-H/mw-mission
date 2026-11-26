# mw-mission

MoonWitness server-class distributed Job, DAG, Cron, and transactional Outbox Queue Broker built in Rust (`axum`, `tokio`, `rusqlite` bundled SQLite WAL).

## Features

- **SQLite WAL Persistence**: Atomic `BEGIN IMMEDIATE` transactions across `missions`, `jobs`, `job_dependencies`, `cron_schedules`, `dlq_entries`, and `outbox_events`.
- **DAG Missions**: Atomic batch creation with Kahn topological cycle detection and automatic `waiting_deps -> queued` fan-out/fan-in promotion upon parent completion.
- **Idempotency Deduplication**: Unique `idempotency_key` enforcement returns the original job without creating duplicates.
- **Capability Tag Routing**: Workers advertise `capability_tags` and only lease ready jobs whose required tags are a subset of the worker's capabilities.
- **Leases, Heartbeats & Zombie Reaping**: Atomic worker leases with TTL renewal (`heartbeat`) and a background Tokio reaper that recovers expired zombie leases.
- **Exponential Backoff & DLQ Replay**: Failed jobs re-queue with exponential backoff (`backoff_base_ms * 2^(attempts-1)`) until `attempts >= max_retries`, then move into `dlq_entries` with one-call replay back to `queued`.
- **Cron Scheduler & Outbox**: 5-field UTC cron schedule dispatch with deterministic per-slot idempotency keys and transactional outbox event recording.

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` | Service health check |
| `GET` | `/api/stats` | Broker queue, mission, DLQ, cron, and outbox metrics |
| `POST` | `/api/missions` | Atomically create a DAG mission (rejects cycles) |
| `GET` | `/api/missions/{id}` | Fetch mission detail, jobs, and status summary |
| `POST` | `/api/jobs` | Enqueue a standalone or dependent job |
| `GET` | `/api/jobs` | List jobs (`?queue=&status=&mission_id=&limit=`) |
| `GET` | `/api/jobs/{id}` | Fetch single job detail |
| `POST` | `/api/workers/lease` | Atomically lease the next eligible job for a worker |
| `POST` | `/api/jobs/{id}/heartbeat` | Extend an active worker lease |
| `POST` | `/api/jobs/{id}/complete` | Complete a leased job and unblock ready DAG children |
| `POST` | `/api/jobs/{id}/fail` | Record failure (exponential retry or move to DLQ) |
| `GET` | `/api/dlq` | List dead-letter queue entries |
| `POST` | `/api/dlq/{id}/replay` | Replay a DLQ job back to `queued` |
| `POST` | `/api/crons` | Create a 5-field UTC cron schedule |
| `GET` | `/api/crons` | List cron schedules |
| `DELETE` | `/api/crons/{id}` | Delete a cron schedule |
| `POST` | `/api/crons/tick` | Trigger immediate dispatch of due cron schedules |
| `GET` | `/api/outbox` | List transactional outbox events |
| `POST` | `/api/outbox/{id}/ack` | Acknowledge an outbox event as published |
| `POST` | `/api/reaper/run` | Trigger an immediate expired-lease reap pass |

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `MW_MISSION_ADDR` | `127.0.0.1:4410` | HTTP bind address (`--addr`) |
| `MW_MISSION_DB` | `./mw-mission.db` | SQLite database path (`--db`) |
| `MW_MISSION_REAP_INTERVAL_SECS` | `5` | Background zombie lease reaper interval (`--reap-interval-secs`) |
| `MW_MISSION_CRON_INTERVAL_SECS` | `15` | Background cron dispatch interval (`--cron-interval-secs`) |

## Build & Test

```bash
cargo test
cargo run --release
```

## Docker

```bash
docker build -t mw-mission .
docker run -p 4410:4410 -v mw-mission-data:/data mw-mission
```
