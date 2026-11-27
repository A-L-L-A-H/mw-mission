# mw-mission — Distributed job, DAG, cron, and transactional outbox broker.

**Repository:** [`A-L-L-A-H/mw-mission`](https://github.com/A-L-L-A-H/mw-mission) · **Stack:** Rust 1.89 · axum · tokio · SQLite WAL

## What it does

Leases jobs to capability-tagged workers with heartbeats, zombie reaping, exponential backoff, DLQ replay, and UTC cron schedules.

## Quick start

```bash
cargo run --release
```

## Verify

```bash
cargo test
```

## Configuration

- **Port:** 4410
- **Key configuration:** MW_MISSION_ADDR · MW_MISSION_DB

```bash
docker build -t mw-mission .
docker run -p 4410:4410 -v mw-mission-data:/data mw-mission
```

## Notes

Reaper and cron intervals are controlled by MW_MISSION_REAP_INTERVAL_SECS and MW_MISSION_CRON_INTERVAL_SECS.
