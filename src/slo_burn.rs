use crate::db::{Database, millis_to_utc};
use crate::error::MissionError;
use chrono::{DateTime, Utc};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tracing::warn;
use uuid::Uuid;

pub const DEFAULT_SLO_TARGET: f64 = 0.999; // 99.9% SLO -> 0.1% error budget
pub const DEFAULT_FAST_SHORT_WINDOW_MS: i64 = 5 * 60 * 1_000; // 5m
pub const DEFAULT_FAST_LONG_WINDOW_MS: i64 = 60 * 60 * 1_000; // 1h
pub const DEFAULT_SLOW_SHORT_WINDOW_MS: i64 = 30 * 60 * 1_000; // 30m
pub const DEFAULT_SLOW_LONG_WINDOW_MS: i64 = 6 * 60 * 60 * 1_000; // 6h
pub const DEFAULT_FAST_BURN_THRESHOLD: f64 = 14.4;
pub const DEFAULT_SLOW_BURN_THRESHOLD: f64 = 6.0;
pub const DEFAULT_MIN_WINDOW_EVENTS: u64 = 5;

const SLO_SCHEMA_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS slo_events (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    job_id TEXT,
    is_error INTEGER NOT NULL CHECK(is_error IN (0, 1)),
    recorded_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_slo_events_window
    ON slo_events(queue, recorded_at_ms DESC, is_error);
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SloBurnSeverity {
    Healthy,
    SlowBurnWarning,
    FastBurnCritical,
}

impl SloBurnSeverity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::SlowBurnWarning => "slow_burn_warning",
            Self::FastBurnCritical => "fast_burn_critical",
        }
    }

    pub fn is_alerting(self) -> bool {
        !matches!(self, Self::Healthy)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloPolicyConfig {
    pub slo_target: f64,
    pub fast_short_window_ms: i64,
    pub fast_long_window_ms: i64,
    pub slow_short_window_ms: i64,
    pub slow_long_window_ms: i64,
    pub fast_burn_threshold: f64,
    pub slow_burn_threshold: f64,
    pub min_window_events: u64,
}

impl Default for SloPolicyConfig {
    fn default() -> Self {
        Self {
            slo_target: DEFAULT_SLO_TARGET,
            fast_short_window_ms: DEFAULT_FAST_SHORT_WINDOW_MS,
            fast_long_window_ms: DEFAULT_FAST_LONG_WINDOW_MS,
            slow_short_window_ms: DEFAULT_SLOW_SHORT_WINDOW_MS,
            slow_long_window_ms: DEFAULT_SLOW_LONG_WINDOW_MS,
            fast_burn_threshold: DEFAULT_FAST_BURN_THRESHOLD,
            slow_burn_threshold: DEFAULT_SLOW_BURN_THRESHOLD,
            min_window_events: DEFAULT_MIN_WINDOW_EVENTS,
        }
    }
}

impl SloPolicyConfig {
    pub fn validate(&self) -> Result<(), MissionError> {
        if !(0.5..1.0).contains(&self.slo_target) {
            return Err(MissionError::Validation(
                "slo_target must be strictly between 0.5 and 1.0".to_string(),
            ));
        }
        if self.fast_short_window_ms <= 0
            || self.fast_long_window_ms <= 0
            || self.slow_short_window_ms <= 0
            || self.slow_long_window_ms <= 0
        {
            return Err(MissionError::Validation(
                "all SLO window durations must be positive".to_string(),
            ));
        }
        if self.fast_burn_threshold <= 0.0 || self.slow_burn_threshold <= 0.0 {
            return Err(MissionError::Validation(
                "burn rate thresholds must be positive".to_string(),
            ));
        }
        Ok(())
    }

    pub fn error_budget_fraction(&self) -> f64 {
        (1.0 - self.slo_target).max(1e-9)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloWindowMetrics {
    pub window_ms: i64,
    pub total_events: u64,
    pub error_events: u64,
    pub error_rate: f64,
    pub burn_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloEvaluationReport {
    pub queue: String,
    pub evaluated_at: DateTime<Utc>,
    pub slo_target: f64,
    pub error_budget_fraction: f64,
    pub severity: SloBurnSeverity,
    pub fast_burn_triggered: bool,
    pub slow_burn_triggered: bool,
    pub fast_short: SloWindowMetrics,
    pub fast_long: SloWindowMetrics,
    pub slow_short: SloWindowMetrics,
    pub slow_long: SloWindowMetrics,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RecordSloOutcomeRequest {
    pub queue: Option<String>,
    pub job_id: Option<String>,
    pub is_error: bool,
    pub recorded_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MwEchoAlertWebhookPayload {
    pub event: String,
    pub source: String,
    pub queue: String,
    pub severity: SloBurnSeverity,
    pub slo_target: f64,
    pub fast_burn_triggered: bool,
    pub slow_burn_triggered: bool,
    pub summary: String,
    pub windows: Value,
    pub evaluated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SloAlertDeliveryOutcome {
    pub report: SloEvaluationReport,
    pub webhook_dispatched: bool,
    pub webhook_status: Option<u16>,
    pub payload: Option<MwEchoAlertWebhookPayload>,
}

#[derive(Clone)]
pub struct SloBurnDetector {
    database: Database,
    policy: SloPolicyConfig,
    echo_webhook_url: Option<String>,
    echo_webhook_token: Option<String>,
    http_client: reqwest::Client,
}

impl SloBurnDetector {
    pub fn new(database: Database) -> Result<Self, MissionError> {
        let echo_webhook_url = std::env::var("MW_MISSION_ECHO_WEBHOOK_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let echo_webhook_token = std::env::var("MW_MISSION_ECHO_WEBHOOK_TOKEN")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Self::with_config(
            database,
            SloPolicyConfig::default(),
            echo_webhook_url,
            echo_webhook_token,
        )
    }

    pub fn with_config(
        database: Database,
        policy: SloPolicyConfig,
        echo_webhook_url: Option<String>,
        echo_webhook_token: Option<String>,
    ) -> Result<Self, MissionError> {
        policy.validate()?;
        database.with_write_tx(|tx| {
            tx.execute_batch(SLO_SCHEMA_SQL)?;
            Ok(())
        })?;

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();

        Ok(Self {
            database,
            policy,
            echo_webhook_url: echo_webhook_url
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            echo_webhook_token: echo_webhook_token
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
            http_client,
        })
    }

    pub fn policy(&self) -> &SloPolicyConfig {
        &self.policy
    }

    pub fn record_outcome(
        &self,
        queue: &str,
        job_id: Option<&str>,
        is_error: bool,
    ) -> Result<String, MissionError> {
        self.record_outcome_at(queue, job_id, is_error, Utc::now().timestamp_millis())
    }

    pub fn record_outcome_at(
        &self,
        queue: &str,
        job_id: Option<&str>,
        is_error: bool,
        now_ms: i64,
    ) -> Result<String, MissionError> {
        let normalized_queue = normalize_slo_queue(queue)?;
        let normalized_job_id = job_id
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToOwned::to_owned);
        let event_id = Uuid::new_v4().to_string();

        self.database.with_write_tx(|tx| {
            tx.execute(
                "INSERT INTO slo_events (id, queue, job_id, is_error, recorded_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    event_id,
                    normalized_queue,
                    normalized_job_id,
                    i32::from(is_error),
                    now_ms,
                ],
            )?;
            Ok(event_id)
        })
    }

    pub fn prune_older_than(&self, cutoff_ms: i64) -> Result<usize, MissionError> {
        self.database.with_write_tx(|tx| {
            let deleted = tx.execute(
                "DELETE FROM slo_events WHERE recorded_at_ms < ?1",
                params![cutoff_ms],
            )?;
            Ok(deleted)
        })
    }

    pub fn evaluate(&self, queue: Option<&str>) -> Result<SloEvaluationReport, MissionError> {
        self.evaluate_at(queue, Utc::now().timestamp_millis())
    }

    pub fn evaluate_at(
        &self,
        queue: Option<&str>,
        now_ms: i64,
    ) -> Result<SloEvaluationReport, MissionError> {
        let normalized_filter = match queue.map(str::trim).filter(|q| !q.is_empty() && *q != "*") {
            Some(q) => Some(normalize_slo_queue(q)?),
            None => None,
        };
        let queue_label = normalized_filter
            .clone()
            .unwrap_or_else(|| "*".to_string());
        let budget = self.policy.error_budget_fraction();

        let fast_short = self.compute_window_metrics(
            normalized_filter.as_deref(),
            now_ms,
            self.policy.fast_short_window_ms,
            budget,
        )?;
        let fast_long = self.compute_window_metrics(
            normalized_filter.as_deref(),
            now_ms,
            self.policy.fast_long_window_ms,
            budget,
        )?;
        let slow_short = self.compute_window_metrics(
            normalized_filter.as_deref(),
            now_ms,
            self.policy.slow_short_window_ms,
            budget,
        )?;
        let slow_long = self.compute_window_metrics(
            normalized_filter.as_deref(),
            now_ms,
            self.policy.slow_long_window_ms,
            budget,
        )?;

        let min_events = self.policy.min_window_events;
        let fast_burn_triggered = fast_short.total_events >= min_events
            && fast_long.total_events >= min_events
            && fast_short.burn_rate >= self.policy.fast_burn_threshold
            && fast_long.burn_rate >= self.policy.fast_burn_threshold;

        let slow_burn_triggered = slow_short.total_events >= min_events
            && slow_long.total_events >= min_events
            && slow_short.burn_rate >= self.policy.slow_burn_threshold
            && slow_long.burn_rate >= self.policy.slow_burn_threshold;

        let severity = if fast_burn_triggered {
            SloBurnSeverity::FastBurnCritical
        } else if slow_burn_triggered {
            SloBurnSeverity::SlowBurnWarning
        } else {
            SloBurnSeverity::Healthy
        };

        Ok(SloEvaluationReport {
            queue: queue_label,
            evaluated_at: millis_to_utc(now_ms)?,
            slo_target: self.policy.slo_target,
            error_budget_fraction: budget,
            severity,
            fast_burn_triggered,
            slow_burn_triggered,
            fast_short,
            fast_long,
            slow_short,
            slow_long,
        })
    }

    pub fn build_echo_payload(&self, report: &SloEvaluationReport) -> MwEchoAlertWebhookPayload {
        let summary = match report.severity {
            SloBurnSeverity::FastBurnCritical => format!(
                "[CRITICAL] SLO fast error-budget burn on queue '{}' (short={:.2}x, long={:.2}x >= {:.2}x threshold)",
                report.queue,
                report.fast_short.burn_rate,
                report.fast_long.burn_rate,
                self.policy.fast_burn_threshold
            ),
            SloBurnSeverity::SlowBurnWarning => format!(
                "[WARNING] SLO slow error-budget burn on queue '{}' (short={:.2}x, long={:.2}x >= {:.2}x threshold)",
                report.queue,
                report.slow_short.burn_rate,
                report.slow_long.burn_rate,
                self.policy.slow_burn_threshold
            ),
            SloBurnSeverity::Healthy => format!(
                "[HEALTHY] SLO error-budget within bounds on queue '{}'",
                report.queue
            ),
        };

        MwEchoAlertWebhookPayload {
            event: "slo.burn_rate.alert".to_string(),
            source: "mw-mission".to_string(),
            queue: report.queue.clone(),
            severity: report.severity,
            slo_target: report.slo_target,
            fast_burn_triggered: report.fast_burn_triggered,
            slow_burn_triggered: report.slow_burn_triggered,
            summary,
            windows: json!({
                "fast_short": report.fast_short,
                "fast_long": report.fast_long,
                "slow_short": report.slow_short,
                "slow_long": report.slow_long,
            }),
            evaluated_at: report.evaluated_at,
        }
    }

    pub async fn evaluate_and_alert(
        &self,
        queue: Option<&str>,
    ) -> Result<SloAlertDeliveryOutcome, MissionError> {
        self.evaluate_and_alert_at(queue, Utc::now().timestamp_millis())
            .await
    }

    pub async fn evaluate_and_alert_at(
        &self,
        queue: Option<&str>,
        now_ms: i64,
    ) -> Result<SloAlertDeliveryOutcome, MissionError> {
        let report = self.evaluate_at(queue, now_ms)?;
        if !report.severity.is_alerting() {
            return Ok(SloAlertDeliveryOutcome {
                report,
                webhook_dispatched: false,
                webhook_status: None,
                payload: None,
            });
        }

        let payload = self.build_echo_payload(&report);
        let Some(ref webhook_url) = self.echo_webhook_url else {
            return Ok(SloAlertDeliveryOutcome {
                report,
                webhook_dispatched: false,
                webhook_status: None,
                payload: Some(payload),
            });
        };

        let mut request_builder = self
            .http_client
            .post(webhook_url)
            .header("Content-Type", "application/json")
            .header("X-MW-Mission-Event", "slo.burn_rate.alert")
            .header("X-MW-SLO-Severity", report.severity.as_str())
            .json(&payload);

        if let Some(ref token) = self.echo_webhook_token {
            request_builder = request_builder.header("Authorization", format!("Bearer {token}"));
        }

        match request_builder.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                Ok(SloAlertDeliveryOutcome {
                    report,
                    webhook_dispatched: resp.status().is_success(),
                    webhook_status: Some(status),
                    payload: Some(payload),
                })
            }
            Err(err) => {
                warn!("failed to deliver SLO burn alert to mw-echo webhook: {err}");
                Ok(SloAlertDeliveryOutcome {
                    report,
                    webhook_dispatched: false,
                    webhook_status: None,
                    payload: Some(payload),
                })
            }
        }
    }

    fn compute_window_metrics(
        &self,
        queue: Option<&str>,
        now_ms: i64,
        window_ms: i64,
        budget_fraction: f64,
    ) -> Result<SloWindowMetrics, MissionError> {
        let cutoff_ms = now_ms.saturating_sub(window_ms);
        let (total_events, error_events): (u64, u64) = self.database.with_read(|conn| {
            if let Some(q) = queue {
                conn.query_row(
                    "SELECT COUNT(*), COALESCE(SUM(is_error), 0)
                     FROM slo_events
                     WHERE queue = ?1 AND recorded_at_ms >= ?2 AND recorded_at_ms <= ?3",
                    params![q, cutoff_ms, now_ms],
                    |row| {
                        let total: i64 = row.get(0)?;
                        let errs: i64 = row.get(1)?;
                        Ok((total.max(0) as u64, errs.max(0) as u64))
                    },
                )
                .map_err(MissionError::from)
            } else {
                conn.query_row(
                    "SELECT COUNT(*), COALESCE(SUM(is_error), 0)
                     FROM slo_events
                     WHERE recorded_at_ms >= ?1 AND recorded_at_ms <= ?2",
                    params![cutoff_ms, now_ms],
                    |row| {
                        let total: i64 = row.get(0)?;
                        let errs: i64 = row.get(1)?;
                        Ok((total.max(0) as u64, errs.max(0) as u64))
                    },
                )
                .map_err(MissionError::from)
            }
        })?;

        let error_rate = if total_events == 0 {
            0.0
        } else {
            (error_events as f64) / (total_events as f64)
        };
        let burn_rate = error_rate / budget_fraction;

        Ok(SloWindowMetrics {
            window_ms,
            total_events,
            error_events,
            error_rate,
            burn_rate,
        })
    }
}

fn normalize_slo_queue(raw: &str) -> Result<String, MissionError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return Err(MissionError::Validation(
            "queue name must be 1..128 characters".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;

    #[test]
    fn test_multi_window_fast_and_slow_burn_detection() {
        let db = Database::open_in_memory().expect("open db");
        // Use 99.0% SLO -> 1% error budget (0.01) so burn rates are clean integers:
        // 15% error rate -> 15.0x burn rate (>= 14.4x fast burn)
        // 8% error rate  -> 8.0x burn rate (>= 6.0x slow burn, < 14.4x fast burn)
        // 0.5% error rate -> 0.5x burn rate (healthy)
        let mut policy = SloPolicyConfig::default();
        policy.slo_target = 0.99;
        policy.min_window_events = 10;

        let detector =
            SloBurnDetector::with_config(db, policy, None, None).expect("init detector");
        let t0: i64 = 1_760_000_000_000;

        // 1. Healthy baseline on queue "ingest": 100 events, 0 errors across 1h
        for idx in 0..100 {
            detector
                .record_outcome_at("ingest", Some(&format!("job-{idx}")), false, t0 - idx * 1_000)
                .expect("record healthy");
        }
        let healthy_report = detector
            .evaluate_at(Some("ingest"), t0)
            .expect("evaluate healthy");
        assert_eq!(healthy_report.severity, SloBurnSeverity::Healthy);
        assert!(!healthy_report.fast_burn_triggered);
        assert!(!healthy_report.slow_burn_triggered);
        assert_eq!(healthy_report.fast_short.burn_rate, 0.0);

        // 2. Slow burn on queue "billing": 8 errors out of 100 events spread over 30m (8% error rate -> 8.0x burn)
        for idx in 0..100 {
            let is_err = idx < 8;
            let ts = t0 - (idx as i64) * 10_000; // within 1000s (< 30m and < 6h)
            detector
                .record_outcome_at("billing", None, is_err, ts)
                .expect("record slow burn");
        }
        let slow_report = detector
            .evaluate_at(Some("billing"), t0)
            .expect("evaluate slow burn");
        assert_eq!(slow_report.severity, SloBurnSeverity::SlowBurnWarning);
        assert!(!slow_report.fast_burn_triggered);
        assert!(slow_report.slow_burn_triggered);
        assert!((slow_report.slow_short.burn_rate - 8.0).abs() < 1e-6);
        assert!((slow_report.slow_long.burn_rate - 8.0).abs() < 1e-6);

        // 3. Fast burn on queue "payments": 20 errors out of 100 events in last 4m (20% error rate -> 20.0x burn)
        for idx in 0..100 {
            let is_err = idx < 20;
            let ts = t0 - (idx as i64) * 2_000; // within 200s (< 5m and < 1h)
            detector
                .record_outcome_at("payments", None, is_err, ts)
                .expect("record fast burn");
        }
        let fast_report = detector
            .evaluate_at(Some("payments"), t0)
            .expect("evaluate fast burn");
        assert_eq!(fast_report.severity, SloBurnSeverity::FastBurnCritical);
        assert!(fast_report.fast_burn_triggered);
        assert!((fast_report.fast_short.burn_rate - 20.0).abs() < 1e-6);
        assert!((fast_report.fast_long.burn_rate - 20.0).abs() < 1e-6);
    }

    #[test]
    fn test_multi_window_prevents_false_alarm_after_short_window_recovers() {
        let db = Database::open_in_memory().expect("open db");
        let mut policy = SloPolicyConfig::default();
        policy.slo_target = 0.99; // 1% error budget
        policy.min_window_events = 10;

        let detector =
            SloBurnDetector::with_config(db, policy, None, None).expect("init detector");
        let t0: i64 = 1_760_000_000_000;

        // Burst of 20 errors out of 50 events happened 20 minutes ago (outside 5m fast_short, inside 1h fast_long)
        let burst_base = t0 - 20 * 60 * 1_000;
        for idx in 0..50 {
            detector
                .record_outcome_at("search", None, idx < 20, burst_base - idx * 1_000)
                .expect("record old burst");
        }
        // In the last 5 minutes, 50 events with 0 errors (recovered!)
        for idx in 0..50 {
            detector
                .record_outcome_at("search", None, false, t0 - idx * 1_000)
                .expect("record recovery");
        }

        let report = detector
            .evaluate_at(Some("search"), t0)
            .expect("evaluate recovered");
        // Long 1h window still has 20/100 = 20x burn, but short 5m window has 0/50 = 0x burn!
        assert!(report.fast_long.burn_rate >= 14.4);
        assert_eq!(report.fast_short.burn_rate, 0.0);
        assert!(!report.fast_burn_triggered);
    }

    #[tokio::test]
    async fn test_mw_echo_alert_webhook_delivery() {
        #[derive(Default)]
        struct CapturedWebhook {
            headers: Vec<(String, String)>,
            body: Option<Value>,
        }

        let captured = Arc::new(Mutex::new(CapturedWebhook::default()));
        let captured_state = captured.clone();

        let app = Router::new()
            .route(
                "/api/echo/webhooks/slo",
                post(
                    |State(store): State<Arc<Mutex<CapturedWebhook>>>,
                     headers: HeaderMap,
                     body: Bytes| async move {
                        let mut guard = store.lock().expect("lock webhook capture");
                        for (k, v) in &headers {
                            if let Ok(val_str) = v.to_str() {
                                guard.headers.push((k.as_str().to_lowercase(), val_str.to_string()));
                            }
                        }
                        guard.body = serde_json::from_slice(&body).ok();
                        StatusCode::ACCEPTED
                    },
                ),
            )
            .with_state(captured_state);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock echo");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let db = Database::open_in_memory().expect("open db");
        let mut policy = SloPolicyConfig::default();
        policy.slo_target = 0.99;
        policy.min_window_events = 5;

        let webhook_url = format!("http://{addr}/api/echo/webhooks/slo");
        let detector = SloBurnDetector::with_config(
            db,
            policy,
            Some(webhook_url),
            Some("echo-secret-token".to_string()),
        )
        .expect("init detector");

        let t0: i64 = 1_760_000_000_000;
        for idx in 0..20 {
            detector
                .record_outcome_at("critical-q", None, idx < 5, t0 - idx * 1_000)
                .expect("record event");
        }

        let outcome = detector
            .evaluate_and_alert_at(Some("critical-q"), t0)
            .await
            .expect("evaluate and alert");

        assert!(outcome.webhook_dispatched);
        assert_eq!(outcome.webhook_status, Some(202));
        assert_eq!(outcome.report.severity, SloBurnSeverity::FastBurnCritical);

        let guard = captured.lock().expect("lock captured");
        let payload = guard.body.as_ref().expect("webhook json received");
        assert_eq!(payload["event"], json!("slo.burn_rate.alert"));
        assert_eq!(payload["source"], json!("mw-mission"));
        assert_eq!(payload["queue"], json!("critical-q"));
        assert_eq!(payload["severity"], json!("fast_burn_critical"));
        assert_eq!(payload["fast_burn_triggered"], json!(true));
        assert!(
            guard
                .headers
                .iter()
                .any(|(k, v)| k == "authorization" && v == "Bearer echo-secret-token")
        );
    }
}
