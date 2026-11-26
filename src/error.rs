use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MissionError {
    #[error("validation error: {0}")]
    Validation(String),

    #[error("dependency cycle detected involving nodes: {0:?}")]
    CycleDetected(Vec<String>),

    #[error("dependency job not found: {0}")]
    DependencyNotFound(String),

    #[error("mission not found: {0}")]
    MissionNotFound(String),

    #[error("job not found: {0}")]
    JobNotFound(String),

    #[error("cron schedule not found: {0}")]
    CronNotFound(String),

    #[error("outbox event not found: {0}")]
    OutboxEventNotFound(String),

    #[error("lease conflict: {0}")]
    LeaseMismatch(String),

    #[error("job is not in DLQ: {0}")]
    NotInDlq(String),

    #[error("duplicate resource: {0}")]
    Conflict(String),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: &'static str,
    message: String,
}

impl MissionError {
    pub fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            Self::Validation(_) => (StatusCode::BAD_REQUEST, "validation_error"),
            Self::CycleDetected(_) => (StatusCode::BAD_REQUEST, "cycle_detected"),
            Self::DependencyNotFound(_) => (StatusCode::BAD_REQUEST, "dependency_not_found"),
            Self::MissionNotFound(_) => (StatusCode::NOT_FOUND, "mission_not_found"),
            Self::JobNotFound(_) => (StatusCode::NOT_FOUND, "job_not_found"),
            Self::CronNotFound(_) => (StatusCode::NOT_FOUND, "cron_not_found"),
            Self::OutboxEventNotFound(_) => (StatusCode::NOT_FOUND, "outbox_event_not_found"),
            Self::LeaseMismatch(_) => (StatusCode::CONFLICT, "lease_mismatch"),
            Self::NotInDlq(_) => (StatusCode::CONFLICT, "not_in_dlq"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::Database(_) => (StatusCode::INTERNAL_SERVER_ERROR, "database_error"),
            Self::Serialization(_) => (StatusCode::INTERNAL_SERVER_ERROR, "serialization_error"),
            Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        }
    }
}

impl IntoResponse for MissionError {
    fn into_response(self) -> Response {
        let (status, code) = self.status_and_code();
        let envelope = ErrorEnvelope {
            error: code,
            message: self.to_string(),
        };
        (status, Json(envelope)).into_response()
    }
}
