//! Confluent-style error responses: `{"error_code": 40401, "message": "..."}`.
//!
//! The HTTP status is always the first three digits of the error code, which is
//! exactly how Confluent's `RestException` family works and what the Java client
//! (`RestClientException#getErrorCode`) relies on.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub code: u32,
    pub message: String,
}

pub type ApiResult<T> = Result<T, ApiError>;

impl ApiError {
    pub fn new(code: u32, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }

    pub fn status(&self) -> StatusCode {
        let status = if self.code >= 1000 {
            let mut c = self.code;
            while c >= 1000 {
                c /= 10;
            }
            c
        } else {
            self.code
        };
        StatusCode::from_u16(status as u16).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    // ---- 401 / 403 ----
    pub fn unauthorized() -> Self {
        Self::new(401, "Unauthorized")
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(403, msg)
    }

    // ---- 404xx ----
    pub fn subject_not_found(subject: &str) -> Self {
        Self::new(40401, format!("Subject '{subject}' not found."))
    }
    pub fn version_not_found(version: impl std::fmt::Display) -> Self {
        Self::new(40402, format!("Version {version} not found."))
    }
    pub fn schema_not_found() -> Self {
        Self::new(40403, "Schema not found")
    }
    pub fn schema_id_not_found(id: impl std::fmt::Display) -> Self {
        Self::new(40403, format!("Schema {id} not found"))
    }
    pub fn subject_soft_deleted(subject: &str) -> Self {
        Self::new(
            40404,
            format!("Subject '{subject}' was soft deleted.Set permanent=true to delete permanently"),
        )
    }
    pub fn subject_not_soft_deleted(subject: &str) -> Self {
        Self::new(
            40405,
            format!("Subject '{subject}' was not deleted first before being permanently deleted"),
        )
    }
    pub fn version_soft_deleted(subject: &str, version: u32) -> Self {
        Self::new(
            40406,
            format!(
                "Subject '{subject}' Version {version} was soft deleted.Set permanent=true to delete permanently"
            ),
        )
    }
    pub fn version_not_soft_deleted(subject: &str, version: u32) -> Self {
        Self::new(
            40407,
            format!(
                "Subject '{subject}' Version {version} was not deleted first before being permanently deleted"
            ),
        )
    }
    pub fn subject_compat_not_configured(subject: &str) -> Self {
        Self::new(40408, format!("Subject '{subject}' does not have subject-level compatibility configured"))
    }
    pub fn subject_mode_not_configured(subject: &str) -> Self {
        Self::new(40409, format!("Subject '{subject}' does not have subject-level mode configured"))
    }
    pub fn exporter_not_found(name: &str) -> Self {
        Self::new(40450, format!("Exporter '{name}' not found."))
    }

    // ---- 409xx ----
    pub fn incompatible(subject: &str, messages: &[String]) -> Self {
        Self::new(
            409,
            format!(
                "Schema being registered is incompatible with an earlier schema for subject \"{subject}\", details: [{}]",
                messages.join(", ")
            ),
        )
    }
    pub fn exporter_exists(name: &str) -> Self {
        Self::new(40950, format!("Exporter '{name}' already exists."))
    }

    // ---- 422xx ----
    pub fn invalid_schema(msg: impl std::fmt::Display) -> Self {
        Self::new(42201, format!("Invalid schema: {msg}"))
    }
    pub fn invalid_version(v: &str) -> Self {
        Self::new(
            42202,
            format!(
                "The specified version '{v}' is not a valid version id. Allowed values are between [1, 2^31-1] and the string \"latest\""
            ),
        )
    }
    pub fn invalid_compatibility(level: &str) -> Self {
        let _ = level;
        Self::new(42203, "Invalid compatibility level. Valid values are none, backward, forward, full, backward_transitive, forward_transitive, and full_transitive")
    }
    pub fn invalid_mode(mode: &str) -> Self {
        let _ = mode;
        Self::new(42204, "Invalid mode. Valid values are READWRITE, READONLY, and IMPORT.")
    }
    pub fn operation_not_permitted(msg: impl Into<String>) -> Self {
        Self::new(42205, msg)
    }
    pub fn reference_exists(msg: impl Into<String>) -> Self {
        Self::new(42206, msg)
    }
    pub fn invalid_subject(subject: &str) -> Self {
        // sic: Confluent's message.
        Self::new(42208, format!("The specified subject '{subject}' is not a valid."))
    }
    pub fn context_not_empty(ctx: &str) -> Self {
        Self::new(42211, format!("The specified context '{ctx}' is not empty."))
    }
    pub fn invalid_exporter(msg: impl Into<String>) -> Self {
        Self::new(42250, msg)
    }
    pub fn unprocessable(msg: impl Into<String>) -> Self {
        Self::new(422, msg)
    }

    // ---- 500xx ----
    pub fn store(msg: impl std::fmt::Display) -> Self {
        Self::new(50001, format!("Error in the backend data store - {msg}"))
    }
    pub fn internal(msg: impl std::fmt::Display) -> Self {
        Self::new(500, format!("Internal error - {msg}"))
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for ApiError {}

impl From<rocksdb::Error> for ApiError {
    fn from(e: rocksdb::Error) -> Self {
        ApiError::store(e)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::store(format!("corrupt record: {e}"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({ "error_code": self.code, "message": self.message });
        crate::api::sr_json(self.status(), &body)
    }
}
