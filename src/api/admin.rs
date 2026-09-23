//! `/_admin`: a read-only inspection UI (one embedded HTML page plus the two
//! JSON endpoints it calls). Everything here is admin-only, see `auth`.
//!
//! The path is outside Confluent's namespace on purpose: `/_admin` collides
//! with no REST resource, and the pre-routing filters in `rewrite` only touch
//! `contexts/...` and the segment after `subjects`, so they leave it alone.

use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use super::{AppState, Caller, Params, Sr, inline};
use crate::error::ApiResult;

/// Rows returned to the UI when it asks for everything.
const DEFAULT_LIMIT: usize = 5_000;

pub async fn page() -> Response {
    let mut resp = (StatusCode::OK, include_str!("admin.html")).into_response();
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    resp
}

pub async fn overview(State(st): State<AppState>, caller: Caller, p: Params) -> ApiResult<Sr<Value>> {
    let prefix = p.get("subjectPrefix").map(String::from);
    let deleted = p.flag("deleted");
    let limit = p.int("limit", DEFAULT_LIMIT as i64)?.max(0) as usize;
    // Everything the page lists is something this caller administers, so no
    // button it offers can come back 403.
    let visible = |ctx: &str, subject: &str| caller.0.is_admin_of(ctx, subject);
    Ok(Sr(inline(&st, |r| r.admin_overview(prefix.as_deref(), deleted, limit, &visible))?))
}

pub async fn subject_detail(State(st): State<AppState>, Path(subject): Path<String>) -> ApiResult<Sr<Value>> {
    Ok(Sr(inline(&st, |r| r.admin_subject(&subject))?))
}
