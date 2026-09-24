//! `/admin`: the administrative UI (one embedded HTML page plus the two JSON
//! endpoints it calls). Everything here is admin-only, see `authz`.
//!
//! The path is outside Confluent's namespace: no REST resource of theirs
//! starts with `admin`, and the pre-routing filters in `rewrite` only touch
//! `contexts/...` and the segment after `subjects`, so they leave it alone.
//! The UI first shipped under `/_admin`, which now redirects here.

use axum::extract::Path;
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

pub async fn overview(st: AppState, caller: Caller, p: Params) -> ApiResult<Sr<Value>> {
    let prefix = p.get("subjectPrefix").map(String::from);
    let deleted = p.flag("deleted");
    let limit = p.int("limit", DEFAULT_LIMIT as i64)?.max(0) as usize;
    // Everything the page lists is something this caller administers, so no
    // button it offers can come back 403.
    let visible = |ctx: &str, subject: &str| caller.0.is_admin_of(ctx, subject);
    Ok(Sr(inline(&st, |r| r.admin_overview(prefix.as_deref(), deleted, limit, &visible))?))
}

pub async fn subject_detail(st: AppState, Path(subject): Path<String>) -> ApiResult<Sr<Value>> {
    Ok(Sr(inline(&st, |r| r.admin_subject(&subject))?))
}

/// The old `/_admin...` path, kept as a redirect.
pub async fn moved(uri: axum::http::Uri) -> Response {
    let rest = uri.path().trim_start_matches("/_admin");
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    axum::response::Redirect::permanent(&format!("/admin{rest}{query}")).into_response()
}
