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
use crate::error::{ApiError, ApiResult};

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

/// `GET /admin/api/backup`: this container as a newline-delimited JSON dump.
/// A logical backup - subjects, versions, ids, references, metadata, rule
/// sets, config, modes, exporters - so it restores into another build, and
/// into Confluent.
pub async fn backup(st: AppState, caller: Caller, p: Params) -> ApiResult<Response> {
    if !caller.0.sees_everything() {
        return Err(ApiError::forbidden("A backup covers every subject, so it needs registry-wide admin"));
    }
    let prefix = p.get("subjectPrefix").map(String::from);
    let dump = crate::backup::dump_local(&st.registry, prefix.as_deref())?;
    let name = format!("{}-{}.ndjson", st.registry.container(), crate::model::now_millis());
    let mut resp = (StatusCode::OK, dump.to_ndjson()).into_response();
    let headers = resp.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"));
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    Ok(resp)
}

/// `POST /admin/api/restore`: replay a dump into this container, keeping ids
/// and version numbers. `?dryRun=true` reports what it would do.
pub async fn restore(st: AppState, caller: Caller, p: Params, body: axum::body::Bytes) -> ApiResult<Sr<Value>> {
    if !caller.0.sees_everything() {
        return Err(ApiError::forbidden("A restore writes every subject, so it needs registry-wide admin"));
    }
    let text = String::from_utf8(body.to_vec()).map_err(|e| ApiError::unprocessable(format!("not text: {e}")))?;
    let dump = crate::backup::Dump::parse(&text).map_err(|e| ApiError::unprocessable(e.to_string()))?;
    let dry_run = p.flag("dryRun");
    let reg = st.registry.clone();
    let r = tokio::task::spawn_blocking(move || crate::backup::restore_local(&reg, &dump, dry_run))
        .await
        .map_err(ApiError::internal)??;
    Ok(Sr(serde_json::json!({
        "dryRun": dry_run,
        "subjects": r.subjects,
        "versions": r.versions,
        "softDeleted": r.soft_deleted,
        "configs": r.configs,
        "modes": r.modes,
        "exporters": r.exporters,
    })))
}

/// The old `/_admin...` path, kept as a redirect.
pub async fn moved(uri: axum::http::Uri) -> Response {
    let rest = uri.path().trim_start_matches("/_admin");
    let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
    axum::response::Redirect::permanent(&format!("/admin{rest}{query}")).into_response()
}
