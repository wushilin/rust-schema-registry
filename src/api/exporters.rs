//! Schema exporter (schema linking) endpoints.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value, json};

use super::{AppState, Body, Sr, blocking};
use crate::error::ApiResult;
use crate::model::ExporterUpdateRequest;
use crate::registry::exporter_status_json;

pub async fn list(State(st): State<AppState>) -> ApiResult<Response> {
    Ok(Sr(blocking(&st, |r| r.list_exporters()).await?).into_response())
}

pub async fn create(State(st): State<AppState>, Body(req): Body<ExporterUpdateRequest>) -> ApiResult<Response> {
    let name = blocking(&st, move |r| r.create_exporter(req)).await?;
    Ok(Sr(json!({ "name": name })).into_response())
}

pub async fn get_info(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    let rec = blocking(&st, move |r| r.get_exporter(&name)).await?;
    Ok(Sr(rec.info).into_response())
}

pub async fn update(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Body(req): Body<ExporterUpdateRequest>,
) -> ApiResult<Response> {
    let name = blocking(&st, move |r| r.update_exporter(&name, req)).await?;
    Ok(Sr(json!({ "name": name })).into_response())
}

pub async fn delete(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    blocking(&st, move |r| r.delete_exporter(&name)).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

pub async fn status(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    let rec = blocking(&st, move |r| r.get_exporter(&name)).await?;
    Ok(Sr(exporter_status_json(&rec)).into_response())
}

pub async fn get_config(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    let rec = blocking(&st, move |r| r.get_exporter(&name)).await?;
    Ok(Sr(rec.info.config).into_response())
}

pub async fn put_config(
    State(st): State<AppState>,
    Path(name): Path<String>,
    Body(cfg): Body<Map<String, Value>>,
) -> ApiResult<Response> {
    let name = blocking(&st, move |r| r.update_exporter_config(&name, cfg)).await?;
    Ok(Sr(json!({ "name": name })).into_response())
}

async fn transition(st: AppState, name: String, action: &'static str) -> ApiResult<Response> {
    let name = blocking(&st, move |r| r.exporter_transition(&name, action)).await?;
    Ok(Sr(json!({ "name": name })).into_response())
}

pub async fn pause(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    transition(st, name, "pause").await
}
pub async fn resume(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    transition(st, name, "resume").await
}
pub async fn reset(State(st): State<AppState>, Path(name): Path<String>) -> ApiResult<Response> {
    transition(st, name, "reset").await
}
