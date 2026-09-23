//! Core Confluent REST endpoints. Validation happens in the same order as in
//! Confluent's resources (bean validation of the body first, then path/query
//! parameters, then the registry), so the first error reported matches.

use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use super::{AppState, Caller, JsonBody, Params, Sr, blocking, inline, jackson, java_int};
use crate::error::{ApiError, ApiResult};
use crate::model::{CompatibilityLevel, ConfigUpdateRequest, Mode, ModeUpdateRequest, RegisterSchemaRequest, TagSchemaRequest};
use crate::registry::VersionSpec;

/// An `Integer` path parameter: anything that isn't one makes Jersey answer 404.
fn parse_id(id: &str) -> ApiResult<i64> {
    java_int(id).map(i64::from).ok_or_else(|| ApiError::new(404, "HTTP 404 Not Found"))
}

fn raw_schema(schema: String) -> Response {
    let mut resp = (StatusCode::OK, schema).into_response();
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(super::SR_CONTENT_TYPE));
    resp
}

// ---------------- misc ----------------

pub async fn root() -> Sr<Value> {
    Sr(json!({}))
}

/// `POST /` takes a `Map<String, String>` and ignores it.
pub async fn root_post(body: JsonBody) -> ApiResult<Sr<Value>> {
    if let Some(v) = body.0 {
        let o = jackson::object(&v, "String>")?;
        for x in o.values() {
            jackson::string(Some(x))?;
        }
    }
    Ok(Sr(json!({})))
}

pub async fn metadata_id(State(st): State<AppState>) -> Sr<Value> {
    Sr(json!({
        "scope": {
            "path": [],
            "clusters": {
                "kafka-cluster": st.registry.cluster_id,
                "schema-registry-cluster": "schema-registry"
            }
        }
    }))
}

pub async fn metadata_version() -> Sr<Value> {
    Sr(json!({ "version": env!("CARGO_PKG_VERSION"), "commitId": "rust-schema-registry" }))
}

pub async fn schema_types() -> Sr<Value> {
    Sr(json!(["JSON", "PROTOBUF", "AVRO"]))
}

// ---------------- schemas ----------------

pub async fn list_schemas(State(st): State<AppState>, caller: Caller, p: Params) -> ApiResult<Response> {
    let prefix = p.get("subjectPrefix").map(String::from);
    let (deleted, latest, aliases) = (p.flag("deleted"), p.flag("latestOnly"), p.flag("aliases"));
    let rule_type = p.get("ruleType").map(String::from);
    let l = st.registry.limits;
    p.int("offset", 0)?;
    p.int("limit", -1)?;
    let list = inline(&st, |r| r.list_schemas(prefix.as_deref(), deleted, latest, aliases, rule_type.as_deref()))?;
    let list = caller.filter(list, |e| e.subject.as_str());
    Ok(Sr(p.page_capped(list, l.schema_default, l.schema_max)?).into_response())
}

pub async fn schema_by_id(State(st): State<AppState>, Path(id): Path<String>, p: Params) -> ApiResult<Response> {
    let id = parse_id(&id)?;
    let subject = p.get("subject").map(String::from);
    let fetch_max = p.flag("fetchMaxId");
    let format = p.get("format").map(String::from);
    let view = inline(&st, |r| r.get_schema_by_id(id, subject.as_deref(), fetch_max, format.as_deref()))?;
    Ok(Sr(view).into_response())
}

pub async fn schema_by_id_raw(State(st): State<AppState>, Path(id): Path<String>, p: Params) -> ApiResult<Response> {
    let id = parse_id(&id)?;
    let subject = p.get("subject").map(String::from);
    let format = p.get("format").map(String::from);
    let view = inline(&st, |r| r.get_schema_by_id(id, subject.as_deref(), false, format.as_deref()))?;
    Ok(raw_schema(view.schema))
}

pub async fn schema_id_subjects(
    State(st): State<AppState>,
    Path(id): Path<String>,
    caller: Caller,
    p: Params,
) -> ApiResult<Response> {
    let id = parse_id(&id)?;
    let subject = p.get("subject").map(String::from);
    let deleted = p.flag("deleted");
    let list = inline(&st, |r| r.id_subjects(id, subject.as_deref(), deleted))?;
    Ok(Sr(caller.filter(list, |s| s.as_str())).into_response())
}

pub async fn schema_id_versions(
    State(st): State<AppState>,
    Path(id): Path<String>,
    caller: Caller,
    p: Params,
) -> ApiResult<Response> {
    let id = parse_id(&id)?;
    let subject = p.get("subject").map(String::from);
    let deleted = p.flag("deleted");
    let list = inline(&st, |r| r.id_versions(id, subject.as_deref(), deleted))?;
    Ok(Sr(caller.filter(list, |v| v.subject.as_str())).into_response())
}

// ---------------- subjects ----------------

pub async fn list_subjects(State(st): State<AppState>, caller: Caller, p: Params) -> ApiResult<Response> {
    let prefix = p.get("subjectPrefix").map(String::from);
    let (deleted, deleted_only) = (p.flag("deleted"), p.flag("deletedOnly"));
    let l = st.registry.limits;
    p.int("offset", 0)?;
    p.int("limit", -1)?;
    let list = inline(&st, |r| r.list_subjects(prefix.as_deref(), deleted, deleted_only))?;
    // Paging applies to what this caller can see, so a scoped user gets whole pages.
    let list = caller.filter(list, |s| s.as_str());
    Ok(Sr(p.page_capped(list, l.subject_default, l.subject_max)?).into_response())
}

pub async fn list_versions(State(st): State<AppState>, Path(subject): Path<String>, p: Params) -> ApiResult<Response> {
    let (deleted, deleted_only) = (p.flag("deleted"), p.flag("deletedOnly"));
    let list = inline(&st, |r| r.list_versions(&subject, deleted, deleted_only))?;
    Ok(Sr(list).into_response())
}

pub async fn register_schema(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    p: Params,
    body: JsonBody,
) -> ApiResult<Response> {
    let req = RegisterSchemaRequest::from_json(&body.require("register.arg5")?)?;
    if let Some(rs) = &req.rule_set {
        jackson::validate_rule_set(rs)?;
    }
    let normalize = p.flag("normalize");
    let resp = blocking(&st, move |r| r.register(&subject, req, normalize)).await?;
    Ok(Sr(resp).into_response())
}

pub async fn lookup_schema(
    State(st): State<AppState>,
    Path(subject): Path<String>,
    p: Params,
    body: JsonBody,
) -> ApiResult<Response> {
    let req = RegisterSchemaRequest::from_json(&body.require("lookUpSchemaUnderSubject.arg5")?)?;
    let (normalize, deleted) = (p.flag("normalize"), p.flag("deleted"));
    let format = p.get("format").map(String::from);
    let view = blocking(&st, move |r| r.lookup(&subject, req, normalize, deleted, format.as_deref())).await?;
    Ok(Sr(view).into_response())
}

pub async fn delete_subject(State(st): State<AppState>, Path(subject): Path<String>, p: Params) -> ApiResult<Response> {
    let permanent = p.flag("permanent");
    let versions = blocking(&st, move |r| r.delete_subject(&subject, permanent)).await?;
    Ok(Sr(versions).into_response())
}

pub async fn get_version(State(st): State<AppState>, Path((subject, version)): Path<(String, String)>, p: Params) -> ApiResult<Response> {
    let spec = VersionSpec::parse(&version)?;
    let deleted = p.flag("deleted");
    let format = p.get("format").map(String::from);
    let view = inline(&st, |r| r.get_version_formatted(&subject, spec, deleted, format.as_deref()))?;
    Ok(Sr(view).into_response())
}

pub async fn latest_with_metadata(State(st): State<AppState>, Path(subject): Path<String>, p: Params) -> ApiResult<Response> {
    let pairs: Vec<(String, String)> = p.all("key").into_iter().zip(p.all("value")).collect();
    let deleted = p.flag("deleted");
    let format = p.get("format").map(String::from);
    let view = inline(&st, |r| r.latest_with_metadata(&subject, &pairs, deleted, format.as_deref()))?;
    Ok(Sr(view).into_response())
}

pub async fn get_version_raw(State(st): State<AppState>, Path((subject, version)): Path<(String, String)>, p: Params) -> ApiResult<Response> {
    let spec = VersionSpec::parse(&version)?;
    let deleted = p.flag("deleted");
    let format = p.get("format").map(String::from);
    let view = inline(&st, |r| r.get_version_formatted(&subject, spec, deleted, format.as_deref()))?;
    Ok(raw_schema(view.schema))
}

pub async fn referenced_by(State(st): State<AppState>, Path((subject, version)): Path<(String, String)>) -> ApiResult<Response> {
    let spec = VersionSpec::parse(&version)?;
    let ids = inline(&st, |r| r.referenced_by(&subject, spec))?;
    Ok(Sr(ids).into_response())
}

pub async fn delete_version(State(st): State<AppState>, Path((subject, version)): Path<(String, String)>, p: Params) -> ApiResult<Response> {
    let spec = VersionSpec::parse(&version)?;
    let permanent = p.flag("permanent");
    let v = blocking(&st, move |r| r.delete_version(&subject, spec, permanent)).await?;
    Ok(Sr(v).into_response())
}

/// `POST /subjects/{subject}/versions/{version}/tags`
pub async fn modify_tags(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    body: JsonBody,
) -> ApiResult<Response> {
    let req = TagSchemaRequest::from_json(&body.require("modifyTags.arg4")?)?;
    if !crate::context::is_valid_subject(&subject) {
        return Err(ApiError::invalid_subject(&subject));
    }
    let spec = VersionSpec::parse(&version)?;
    if (req.rules_to_merge.is_some() || !req.rules_to_remove.is_empty()) && req.rule_set.is_some() {
        return Err(ApiError::new(42210, "ruleSet should be omitted if specifying rulesToMerge or rulesToRemove"));
    }
    for rs in [&req.rules_to_merge, &req.rule_set].into_iter().flatten() {
        jackson::validate_rule_set(rs)?;
    }
    let resp = blocking(&st, move |r| r.modify_tags(&subject, spec, req)).await?;
    Ok(Sr(resp).into_response())
}

// ---------------- compatibility ----------------

fn compat_response(messages: Vec<String>, verbose: bool) -> Response {
    let ok = messages.is_empty();
    let mut body = json!({ "is_compatible": ok });
    if verbose {
        body["messages"] = json!(messages);
    }
    Sr(body).into_response()
}

pub async fn compat_all(State(st): State<AppState>, Path(subject): Path<String>, p: Params, body: JsonBody) -> ApiResult<Response> {
    let req = RegisterSchemaRequest::from_json(&body.require("testCompatibilityForSubject.arg3")?)?;
    let (normalize, verbose) = (p.flag("normalize"), p.flag("verbose"));
    let msgs = blocking(&st, move |r| r.test_compatibility(&subject, None, req, normalize, verbose)).await?;
    Ok(compat_response(msgs, verbose))
}

pub async fn compat_version(
    State(st): State<AppState>,
    Path((subject, version)): Path<(String, String)>,
    p: Params,
    body: JsonBody,
) -> ApiResult<Response> {
    let req = RegisterSchemaRequest::from_json(&body.require("testCompatibilityBySubjectName.arg4")?)?;
    let spec = VersionSpec::parse(&version)?;
    let (normalize, verbose) = (p.flag("normalize"), p.flag("verbose"));
    let msgs = blocking(&st, move |r| r.test_compatibility(&subject, Some(spec), req, normalize, verbose)).await?;
    Ok(compat_response(msgs, verbose))
}

// ---------------- config ----------------

async fn config_get(st: AppState, subject: Option<String>, p: Params) -> ApiResult<Response> {
    let dtg = p.flag("defaultToGlobal");
    let cfg = inline(&st, |r| r.get_config(subject.as_deref(), dtg))?;
    Ok(Sr(cfg).into_response())
}

/// `ConfigResource#update*Config`: the level is checked, then the subject,
/// then the registry; the answer echoes the request.
async fn config_put(st: AppState, subject: Option<String>, body: Value) -> ApiResult<Response> {
    let req = ConfigUpdateRequest::from_json(&body)?;
    if let Some(level) = &req.compatibility {
        CompatibilityLevel::parse(level)?;
    }
    for rs in [&req.default_rule_set, &req.override_rule_set].into_iter().flatten() {
        jackson::validate_rule_set(rs)?;
    }
    if let Some(s) = &subject
        && !crate::context::is_valid_subject(s)
    {
        return Err(ApiError::invalid_subject(s));
    }
    let echo = serde_json::to_value(&req).unwrap_or_else(|_| json!({}));
    let update = req.into_record()?;
    blocking(&st, move |r| r.set_config(subject.as_deref(), update)).await?;
    Ok(Sr(echo).into_response())
}

async fn config_delete(st: AppState, subject: Option<String>) -> ApiResult<Response> {
    let prev = blocking(&st, move |r| r.delete_config(subject.as_deref())).await?;
    Ok(Sr(prev).into_response())
}

pub async fn get_global_config(State(st): State<AppState>, p: Params) -> ApiResult<Response> {
    config_get(st, None, p).await
}
pub async fn put_global_config(State(st): State<AppState>, body: JsonBody) -> ApiResult<Response> {
    let body = body.require("updateTopLevelConfig.arg1")?;
    config_put(st, None, body).await
}
pub async fn delete_global_config(State(st): State<AppState>) -> ApiResult<Response> {
    config_delete(st, None).await
}
pub async fn get_subject_config(State(st): State<AppState>, Path(s): Path<String>, p: Params) -> ApiResult<Response> {
    config_get(st, Some(s), p).await
}
pub async fn put_subject_config(State(st): State<AppState>, Path(s): Path<String>, body: JsonBody) -> ApiResult<Response> {
    let body = body.require("updateSubjectLevelConfig.arg2")?;
    config_put(st, Some(s), body).await
}
pub async fn delete_subject_config(State(st): State<AppState>, Path(s): Path<String>) -> ApiResult<Response> {
    config_delete(st, Some(s)).await
}

// ---------------- mode ----------------

fn mode_json(m: Mode) -> Response {
    Sr(json!({ "mode": m.as_str() })).into_response()
}

async fn mode_get(st: AppState, subject: Option<String>, p: Params) -> ApiResult<Response> {
    let dtg = p.flag("defaultToGlobal");
    Ok(mode_json(inline(&st, |r| r.get_mode(subject.as_deref(), dtg))?))
}

/// `ModeResource#updateMode`: subject, then mode, then the registry; the answer echoes the request.
async fn mode_put(st: AppState, subject: Option<String>, p: Params, body: Value) -> ApiResult<Response> {
    let req = ModeUpdateRequest::from_json(&body)?;
    if let Some(s) = &subject
        && !crate::context::is_valid_subject(s)
    {
        return Err(ApiError::invalid_subject(s));
    }
    // Confluent dereferences a missing mode (NullPointerException).
    let raw = req.mode.clone().ok_or_else(|| ApiError::new(500, "Internal Server Error"))?;
    let mode = Mode::parse(&raw)?;
    let force = p.flag("force");
    blocking(&st, move |r| r.set_mode(subject.as_deref(), mode, force)).await?;
    Ok(Sr(json!({ "mode": raw })).into_response())
}

async fn mode_delete(st: AppState, subject: String) -> ApiResult<Response> {
    Ok(mode_json(blocking(&st, move |r| r.delete_mode(&subject)).await?))
}

pub async fn get_global_mode(State(st): State<AppState>, p: Params) -> ApiResult<Response> {
    mode_get(st, None, p).await
}
pub async fn put_global_mode(State(st): State<AppState>, p: Params, body: JsonBody) -> ApiResult<Response> {
    let body = body.require("updateTopLevelMode.arg1")?;
    mode_put(st, None, p, body).await
}
pub async fn get_subject_mode(State(st): State<AppState>, Path(s): Path<String>, p: Params) -> ApiResult<Response> {
    mode_get(st, Some(s), p).await
}
pub async fn put_subject_mode(State(st): State<AppState>, Path(s): Path<String>, p: Params, body: JsonBody) -> ApiResult<Response> {
    let body = body.require("updateMode.arg2")?;
    mode_put(st, Some(s), p, body).await
}
pub async fn delete_subject_mode(State(st): State<AppState>, Path(s): Path<String>) -> ApiResult<Response> {
    mode_delete(st, s).await
}

// ---------------- contexts ----------------

pub async fn list_contexts(State(st): State<AppState>, caller: Caller) -> ApiResult<Response> {
    let list = inline(&st, |r| r.list_contexts())?;
    let list: Vec<String> = list.into_iter().filter(|c| caller.0.can_see_context(c)).collect();
    Ok(Sr(list).into_response())
}

pub async fn delete_context(State(st): State<AppState>, Path(ctx): Path<String>) -> ApiResult<Response> {
    blocking(&st, move |r| r.delete_context(&ctx)).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
