//! HTTP layer (axum). Handlers are thin: parse path/query/body, hop onto the
//! blocking pool (RocksDB and schema compilation are synchronous), call the
//! registry, and render JSON with Confluent's media type.

mod admin;
mod exporters;
mod handlers;
pub mod jackson;
pub mod rewrite;

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, FromRequest, Request};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::auth::Auth;
use crate::error::{ApiError, ApiResult};
use crate::registry::Registry;

pub const SR_CONTENT_TYPE: &str = "application/vnd.schemaregistry.v1+json";

/// What the router is built with: every host container, and how to
/// authenticate. One per process.
#[derive(Clone)]
pub struct Shared {
    pub containers: Arc<crate::containers::Containers>,
    pub auth: Arc<Auth>,
}

/// What a handler works with: the one container this request reached, and how
/// to authenticate. Resolved from the `Host` header by [`route_container`]
/// before anything else runs, so a handler cannot forget to ask which
/// registry it is talking to - it is handed one.
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub auth: Arc<Auth>,
}

impl axum::extract::FromRequestParts<Shared> for AppState {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, shared: &Shared) -> Result<Self, Self::Rejection> {
        let registry = parts
            .extensions
            .get::<Arc<Registry>>()
            .cloned()
            .ok_or_else(|| ApiError::internal("request reached a handler without a host container"))?;
        Ok(AppState { registry, auth: shared.auth.clone() })
    }
}

/// Resolve the `Host` header to a container, once, in front of everything.
pub async fn route_container(
    axum::extract::State(shared): axum::extract::State<Shared>,
    mut req: Request,
    next: axum::middleware::Next,
) -> Response {
    let host = req.headers().get(axum::http::header::HOST).and_then(|v| v.to_str().ok()).map(String::from);
    let Some((_, registry)) = shared.containers.route(host.as_deref()) else {
        return crate::containers::Containers::no_such_host(host.as_deref()).into_response();
    };
    req.extensions_mut().insert(registry.clone());
    next.run(req).await
}

/// Render a JSON body with the schema registry media type.
pub fn sr_json<T: Serialize + ?Sized>(status: StatusCode, body: &T) -> Response {
    match serde_json::to_vec(body) {
        Ok(bytes) => {
            let mut resp = (status, bytes).into_response();
            resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(SR_CONTENT_TYPE));
            resp
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// 200 OK JSON response.
pub struct Sr<T>(pub T);

impl<T: Serialize> IntoResponse for Sr<T> {
    fn into_response(self) -> Response {
        sr_json(StatusCode::OK, &self.0)
    }
}

/// Media types the Confluent REST resources consume (`@Consumes`).
const CONSUMED: &[&str] = &[
    "application/vnd.schemaregistry.v1+json",
    "application/vnd.schemaregistry+json",
    "application/json",
    "application/octet-stream",
];

/// A JSON request body read like Jersey + Jackson (see `jackson`): `None` for
/// an empty or `null` body, which each endpoint rejects with its own
/// bean-validation message via [`JsonBody::require`].
pub struct JsonBody(pub Option<serde_json::Value>);

impl JsonBody {
    /// Confluent's `@NotNull` failure: `{method}.arg{n} must not be null (was null)`.
    pub fn require(self, param: &str) -> ApiResult<serde_json::Value> {
        self.0.ok_or_else(|| ApiError::new(422, format!("{param} must not be null (was null)")))
    }
}

impl<S: Send + Sync> FromRequest<S> for JsonBody {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if let Some(ct) = req.headers().get(header::CONTENT_TYPE) {
            let ct = ct.to_str().unwrap_or("");
            let essence = ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
            if !essence.is_empty() && !CONSUMED.contains(&essence.as_str()) {
                return Err(ApiError::new(415, "HTTP 415 Unsupported Media Type"));
            }
        }
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|e| ApiError::new(e.status().as_u16() as u32, e.body_text()))?;
        if bytes.iter().all(|b| b.is_ascii_whitespace()) {
            return Ok(JsonBody(None));
        }
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(serde_json::Value::Null) => Ok(JsonBody(None)),
            Ok(v) => Ok(JsonBody(Some(v))),
            Err(e) => Err(jackson::bad(format!("Unexpected JSON input: {e}"))),
        }
    }
}

/// A serde-typed JSON body, for our own (non-Confluent) endpoints.
pub struct Body<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for Body<T> {
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let JsonBody(v) = JsonBody::from_request(req, state).await?;
        serde_json::from_value(v.unwrap_or_else(|| serde_json::json!({})))
            .map(Body)
            .map_err(|e| ApiError::new(422, format!("Unrecognized request body: {e}")))
    }
}

/// Who is asking. Listings are filtered to what this caller may see; with
/// authentication off every request carries an unrestricted caller.
pub struct Caller(pub std::sync::Arc<crate::authz::Principal>);

impl Caller {
    /// Keep only the subjects this caller may see. `subject_of` reads the
    /// context-qualified subject out of each item.
    pub fn filter<T>(&self, items: Vec<T>, subject_of: impl Fn(&T) -> &str) -> Vec<T> {
        if self.0.sees_everything() {
            return items;
        }
        items
            .into_iter()
            .filter(|x| match crate::context::QualifiedSubject::parse(subject_of(x)) {
                Ok(q) => self.0.can_see_subject(&q.context, &q.subject),
                Err(_) => false,
            })
            .collect()
    }
}

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for Caller {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(Caller(match parts.extensions.get::<std::sync::Arc<crate::authz::Principal>>() {
            Some(p) => p.clone(),
            None => std::sync::Arc::new(crate::authz::Principal::unrestricted()),
        }))
    }
}

/// Query parameters, Confluent style (`?deleted=true`). Keeps repeated keys in `pairs`.
pub struct Params(pub HashMap<String, String>, pub Vec<(String, String)>);

impl Params {
    pub fn flag(&self, name: &str) -> bool {
        self.0.get(name).is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }
    pub fn all(&self, name: &str) -> Vec<String> {
        self.1.iter().filter(|(k, _)| k == name).map(|(_, v)| v.clone()).collect()
    }
    /// A Java `int` query parameter. Like Jersey, a value that doesn't parse
    /// makes the whole resource "not found".
    pub fn int(&self, name: &str, default: i64) -> ApiResult<i64> {
        match self.get(name) {
            None => Ok(default),
            Some(v) => java_int(v).map(i64::from).ok_or_else(|| ApiError::new(404, "HTTP 404 Not Found")),
        }
    }
    /// Apply `offset` and a Confluent-style capped `limit` (see `SearchLimits`).
    /// (A negative offset crashes Confluent; we treat it as 0.)
    pub fn page_capped<T>(&self, items: Vec<T>, default: usize, max: usize) -> ApiResult<Vec<T>> {
        let offset = self.int("offset", 0)?.max(0) as usize;
        let limit = crate::registry::SearchLimits::normalize(self.int("limit", -1)?, default, max);
        Ok(items.into_iter().skip(offset).take(limit).collect())
    }
}

impl<S: Send + Sync> axum::extract::FromRequestParts<S> for Params {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut axum::http::request::Parts, _: &S) -> Result<Self, Self::Rejection> {
        let q = parts.uri.query().unwrap_or("");
        let pairs = query_pairs(q);
        // Jersey's `@QueryParam` takes the first of repeated values.
        let mut first = HashMap::new();
        for (k, v) in &pairs {
            first.entry(k.clone()).or_insert_with(|| v.clone());
        }
        Ok(Params(first, pairs))
    }
}

/// `Integer.parseInt`: optional sign, ASCII digits, no whitespace, 32-bit range.
pub fn java_int(s: &str) -> Option<i32> {
    let digits = s.strip_prefix(['+', '-']).unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<i32>().ok()
}

pub fn query_pairs(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

pub fn percent_decode(s: &str) -> String {
    fn hex(b: u8) -> Option<u8> {
        (b as char).to_digit(16).map(|d| d as u8)
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => match (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                (Some(h), Some(l)) => {
                    out.push(h << 4 | l);
                    i += 2;
                }
                _ => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn percent_encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~' | b':') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Run a read directly on the async worker. Reads are served from the
/// in-memory snapshot and caches (a cache miss is one RocksDB point read),
/// so hopping to the blocking pool would cost more than the work itself.
pub fn inline<T>(state: &AppState, f: impl FnOnce(&Registry) -> ApiResult<T>) -> ApiResult<T> {
    f(&state.registry)
}

/// Run a registry call on the blocking pool (writes: fsync; parsing: CPU).
pub async fn blocking<T, F>(state: &AppState, f: F) -> ApiResult<T>
where
    T: Send + 'static,
    F: FnOnce(&Registry) -> ApiResult<T> + Send + 'static,
{
    let reg = state.registry.clone();
    tokio::task::spawn_blocking(move || f(&reg)).await.map_err(ApiError::internal)?
}

/// The complete HTTP service: Confluent's pre-matching URI filters in front of the router.
pub type Service = Router;

pub fn service(shared: Shared, max_body_bytes: usize) -> Service {
    let inner = router(shared.clone(), max_body_bytes);
    Router::new()
        .fallback_service(inner)
        // Innermost first: the container is resolved before the URI filters,
        // because rewriting an alias needs that container's snapshot.
        .layer(axum::middleware::map_request(prematch))
        .layer(axum::middleware::from_fn_with_state(shared, route_container))
}

/// `ContextFilter` + `AliasFilter` (see `rewrite`), before routing.
async fn prematch(mut req: Request) -> Result<Request, ApiError> {
    let is_delete_context = req.method() == axum::http::Method::DELETE
        && req.uri().path().trim_matches('/').split('/').count() == 2
        && req.uri().path().trim_start_matches('/').starts_with("contexts/");
    let Some(registry) = req.extensions().get::<Arc<Registry>>().cloned() else {
        return Ok(req); // no container: the router answers with 421 below
    };
    let alias_of = move |subject: &str| registry.alias_of(subject);
    let new = rewrite::prematch(req.uri().path(), req.uri().query(), is_delete_context, &alias_of)
        .map_err(|e| ApiError::new(400, e))?;
    if let Ok(uri) = new.parse::<axum::http::Uri>() {
        *req.uri_mut() = uri;
    }
    Ok(req)
}

pub fn router(shared: Shared, max_body_bytes: usize) -> Router {
    use handlers::*;
    let api = Router::new()
        .route("/", get(root).post(root_post))
        .route("/v1/metadata/id", get(metadata_id))
        .route("/v1/metadata/version", get(metadata_version))
        // schemas
        .route("/schemas", get(list_schemas))
        .route("/schemas/types", get(schema_types))
        .route("/schemas/ids/{id}", get(schema_by_id))
        .route("/schemas/ids/{id}/schema", get(schema_by_id_raw))
        .route("/schemas/ids/{id}/subjects", get(schema_id_subjects))
        .route("/schemas/ids/{id}/versions", get(schema_id_versions))
        // subjects
        .route("/subjects", get(list_subjects))
        .route("/subjects/{subject}", post(lookup_schema).delete(delete_subject))
        .route("/subjects/{subject}/versions", get(list_versions).post(register_schema))
        .route("/subjects/{subject}/versions/{version}", get(get_version).delete(delete_version))
        .route("/subjects/{subject}/versions/{version}/schema", get(get_version_raw))
        .route("/subjects/{subject}/versions/{version}/referencedby", get(referenced_by))
        .route("/subjects/{subject}/versions/{version}/tags", post(modify_tags))
        .route("/subjects/{subject}/metadata", get(latest_with_metadata))
        // compatibility
        .route("/compatibility/subjects/{subject}/versions", post(compat_all))
        .route("/compatibility/subjects/{subject}/versions/{version}", post(compat_version))
        // config
        .route("/config", get(get_global_config).put(put_global_config).delete(delete_global_config))
        .route("/config/{subject}", get(get_subject_config).put(put_subject_config).delete(delete_subject_config))
        // mode
        .route("/mode", get(get_global_mode).put(put_global_mode))
        .route("/mode/{subject}", get(get_subject_mode).put(put_subject_mode).delete(delete_subject_mode))
        // contexts
        .route("/contexts", get(list_contexts))
        .route("/contexts/{context}", axum::routing::delete(delete_context))
        // exporters
        .route("/exporters", get(exporters::list).post(exporters::create))
        .route("/exporters/{name}", get(exporters::get_info).put(exporters::update).delete(exporters::delete))
        .route("/exporters/{name}/status", get(exporters::status))
        .route("/exporters/{name}/config", get(exporters::get_config).put(exporters::put_config))
        .route("/exporters/{name}/pause", put(exporters::pause))
        .route("/exporters/{name}/resume", put(exporters::resume))
        .route("/exporters/{name}/reset", put(exporters::reset))
        // administrative UI (admin role only)
        .route("/admin", get(admin::page))
        .route("/admin/", get(admin::page))
        .route("/admin/api/overview", get(admin::overview))
        .route("/admin/api/subjects/{subject}", get(admin::subject_detail))
        // The UI used to live under /_admin; keep those links working.
        .route("/_admin", get(admin::moved))
        .route("/_admin/", get(admin::moved))
        .route("/_admin/{*rest}", get(admin::moved))
        .fallback(not_found)
        .method_not_allowed_fallback(method_not_allowed);

    api.layer(axum::middleware::from_fn_with_state(shared.clone(), crate::auth::middleware))
        .layer(DefaultBodyLimit::max(max_body_bytes))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        // A panic anywhere in a request (a schema parser meeting input it
        // cannot handle, say) becomes a 500 for that request; the server and
        // every other connection carry on.
        .layer(tower_http::catch_panic::CatchPanicLayer::custom(on_panic))
        .with_state(shared)
}

/// Turn a panic into Confluent's error shape, and log it with the payload.
fn on_panic(err: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let details = err
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| err.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic");
    tracing::error!("panic while handling a request: {details}");
    ApiError::new(500, "Internal Server Error").into_response()
}

async fn not_found() -> ApiError {
    ApiError::new(404, "HTTP 404 Not Found")
}

async fn method_not_allowed() -> ApiError {
    ApiError::new(405, "HTTP 405 Method Not Allowed")
}
