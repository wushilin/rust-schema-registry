//! `schema-registry migrate`: copy everything from another registry into this
//! one (or between any two registries) over the REST API.
//!
//! Schemas keep their ids and version numbers, which means the destination
//! subject has to be in IMPORT mode while it is written; the tool sets that,
//! writes, and then restores whatever mode the subject should end with.
//! Referenced schemas are migrated before the schemas that use them, and
//! soft-deleted versions are recreated and re-deleted so the destination ends
//! up in the same state. Subject, context and global configuration and modes
//! are copied too.
//!
//! Re-running is safe: registering the same id and version again is a no-op on
//! the destination, so an interrupted migration can simply be repeated.

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use crate::api::percent_encode_segment;
use crate::context::{DEFAULT_CONTEXT, QualifiedSubject};

pub struct Endpoint {
    client: reqwest::Client,
    base: String,
    auth: Option<(String, String)>,
}

impl Endpoint {
    pub fn new(url: &str, user_info: Option<&str>) -> anyhow::Result<Self> {
        let auth = match user_info {
            Some(u) => {
                let (user, pass) = u.split_once(':').unwrap_or((u, ""));
                Some((user.to_string(), pass.to_string()))
            }
            None => None,
        };
        Ok(Self {
            client: reqwest::Client::builder().timeout(std::time::Duration::from_secs(60)).build()?,
            base: url.trim_end_matches('/').to_string(),
            auth,
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut r = self
            .client
            .request(method, format!("{}{path}", self.base))
            .header("Content-Type", crate::api::SR_CONTENT_TYPE);
        if let Some((u, p)) = &self.auth {
            r = r.basic_auth(u, Some(p));
        }
        r
    }

    async fn send(&self, rb: reqwest::RequestBuilder, ok_codes: &[u64]) -> anyhow::Result<Value> {
        let resp = rb.send().await?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if status.is_success() {
            return Ok(body);
        }
        let code = body.get("error_code").and_then(Value::as_u64).unwrap_or(status.as_u16() as u64);
        if ok_codes.contains(&code) {
            return Ok(Value::Null);
        }
        anyhow::bail!("{status}: {body}")
    }

    pub(crate) async fn get(&self, path: &str, ok_codes: &[u64]) -> anyhow::Result<Value> {
        self.send(self.request(reqwest::Method::GET, path), ok_codes).await
    }

    pub(crate) async fn post(&self, path: &str, body: &Value, ok_codes: &[u64]) -> anyhow::Result<Value> {
        self.send(self.request(reqwest::Method::POST, path).json(body), ok_codes).await
    }

    pub(crate) async fn put(&self, path: &str, body: &Value, ok_codes: &[u64]) -> anyhow::Result<Value> {
        self.send(self.request(reqwest::Method::PUT, path).json(body), ok_codes).await
    }

    pub(crate) async fn delete(&self, path: &str, ok_codes: &[u64]) -> anyhow::Result<Value> {
        self.send(self.request(reqwest::Method::DELETE, path), ok_codes).await
    }
}

/// Put a scope's configuration, replacing rather than merging.
pub(crate) async fn put_config(dst: &Endpoint, subject: Option<&str>, config: &Value) -> anyhow::Result<()> {
    let path = match subject {
        Some(s) => format!("/config/{}", percent_encode_segment(s)),
        None => "/config".to_string(),
    };
    let mut body = serde_json::Map::new();
    for (k, v) in config.as_object().into_iter().flatten() {
        // `compatibilityLevel` on the way out is `compatibility` on the way in.
        body.insert(if k == "compatibilityLevel" { "compatibility".into() } else { k.clone() }, v.clone());
    }
    if subject.is_some() {
        dst.delete(&path, &[40401, 40408]).await?;
    }
    dst.put(&path, &Value::Object(body), &[]).await?;
    Ok(())
}

/// Put a scope's mode, forcing it: the point is to get where the source was.
pub(crate) async fn put_mode(dst: &Endpoint, subject: Option<&str>, mode: &str) -> anyhow::Result<()> {
    let path = match subject {
        Some(s) => format!("/mode/{}?force=true", percent_encode_segment(s)),
        None => "/mode?force=true".to_string(),
    };
    dst.put(&path, &json!({"mode": mode}), &[]).await?;
    Ok(())
}

#[derive(Default)]
pub struct Summary {
    pub subjects: usize,
    pub versions: usize,
    pub soft_deleted: usize,
    pub configs: usize,
    pub modes: usize,
    pub skipped: Vec<String>,
}

pub struct Options {
    pub subject_prefix: Option<String>,
    pub dry_run: bool,
    pub include_deleted: bool,
}

/// Copy every subject (and configuration) from `src` to `dst`.
pub async fn run(src: &Endpoint, dst: &Endpoint, opts: &Options) -> anyhow::Result<Summary> {
    let mut summary = Summary::default();
    let mut done: HashSet<(String, u32)> = HashSet::new();
    // Subjects in every context, soft-deleted ones included.
    let prefix = opts.subject_prefix.clone().unwrap_or_else(|| ":*:".to_string());
    let subjects: Vec<String> = serde_json::from_value(
        src.get(&format!("/subjects?subjectPrefix={}&deleted=true", percent_encode_segment(&prefix)), &[]).await?,
    )?;

    // Global and context configuration first, so registrations see it.
    copy_config(src, dst, None, opts, &mut summary).await?;
    let contexts: Vec<String> = serde_json::from_value(src.get("/contexts", &[]).await?).unwrap_or_default();
    for ctx in contexts.iter().filter(|c| *c != DEFAULT_CONTEXT) {
        copy_config(src, dst, Some(&format!(":{ctx}:")), opts, &mut summary).await?;
    }

    for subject in &subjects {
        match migrate_subject(src, dst, subject, opts, &mut done, &mut summary).await {
            Ok(()) => summary.subjects += 1,
            Err(e) => summary.skipped.push(format!("{subject}: {e}")),
        }
    }
    Ok(summary)
}

/// The versions of one subject, oldest first, with their references migrated first.
async fn migrate_subject(
    src: &Endpoint,
    dst: &Endpoint,
    subject: &str,
    opts: &Options,
    done: &mut HashSet<(String, u32)>,
    summary: &mut Summary,
) -> anyhow::Result<()> {
    let enc = percent_encode_segment(subject);
    let all: Vec<u32> = serde_json::from_value(src.get(&format!("/subjects/{enc}/versions?deleted=true"), &[]).await?)?;
    let live: Vec<u32> =
        serde_json::from_value(src.get(&format!("/subjects/{enc}/versions"), &[40401]).await.unwrap_or(json!([]))).unwrap_or_default();
    let mut versions = all;
    versions.sort_unstable();
    if !opts.include_deleted {
        versions.retain(|v| live.contains(v));
    }
    // Schemas must land byte for byte as the source stored them, so turn
    // normalization off for this subject while it is written (this registry
    // normalizes by default, Confluent does not). The subject's real
    // configuration is written afterwards.
    if !opts.dry_run && !versions.is_empty() {
        // IMPORT first: it is what keeps ids and versions, and it lifts a
        // READONLY mode a previous run may have copied over.
        dst.put(&format!("/mode/{enc}?force=true"), &json!({"mode": "IMPORT"}), &[]).await?;
        dst.put(&format!("/config/{enc}"), &json!({"normalize": false}), &[]).await?;
    }
    // Every version is written before any is deleted: soft-deleting the last
    // live version of a subject drops its config and mode (as in Confluent),
    // which would undo the setting above midway through.
    for version in &versions {
        copy_version(src, dst, subject, *version, opts, done, summary).await?;
    }
    for version in versions.iter().filter(|v| !live.contains(v)) {
        if !opts.dry_run {
            dst.delete(&format!("/subjects/{enc}/versions/{version}"), &[40402, 40406]).await?;
        }
        summary.soft_deleted += 1;
    }
    copy_config(src, dst, Some(subject), opts, summary).await?;
    copy_mode(src, dst, subject, opts, summary).await?;
    Ok(())
}

async fn copy_version(
    src: &Endpoint,
    dst: &Endpoint,
    subject: &str,
    version: u32,
    opts: &Options,
    done: &mut HashSet<(String, u32)>,
    summary: &mut Summary,
) -> anyhow::Result<()> {
    if !done.insert((subject.to_string(), version)) {
        return Ok(());
    }
    let enc = percent_encode_segment(subject);
    let schema = src.get(&format!("/subjects/{enc}/versions/{version}?deleted=true"), &[]).await?;

    // References first, so the destination can resolve them.
    for r in schema.get("references").and_then(Value::as_array).into_iter().flatten() {
        let (Some(rs), Some(rv)) = (r.get("subject").and_then(Value::as_str), r.get("version").and_then(Value::as_i64)) else {
            continue;
        };
        let target = QualifiedSubject::parse(rs)?;
        let parent = QualifiedSubject::parse(subject)?;
        let qualified = if rs.starts_with(":.") { rs.to_string() } else { QualifiedSubject::new(&parent.context, &target.subject).qualified() };
        Box::pin(copy_version(src, dst, &qualified, rv.max(1) as u32, opts, done, summary)).await?;
    }

    if opts.dry_run {
        summary.versions += 1;
        return Ok(());
    }
    // IMPORT mode is what lets the destination keep the id and version.
    dst.put(&format!("/mode/{enc}?force=true"), &json!({"mode": "IMPORT"}), &[]).await?;
    let mut body = json!({
        "schema": schema.get("schema").cloned().unwrap_or(Value::Null),
        "references": schema.get("references").cloned().unwrap_or_else(|| json!([])),
        "id": schema.get("id").cloned().unwrap_or(Value::Null),
        "version": version,
    });
    if let Some(t) = schema.get("schemaType") {
        body["schemaType"] = t.clone();
    }
    for key in ["metadata", "ruleSet"] {
        if let Some(v) = schema.get(key) {
            body[key] = v.clone();
        }
    }
    dst.post(&format!("/subjects/{enc}/versions"), &body, &[]).await?;
    summary.versions += 1;
    Ok(())
}

/// Copy a subject's, a context's or the global configuration.
async fn copy_config(
    src: &Endpoint,
    dst: &Endpoint,
    subject: Option<&str>,
    opts: &Options,
    summary: &mut Summary,
) -> anyhow::Result<()> {
    let path = match subject {
        Some(s) => format!("/config/{}", percent_encode_segment(s)),
        None => "/config".to_string(),
    };
    // 40401/40408: nothing configured at this level.
    // A tolerated 404/40408 comes back as the error body: not a configuration.
    let cfg = src.get(&path, &[40401, 40408]).await?;
    let Some(o) = cfg.as_object().filter(|o| !o.is_empty() && !o.contains_key("error_code")) else {
        // Nothing configured at the source: clear what we may have set here.
        if !opts.dry_run && subject.is_some() {
            dst.delete(&path, &[40401, 40408]).await?;
        }
        return Ok(());
    };
    let mut body = serde_json::Map::new();
    for (k, v) in o {
        // `compatibilityLevel` on the way out is `compatibility` on the way in.
        body.insert(if k == "compatibilityLevel" { "compatibility".into() } else { k.clone() }, v.clone());
    }
    if !opts.dry_run {
        // Replace rather than merge, so nothing of ours is left behind.
        if subject.is_some() {
            dst.delete(&path, &[40401, 40408]).await?;
        }
        dst.put(&path, &Value::Object(body), &[]).await?;
    }
    summary.configs += 1;
    Ok(())
}

/// Copy a subject's mode, which also ends the subject's IMPORT window.
async fn copy_mode(src: &Endpoint, dst: &Endpoint, subject: &str, opts: &Options, summary: &mut Summary) -> anyhow::Result<()> {
    let enc = percent_encode_segment(subject);
    let mode = src.get(&format!("/mode/{enc}"), &[40401, 40409]).await?;
    let wanted = mode.get("mode").and_then(Value::as_str).unwrap_or("READWRITE").to_string();
    if opts.dry_run {
        return Ok(());
    }
    if mode.is_null() {
        // The source has no subject mode: leave the destination without one too.
        dst.delete(&format!("/mode/{enc}"), &[40401, 40409]).await?;
    } else {
        dst.put(&format!("/mode/{enc}?force=true"), &json!({"mode": wanted}), &[]).await?;
        summary.modes += 1;
    }
    Ok(())
}

/// Counts of what the source holds, for a quick before/after comparison.
pub async fn describe(e: &Endpoint) -> anyhow::Result<HashMap<String, usize>> {
    let subjects: Vec<String> = serde_json::from_value(e.get("/subjects?subjectPrefix=:*:&deleted=true", &[]).await?)?;
    let mut versions = 0;
    for s in &subjects {
        let v: Vec<u32> =
            serde_json::from_value(e.get(&format!("/subjects/{}/versions?deleted=true", percent_encode_segment(s)), &[40401]).await?)
                .unwrap_or_default();
        versions += v.len();
    }
    Ok(HashMap::from([("subjects".to_string(), subjects.len()), ("versions".to_string(), versions)]))
}
