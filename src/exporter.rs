//! Schema exporter worker (Confluent "schema linking", single-process edition).
//!
//! Every mutation appends to the change log (`log` CF). Each exporter keeps an
//! offset into that log, exactly like Confluent's exporter keeps an offset
//! into `_schemas`. The worker replays matching events against the
//! destination registry using the normal REST API in IMPORT mode, so schema
//! ids and versions are preserved:
//!
//! 1. `PUT /mode/{dest-subject}?force=true {"mode":"IMPORT"}` (once per subject)
//! 2. referenced subject-versions are exported first (recursively)
//! 3. `POST /subjects/{dest-subject}/versions {schema, schemaType, references, id, version, ...}`
//! 4. soft/hard deletes are replayed as `DELETE ...[?permanent=true]`
//!
//! Progress is committed after each batch; on failure the exporter goes to
//! ERROR with a trace and is retried on the next poll tick, resuming from the
//! failed event. Replays are idempotent on the destination, so at-least-once
//! delivery is fine.
//!
//! The log is pruned to what every exporter has already consumed, so it does
//! not grow forever. An exporter whose next event has been pruned - a new one
//! starting at 0, or one that fell behind - exports the current state instead
//! and then continues from the end of the log.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::api::percent_encode_segment;
use crate::context::{DEFAULT_CONTEXT, QualifiedSubject, qualify};
use crate::model::*;
use crate::registry::Registry;

const BATCH: usize = 500;
/// How often the change log is trimmed (it is a store write, not free).
const PRUNE_EVERY: Duration = Duration::from_secs(30);

pub async fn run(reg: Arc<Registry>, poll: Duration) {
    let client = match reqwest::Client::builder().timeout(Duration::from_secs(30)).build() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("exporter disabled: cannot build HTTP client: {e}");
            return;
        }
    };
    let mut import_mode_set: HashSet<(String, String)> = HashSet::new();
    let mut last_prune = std::time::Instant::now();
    loop {
        // Register interest before scanning so a change during the scan isn't missed.
        let notified = reg.changes.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let mut more = false;
        let exporters = match reg.store.list_exporters() {
            Ok(e) => e,
            Err(e) => {
                tracing::error!("exporter: listing exporters failed: {e}");
                Vec::new()
            }
        };
        for rec in exporters {
            if rec.state == ExporterState::Paused {
                continue;
            }
            match export_batch(&reg, &client, &rec, &mut import_mode_set).await {
                Ok(progressed) => more |= progressed,
                Err(e) => {
                    // The destination may have left IMPORT mode (an operator
                    // changed it, or its last live version was deleted, which
                    // drops the subject's mode); re-send it on the retry.
                    if let Ok(dest) = Destination::new(&client, &rec.info, &reg.cluster_id) {
                        import_mode_set.retain(|(base, _)| *base != dest.base);
                    }
                    tracing::warn!(exporter = %rec.info.name, "export failed: {e}");
                }
            }
        }
        // Keep only what some exporter still needs (nothing, when there are
        // none). Pruning writes to the store, so it runs on its own slow beat
        // rather than after every change.
        if last_prune.elapsed() >= PRUNE_EVERY {
            last_prune = std::time::Instant::now();
            let keep = exporter_offsets(&reg);
            let reg2 = reg.clone();
            let _ = tokio::task::spawn_blocking(move || reg2.store.prune_log(keep)).await;
        }
        if more {
            continue;
        }
        tokio::select! {
            _ = &mut notified => {}
            _ = tokio::time::sleep(poll) => {}
        }
    }
}

/// The lowest offset any exporter still needs; everything below it can go.
fn exporter_offsets(reg: &Arc<Registry>) -> u64 {
    let exporters = reg.store.list_exporters().unwrap_or_default();
    let log_seq = reg.store.log_seq().unwrap_or(0);
    exporters.iter().map(|e| e.offset).min().unwrap_or(log_seq).min(log_seq)
}

/// Export one batch for one exporter. Returns true if a full batch was
/// processed (there may be more waiting).
async fn export_batch(
    reg: &Arc<Registry>,
    client: &reqwest::Client,
    rec: &ExporterRecord,
    import_mode_set: &mut HashSet<(String, String)>,
) -> anyhow::Result<bool> {
    if rec.offset < reg.store.log_floor()? {
        // The events this exporter still needed are gone: send the current
        // state instead and pick the log up from its end.
        let dest = Destination::new(client, &rec.info, &reg.cluster_id)?;
        let now = reg.store.log_seq()?;
        let mut exported = HashSet::new();
        for (ctx, subject, version) in reg.all_subject_versions()? {
            if !subject_matches(&rec.info.subjects, &ctx, &subject) {
                continue;
            }
            if let Err(e) = dest.export_version(reg, &ctx, &subject, version, import_mode_set, &mut exported, 0).await {
                let trace = format!("bootstrap ({}:{version}): {e}", qualify(&ctx, &subject));
                reg.exporter_progress(&rec.info.name, rec.offset, rec.offset, Some(trace.clone()))?;
                anyhow::bail!(trace);
            }
        }
        reg.exporter_progress(&rec.info.name, rec.offset, now, None)?;
        return Ok(true);
    }
    let events = {
        let reg = reg.clone();
        let from = rec.offset;
        tokio::task::spawn_blocking(move || reg.store.read_log(from, BATCH)).await??
    };
    if events.is_empty() {
        if rec.state == ExporterState::Error {
            reg.exporter_progress(&rec.info.name, rec.offset, rec.offset, None)?;
        }
        return Ok(false);
    }
    let dest = Destination::new(client, &rec.info, &reg.cluster_id)?;
    let mut offset = rec.offset;
    let mut exported: HashSet<(String, String, u32)> = HashSet::new();
    for (seq, ev) in &events {
        if subject_matches(&rec.info.subjects, &ev.ctx, &ev.subject) {
            let result = match ev.kind {
                LogEventKind::Register => {
                    dest.export_version(reg, &ev.ctx, &ev.subject, ev.version, import_mode_set, &mut exported, 0).await
                }
                LogEventKind::SoftDelete => dest.delete(&ev.ctx, &ev.subject, ev.version, false).await,
                LogEventKind::HardDelete => dest.delete(&ev.ctx, &ev.subject, ev.version, true).await,
            };
            if let Err(e) = result {
                let trace = format!("event {seq} ({:?} {}:{}): {e}", ev.kind, qualify(&ev.ctx, &ev.subject), ev.version);
                reg.exporter_progress(&rec.info.name, rec.offset, offset, Some(trace.clone()))?;
                anyhow::bail!(trace);
            }
        }
        offset = seq + 1;
    }
    reg.exporter_progress(&rec.info.name, rec.offset, offset, None)?;
    Ok(events.len() == BATCH)
}

/// Exporter `subjects` patterns: `*`/globs match the default context;
/// `:.ctx:pattern` targets a context; `:*:pattern` matches every context.
pub fn subject_matches(patterns: &[String], ctx: &str, subject: &str) -> bool {
    patterns.iter().any(|p| {
        let Ok(q) = QualifiedSubject::parse(p) else { return false };
        let ctx_ok = q.is_wildcard() || q.context == ctx;
        let pat = if q.subject.is_empty() { "*" } else { q.subject.as_str() };
        ctx_ok && glob::Pattern::new(pat).map(|g| g.matches(subject)).unwrap_or(false)
    })
}

struct Destination<'a> {
    client: &'a reqwest::Client,
    base: String,
    user_info: Option<(String, String)>,
    info: &'a ExporterInfo,
    cluster_id: &'a str,
}

impl<'a> Destination<'a> {
    fn new(client: &'a reqwest::Client, info: &'a ExporterInfo, cluster_id: &'a str) -> anyhow::Result<Self> {
        let base = info
            .config
            .get("schema.registry.url")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::anyhow!("missing schema.registry.url"))?
            .split(',')
            .next()
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();
        let user_info = info
            .config
            .get("basic.auth.user.info")
            .and_then(Value::as_str)
            .and_then(|s| s.split_once(':'))
            .map(|(u, p)| (u.to_string(), p.to_string()));
        Ok(Self { client, base, user_info, info, cluster_id })
    }

    /// Destination context for a source context, per `contextType`.
    fn dest_context(&self, src_ctx: &str) -> String {
        match self.info.context_type.as_str() {
            "CUSTOM" => self
                .info
                .context
                .as_deref()
                .and_then(crate::context::normalize_context)
                .unwrap_or_else(|| DEFAULT_CONTEXT.to_string()),
            "NONE" => src_ctx.to_string(),
            "DEFAULT" => DEFAULT_CONTEXT.to_string(),
            _ => {
                // AUTO: namespace everything under the source cluster id.
                let id: String = self
                    .cluster_id
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '-' })
                    .collect();
                if src_ctx == DEFAULT_CONTEXT { format!(".{id}") } else { format!(".{id}{src_ctx}") }
            }
        }
    }

    fn dest_subject(&self, ctx: &str, subject: &str) -> String {
        let renamed = match self.info.subject_rename_format.as_deref() {
            Some(f) if !f.is_empty() => f.replace("${subject}", subject),
            _ => subject.to_string(),
        };
        qualify(&self.dest_context(ctx), &renamed)
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut r = self
            .client
            .request(method, format!("{}{path}", self.base))
            .header("Content-Type", crate::api::SR_CONTENT_TYPE);
        if let Some((u, p)) = &self.user_info {
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
            return Ok(body);
        }
        anyhow::bail!("destination returned {status}: {body}")
    }

    async fn ensure_import_mode(&self, dest_subject: &str, cache: &mut HashSet<(String, String)>) -> anyhow::Result<()> {
        let key = (self.base.clone(), dest_subject.to_string());
        if cache.contains(&key) {
            return Ok(());
        }
        let path = format!("/mode/{}?force=true", percent_encode_segment(dest_subject));
        self.send(self.req(reqwest::Method::PUT, &path).json(&json!({"mode": "IMPORT"})), &[]).await?;
        cache.insert(key);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn export_version(
        &self,
        reg: &Arc<Registry>,
        ctx: &str,
        subject: &str,
        version: u32,
        import_mode_set: &mut HashSet<(String, String)>,
        exported: &mut HashSet<(String, String, u32)>,
        depth: usize,
    ) -> anyhow::Result<()> {
        if depth > 32 || !exported.insert((ctx.to_string(), subject.to_string(), version)) {
            return Ok(());
        }
        let payload = {
            let (reg, c, s) = (reg.clone(), ctx.to_string(), subject.to_string());
            tokio::task::spawn_blocking(move || reg.export_payload(&c, &s, version)).await??
        };
        // Hard-deleted since the event was logged: the delete event will follow.
        let Some((vr, rec)) = payload else { return Ok(()) };

        let mut refs = Vec::with_capacity(rec.references.len());
        for r in &rec.references {
            let target = QualifiedSubject::parse(&r.subject)?;
            let rctx = if r.subject.starts_with(':') { target.context.clone() } else { ctx.to_string() };
            Box::pin(self.export_version(reg, &rctx, &target.subject, r.version.max(1) as u32, import_mode_set, exported, depth + 1))
                .await?;
            refs.push(json!({ "name": r.name, "subject": self.dest_subject(&rctx, &target.subject), "version": r.version }));
        }

        let dest_subject = self.dest_subject(ctx, subject);
        self.ensure_import_mode(&dest_subject, import_mode_set).await?;
        let mut body = json!({
            "schema": rec.schema,
            "schemaType": rec.schema_type.as_str(),
            "references": refs,
            "id": vr.id,
            "version": version,
        });
        if let Some(m) = &rec.metadata {
            body["metadata"] = m.clone();
        }
        if let Some(r) = &rec.rule_set {
            body["ruleSet"] = r.clone();
        }
        let path = format!("/subjects/{}/versions", percent_encode_segment(&dest_subject));
        self.send(self.req(reqwest::Method::POST, &path).json(&body), &[]).await?;
        if vr.deleted {
            // Already soft-deleted at the source; mirror that too.
            self.delete(ctx, subject, version, false).await?;
        }
        Ok(())
    }

    async fn delete(&self, ctx: &str, subject: &str, version: u32, permanent: bool) -> anyhow::Result<()> {
        let dest_subject = self.dest_subject(ctx, subject);
        let mut path = format!("/subjects/{}/versions/{version}", percent_encode_segment(&dest_subject));
        if permanent {
            path.push_str("?permanent=true");
        }
        // Already gone / already soft-deleted / not soft-deleted-yet are all fine for a replay.
        self.send(self.req(reqwest::Method::DELETE, &path), &[40401, 40402, 40406, 40407]).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::subject_matches;

    #[test]
    fn patterns() {
        let p = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(subject_matches(&p(&["*"]), ".", "orders-value"));
        assert!(!subject_matches(&p(&["*"]), ".dev", "orders-value"));
        assert!(subject_matches(&p(&[":*:*"]), ".dev", "orders-value"));
        assert!(subject_matches(&p(&[":.dev:orders-*"]), ".dev", "orders-value"));
        assert!(!subject_matches(&p(&["orders-*"]), ".", "payments-value"));
    }
}
