//! Schema exporter worker (Confluent "schema linking", single-process edition).
//!
//! Every mutation appends to the change log (`log` CF). Each exporter keeps an
//! offset into that log, exactly like Confluent's exporter keeps an offset
//! into `_schemas`. The worker replays matching events against the
//! destination registry using the normal REST API in IMPORT mode, so schema
//! ids and versions are preserved:
//!
//! 1. the destination context must be in IMPORT mode - that is what allows a
//!    write to carry its own id and version. The exporter *checks* it; it only
//!    sets it when the destination context is still empty, so a context an
//!    operator has taken out of IMPORT mode is never quietly forced back in.
//! 2. referenced subject-versions are exported first (recursively)
//! 3. `POST /subjects/{dest-subject}/versions {schema, schemaType, references, id, version, ...}`
//! 4. soft/hard deletes are replayed as `DELETE ...[?permanent=true]`
//!
//! Progress is committed after each batch. What a failure does depends on what
//! kind it is:
//!
//! * transient (destination down, 5xx, timeout) -> ERROR, retried from the
//!   failed event on the next tick. Replays are idempotent, so at-least-once
//!   delivery is fine.
//! * the destination is not in IMPORT mode -> PAUSED with a trace saying so.
//!   Retrying cannot fix it and forcing the mode back would override the
//!   operator, so it stops and waits for `resume`.
//! * the replay conflicts with what the destination already holds (that id or
//!   version is a different schema there) -> FAILED. Resuming would hit the
//!   same wall, so the only way on is `reset`, which starts over.
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
    // Destination contexts already checked to be in IMPORT mode.
    let mut ready: HashSet<(String, String)> = HashSet::new();
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
            // Paused and Failed both wait for an operator (resume / reset).
            if matches!(rec.state, ExporterState::Paused | ExporterState::Failed) {
                continue;
            }
            match export_batch(&reg, &client, &rec, &mut ready).await {
                Ok(progressed) => more |= progressed,
                Err(e) => {
                    // Whatever went wrong, re-check the destination's mode
                    // before the next attempt rather than assuming it.
                    if let Ok(dest) = Destination::new(&client, &rec.info, &reg.cluster_id) {
                        ready.retain(|(base, _)| *base != dest.base);
                    }
                    tracing::warn!(exporter = %rec.info.name, "export {}: {e}", e.state_word());
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

/// Why an export attempt stopped, and what that means for the exporter.
#[derive(Debug)]
pub struct Halt {
    pub state: ExporterState,
    pub trace: String,
}

impl Halt {
    fn retry(trace: impl Into<String>) -> Self {
        Self { state: ExporterState::Error, trace: trace.into() }
    }
    fn paused(trace: impl Into<String>) -> Self {
        Self { state: ExporterState::Paused, trace: trace.into() }
    }
    fn failed(trace: impl Into<String>) -> Self {
        Self { state: ExporterState::Failed, trace: trace.into() }
    }
    fn at(mut self, what: &str) -> Self {
        self.trace = format!("{what}: {}", self.trace);
        self
    }
    pub fn state_word(&self) -> &'static str {
        match self.state {
            ExporterState::Paused => "paused",
            ExporterState::Failed => "failed",
            _ => "failed (will retry)",
        }
    }
}

impl std::fmt::Display for Halt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.trace)
    }
}

impl From<anyhow::Error> for Halt {
    fn from(e: anyhow::Error) -> Self {
        Self::retry(e.to_string())
    }
}

impl From<crate::error::ApiError> for Halt {
    fn from(e: crate::error::ApiError) -> Self {
        Self::retry(e.to_string())
    }
}

impl From<tokio::task::JoinError> for Halt {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::retry(e.to_string())
    }
}

/// Export one batch for one exporter. Returns true if a full batch was
/// processed (there may be more waiting).
async fn export_batch(
    reg: &Arc<Registry>,
    client: &reqwest::Client,
    rec: &ExporterRecord,
    ready: &mut HashSet<(String, String)>,
) -> Result<bool, Halt> {
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
            if let Err(e) = dest.export_version(reg, &ctx, &subject, version, ready, &mut exported, 0).await {
                let halt = e.at(&format!("bootstrap ({}:{version})", qualify(&ctx, &subject)));
                reg.exporter_state(&rec.info.name, rec.offset, rec.offset, halt.state, Some(halt.trace.clone()))?;
                return Err(halt);
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
                    dest.export_version(reg, &ev.ctx, &ev.subject, ev.version, ready, &mut exported, 0).await
                }
                LogEventKind::SoftDelete => dest.delete(&ev.ctx, &ev.subject, ev.version, false).await,
                LogEventKind::HardDelete => dest.delete(&ev.ctx, &ev.subject, ev.version, true).await,
            };
            if let Err(e) = result {
                let halt = e.at(&format!("event {seq} ({:?} {}:{})", ev.kind, qualify(&ev.ctx, &ev.subject), ev.version));
                // Progress up to the event before this one, so a resume picks
                // up exactly where it stopped.
                reg.exporter_state(&rec.info.name, rec.offset, offset, halt.state, Some(halt.trace.clone()))?;
                return Err(halt);
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

    /// The destination context's mode endpoint (`/mode` is the default context).
    fn mode_path(dest_ctx: &str) -> String {
        if dest_ctx == DEFAULT_CONTEXT { "/mode".to_string() } else { format!("/mode/{}", percent_encode_segment(&format!(":{dest_ctx}:"))) }
    }

    /// What the destination context's mode is right now, `None` if it has none.
    async fn mode_of(&self, dest_ctx: &str) -> anyhow::Result<Option<String>> {
        // 40401/40409: no mode configured at that scope.
        let body = self.send(self.req(reqwest::Method::GET, &Self::mode_path(dest_ctx)), &[40401, 40409]).await?;
        Ok(body.get("mode").and_then(Value::as_str).map(String::from))
    }

    /// The mode that actually applies to one destination subject: its own,
    /// else its context's, else the global one (`defaultToGlobal`).
    async fn effective_mode(&self, dest_subject: &str) -> anyhow::Result<Option<String>> {
        let path = format!("/mode/{}?defaultToGlobal=true", percent_encode_segment(dest_subject));
        let body = self.send(self.req(reqwest::Method::GET, &path), &[40401, 40409]).await?;
        Ok(body.get("mode").and_then(Value::as_str).map(String::from))
    }

    /// A write that carries its own id and version is only legal where the
    /// destination is in IMPORT mode. We check; we set the mode only for a
    /// context that is still empty, so that an operator who takes a context
    /// out of IMPORT mode is never overruled - the exporter pauses instead.
    async fn ensure_import_mode(&self, dest_ctx: &str, ready: &mut HashSet<(String, String)>) -> Result<(), Halt> {
        let key = (self.base.clone(), dest_ctx.to_string());
        if ready.contains(&key) {
            return Ok(());
        }
        let ctx_name = qualify(dest_ctx, "");
        if self.mode_of(dest_ctx).await?.as_deref() == Some("IMPORT") {
            ready.insert(key);
            return Ok(());
        }
        let existing = self
            .send(
                self.req(reqwest::Method::GET, &format!("/subjects?subjectPrefix={}&deleted=true", percent_encode_segment(&ctx_name))),
                &[],
            )
            .await?;
        if existing.as_array().is_some_and(|a| !a.is_empty()) {
            return Err(Halt::paused(format!(
                "destination context {ctx_name} is not in IMPORT mode and is not empty; \
                 put it in IMPORT mode (PUT /mode/{ctx_name} {{\"mode\":\"IMPORT\"}}) and resume"
            )));
        }
        // Empty destination: it is ours to prepare.
        let path = format!("{}?force=true", Self::mode_path(dest_ctx));
        self.send(self.req(reqwest::Method::PUT, &path).json(&json!({"mode": "IMPORT"})), &[]).await?;
        ready.insert(key);
        Ok(())
    }

    /// Turn a rejected write into the state the exporter should stop in.
    /// A destination that has left IMPORT mode and a replay that conflicts
    /// with what is already there both come back as 42205, so the mode that
    /// applies to that subject is re-read rather than the message parsed.
    async fn classify(&self, e: anyhow::Error, dest_subject: &str) -> Halt {
        let msg = e.to_string();
        if !(msg.contains("\"error_code\":42205") || msg.contains("409 Conflict")) {
            return Halt::retry(msg);
        }
        match self.effective_mode(dest_subject).await {
            // Still in IMPORT mode, so this is the destination's own state
            // disagreeing with the replay: nothing but a reset gets past it.
            Ok(Some(m)) if m == "IMPORT" => Halt::failed(msg),
            Ok(_) => Halt::paused(format!("destination {dest_subject} is not in IMPORT mode: {msg}")),
            // Cannot tell: treat it as transient rather than stopping for good.
            Err(_) => Halt::retry(msg),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn export_version(
        &self,
        reg: &Arc<Registry>,
        ctx: &str,
        subject: &str,
        version: u32,
        ready: &mut HashSet<(String, String)>,
        exported: &mut HashSet<(String, String, u32)>,
        depth: usize,
    ) -> Result<(), Halt> {
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
            Box::pin(self.export_version(reg, &rctx, &target.subject, r.version.max(1) as u32, ready, exported, depth + 1)).await?;
            refs.push(json!({ "name": r.name, "subject": self.dest_subject(&rctx, &target.subject), "version": r.version }));
        }

        let dest_ctx = self.dest_context(ctx);
        let dest_subject = self.dest_subject(ctx, subject);
        self.ensure_import_mode(&dest_ctx, ready).await?;
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
        if let Err(e) = self.send(self.req(reqwest::Method::POST, &path).json(&body), &[]).await {
            return Err(self.classify(e, &dest_subject).await);
        }
        if vr.deleted {
            // Already soft-deleted at the source; mirror that too.
            self.delete(ctx, subject, version, false).await?;
        }
        Ok(())
    }

    async fn delete(&self, ctx: &str, subject: &str, version: u32, permanent: bool) -> Result<(), Halt> {
        let dest_subject = self.dest_subject(ctx, subject);
        let mut path = format!("/subjects/{}/versions/{version}", percent_encode_segment(&dest_subject));
        if permanent {
            path.push_str("?permanent=true");
        }
        // Already gone / already soft-deleted / not soft-deleted-yet are all fine for a replay.
        if let Err(e) = self.send(self.req(reqwest::Method::DELETE, &path), &[40401, 40402, 40406, 40407]).await {
            return Err(self.classify(e, &dest_subject).await);
        }
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
