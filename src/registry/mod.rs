//! Registry semantics on top of the store: registration, lookup, soft/hard
//! delete, compatibility, config, mode, contexts and exporter bookkeeping.
//!
//! Concurrency model: reads go straight to RocksDB with no locking. Every
//! mutation takes `write_lock`, re-reads what it needs, and commits one atomic
//! `WriteBatch`. With a single server this gives the same linearizable
//! behaviour Confluent gets from its single-leader Kafka log, without the log.

mod admin;
mod listings;
mod locks;
pub(crate) use locks::*;
mod lookup;
mod resolution;
mod schemas;
mod views;
pub(crate) use views::*;

use std::collections::HashSet;
use std::sync::Mutex;

use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::context::{DEFAULT_CONTEXT, QualifiedSubject, qualify};
use crate::error::{ApiError, ApiResult};
use crate::model::*;
use crate::schema::{self, ParsedSchema, ResolvedRef};
use crate::snapshot::Snapshot;
use crate::store::{Scope, Store};

use arc_swap::ArcSwap;
use moka::sync::Cache;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// A consistent, lock-free view of the registry for the duration of one
/// request: one metadata snapshot plus read-through access to schema bodies.
pub struct Reader<'a> {
    snap: Arc<Snapshot>,
    reg: &'a Registry,
}

impl Reader<'_> {
    pub fn referenced_by(&self, ctx: &str, subject: &str, version: u32) -> ApiResult<Vec<u32>> {
        Ok(self.snap.referenced_by(ctx, subject, version))
    }

    /// All subject-level configs as (context, subject, config).
    pub fn subject_configs(&self) -> Vec<(String, String, ConfigRecord)> {
        self.snap
            .config
            .iter()
            .filter_map(|(scope, c)| match scope {
                Scope::Subject(ctx, s) => Some((ctx.clone(), s.clone(), c.clone())),
                _ => None,
            })
            .collect()
    }

    pub fn list_contexts(&self) -> ApiResult<Vec<String>> {
        Ok(self.snap.list_contexts())
    }

    pub fn get_schema(&self, ctx: &str, id: u32) -> ApiResult<Option<Arc<SchemaRecord>>> {
        let key = (ctx.to_string(), id);
        if let Some(rec) = self.reg.bodies.get(&key) {
            return Ok(Some(rec));
        }
        // Miss: bodies are immutable, so reading the store is consistent with any snapshot.
        match self.reg.store.get_schema(ctx, id)? {
            Some(rec) => {
                let rec = Arc::new(rec);
                self.reg.bodies.insert(key, rec.clone());
                Ok(Some(rec))
            }
            None => Ok(None),
        }
    }
    pub fn get_version(&self, ctx: &str, subject: &str, version: u32) -> ApiResult<Option<VersionRecord>> {
        Ok(self.snap.get_version(ctx, subject, version))
    }
    pub fn list_versions(&self, ctx: &str, subject: &str) -> ApiResult<Vec<(u32, VersionRecord)>> {
        Ok(self.snap.list_versions(ctx, subject))
    }
    pub fn list_subject_names(&self, ctx: &str) -> ApiResult<Vec<String>> {
        Ok(self.snap.list_subject_names(ctx))
    }
    pub fn id_usages(&self, ctx: &str, id: u32) -> ApiResult<Vec<(String, u32)>> {
        Ok(self.snap.id_usages(ctx, id))
    }
    pub fn id_for_fingerprint(&self, ctx: &str, fp: &str) -> ApiResult<Option<u32>> {
        Ok(self.snap.id_for_fingerprint(ctx, fp))
    }
    pub fn next_id(&self, ctx: &str) -> ApiResult<u32> {
        Ok(self.snap.next_id(ctx))
    }
    pub fn get_config(&self, scope: &Scope) -> ApiResult<Option<ConfigRecord>> {
        Ok(self.snap.get_config(scope))
    }
    pub fn get_mode(&self, scope: &Scope) -> ApiResult<Option<Mode>> {
        Ok(self.snap.get_mode(scope))
    }
    /// Whether any alias is configured at all.
    pub fn has_aliases(&self) -> bool {
        self.snap.has_aliases
    }

}

pub struct Registry {
    pub store: Store,
    /// Current metadata snapshot; swapped atomically after each commit.
    snapshot: ArcSwap<Snapshot>,
    /// Schema bodies by (context, id). Bounded; content under an id is immutable.
    bodies: Cache<(String, u32), Arc<SchemaRecord>>,
    /// Parsed stored schemas by (context, id), for compatibility checks and formatting.
    /// Parsed stored schemas by (context, id, what its references resolved to).
    parsed_by_id: Cache<(String, u32, [u8; 32]), Arc<ParsedSchema>>,
    /// Parsed request schemas by hash(type, text, references), for repeated register/lookup.
    parsed_by_text: Cache<[u8; 32], Arc<ParsedSchema>>,
    locks: Locks,
    pub default_compatibility: CompatibilityLevel,
    /// Server-wide default for `normalize` (see `ServerConfig::normalize`).
    normalize_default: bool,
    pub cluster_id: String,
    pub limits: SearchLimits,
    /// Woken after every committed change so exporters don't have to poll.
    pub changes: Notify,
}

// ---------------------------------------------------------------------------
// Response views
// ---------------------------------------------------------------------------

/// A version selector from a URL path: a number, `latest` or `-1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionSpec {
    Latest,
    Exact(u32),
}

impl VersionSpec {
    /// Confluent's `VersionId(String)`: trimmed, `latest` in any case, or a Java int >= 1 (or -1).
    pub fn parse(s: &str) -> ApiResult<Self> {
        let t = s.trim();
        if t.eq_ignore_ascii_case("latest") {
            return Ok(Self::Latest);
        }
        match crate::api::java_int(t) {
            Some(-1) => Ok(Self::Latest),
            Some(n) if n >= 1 => Ok(Self::Exact(n as u32)),
            Some(n) => Err(ApiError::invalid_version(&n.to_string())),
            None => Err(ApiError::invalid_version(s)),
        }
    }
}

/// As Confluent prints it in messages: `latest` is -1.
impl std::fmt::Display for VersionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Latest => f.write_str("-1"),
            Self::Exact(n) => write!(f, "{n}"),
        }
    }
}

fn schema_type_view(t: SchemaType) -> Option<&'static str> {
    (!t.is_avro()).then(|| t.as_str())
}


/// Result caps like Confluent's `schema.search.*` / `subject.search.*` limits.
#[derive(Debug, Clone, Copy)]
pub struct SearchLimits {
    pub schema_default: usize,
    pub schema_max: usize,
    pub subject_default: usize,
    pub subject_max: usize,
}

impl Default for SearchLimits {
    fn default() -> Self {
        Self { schema_default: 1000, schema_max: 1000, subject_default: 20_000, subject_max: 20_000 }
    }
}

impl SearchLimits {
    /// Confluent's `normalizeLimit`: out-of-range or missing limits use the default.
    pub fn normalize(supplied: i64, default: usize, max: usize) -> usize {
        if supplied > 0 && supplied as usize <= max { supplied as usize } else { default }
    }
}

/// Confluent's `RuleSet#hasRulesWithType`: domain or migration rules only.
fn has_rules_with_type(rule_set: &Option<Value>, ty: &str) -> bool {
    let Some(rs) = rule_set else { return false };
    ["domainRules", "migrationRules"].iter().any(|k| {
        rs.get(*k).and_then(Value::as_array).is_some_and(|a| a.iter().any(|r| r.get("type").and_then(Value::as_str) == Some(ty)))
    })
}

// ---------------------------------------------------------------------------
// Metadata / rule-set merging (data contracts)
// ---------------------------------------------------------------------------

/// Merge Confluent metadata objects: `tags` and `properties` maps merge key-wise,
/// `sensitive` is a set union. Later arguments win.
fn merge_metadata(layers: &[Option<&Value>]) -> Option<Value> {
    let present: Vec<&Value> = layers.iter().flatten().copied().filter(|v| !v.is_null()).collect();
    if present.is_empty() {
        return None;
    }
    let mut tags = serde_json::Map::new();
    let mut props = serde_json::Map::new();
    let mut sensitive: Vec<Value> = Vec::new();
    for m in present {
        if let Some(t) = m.get("tags").and_then(Value::as_object) {
            tags.extend(t.clone());
        }
        if let Some(p) = m.get("properties").and_then(Value::as_object) {
            props.extend(p.clone());
        }
        if let Some(s) = m.get("sensitive").and_then(Value::as_array) {
            for v in s {
                if !sensitive.contains(v) {
                    sensitive.push(v.clone());
                }
            }
        }
    }
    let mut out = serde_json::Map::new();
    if !tags.is_empty() {
        out.insert("tags".into(), Value::Object(tags));
    }
    if !props.is_empty() {
        out.insert("properties".into(), Value::Object(props));
    }
    if !sensitive.is_empty() {
        out.insert("sensitive".into(), Value::Array(sensitive));
    }
    Some(Value::Object(out))
}

/// Merge rule sets: rule lists concatenate, a later rule with the same `name` replaces an earlier one.
fn merge_rule_sets(layers: &[Option<&Value>]) -> Option<Value> {
    let present: Vec<&Value> = layers.iter().flatten().copied().filter(|v| !v.is_null()).collect();
    if present.is_empty() {
        return None;
    }
    let mut out = serde_json::Map::new();
    for key in ["migrationRules", "domainRules", "encodingRules"] {
        let mut rules: Vec<Value> = Vec::new();
        for rs in &present {
            for r in rs.get(key).and_then(Value::as_array).into_iter().flatten() {
                let name = r.get("name");
                if let Some(i) = rules.iter().position(|x| name.is_some() && x.get("name") == name) {
                    rules[i] = r.clone();
                } else {
                    rules.push(r.clone());
                }
            }
        }
        if !rules.is_empty() {
            out.insert(key.into(), Value::Array(rules));
        }
    }
    Some(Value::Object(out))
}

fn metadata_property<'a>(m: &'a Option<Value>, key: &str) -> Option<&'a Value> {
    m.as_ref()?.get("properties")?.get(key)
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

impl Registry {
    pub fn new(
        store: Store,
        snapshot: Snapshot,
        default_compatibility: CompatibilityLevel,
        cluster_id: String,
        cache_max_entries: u64,
        normalize_default: bool,
    ) -> Self {
        Self {
            store,
            snapshot: ArcSwap::from_pointee(snapshot),
            bodies: Cache::new(cache_max_entries),
            parsed_by_id: Cache::new(cache_max_entries),
            parsed_by_text: Cache::new(cache_max_entries),
            locks: Locks::default(),
            default_compatibility,
            normalize_default,
            cluster_id,
            limits: SearchLimits::default(),
            changes: Notify::new(),
        }
    }

    /// Exclude other writers from what this mutation touches. See [`Locks`].
    pub(crate) fn write_lock(&self, target: &crate::mutations::Target) -> WriteGuard<'_> {
        self.locks.acquire(target)
    }

    /// Which host container this registry is.
    pub fn container(&self) -> &crate::tenant::TenantId {
        self.store.tenant()
    }

    /// Pin the current snapshot for one request.
    pub fn reader(&self) -> Reader<'_> {
        Reader { snap: self.snapshot.load_full(), reg: self }
    }

    /// Durably commit `tx`, then publish the new snapshot with one pointer swap.
    /// Must be called with the write lock held (writers are serialized, so
    /// "load, apply, store" can't lose an update).
    pub(crate) fn commit(&self, tx: crate::store::Tx<'_>) -> ApiResult<()> {
        let ops = tx.commit()?;
        // Writers to different contexts run in parallel, but the snapshot is
        // one object: "load, apply, store" has to be serialised or one of them
        // loses its ops. Only this part, though - never the fsync above.
        let _publish = self.locks.publish.lock().unwrap_or_else(|e| e.into_inner());
        let mut next = Snapshot::clone(&self.snapshot.load());
        for op in &ops {
            if let crate::snapshot::Op::PutSchema { ctx, id, rec, .. } = op {
                self.bodies.insert((ctx.clone(), *id), rec.clone());
            }
            next.apply(op);
        }
        self.snapshot.store(Arc::new(next));
        self.changes.notify_waiters();
        Ok(())
    }


    // ---------------- config & mode resolution (Confluent's lookup cache) ----------------














    // ---------------- references ----------------









    // ---------------- compatibility ----------------



    // ---------------- registration (port of KafkaSchemaRegistry) ----------------





    /// `POST /subjects/{subject}/versions` (`SubjectVersionsResource#register`
    /// then `registerOrForward`).
    /// `register`, in `mutations::RegisterSchema`.
    pub fn register(&self, subject: &str, req: RegisterSchemaRequest, normalize: bool) -> ApiResult<RegisterResponse> {
        crate::engine::run(self, crate::mutations::RegisterSchema::new(subject, req, normalize)?)
    }

    pub(crate) fn register_fast_path(&self, r: &Reader<'_>, q: &QualifiedSubject, d: &Draft, normalize: bool) -> ApiResult<Option<RegisterResponse>> {
        let cfg = self.config_in_scope(r, q)?;
        if cfg.has_defaults_or_overrides() {
            return Ok(None);
        }
        let is_latest = d.version == -1;
        let Some(existing) = self.lookup_under_subject(r, q, d, normalize, false, is_latest)? else { return Ok(None) };
        if d.version == 0 || is_latest {
            if d.id < 0 || d.id as u32 == existing.id {
                return Ok(Some(RegisterResponse::id(existing.id)));
            }
        } else if d.id >= 0 && existing.id == d.id as u32 {
            if d.version > 0 && existing.version == d.version as u32 {
                return Ok(Some(RegisterResponse::full(&existing)));
            }
            if d.version > 0
                && let Some((v, vr)) = self.get_exact(r, q, VersionSpec::Exact(d.version as u32), false)?
            {
                let older = self.entity(r, q, v, &vr)?;
                if older.id == existing.id && older.fingerprint() == existing.fingerprint() {
                    return Ok(Some(RegisterResponse::full(&older)));
                }
            }
        }
        Ok(None)
    }


    /// `POST /subjects/{subject}/versions/{version}/tags`
    /// (`SubjectVersionsResource#modifyTags`, then `modifySchemaTags`): tags
    /// are written into the schema itself and the result is registered as a
    /// new version.
    pub fn modify_tags(&self, subject: &str, spec: VersionSpec, req: TagSchemaRequest) -> ApiResult<RegisterResponse> {
        let (schema_type, text, references, metadata, rule_set) = {
            let r = &self.reader();
            let q = QualifiedSubject::parse(subject)?;
            let Some((cq, v, vr)) = self.get_using_contexts(r, &q, spec, false)? else {
                return Err(if self.has_subjects(r, &q, false)? {
                    ApiError::version_not_found(spec)
                } else {
                    ApiError::subject_not_found(subject)
                });
            };
            let _ = v;
            let rec = r.get_schema(&cq.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            let new_version = req.new_version.unwrap_or(0);
            let metadata = with_confluent_version(req.metadata.clone().or_else(|| rec.metadata.clone()), new_version.max(0) as u32);
            let rule_set = self.rule_set_for_tags(r, &q, &req)?;
            let text = schema::tags::apply(rec.schema_type, &rec.schema, &req.tags_to_add, &req.tags_to_remove)
                .map_err(|e| ApiError::new(42201, e))?;
            (rec.schema_type, text, rec.references.clone(), metadata, rule_set)
        };
        self.register(
            subject,
            RegisterSchemaRequest {
                schema: Some(text),
                schema_type: Some(schema_type.as_str().to_string()),
                references: Some(references.into_iter().map(RefIn::from).collect()),
                metadata,
                rule_set,
                version: req.new_version,
                id: None,
            },
            false,
        )
    }




    // ---------------- reads ----------------

















    /// `GET .../versions/{v}/referencedby`: resolved like `getSchemaByVersion`
    /// with deleted versions included.
    pub fn referenced_by(&self, subject: &str, spec: VersionSpec) -> ApiResult<Vec<u32>> {
        let r = &self.reader();
        let (cq, v, _) = self.version_or_error(r, subject, spec, true)?;
        self.live_referrers(r, &cq.context, &cq.subject, v)
    }

    // ---------------- deletes ----------------


    /// The rows a hard delete of (subject, version) removes: the version
    /// itself, and the id's outgoing reference edges once nothing uses that id
    /// any more. Read-only - it plans, it does not write - so both the
    /// register verb and the legacy delete paths can share it.
    pub(crate) fn hard_delete_writes(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        version: u32,
        vr: &VersionRecord,
        also_removed: &HashSet<(String, u32)>,
    ) -> ApiResult<(Vec<crate::mutations::Write>, LogEvent)> {
        use crate::mutations::Write;
        let mut writes = vec![Write::DeleteVersion {
            ctx: q.context.clone(),
            subject: q.subject.clone(),
            version,
            id: vr.id,
        }];
        let still_used = r
            .id_usages(&q.context, vr.id)?
            .into_iter()
            .any(|(s, v)| !(s == q.subject && (v == version || also_removed.contains(&(s.clone(), v)))));
        if !still_used && let Some(rec) = r.get_schema(&q.context, vr.id)? {
            for reference in &rec.references {
                let t = Self::ref_target(&q.context, reference)?;
                writes.push(Write::DeleteRefby {
                    ctx: t.context.clone(),
                    subject: t.subject.clone(),
                    version: reference.version.max(1) as u32,
                    id: vr.id,
                });
            }
            // Nothing holds this id any more, so its content goes with it - as
            // in Confluent, where a tombstone drops the id from the index once
            // no subject-version refers to it (`InMemoryCache#schemaTombstoned`
            // removes the guid when its subject-version map empties). It is
            // also the only thing that ever frees what a deleted schema took.
            writes.push(Write::DeleteSchema { ctx: q.context.clone(), id: vr.id });
        }
        let event = LogEvent {
            ctx: q.context.clone(),
            subject: q.subject.clone(),
            version,
            id: vr.id,
            kind: LogEventKind::HardDelete,
        };
        Ok((writes, event))
    }

    /// `DELETE /subjects/{subject}` (`SubjectsResource#deleteSubject`, then `deleteSubject`).
    /// `deleteSubject`, in `mutations::DeleteSubject`.
    pub fn delete_subject(&self, subject: &str, permanent: bool) -> ApiResult<Vec<u32>> {
        crate::engine::run(self, crate::mutations::DeleteSubject::new(subject, permanent)?)
    }

    /// `DELETE /subjects/{subject}/versions/{version}` (`SubjectVersionsResource#deleteSchemaVersion`,
    /// then `deleteSchemaVersion`).
    /// `deleteSchemaVersion`, in `mutations::DeleteSubjectVersion`.
    pub fn delete_version(&self, subject: &str, spec: VersionSpec, permanent: bool) -> ApiResult<u32> {
        crate::engine::run(self, crate::mutations::DeleteSubjectVersion::new(subject, spec, permanent)?)
    }

    // ---------------- config ----------------


    /// `updateConfig`. The work is in `mutations::UpdateCompatibility`; this
    /// is the name the rest of the code still calls it by.
    pub fn set_config(&self, subject: Option<&str>, update: ConfigRecord) -> ApiResult<()> {
        crate::engine::run(self, crate::mutations::UpdateCompatibility::new(subject, update)?)
    }

    /// `deleteConfig`, in `mutations::DeleteSubjectConfig`.
    pub fn delete_config(&self, subject: Option<&str>) -> ApiResult<ConfigRecord> {
        crate::engine::run(self, crate::mutations::DeleteSubjectConfig::new(subject)?)
    }

    // ---------------- mode ----------------


    /// `setMode`: entering IMPORT without `force` requires that no live
    /// subject is in scope, and hard-deletes the soft-deleted ones.
    /// `setMode`, in `mutations::SetMode`.
    pub fn set_mode(&self, subject: Option<&str>, mode: Mode, force: bool) -> ApiResult<()> {
        crate::engine::run(self, crate::mutations::SetMode::new(subject, mode, force)?)
    }

    /// Returns the mode that was set (`getMode`).
    /// `deleteSubjectMode`, in `mutations::DeleteMode`.
    pub fn delete_mode(&self, subject: &str) -> ApiResult<Mode> {
        crate::engine::run(self, crate::mutations::DeleteMode::new(subject)?)
    }


    // ---------------- admin views ----------------




    // ---------------- contexts ----------------

    pub fn list_contexts(&self) -> ApiResult<Vec<String>> {
        let r = &self.reader();
        r.list_contexts()
    }

    /// Deleting an empty context, in `mutations::DeleteContext`.
    pub fn delete_context(&self, ctx: &str) -> ApiResult<()> {
        crate::engine::run(self, crate::mutations::DeleteContext::new(ctx)?)
    }

    // ---------------- exporters ----------------

    /// The worker's own bookkeeping takes the exporter lock directly: a cursor
    /// written after every batch is not a mutation, and running it through the
    /// engine would make an export wait behind registrations.
    fn exporter_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.locks.exporters.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The exporter verbs, in `mutations::exporters`.
    pub fn create_exporter(&self, req: ExporterUpdateRequest) -> ApiResult<String> {
        crate::engine::run(self, crate::mutations::CreateExporter::new(req))
    }

    pub fn update_exporter(&self, name: &str, req: ExporterUpdateRequest) -> ApiResult<String> {
        crate::engine::run(self, crate::mutations::UpdateExporter::new(name, req))
    }

    pub fn update_exporter_config(&self, name: &str, cfg: serde_json::Map<String, Value>) -> ApiResult<String> {
        crate::engine::run(self, crate::mutations::UpdateExporter::config_only(name, cfg))
    }

    pub fn get_exporter(&self, name: &str) -> ApiResult<ExporterRecord> {
        self.store.get_exporter(name)?.ok_or_else(|| ApiError::exporter_not_found(name))
    }

    pub fn list_exporters(&self) -> ApiResult<Vec<String>> {
        Ok(self.store.list_exporters()?.into_iter().map(|e| e.info.name).collect())
    }

    pub fn delete_exporter(&self, name: &str) -> ApiResult<()> {
        crate::engine::run(self, crate::mutations::DeleteExporter::new(name))
    }

    /// Pause / resume / reset.
    /// Pause / resume / reset.
    pub fn exporter_transition(&self, name: &str, action: &str) -> ApiResult<String> {
        crate::engine::run(self, crate::mutations::TransitionExporter::new(name, action))
    }

    /// Called by the exporter worker. Only applies if the exporter still exists
    /// and wasn't reset/reconfigured concurrently (offset/state unchanged).
    pub fn exporter_progress(&self, name: &str, expected_offset: u64, new_offset: u64, error: Option<String>) -> ApiResult<()> {
        let state = if error.is_some() { ExporterState::Error } else { ExporterState::Running };
        self.exporter_state(name, expected_offset, new_offset, state, error)
    }

    /// Called by the exporter worker with the state its last attempt earned:
    /// `Running`, `Error` (retried), `Paused` (the destination needs an
    /// operator) or `Failed` (the replay conflicts; only a reset moves on).
    /// Only applies if the exporter still exists and wasn't reset or paused
    /// meanwhile (offset and state unchanged).
    pub fn exporter_state(
        &self,
        name: &str,
        expected_offset: u64,
        new_offset: u64,
        state: ExporterState,
        trace: Option<String>,
    ) -> ApiResult<()> {
        let _g = self.exporter_lock();
        let Some(mut rec) = self.store.get_exporter(name)? else { return Ok(()) };
        if rec.offset != expected_offset || matches!(rec.state, ExporterState::Paused | ExporterState::Failed) {
            return Ok(());
        }
        rec.offset = new_offset;
        rec.ts = now_millis();
        rec.state = state;
        rec.trace = trace.unwrap_or_default();
        self.store.put_exporter(&rec, &crate::modegate::Allowed::not_schema_state())
    }


}

pub fn exporter_status_json(rec: &ExporterRecord) -> Value {
    let mut v = json!({
        "name": rec.info.name,
        "state": rec.state,
        "offset": rec.offset,
        "ts": rec.ts,
    });
    if !rec.trace.is_empty() {
        v["trace"] = Value::String(rec.trace.clone());
    }
    v
}

