//! Registry semantics on top of the store: registration, lookup, soft/hard
//! delete, compatibility, config, mode, contexts and exporter bookkeeping.
//!
//! Concurrency model: reads go straight to RocksDB with no locking. Every
//! mutation takes `write_lock`, re-reads what it needs, and commits one atomic
//! `WriteBatch`. With a single server this gives the same linearizable
//! behaviour Confluent gets from its single-leader Kafka log, without the log.

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
    pub fn referenced_by(&self, ctx: &str, subject: &str, version: u32) -> ApiResult<Vec<u32>> {
        Ok(self.snap.referenced_by(ctx, subject, version))
    }
    pub fn id_for_fingerprint(&self, ctx: &str, fp: &str) -> ApiResult<Option<u32>> {
        Ok(self.snap.id_for_fingerprint(ctx, fp))
    }
    pub fn next_id(&self, ctx: &str) -> ApiResult<u32> {
        Ok(self.snap.next_id(ctx))
    }
    pub fn list_contexts(&self) -> ApiResult<Vec<String>> {
        Ok(self.snap.list_contexts())
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
    write_lock: Mutex<()>,
    exporter_lock: Mutex<()>,
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

fn is_avro(t: &Option<&'static str>) -> bool {
    t.is_none()
}

/// A subject-version, as returned by `GET /subjects/{s}/versions/{v}`, lookup and `GET /schemas`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaView {
    pub subject: String,
    pub version: u32,
    pub id: u32,
    #[serde(skip_serializing_if = "is_avro")]
    pub schema_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<Value>,
    pub schema: String,
    /// Subjects aliasing this one (`GET /schemas?aliases=true` only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aliases: Option<Vec<String>>,
}

/// `GET /schemas/ids/{id}`
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaByIdView {
    #[serde(skip_serializing_if = "is_avro")]
    pub schema_type: Option<&'static str>,
    pub schema: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SubjectVersion {
    pub subject: String,
    pub version: u32,
}

/// Confluent's `RegisterSchemaResponse`: just `{"id"}` normally; the full
/// entity when the registered schema was "modified" (metadata or rules
/// merged in, or the schema taken from the previous version) and for IMPORT
/// replays.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponse {
    pub id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl RegisterResponse {
    fn id(id: u32) -> Self {
        Self { id, version: None, schema_type: None, references: Vec::new(), metadata: None, rule_set: None, schema: None }
    }

    /// `new RegisterSchemaResponse(schema)` for a "modified" or replayed schema.
    fn full(e: &Entity) -> Self {
        Self {
            id: e.id,
            version: Some(e.version),
            schema_type: schema_type_view(e.schema_type),
            references: e.references.clone(),
            metadata: e.metadata.clone(),
            rule_set: e.rule_set.clone(),
            schema: Some(e.schema.clone()),
        }
    }
}

/// Confluent's `Schema` entity while a request is processed (`new Schema(subject, request)`).
#[derive(Debug, Clone)]
struct Draft {
    schema: Option<String>,
    /// As sent; `None` is AVRO.
    schema_type: Option<String>,
    refs: Vec<RefIn>,
    metadata: Option<Value>,
    rule_set: Option<Value>,
    /// 0 when not given.
    version: i32,
    /// -1 when not given.
    id: i32,
}

impl From<RegisterSchemaRequest> for Draft {
    fn from(r: RegisterSchemaRequest) -> Self {
        Self {
            schema: r.schema,
            schema_type: r.schema_type,
            refs: r.references.unwrap_or_default(),
            metadata: r.metadata,
            rule_set: r.rule_set,
            version: r.version.unwrap_or(0),
            id: r.id.unwrap_or(-1),
        }
    }
}

impl Draft {
    fn refs_string(&self) -> String {
        format!("[{}]", self.refs.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "))
    }

    /// `Schema#toString`.
    fn entity_string(&self, q: &QualifiedSubject) -> String {
        format!(
            "{{subject={},version={},id={},schemaType={},references={},metadata={},ruleSet={},schema={},schemaTags=null}}",
            q.qualified(),
            self.version,
            self.id,
            self.schema_type.as_deref().unwrap_or("AVRO"),
            self.refs_string(),
            java_metadata(&self.metadata),
            java_rule_set(&self.rule_set),
            self.schema.as_deref().unwrap_or("null"),
        )
    }

    /// Confluent's 42201 for a schema that failed to parse or to validate.
    fn invalid(&self, q: &QualifiedSubject, kind: schema::ErrorKind, detail: &str) -> ApiError {
        let entity = self.entity_string(q);
        ApiError::new(
            42201,
            match kind {
                schema::ErrorKind::Parse => format!(
                    "Invalid schema {entity} with refs {} of type {}, details: {detail}",
                    self.refs_string(),
                    self.schema_type.as_deref().unwrap_or("AVRO")
                ),
                schema::ErrorKind::Validate => format!("Invalid schema {entity}, details: {detail}"),
            },
        )
    }
}

/// Java's `AbstractMap#toString` / `AbstractCollection#toString` of a JSON value.
fn java_string(v: &Value) -> String {
    match v {
        Value::Object(o) => format!("{{{}}}", o.iter().map(|(k, x)| format!("{k}={}", java_string(x))).collect::<Vec<_>>().join(", ")),
        Value::Array(a) => format!("[{}]", a.iter().map(java_string).collect::<Vec<_>>().join(", ")),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `Metadata#toString`.
fn java_metadata(m: &Option<Value>) -> String {
    let Some(m) = m else { return "null".into() };
    let part = |k: &str, empty: &str| m.get(k).map(java_string).unwrap_or_else(|| empty.into());
    format!("Metadata{{tags={}, properties={}, sensitive={}}}", part("tags", "{}"), part("properties", "{}"), part("sensitive", "[]"))
}

/// `RuleSet#toString` (encoding rules are not printed).
fn java_rule_set(r: &Option<Value>) -> String {
    let Some(r) = r else { return "null".into() };
    let rules = |k: &str| {
        r.get(k).and_then(Value::as_array).map_or_else(
            || "[]".to_string(),
            |a| {
                let one = |x: &Value| {
                    let f = |n: &str| x.get(n).map(java_string).unwrap_or_else(|| "null".into());
                    format!(
                        "Rule{{name={}, doc={}, kind={}, mode={}, type='{}', tags='{}', params='{}', expr='{}', onSuccess='{}', onFailure='{}', disabled='{}'}}",
                        f("name"), f("doc"), f("kind"), f("mode"), f("type"), f("tags"), f("params"), f("expr"), f("onSuccess"), f("onFailure"),
                        x.get("disabled").map(java_string).unwrap_or_else(|| "false".into())
                    )
                };
                format!("[{}]", a.iter().map(one).collect::<Vec<_>>().join(", "))
            },
        )
    };
    format!("Rules{{migrationRules={}, domainRules={}}}", rules("migrationRules"), rules("domainRules"))
}

/// A request schema after `canonicalizeSchema`.
struct Canon {
    schema_type: SchemaType,
    /// Canonical, or normalized when normalizing.
    text: String,
    refs: Vec<SchemaReference>,
    targets: Vec<(QualifiedSubject, u32)>,
    metadata: Option<Value>,
    rule_set: Option<Value>,
    parsed: Arc<ParsedSchema>,
}

impl Canon {
    /// Confluent's `MD5.ofSchema` (plus the type: identical text under two
    /// types is two schemas here; Confluent rejects the second with 42205).
    fn fingerprint(&self) -> String {
        schema::fingerprint(self.schema_type, &self.text, &self.refs, self.metadata.as_ref(), self.rule_set.as_ref())
    }
}

/// A stored or matched `Schema` entity: what lookups and version reads answer.
#[derive(Debug, Clone)]
struct Entity {
    subject: String,
    version: u32,
    id: u32,
    schema_type: SchemaType,
    references: Vec<SchemaReference>,
    metadata: Option<Value>,
    rule_set: Option<Value>,
    schema: String,
}

impl Entity {
    fn stored(q: &QualifiedSubject, version: u32, id: u32, rec: &SchemaRecord) -> Self {
        Self {
            subject: q.qualified(),
            version,
            id,
            schema_type: rec.schema_type,
            references: rec.references.clone(),
            metadata: rec.metadata.clone(),
            rule_set: rec.rule_set.clone(),
            schema: rec.schema.clone(),
        }
    }

    fn from_canon(q: &QualifiedSubject, version: u32, id: u32, c: &Canon) -> Self {
        Self {
            subject: q.qualified(),
            version,
            id,
            schema_type: c.schema_type,
            references: c.refs.clone(),
            metadata: c.metadata.clone(),
            rule_set: c.rule_set.clone(),
            schema: c.text.clone(),
        }
    }

    fn fingerprint(&self) -> String {
        schema::fingerprint(self.schema_type, &self.schema, &self.references, self.metadata.as_ref(), self.rule_set.as_ref())
    }

    fn view(self) -> SchemaView {
        SchemaView {
            subject: self.subject,
            version: self.version,
            id: self.id,
            schema_type: schema_type_view(self.schema_type),
            references: self.references,
            metadata: self.metadata,
            rule_set: self.rule_set,
            schema: self.schema,
            aliases: None,
        }
    }
}

fn confluent_version(m: &Option<Value>) -> Option<String> {
    metadata_property(m, "confluent:version").and_then(Value::as_str).map(String::from)
}

/// `Metadata.removeConfluentVersion`, in canonical form.
fn without_confluent_version(m: &Option<Value>) -> Option<Value> {
    let mut m = m.clone()?;
    if let Some(props) = m.get_mut("properties").and_then(Value::as_object_mut) {
        props.remove("confluent:version");
        if props.is_empty() {
            m.as_object_mut().expect("object").remove("properties");
        }
    }
    Some(m)
}

/// `ParsedSchema#equivalent`: same type, canonical text, metadata and rules.
fn equivalent(c: &Canon, c_meta: &Option<Value>, p: &Entity, p_meta: &Option<Value>) -> bool {
    c.schema_type == p.schema_type && c.text == p.schema && c_meta == p_meta && c.rule_set == p.rule_set
}

/// `AbstractSchemaProvider.canLookupIgnoringVersion`.
fn can_lookup_ignoring_version(c: &Canon, p: &Entity) -> bool {
    let cv = confluent_version(&c.metadata).and_then(|v| v.parse::<u32>().ok());
    let pv = confluent_version(&p.metadata).and_then(|v| v.parse::<u32>().ok());
    let empty = || Some(json!({}));
    match (cv, pv) {
        (None, Some(_)) => equivalent(c, &c.metadata.clone().or_else(empty), p, &without_confluent_version(&p.metadata)),
        (Some(v), None) => {
            v == p.version && equivalent(c, &without_confluent_version(&c.metadata), p, &p.metadata.clone().or_else(empty))
        }
        _ => equivalent(c, &c.metadata, p, &p.metadata),
    }
}

/// `ParsedSchema#canLookup`: exact content matches are found through the
/// hash index; this only covers a request without references matching a
/// stored schema with them, and `confluent:version` bookkeeping.
fn can_lookup(c: &Canon, p: &Entity) -> bool {
    if c.refs.is_empty() && !p.references.is_empty() && can_lookup_ignoring_version(c, p) {
        return true;
    }
    if confluent_version(&c.metadata).is_some() || confluent_version(&p.metadata).is_some() {
        return c.refs == p.references && can_lookup_ignoring_version(c, p);
    }
    false
}

/// Set `metadata.properties["confluent:version"]`.
fn with_confluent_version(metadata: Option<Value>, version: u32) -> Option<Value> {
    let mut m = metadata.unwrap_or_else(|| json!({}));
    if !m.get("properties").is_some_and(Value::is_object) {
        m["properties"] = json!({});
    }
    m["properties"]["confluent:version"] = Value::String(version.to_string());
    Some(m)
}

/// A live version considered for a compatibility check.
struct OldVersion {
    version: u32,
    id: u32,
    rec: Arc<SchemaRecord>,
}

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
            write_lock: Mutex::new(()),
            exporter_lock: Mutex::new(()),
            default_compatibility,
            normalize_default,
            cluster_id,
            limits: SearchLimits::default(),
            changes: Notify::new(),
        }
    }

    pub(crate) fn write_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock.lock().unwrap_or_else(|e| e.into_inner())
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

    /// Parse a request schema, memoized by (type, text, resolved references).
    fn parse_cached(
        &self,
        schema_type: SchemaType,
        text: &str,
        refs: &[ResolvedRef],
        validate_defaults: bool,
    ) -> Result<Arc<ParsedSchema>, schema::SchemaError> {
        let mut h = Sha256::new();
        h.update(schema_type.as_str());
        h.update([validate_defaults as u8]);
        h.update(text);
        for r in refs {
            h.update([0]);
            h.update(&r.name);
            h.update([1]);
            h.update(&r.schema);
        }
        let key: [u8; 32] = h.finalize().into();
        if let Some(p) = self.parsed_by_text.get(&key) {
            return Ok(p);
        }
        let parsed = Arc::new(schema::parse_with(schema_type, text, refs, validate_defaults)?);
        self.parsed_by_text.insert(key, parsed.clone());
        Ok(parsed)
    }

    // ---------------- config & mode resolution (Confluent's lookup cache) ----------------

    /// The config/mode key of a subject: `:.ctx:` (no subject) is the context itself.
    fn scope_for(q: &QualifiedSubject) -> Scope {
        if q.is_context_only() {
            Scope::Context(q.context.clone())
        } else {
            Scope::Subject(q.context.clone(), q.subject.clone())
        }
    }

    /// Confluent fills a missing level with the server default on every read.
    /// Our `normalize` server default (on unless configured off) is filled the same way.
    fn filled(&self, mut c: ConfigRecord) -> ConfigRecord {
        c.compatibility_level.get_or_insert(self.default_compatibility);
        if self.normalize_default {
            c.normalize.get_or_insert(true);
        }
        c
    }

    /// `getConfig(subject)`: the record stored for exactly this scope (the
    /// global scope always has one: stored or the default).
    pub(crate) fn config_of(&self, r: &Reader<'_>, q: Option<&QualifiedSubject>) -> ApiResult<Option<ConfigRecord>> {
        Ok(match q {
            None => Some(self.filled(r.get_config(&Scope::Global)?.unwrap_or_default())),
            Some(q) => r.get_config(&Self::scope_for(q))?.map(|c| self.filled(c)),
        })
    }

    /// `getConfigInScope(subject)`: the subject's record, else its context's
    /// (non-default contexts) or the global one (default context), else the
    /// server default. Records are not merged.
    pub fn config_in_scope(&self, r: &Reader<'_>, q: &QualifiedSubject) -> ApiResult<ConfigRecord> {
        if let Some(c) = r.get_config(&Self::scope_for(q))? {
            return Ok(self.filled(c));
        }
        let parent = if q.context != DEFAULT_CONTEXT {
            r.get_config(&Scope::Context(q.context.clone()))?
        } else {
            r.get_config(&Scope::Global)?
        };
        Ok(self.filled(parent.unwrap_or_default()))
    }

    /// The effective `normalize` for a request (our server default applies
    /// when no config in scope says otherwise).
    fn normalize_in_scope(&self, r: &Reader<'_>, q: &QualifiedSubject) -> ApiResult<bool> {
        Ok(self.config_in_scope(r, q)?.normalize == Some(true))
    }

    pub(crate) fn global_mode_in(&self, r: &Reader<'_>) -> ApiResult<Mode> {
        Ok(r.get_mode(&Scope::Global)?.unwrap_or(Mode::Readwrite))
    }

    /// `getMode(subject)`: a global READONLY_OVERRIDE wins, else exactly this scope's mode.
    fn mode_of(&self, r: &Reader<'_>, q: Option<&QualifiedSubject>) -> ApiResult<Option<Mode>> {
        let global = self.global_mode_in(r)?;
        if global == Mode::ReadonlyOverride {
            return Ok(Some(global));
        }
        match q {
            None => Ok(Some(global)),
            Some(q) => r.get_mode(&Self::scope_for(q)),
        }
    }

    /// `getModeInScope(subject)`: a global READONLY_OVERRIDE wins; else the
    /// subject's mode, else its context's (non-default contexts) or the global
    /// one (default context), else READWRITE.
    pub fn mode_in_scope(&self, r: &Reader<'_>, q: &QualifiedSubject) -> ApiResult<Mode> {
        let global = self.global_mode_in(r)?;
        if global == Mode::ReadonlyOverride {
            return Ok(global);
        }
        if let Some(m) = r.get_mode(&Self::scope_for(q))? {
            return Ok(m);
        }
        let parent = if q.context != DEFAULT_CONTEXT { r.get_mode(&Scope::Context(q.context.clone()))? } else { Some(global) };
        Ok(parent.unwrap_or(Mode::Readwrite))
    }

    /// `isReadOnlyMode` guard shared by every write except register.
    fn check_not_read_only(&self, r: &Reader<'_>, q: Option<&QualifiedSubject>) -> ApiResult<crate::modegate::Allowed> {
        let mode = match q {
            Some(q) => self.mode_in_scope(r, q)?,
            None => self.global_mode_in(r)?,
        };
        let name = q.map(|q| q.qualified()).unwrap_or_else(|| "null".into());
        crate::modegate::check(crate::modegate::Intent::Modify, mode, &name)
    }

    /// `hasSubjects(subject, lookupDeleted)`: the subject has a (live) version;
    /// a context-only name (`:.ctx:`) matches any subject in the context.
    fn has_subjects(&self, r: &Reader<'_>, q: &QualifiedSubject, deleted: bool) -> ApiResult<bool> {
        let any = |vs: Vec<(u32, VersionRecord)>| vs.iter().any(|(_, v)| deleted || !v.deleted);
        if any(r.list_versions(&q.context, &q.subject)?) {
            return Ok(true);
        }
        if q.is_context_only() {
            for name in r.list_subject_names(&q.context)? {
                if any(r.list_versions(&q.context, &name)?) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// `hasSubjects(null, ..)`: any subject anywhere.
    fn has_any_subject(&self, r: &Reader<'_>, deleted: bool) -> ApiResult<bool> {
        for ctx in r.list_contexts()? {
            for name in r.list_subject_names(&ctx)? {
                if r.list_versions(&ctx, &name)?.iter().any(|(_, v)| deleted || !v.deleted) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// `get(subject, version, returnDeleted)`: `latest` is the newest live version.
    fn get_exact(&self, r: &Reader<'_>, q: &QualifiedSubject, spec: VersionSpec, deleted: bool) -> ApiResult<Option<(u32, VersionRecord)>> {
        let vs = r.list_versions(&q.context, &q.subject)?;
        Ok(match spec {
            VersionSpec::Latest => vs.into_iter().rev().find(|(_, v)| !v.deleted),
            VersionSpec::Exact(n) => vs.into_iter().find(|(v, x)| *v == n && (deleted || !x.deleted)),
        })
    }

    /// `getUsingContexts`: an unqualified subject not found in the default
    /// context is looked up under the same name in every other context.
    fn get_using_contexts(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        spec: VersionSpec,
        deleted: bool,
    ) -> ApiResult<Option<(QualifiedSubject, u32, VersionRecord)>> {
        if let Some((v, vr)) = self.get_exact(r, q, spec, deleted)? {
            return Ok(Some((q.clone(), v, vr)));
        }
        if q.context != DEFAULT_CONTEXT {
            return Ok(None);
        }
        for ctx in r.list_contexts()?.into_iter().filter(|c| c != DEFAULT_CONTEXT) {
            let cq = QualifiedSubject::new(&ctx, &q.subject);
            if let Some((v, vr)) = self.get_exact(r, &cq, spec, deleted)? {
                return Ok(Some((cq, v, vr)));
            }
        }
        Ok(None)
    }

    fn entity(&self, r: &Reader<'_>, q: &QualifiedSubject, version: u32, vr: &VersionRecord) -> ApiResult<Entity> {
        let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        Ok(Entity::stored(q, version, vr.id, &rec))
    }

    // ---------------- references ----------------

    fn ref_target(ctx: &str, r: &SchemaReference) -> ApiResult<QualifiedSubject> {
        let q = QualifiedSubject::parse_subject(&r.subject)?;
        Ok(if r.subject.starts_with(":.") { q } else { QualifiedSubject::new(ctx, &q.subject) })
    }

    /// Confluent's `AbstractSchemaProvider#resolveReferences`: every reference
    /// must name a subject and version (`-1` is the latest); soft-deleted
    /// versions resolve. Returns the concrete references, the transitive
    /// closure (dependencies first) for the parsers, and the direct targets.
    fn resolve_references(
        &self,
        r: &Reader<'_>,
        ctx: &str,
        refs: &[RefIn],
    ) -> Result<(Vec<SchemaReference>, Vec<ResolvedRef>, Vec<(QualifiedSubject, u32)>), String> {
        let mut concrete = Vec::with_capacity(refs.len());
        let mut targets = Vec::with_capacity(refs.len());
        let mut out = Vec::new();
        let mut visited = HashSet::new();
        for rin in refs {
            if rin.null {
                return Err("Cannot invoke \"io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference.getName()\" because \"reference\" is null".into());
            }
            let (Some(name), Some(subject), Some(version)) = (&rin.name, &rin.subject, rin.version) else {
                return Err(format!("Invalid reference: {rin}"));
            };
            let sr = SchemaReference { name: name.clone(), subject: subject.clone(), version };
            let q = Self::ref_target(ctx, &sr).map_err(|e| e.message)?;
            let spec = match version {
                -1 => VersionSpec::Latest,
                v if v > 0 => VersionSpec::Exact(v as u32),
                v => return Err(format!("io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException: {v}")),
            };
            let Some((v, _)) = self.get_exact(r, &q, spec, true).map_err(|e| e.message)? else {
                return Err(format!("No schema reference found for subject \"{}\" and version {version}", q.qualified()));
            };
            let sr = SchemaReference { version: v as i32, ..sr };
            self.collect_ref(r, &sr.name, &q, v, &mut visited, &mut out, 0).map_err(|e| e.message)?;
            concrete.push(sr);
            targets.push((q, v));
        }
        Ok((concrete, out, targets))
    }

    /// Resolve the references of a stored schema.
    fn resolve_stored(&self, r: &Reader<'_>, ctx: &str, refs: &[SchemaReference]) -> ApiResult<Vec<ResolvedRef>> {
        let ins: Vec<RefIn> = refs.iter().cloned().map(RefIn::from).collect();
        self.resolve_references(r, ctx, &ins).map(|(_, resolved, _)| resolved).map_err(ApiError::internal)
    }

    fn collect_ref(
        &self,
        r: &Reader<'_>,
        name: &str,
        q: &QualifiedSubject,
        version: u32,
        visited: &mut HashSet<(String, String, u32)>,
        out: &mut Vec<ResolvedRef>,
        depth: usize,
    ) -> ApiResult<()> {
        if depth > 64 {
            return Err(ApiError::invalid_schema("reference chain too deep"));
        }
        if !visited.insert((q.context.clone(), q.subject.clone(), version)) {
            return Ok(());
        }
        let vr = r.get_version(&q.context, &q.subject, version)?.ok_or_else(|| {
            ApiError::new(42201, format!("No schema reference found for subject \"{}\" and version {version}", q.qualified()))
        })?;
        let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        for sr in &rec.references {
            let t = Self::ref_target(&q.context, sr)?;
            self.collect_ref(r, &sr.name, &t, sr.version.max(1) as u32, visited, out, depth + 1)?;
        }
        out.push(ResolvedRef { name: name.to_string(), schema: rec.schema.clone() });
        Ok(())
    }

    /// Apply a `format` query parameter. Only Protobuf `serialized` (base64
    /// FileDescriptorProto, used by non-Java clients) changes the output.
    fn formatted(&self, r: &Reader<'_>, ctx: &str, id: u32, rec: &SchemaRecord, format: Option<&str>) -> ApiResult<String> {
        if rec.schema_type == SchemaType::Protobuf
            && format == Some("ignore_extensions")
            && let Some(text) = schema::proto_wire::without_extensions(&rec.schema)
        {
            return Ok(text);
        }
        if rec.schema_type == SchemaType::Protobuf
            && format == Some("serialized")
            && let schema::Parsed::Protobuf(p) = &self.parse_record(r, ctx, id, rec)?.inner
            && let Some(s) = p.serialized()
        {
            return Ok(s);
        }
        Ok(rec.schema.clone())
    }

    /// Parse a stored schema, through the cache.
    ///
    /// Content under an id never changes (registering over an id is refused,
    /// and a hard delete removes versions, not bodies), so `(context, id)`
    /// would be enough for the schema itself. It is not enough for the parse:
    /// that also depends on what the schema's references resolved to, and a
    /// reference is a (subject, version) pair whose content *can* change -
    /// hard-delete the referrer, then re-import its target differently, and
    /// the same id would parse against something new. Keying on the resolved
    /// closure as well makes a stale entry impossible to look up rather than
    /// merely unlikely.
    fn parse_record(&self, r: &Reader<'_>, ctx: &str, id: u32, rec: &SchemaRecord) -> ApiResult<Arc<ParsedSchema>> {
        let resolved = self.resolve_stored(r, ctx, &rec.references)?;
        let key = (ctx.to_string(), id, Self::deps_fingerprint(&resolved));
        if let Some(p) = self.parsed_by_id.get(&key) {
            return Ok(p);
        }
        let parsed = schema::parse_with(rec.schema_type, &rec.schema, &resolved, false)
            .map_err(|e| ApiError::internal(format!("stored schema no longer parses: {e}")))?;
        let parsed = Arc::new(parsed);
        self.parsed_by_id.insert(key, parsed.clone());
        Ok(parsed)
    }

    /// Parse a stored schema by id, for tests that need to see whether the
    /// cache handed back the same parse.
    #[cfg(test)]
    pub(crate) fn parse_for_test(&self, ctx: &str, id: u32) -> ApiResult<Arc<ParsedSchema>> {
        let r = &self.reader();
        let rec = r.get_schema(ctx, id)?.ok_or_else(ApiError::schema_not_found)?;
        self.parse_record(r, ctx, id, &rec)
    }

    /// What a schema's references resolved to, as a cache key component.
    /// A schema without references - the common case - costs nothing.
    fn deps_fingerprint(resolved: &[ResolvedRef]) -> [u8; 32] {
        if resolved.is_empty() {
            return [0; 32];
        }
        let mut h = Sha256::new();
        for r in resolved {
            h.update([0]);
            h.update(&r.name);
            h.update([1]);
            h.update(&r.schema);
        }
        h.finalize().into()
    }

    // ---------------- compatibility ----------------

    /// Check `new` against `olds` (newest first) at `level`, with the server's
    /// trailer (`{validateFields: ..., compatibility: ...}`) on failure.
    fn check_compatibility(
        &self,
        r: &Reader<'_>,
        ctx: &str,
        new: &ParsedSchema,
        olds: &[OldVersion],
        level: CompatibilityLevel,
        cfg: &ConfigRecord,
    ) -> ApiResult<Vec<String>> {
        if level == CompatibilityLevel::None || olds.is_empty() {
            return Ok(Vec::new());
        }
        let needed = if level.transitive() { olds.len() } else { 1 };
        let mut parsed = Vec::with_capacity(needed);
        for OldVersion { version, id, rec } in &olds[..needed] {
            parsed.push((*version, self.parse_record(r, ctx, *id, rec)?));
        }
        let previous: Vec<schema::Previous<'_>> =
            parsed.iter().map(|(v, p)| schema::Previous { version: *v, schema: p.as_ref() }).collect();
        let mut msgs = schema::check_level(new, &previous, level);
        if !msgs.is_empty() {
            msgs.push(format!(
                "{{validateFields: '{}', compatibility: '{}'}}",
                cfg.validate_fields.unwrap_or(false),
                level.as_str()
            ));
        }
        Ok(msgs)
    }

    /// Live versions of a subject that participate in compatibility checks, newest first.
    fn compat_candidates(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        cfg: &ConfigRecord,
        new_metadata: &Option<Value>,
    ) -> ApiResult<Vec<OldVersion>> {
        let mut out = Vec::new();
        for (v, vr) in r.list_versions(&q.context, &q.subject)?.into_iter().rev() {
            if vr.deleted {
                continue;
            }
            let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            if let Some(group) = cfg.compatibility_group.as_deref()
                && metadata_property(&rec.metadata, group) != metadata_property(new_metadata, group)
            {
                continue;
            }
            out.push(OldVersion { version: v, id: vr.id, rec });
        }
        Ok(out)
    }

    // ---------------- registration (port of KafkaSchemaRegistry) ----------------

    /// Parse, validate and (optionally) normalize a request schema:
    /// `canonicalizeSchema`. `None` for an empty schema.
    fn canonicalize(&self, r: &Reader<'_>, q: &QualifiedSubject, d: &Draft, is_new: bool, normalize: bool) -> ApiResult<Option<Canon>> {
        let Some(text) = d.schema.as_deref().filter(|s| !s.trim().is_empty()) else { return Ok(None) };
        let schema_type = SchemaType::parse(d.schema_type.as_deref())?;
        let (refs, resolved, targets) = self
            .resolve_references(r, &q.context, &d.refs)
            .map_err(|detail| d.invalid(q, schema::ErrorKind::Parse, &detail))?;
        let parsed = self
            .parse_cached(schema_type, text, &resolved, normalize && is_new)
            .map_err(|e| d.invalid(q, e.kind, &e.message))?;
        if normalize && let Some(e) = &parsed.normalize_error {
            return Err(d.invalid(q, schema::ErrorKind::Validate, e));
        }
        let text = if normalize { parsed.normalized.clone() } else { parsed.canonical.clone() };
        Ok(Some(Canon { schema_type, text, refs, targets, metadata: d.metadata.clone(), rule_set: d.rule_set.clone(), parsed }))
    }

    /// Confluent's `lookupCache.schemaIdAndSubjects(schema)`: the id holding
    /// this exact content (text, references, metadata, rules) in the context,
    /// if any subject-version still uses it, and this subject's version of it.
    fn hash_lookup(&self, r: &Reader<'_>, q: &QualifiedSubject, c: &Canon) -> ApiResult<Option<(u32, Option<(u32, VersionRecord)>)>> {
        let Some(id) = r.id_for_fingerprint(&q.context, &c.fingerprint())? else { return Ok(None) };
        let usages = r.id_usages(&q.context, id)?;
        if usages.is_empty() {
            return Ok(None);
        }
        let v = usages.iter().filter(|(s, _)| *s == q.subject).map(|(_, v)| *v).max();
        let sv = match v {
            Some(v) => r.get_version(&q.context, &q.subject, v)?.map(|vr| (v, vr)),
            None => None,
        };
        Ok(Some((id, sv)))
    }

    /// `lookUpSchemaUnderSubject`.
    fn lookup_under_subject(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        d: &Draft,
        normalize: bool,
        lookup_deleted: bool,
        latest_only: bool,
    ) -> ApiResult<Option<Entity>> {
        let canon = self.canonicalize(r, q, d, false, normalize)?;
        if let Some(c) = &canon
            && !latest_only
            && let Some((id, Some((v, vr)))) = self.hash_lookup(r, q, c)?
            && (lookup_deleted || !vr.deleted)
        {
            return Ok(Some(Entity::from_canon(q, v, id, c)));
        }
        let Some(c) = &canon else { return Ok(None) };
        let versions = r.list_versions(&q.context, &q.subject)?;
        if latest_only {
            if let Some((v, vr)) = versions.iter().rev().find(|(_, x)| !x.deleted) {
                let prev = self.entity(r, q, *v, vr)?;
                if can_lookup(c, &prev) {
                    return Ok(Some(prev));
                }
            }
        } else {
            for (v, vr) in versions.iter().rev() {
                if vr.deleted && !lookup_deleted {
                    continue;
                }
                let prev = self.entity(r, q, *v, vr)?;
                if can_lookup(c, &prev) {
                    return Ok(Some(prev));
                }
            }
        }
        Ok(None)
    }

    /// `lookUpSchemaUnderSubjectUsingContexts`: an unqualified subject is
    /// also looked up under the same name in every other context.
    fn lookup_using_contexts(&self, r: &Reader<'_>, q: &QualifiedSubject, d: &Draft, normalize: bool, deleted: bool) -> ApiResult<Option<Entity>> {
        if let Some(e) = self.lookup_under_subject(r, q, d, normalize, deleted, false)? {
            return Ok(Some(e));
        }
        if q.context != DEFAULT_CONTEXT {
            return Ok(None);
        }
        for ctx in r.list_contexts()?.into_iter().filter(|c| c != DEFAULT_CONTEXT) {
            let cq = QualifiedSubject::new(&ctx, &q.subject);
            match self.lookup_under_subject(r, &cq, d, normalize, deleted, false) {
                Ok(Some(e)) => return Ok(Some(e)),
                Ok(None) => {}
                Err(e) if e.code == 42201 => {}
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// `POST /subjects/{subject}/versions` (`SubjectVersionsResource#register`
    /// then `registerOrForward`).
    pub fn register(&self, subject: &str, req: RegisterSchemaRequest, normalize: bool) -> ApiResult<RegisterResponse> {
        if !crate::context::is_valid_subject(subject) {
            return Err(ApiError::invalid_subject(subject));
        }
        let q = QualifiedSubject::parse(subject)?;
        let d = Draft::from(req);
        {
            // registerOrForward's check, lock-free on one snapshot: by far the
            // most common "write" is re-registering an existing schema.
            let r = &self.reader();
            let normalize = normalize || self.normalize_in_scope(r, &q)?;
            if let Some(resp) = self.register_fast_path(r, &q, &d, normalize)? {
                return Ok(resp);
            }
        }
        let _guard = self.write_lock();
        let r = &self.reader();
        let normalize = normalize || self.normalize_in_scope(r, &q)?;
        self.register_locked(r, &q, d, normalize)
    }

    fn register_fast_path(&self, r: &Reader<'_>, q: &QualifiedSubject, d: &Draft, normalize: bool) -> ApiResult<Option<RegisterResponse>> {
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

    /// `maybePopulateFromPrevious`: an empty schema re-uses the latest
    /// version's; metadata and rules are inherited from it and wrapped with the
    /// configured defaults/overrides; `confluent:version` tracks the version.
    fn populate_from_previous(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        cfg: &ConfigRecord,
        d: &mut Draft,
        latest_live: Option<&(u32, VersionRecord)>,
        new_version: u32,
    ) -> ApiResult<bool> {
        let previous = match latest_live {
            Some((v, vr)) => Some(self.entity(r, q, *v, vr)?),
            None => None,
        };
        let mut populated = false;
        if d.schema.as_deref().is_none_or(|s| s.trim().is_empty()) {
            let Some(p) = &previous else { return Err(ApiError::new(42201, "Empty schema")) };
            d.schema = Some(p.schema.clone());
            d.schema_type = Some(p.schema_type.as_str().to_string());
            d.refs = p.references.iter().cloned().map(RefIn::from).collect();
            populated = true;
        }
        let specific_meta = d.metadata.clone().or_else(|| previous.as_ref().and_then(|p| p.metadata.clone()));
        let mut metadata = merge_metadata(&[cfg.default_metadata.as_ref(), specific_meta.as_ref(), cfg.override_metadata.as_ref()]);
        let specific_rules = d.rule_set.clone().or_else(|| previous.as_ref().and_then(|p| p.rule_set.clone()));
        let rule_set = merge_rule_sets(&[cfg.default_rule_set.as_ref(), specific_rules.as_ref(), cfg.override_rule_set.as_ref()]);
        if d.version != 0 || metadata_property(&metadata, "confluent:version").is_some() {
            metadata = with_confluent_version(metadata, new_version);
        }
        if metadata.is_some() || rule_set.is_some() {
            d.metadata = metadata;
            d.rule_set = rule_set;
            return Ok(true);
        }
        Ok(populated)
    }

    /// `KafkaSchemaRegistry#register`, under the write lock.
    fn register_locked(&self, r: &Reader<'_>, q: &QualifiedSubject, mut d: Draft, normalize: bool) -> ApiResult<RegisterResponse> {
        // checkRegisterMode: the same table the route-level gate uses, so a
        // registration cannot slip past it by arriving through another path.
        let mode = self.mode_in_scope(r, q)?;
        let intent = if d.id >= 0 { crate::modegate::Intent::Import } else { crate::modegate::Intent::Write };
        let allowed = &crate::modegate::check(intent, mode, &q.qualified())?;
        let import = mode == Mode::Import;
        let versions = r.list_versions(&q.context, &q.subject)?;
        let new_version = versions.iter().map(|(v, _)| *v).max().map_or(1, |m| m + 1);
        let undeleted: Vec<(u32, VersionRecord)> = versions.iter().rev().filter(|(_, v)| !v.deleted).cloned().collect();
        let cfg = self.config_in_scope(r, q)?;
        let modified = !import && self.populate_from_previous(r, q, &cfg, &mut d, undeleted.first(), new_version)?;

        let mut schema_id = d.id;
        let canon = self.canonicalize(r, q, &d, schema_id < 0, normalize)?;
        if let Some(c) = &canon
            && let Some((id, sv)) = self.hash_lookup(r, q, c)?
            && (schema_id < 0 || schema_id as u32 == id)
        {
            if d.version == 0
                && let Some((v, vr)) = sv
                && !vr.deleted
            {
                return Ok(if modified { RegisterResponse::full(&Entity::from_canon(q, v, id, c)) } else { RegisterResponse::id(id) });
            }
            schema_id = id as i32;
        }
        if d.version == 0
            && let Some(c) = &canon
        {
            for (v, vr) in &undeleted {
                if schema_id >= 0 && schema_id as u32 != vr.id {
                    continue;
                }
                if can_lookup(c, &self.entity(r, q, *v, vr)?) {
                    return Ok(if modified { RegisterResponse::full(&Entity::from_canon(q, *v, vr.id, c)) } else { RegisterResponse::id(vr.id) });
                }
            }
        }
        // IMPORT with an empty schema: Confluent would store it unparsed.
        let c = canon.ok_or_else(|| ApiError::new(42201, "Empty schema"))?;
        if !import {
            let level = cfg.compatibility_level.unwrap_or(self.default_compatibility);
            let olds = self.compat_candidates(r, q, &cfg, &c.metadata)?;
            let msgs = self.check_compatibility(r, &q.context, &c.parsed, &olds, level, &cfg)?;
            if !msgs.is_empty() {
                return Err(ApiError::incompatible(&q.qualified(), &msgs));
            }
        }
        let version = if d.version <= 0 {
            new_version
        } else if new_version != d.version as u32 && !import {
            return Err(ApiError::new(42201, "Version is not one more than previous version"));
        } else {
            d.version as u32
        };
        let fp = c.fingerprint();
        let next_id = r.next_id(&q.context)?;
        let id = if schema_id >= 0 {
            let id = schema_id as u32;
            // checkIfSchemaWithIdExist
            if !r.id_usages(&q.context, id)?.is_empty()
                && let Some(existing) = r.get_schema(&q.context, id)?
                && existing.fingerprint != fp
            {
                return Err(ApiError::operation_not_permitted(format!("Overwrite new schema with id {id} is not permitted.")));
            }
            id
        } else {
            next_id
        };

        let mut tx = self.store.tx()?;
        // IMPORT may overwrite an existing version (the log is keyed by subject+version).
        let overwritten = versions.iter().find(|(v, _)| *v == version).map(|(_, vr)| vr.clone());
        if let Some(old) = &overwritten {
            self.hard_delete_rows(r, &mut tx, q, version, old, &HashSet::new(), allowed)?;
        }
        // Older soft-deleted versions carrying the same id are removed for good.
        let stale: Vec<(u32, VersionRecord)> =
            versions.iter().filter(|(v, vr)| vr.deleted && vr.id == id && *v < version).cloned().collect();
        let stale_keys: HashSet<(String, u32)> = stale.iter().map(|(v, _)| (q.subject.clone(), *v)).collect();
        for (v, vr) in &stale {
            self.hard_delete_rows(r, &mut tx, q, *v, vr, &stale_keys, allowed)?;
        }
        let rec = match r.get_schema(&q.context, id)? {
            Some(existing) if existing.fingerprint == fp => SchemaRecord::clone(&existing),
            _ => SchemaRecord {
                schema_type: c.schema_type,
                schema: c.text.clone(),
                references: c.refs.clone(),
                metadata: c.metadata.clone(),
                rule_set: c.rule_set.clone(),
                fingerprint: fp,
                schema_fingerprint: schema::fingerprint(c.schema_type, &c.parsed.normalized, &c.refs, None, None),
                guid: uuid::Uuid::new_v4().to_string(),
            },
        };
        // Like Confluent's hash index, the latest registration of a content wins.
        tx.put_schema(&q.context, id, &rec, true, allowed)?;
        if id >= next_id {
            tx.set_next_id(&q.context, id + 1);
        }
        tx.put_version(&q.context, &q.subject, version, &VersionRecord { id, deleted: false, ts: now_millis() }, allowed)?;
        for (t, v) in &c.targets {
            tx.put_refby(&t.context, &t.subject, *v, id);
        }
        tx.append_log(&LogEvent { ctx: q.context.clone(), subject: q.subject.clone(), version, id, kind: LogEventKind::Register })?;
        self.commit(tx)?;
        Ok(if modified { RegisterResponse::full(&Entity::from_canon(q, version, id, &c)) } else { RegisterResponse::id(id) })
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

    /// `maybeModifyPreviousRuleSet`: `rulesToMerge` / `rulesToRemove` apply to
    /// the rules of the version before the new one.
    fn rule_set_for_tags(&self, r: &Reader<'_>, q: &QualifiedSubject, req: &TagSchemaRequest) -> ApiResult<Option<Value>> {
        if req.rules_to_merge.is_none() && req.rules_to_remove.is_empty() {
            return Ok(req.rule_set.clone());
        }
        let spec = match req.new_version {
            Some(v) if v > 1 => VersionSpec::Exact(v as u32 - 1),
            Some(_) => VersionSpec::Exact(1),
            None => VersionSpec::Latest,
        };
        let previous = match self.get_exact(r, q, spec, false)? {
            Some((v, vr)) => self.entity(r, q, v, &vr)?.rule_set,
            None => None,
        };
        let mut rules = match &req.rules_to_merge {
            Some(merge) => merge_rule_sets(&[previous.as_ref(), Some(merge)]),
            None => previous,
        };
        if !req.rules_to_remove.is_empty()
            && let Some(rs) = rules.as_mut().and_then(Value::as_object_mut)
        {
            for key in ["migrationRules", "domainRules"] {
                if let Some(list) = rs.get_mut(key).and_then(Value::as_array_mut) {
                    list.retain(|x| !x.get("name").and_then(Value::as_str).is_some_and(|n| req.rules_to_remove.iter().any(|r| r == n)));
                }
            }
        }
        Ok(rules)
    }

    /// `POST /subjects/{subject}` (`SubjectsResource#lookUpSchemaUnderSubject`).
    pub fn lookup(
        &self,
        subject: &str,
        req: RegisterSchemaRequest,
        normalize: bool,
        deleted: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaView> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let normalize = normalize || self.normalize_in_scope(r, &q)?;
        let d = Draft::from(req);
        let Some(e) = self.lookup_using_contexts(r, &q, &d, normalize, deleted)? else {
            return Err(if self.has_subjects(r, &q, deleted)? { ApiError::schema_not_found() } else { ApiError::subject_not_found(&q.qualified()) });
        };
        let mut view = e.view();
        if format.is_some_and(|f| !f.trim().is_empty()) {
            let eq = QualifiedSubject::parse(&view.subject)?;
            if let Some(rec) = r.get_schema(&eq.context, view.id)? {
                view.schema = self.formatted(r, &eq.context, view.id, &rec, format)?;
            }
        }
        Ok(view)
    }

    /// `POST /compatibility/subjects/{subject}/versions[/{version}]`
    /// (`CompatibilityResource` then `isCompatible`). With `verbose`, an
    /// invalid schema is reported as an incompatibility message.
    pub fn test_compatibility(
        &self,
        subject: &str,
        version: Option<VersionSpec>,
        req: RegisterSchemaRequest,
        normalize: bool,
        verbose: bool,
    ) -> ApiResult<Vec<String>> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let previous: Vec<(u32, VersionRecord)> = match version {
            Some(spec) => match self.get_exact(r, &q, spec, false)? {
                Some(found) => vec![found],
                None if spec == VersionSpec::Latest => Vec::new(),
                None => return Err(ApiError::version_not_found(spec)),
            },
            None => r.list_versions(&q.context, &q.subject)?.into_iter().rev().filter(|(_, v)| !v.deleted).collect(),
        };
        let normalize = normalize || self.normalize_in_scope(r, &q)?;
        let d = Draft::from(req);
        let result = (|| {
            let c = self.canonicalize(r, &q, &d, true, normalize)?.ok_or_else(|| ApiError::new(42201, "Empty schema"))?;
            let cfg = self.config_in_scope(r, &q)?;
            let mut olds = Vec::with_capacity(previous.len());
            for (v, vr) in &previous {
                let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
                if let Some(group) = cfg.compatibility_group.as_deref()
                    && metadata_property(&rec.metadata, group) != metadata_property(&c.metadata, group)
                {
                    continue;
                }
                olds.push(OldVersion { version: *v, id: vr.id, rec });
            }
            let level = cfg.compatibility_level.unwrap_or(self.default_compatibility);
            self.check_compatibility(r, &q.context, &c.parsed, &olds, level, &cfg)
        })();
        match result {
            Err(e) if e.code == 42201 && verbose => Ok(vec![e.message]),
            other => other,
        }
    }

    // ---------------- reads ----------------

    /// `GET /subjects/{subject}/versions/{version}` (`getSchemaByVersion`).
    pub fn get_version_formatted(
        &self,
        subject: &str,
        spec: VersionSpec,
        deleted: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaView> {
        let r = &self.reader();
        let (cq, v, vr) = self.version_or_error(r, subject, spec, deleted)?;
        let rec = r.get_schema(&cq.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        let mut view = Entity::stored(&cq, v, vr.id, &rec).view();
        if format.is_some_and(|f| !f.trim().is_empty()) {
            view.schema = self.formatted(r, &cq.context, vr.id, &rec, format)?;
        }
        Ok(view)
    }

    fn version_or_error(&self, r: &Reader<'_>, subject: &str, spec: VersionSpec, deleted: bool) -> ApiResult<(QualifiedSubject, u32, VersionRecord)> {
        let q = QualifiedSubject::parse(subject)?;
        match self.get_using_contexts(r, &q, spec, deleted)? {
            Some(found) => Ok(found),
            None if !self.has_subjects(r, &q, deleted)? => Err(ApiError::subject_not_found(&q.qualified())),
            None => Err(ApiError::version_not_found(spec)),
        }
    }

    /// `GET /subjects/{subject}/metadata?key=k&value=v...`: the newest version
    /// whose metadata properties contain every given pair.
    pub fn latest_with_metadata(
        &self,
        subject: &str,
        pairs: &[(String, String)],
        deleted: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaView> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let wanted: std::collections::HashMap<&str, &str> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        for (v, vr) in r.list_versions(&q.context, &q.subject)?.iter().rev() {
            if vr.deleted && !deleted {
                continue;
            }
            let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            let Some(props) = rec.metadata.as_ref().and_then(|m| m.get("properties")).and_then(Value::as_object) else { continue };
            if wanted.iter().all(|(k, val)| props.get(*k).and_then(Value::as_str) == Some(*val)) {
                let mut view = Entity::stored(&q, *v, vr.id, &rec).view();
                if format.is_some_and(|f| !f.trim().is_empty()) {
                    view.schema = self.formatted(r, &q.context, vr.id, &rec, format)?;
                }
                return Ok(view);
            }
        }
        Err(if self.has_subjects(r, &q, deleted)? { ApiError::schema_not_found() } else { ApiError::subject_not_found(&q.qualified()) })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn get_version(&self, subject: &str, spec: VersionSpec, deleted: bool) -> ApiResult<SchemaView> {
        self.get_version_formatted(subject, spec, deleted, None)
    }

    pub fn list_versions(&self, subject: &str, deleted: bool, deleted_only: bool) -> ApiResult<Vec<u32>> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        if !self.has_subjects(r, &q, deleted || deleted_only)? {
            return Err(ApiError::subject_not_found(&q.qualified()));
        }
        Ok(r.list_versions(&q.context, &q.subject)?
            .iter()
            .filter(|(_, v)| if deleted_only { v.deleted } else { deleted || !v.deleted })
            .map(|(v, _)| *v)
            .collect())
    }

    /// Contexts matched by a (possibly wildcard) context name.
    fn contexts_matching(&self, r: &Reader<'_>, ctx: &str) -> ApiResult<Vec<String>> {
        if ctx == crate::context::WILDCARD_CONTEXT {
            r.list_contexts()
        } else {
            Ok(vec![ctx.to_string()])
        }
    }

    /// Parse a `subjectPrefix` parameter. Default (absent) is every context.
    fn parse_prefix(prefix: Option<&str>) -> ApiResult<QualifiedSubject> {
        match prefix {
            None => Ok(QualifiedSubject::new(crate::context::WILDCARD_CONTEXT, "")),
            Some(p) => QualifiedSubject::parse(p),
        }
    }

    pub fn list_subjects(&self, prefix: Option<&str>, deleted: bool, deleted_only: bool) -> ApiResult<Vec<String>> {
        self.list_subjects_in(&self.reader(), prefix, deleted, deleted_only)
    }

    fn list_subjects_in(&self, r: &Reader<'_>, prefix: Option<&str>, deleted: bool, deleted_only: bool) -> ApiResult<Vec<String>> {
        let p = Self::parse_prefix(prefix)?;
        let mut out = Vec::new();
        for ctx in self.contexts_matching(r, &p.context)? {
            for name in r.list_subject_names(&ctx)? {
                if !name.starts_with(&p.subject) {
                    continue;
                }
                let versions = r.list_versions(&ctx, &name)?;
                let live = versions.iter().any(|(_, v)| !v.deleted);
                let include = if deleted_only { !live && !versions.is_empty() } else { live || (deleted && !versions.is_empty()) };
                if include {
                    out.push(qualify(&ctx, &name));
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// `GET /schemas`. With `include_aliases`, each schema lists the subjects
    /// aliasing it, and alias targets outside the prefix are included too
    /// (Confluent's `allVersionsIncludingAliasesWithSubjectPrefix`).
    pub fn list_schemas(
        &self,
        prefix: Option<&str>,
        deleted: bool,
        latest_only: bool,
        include_aliases: bool,
        rule_type: Option<&str>,
    ) -> ApiResult<Vec<SchemaView>> {
        let r = &self.reader();
        let p = Self::parse_prefix(prefix)?;
        let prefix_ctxs = self.contexts_matching(r, &p.context)?;
        let in_prefix = |ctx: &str, name: &str| prefix_ctxs.iter().any(|c| c == ctx) && name.starts_with(&p.subject);

        // alias target (qualified) -> aliasing subjects (qualified), from configs within the prefix
        let mut aliases: std::collections::BTreeMap<String, Vec<String>> = std::collections::BTreeMap::new();
        if include_aliases {
            for (ctx, subject, cfg) in r.subject_configs() {
                let Some(alias) = cfg.alias.filter(|a| !a.is_empty()) else { continue };
                if !in_prefix(&ctx, &subject) {
                    continue;
                }
                let target = QualifiedSubject::parse_subject(&alias)?;
                let target = if alias.starts_with(':') { target } else { QualifiedSubject::new(&ctx, &target.subject) };
                aliases.entry(target.qualified()).or_default().push(qualify(&ctx, &subject));
            }
        }

        let mut subjects: Vec<QualifiedSubject> = Vec::new();
        for ctx in &prefix_ctxs {
            for name in r.list_subject_names(ctx)? {
                if name.starts_with(&p.subject) {
                    subjects.push(QualifiedSubject::new(ctx, &name));
                }
            }
        }
        for target in aliases.keys() {
            let q = QualifiedSubject::parse_subject(target)?;
            if !subjects.contains(&q) {
                subjects.push(q);
            }
        }

        let mut out = Vec::new();
        for q in subjects {
            let versions: Vec<(u32, VersionRecord)> =
                r.list_versions(&q.context, &q.subject)?.into_iter().filter(|(_, v)| deleted || !v.deleted).collect();
            let chosen: Vec<&(u32, VersionRecord)> =
                if latest_only { versions.last().into_iter().collect() } else { versions.iter().collect() };
            for (v, vr) in chosen {
                let mut view = self.entity(r, &q, *v, vr)?.view();
                if let Some(t) = rule_type.filter(|t| !t.is_empty())
                    && !has_rules_with_type(&view.rule_set, t)
                {
                    continue;
                }
                if include_aliases {
                    // Confluent's config store iterates subjects in sorted order.
                    view.aliases = aliases.get(&q.qualified()).map(|a| {
                        let mut a = a.clone();
                        a.sort();
                        a
                    });
                }
                out.push(view);
            }
        }
        out.sort_by(|a, b| a.subject.cmp(&b.subject).then(a.version.cmp(&b.version)));
        Ok(out)
    }

    /// Find the context holding schema `id`. Mirrors Confluent 7.9 exactly
    /// (verified against a live Confluent server):
    ///
    /// | hint                      | where we look                                        |
    /// |---------------------------|------------------------------------------------------|
    /// | none / `:.:`              | default context (any subject using the id)           |
    /// | `foo` / `:.:foo`          | first context (default, then others in order) in     |
    /// |                           | which `foo` itself uses the id; else default context |
    /// |                           | (any subject)                                        |
    /// | `:.ctx:`                  | that context (any subject)                           |
    /// | `:.ctx:foo`               | that context, only if `foo` uses the id              |
    ///
    /// The fallback is what lets a plain deserializer (which passes an
    /// unqualified topic subject) read data whose schema lives in another
    /// context under the same subject name, e.g. one brought in by an exporter.
    fn locate_id(&self, r: &Reader<'_>, id: u32, subject: Option<&str>) -> ApiResult<(String, Arc<SchemaRecord>)> {
        let found = |ctx: &str, name: Option<&str>| -> ApiResult<Option<Arc<SchemaRecord>>> {
            let usages = r.id_usages(ctx, id)?;
            let used = match name {
                None => !usages.is_empty(),
                Some(n) => usages.iter().any(|(s, _)| s == n),
            };
            if used { r.get_schema(ctx, id) } else { Ok(None) }
        };
        let hint = match subject.filter(|s| !s.is_empty()) {
            Some(s) => Some((QualifiedSubject::parse(s)?, s.starts_with(":."))),
            None => None,
        };
        match hint {
            Some((q, true)) if q.context != DEFAULT_CONTEXT => {
                let name = (!q.subject.is_empty()).then_some(q.subject.as_str());
                if let Some(rec) = found(&q.context, name)? {
                    return Ok((q.context, rec));
                }
            }
            hint => {
                // A context where this very subject uses the id wins (default
                // context first, then the rest in order) ...
                if let Some((q, _)) = hint.filter(|(q, _)| !q.subject.is_empty()) {
                    let mut ctxs = vec![DEFAULT_CONTEXT.to_string()];
                    ctxs.extend(r.list_contexts()?.into_iter().filter(|c| c != DEFAULT_CONTEXT));
                    for ctx in ctxs {
                        if let Some(rec) = found(&ctx, Some(&q.subject))? {
                            return Ok((ctx, rec));
                        }
                    }
                }
                // ... otherwise any subject in the default context.
                if let Some(rec) = found(DEFAULT_CONTEXT, None)? {
                    return Ok((DEFAULT_CONTEXT.to_string(), rec));
                }
            }
        }
        Err(ApiError::schema_id_not_found(id))
    }

    pub fn get_schema_by_id(
        &self,
        id: i64,
        subject: Option<&str>,
        fetch_max_id: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaByIdView> {
        let r = &self.reader();
        let uid = u32::try_from(id).ok().filter(|i| *i > 0).ok_or_else(|| ApiError::schema_id_not_found(id))?;
        let (ctx, rec) = self.locate_id(r, uid, subject)?;
        let max_id = if fetch_max_id { Some(r.next_id(&ctx)?.saturating_sub(1)) } else { None };
        Ok(SchemaByIdView {
            schema_type: schema_type_view(rec.schema_type),
            schema: self.formatted(r, &ctx, uid, &rec, format)?,
            references: rec.references.clone(),
            metadata: rec.metadata.clone(),
            rule_set: rec.rule_set.clone(),
            max_id,
        })
    }

    /// `listVersionsForId`: one entry per subject (its latest version with the
    /// id), in the iteration order of Confluent's per-id `ConcurrentHashMap`
    /// (subjects in the order they first used the id).
    pub fn id_versions(&self, id: i64, subject: Option<&str>, deleted: bool) -> ApiResult<Vec<SubjectVersion>> {
        self.id_versions_in(&self.reader(), id, subject, deleted)
    }

    fn id_versions_in(&self, r: &Reader<'_>, id: i64, subject: Option<&str>, deleted: bool) -> ApiResult<Vec<SubjectVersion>> {
        let uid = u32::try_from(id).ok().filter(|i| *i > 0).ok_or_else(ApiError::schema_not_found)?;
        let (ctx, _) = self.locate_id(r, uid, subject).map_err(|_| ApiError::schema_not_found())?;
        // subject -> (latest version with this id, first registration time)
        let mut per_subject: Vec<(String, u32, i64)> = Vec::new();
        for (s, v) in r.id_usages(&ctx, uid)? {
            let Some(vr) = r.get_version(&ctx, &s, v)? else { continue };
            match per_subject.iter_mut().find(|(x, ..)| *x == s) {
                Some(e) => {
                    e.1 = e.1.max(v);
                    e.2 = e.2.min(vr.ts);
                }
                None => per_subject.push((s, v, vr.ts)),
            }
        }
        per_subject.sort_by(|a, b| a.2.cmp(&b.2).then(a.1.cmp(&b.1)).then(a.0.cmp(&b.0)));
        let names: Vec<String> = per_subject.iter().map(|(s, ..)| qualify(&ctx, s)).collect();
        let mut out = Vec::new();
        for name in schema::java_order::chm_order(&names) {
            let q = QualifiedSubject::parse(&name)?;
            let (_, v, _) = per_subject.iter().find(|(s, ..)| *s == q.subject).expect("present");
            if deleted || r.get_version(&ctx, &q.subject, *v)?.is_some_and(|vr| !vr.deleted) {
                out.push(SubjectVersion { subject: name, version: *v });
            }
        }
        Ok(out)
    }

    pub fn id_subjects(&self, id: i64, subject: Option<&str>, deleted: bool) -> ApiResult<Vec<String>> {
        let r = &self.reader();
        Ok(self.id_versions_in(r, id, subject, deleted)?.into_iter().map(|sv| sv.subject).collect())
    }

    /// Ids of live schemas referencing (subject, version). Confluent drops a
    /// referrer from this index when it is soft-deleted.
    fn live_referrers(&self, r: &Reader<'_>, ctx: &str, subject: &str, version: u32) -> ApiResult<Vec<u32>> {
        let mut out = Vec::new();
        for id in r.referenced_by(ctx, subject, version)? {
            let mut used = false;
            for (s, v) in r.id_usages(ctx, id)? {
                if r.get_version(ctx, &s, v)?.is_some_and(|vr| !vr.deleted) {
                    used = true;
                    break;
                }
            }
            if used {
                out.push(id);
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// `GET .../versions/{v}/referencedby`: resolved like `getSchemaByVersion`
    /// with deleted versions included.
    pub fn referenced_by(&self, subject: &str, spec: VersionSpec) -> ApiResult<Vec<u32>> {
        let r = &self.reader();
        let (cq, v, _) = self.version_or_error(r, subject, spec, true)?;
        self.live_referrers(r, &cq.context, &cq.subject, v)
    }

    // ---------------- deletes ----------------

    fn check_not_referenced(&self, r: &Reader<'_>, q: &QualifiedSubject, version: u32) -> ApiResult<()> {
        if !self.live_referrers(r, &q.context, &q.subject, version)?.is_empty() {
            return Err(ApiError::reference_exists(format!(
                "One or more references exist to the schema {{magic=1,keytype=SCHEMA,subject={},version={version}}}.",
                q.qualified()
            )));
        }
        Ok(())
    }

    /// Hard-delete rows for (subject, version); drops the id's outgoing refby
    /// entries once nothing uses the id any more.
    fn hard_delete_rows(
        &self,
        r: &Reader<'_>,
        tx: &mut crate::store::Tx<'_>,
        q: &QualifiedSubject,
        version: u32,
        vr: &VersionRecord,
        also_removed: &HashSet<(String, u32)>,
        allowed: &crate::modegate::Allowed,
    ) -> ApiResult<()> {
        tx.delete_version(&q.context, &q.subject, version, vr.id, allowed);
        let still_used = r
            .id_usages(&q.context, vr.id)?
            .into_iter()
            .any(|(s, v)| !(s == q.subject && (v == version || also_removed.contains(&(s.clone(), v)))));
        if !still_used && let Some(rec) = r.get_schema(&q.context, vr.id)? {
            for r in &rec.references {
                let t = Self::ref_target(&q.context, r)?;
                tx.delete_refby(&t.context, &t.subject, r.version.max(1) as u32, vr.id);
            }
        }
        tx.append_log(&LogEvent {
            ctx: q.context.clone(),
            subject: q.subject.clone(),
            version,
            id: vr.id,
            kind: LogEventKind::HardDelete,
        })?;
        Ok(())
    }

    /// `DELETE /subjects/{subject}` (`SubjectsResource#deleteSubject`, then `deleteSubject`).
    pub fn delete_subject(&self, subject: &str, permanent: bool) -> ApiResult<Vec<u32>> {
        let q = QualifiedSubject::parse(subject)?;
        let _guard = self.write_lock();
        let r = &self.reader();
        if !self.has_subjects(r, &q, true)? {
            return Err(ApiError::subject_not_found(&q.qualified()));
        }
        if !permanent && !self.has_subjects(r, &q, false)? {
            return Err(ApiError::subject_soft_deleted(&q.qualified()));
        }
        let allowed = self.check_not_read_only(r, Some(&q))?;
        let versions: Vec<(u32, VersionRecord)> =
            r.list_versions(&q.context, &q.subject)?.into_iter().filter(|(_, v)| permanent || !v.deleted).collect();
        for (v, vr) in &versions {
            self.check_not_referenced(r, &q, *v)?;
            if permanent && !vr.deleted {
                return Err(ApiError::subject_not_soft_deleted(&q.qualified()));
            }
        }
        let mut tx = self.store.tx()?;
        let scope = Self::scope_for(&q);
        if !permanent {
            for (v, vr) in &versions {
                tx.put_version(&q.context, &q.subject, *v, &VersionRecord { deleted: true, ..vr.clone() }, &allowed)?;
                tx.append_log(&LogEvent {
                    ctx: q.context.clone(),
                    subject: q.subject.clone(),
                    version: *v,
                    id: vr.id,
                    kind: LogEventKind::SoftDelete,
                })?;
            }
            tx.delete_mode(&scope, &allowed);
            tx.delete_config(&scope, &allowed);
        } else {
            let all: HashSet<(String, u32)> = versions.iter().map(|(v, _)| (q.subject.clone(), *v)).collect();
            for (v, vr) in &versions {
                self.hard_delete_rows(r, &mut tx, &q, *v, vr, &all, &allowed)?;
            }
        }
        self.commit(tx)?;
        Ok(versions.iter().map(|(v, _)| *v).collect())
    }

    /// `DELETE /subjects/{subject}/versions/{version}` (`SubjectVersionsResource#deleteSchemaVersion`,
    /// then `deleteSchemaVersion`).
    pub fn delete_version(&self, subject: &str, spec: VersionSpec, permanent: bool) -> ApiResult<u32> {
        let q = QualifiedSubject::parse(subject)?;
        let _guard = self.write_lock();
        let r = &self.reader();
        let any = self.get_exact(r, &q, spec, true)?;
        if any.is_some() && !permanent && self.get_exact(r, &q, spec, false)?.is_none() {
            let (v, _) = any.expect("checked");
            return Err(ApiError::version_soft_deleted(&q.qualified(), v));
        }
        let Some((version, vr)) = any else {
            return Err(if self.has_subjects(r, &q, true)? {
                ApiError::version_not_found(spec)
            } else {
                ApiError::subject_not_found(&q.qualified())
            });
        };
        let allowed = self.check_not_read_only(r, Some(&q))?;
        self.check_not_referenced(r, &q, version)?;
        if permanent && !vr.deleted {
            return Err(ApiError::version_not_soft_deleted(&q.qualified(), version));
        }
        let mut tx = self.store.tx()?;
        if !permanent {
            tx.put_version(&q.context, &q.subject, version, &VersionRecord { deleted: true, ..vr.clone() }, &allowed)?;
            if !r.list_versions(&q.context, &q.subject)?.iter().any(|(v, x)| *v != version && !x.deleted) {
                // That was the last live version: the subject's mode and config go too.
                let scope = Self::scope_for(&q);
                tx.delete_mode(&scope, &allowed);
                tx.delete_config(&scope, &allowed);
            }
            tx.append_log(&LogEvent {
                ctx: q.context.clone(),
                subject: q.subject.clone(),
                version,
                id: vr.id,
                kind: LogEventKind::SoftDelete,
            })?;
        } else {
            self.hard_delete_rows(r, &mut tx, &q, version, &vr, &HashSet::new(), &allowed)?;
        }
        self.commit(tx)?;
        Ok(version)
    }

    // ---------------- config ----------------

    pub fn get_config(&self, subject: Option<&str>, default_to_global: bool) -> ApiResult<ConfigRecord> {
        let r = &self.reader();
        let Some(s) = subject else { return Ok(self.config_of(r, None)?.expect("global config")) };
        let q = QualifiedSubject::parse(s)?;
        if default_to_global {
            return self.config_in_scope(r, &q);
        }
        self.config_of(r, Some(&q))?.ok_or_else(|| ApiError::subject_compat_not_configured(&q.qualified()))
    }

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

    pub fn get_mode(&self, subject: Option<&str>, default_to_global: bool) -> ApiResult<Mode> {
        let r = &self.reader();
        let Some(s) = subject else { return Ok(self.mode_of(r, None)?.expect("global mode")) };
        let q = QualifiedSubject::parse(s)?;
        if default_to_global {
            return self.mode_in_scope(r, &q);
        }
        self.mode_of(r, Some(&q))?.ok_or_else(|| ApiError::subject_mode_not_configured(&q.qualified()))
    }

    /// `setMode`: entering IMPORT without `force` requires that no live
    /// subject is in scope, and hard-deletes the soft-deleted ones.
    pub fn set_mode(&self, subject: Option<&str>, mode: Mode, force: bool) -> ApiResult<()> {
        let q = subject.map(QualifiedSubject::parse).transpose()?;
        let _guard = self.write_lock();
        let r = &self.reader();
        let scope = q.as_ref().map(Self::scope_for).unwrap_or(Scope::Global);
        let mut tx = self.store.tx()?;
        let current = match &q {
            Some(q) => self.mode_in_scope(r, q)?,
            None => self.global_mode_in(r)?,
        };
        if mode == Mode::Import && current != Mode::Import && !force {
            let has = match &q {
                Some(q) => self.has_subjects(r, q, false)?,
                None => self.has_any_subject(r, false)?,
            };
            if has {
                return Err(ApiError::operation_not_permitted("Cannot import since found existing subjects"));
            }
            let mut doomed: Vec<(QualifiedSubject, u32, VersionRecord)> = Vec::new();
            let subjects: Vec<QualifiedSubject> = match &q {
                None => {
                    let mut all = Vec::new();
                    for ctx in r.list_contexts()? {
                        all.extend(r.list_subject_names(&ctx)?.into_iter().map(|n| QualifiedSubject::new(&ctx, &n)));
                    }
                    all
                }
                Some(q) if q.is_context_only() => {
                    r.list_subject_names(&q.context)?.into_iter().map(|n| QualifiedSubject::new(&q.context, &n)).collect()
                }
                Some(q) => vec![q.clone()],
            };
            for sq in subjects {
                for (v, vr) in r.list_versions(&sq.context, &sq.subject)? {
                    self.check_not_referenced(r, &sq, v)?;
                    doomed.push((sq.clone(), v, vr));
                }
            }
            let keys: HashSet<(String, u32)> = doomed.iter().map(|(q, v, _)| (q.subject.clone(), *v)).collect();
            // Entering IMPORT with `force` clears what is there, as in
            // Confluent: that emptying is part of the mode change itself.
            let allowed = crate::modegate::Allowed::is_a_mode_change();
            for (sq, v, vr) in &doomed {
                self.hard_delete_rows(r, &mut tx, sq, *v, vr, &keys, &allowed)?;
            }
        }
        tx.put_mode(&scope, mode, &crate::modegate::Allowed::is_a_mode_change())?;
        self.commit(tx)
    }

    /// Returns the mode that was set (`getMode`).
    pub fn delete_mode(&self, subject: &str) -> ApiResult<Mode> {
        let q = QualifiedSubject::parse(subject)?;
        let _guard = self.write_lock();
        let r = &self.reader();
        let previous = self.mode_of(r, Some(&q))?.ok_or_else(|| ApiError::subject_not_found(&q.qualified()))?;
        let mut tx = self.store.tx()?;
        tx.delete_mode(&Self::scope_for(&q), &crate::modegate::Allowed::is_a_mode_change());
        self.commit(tx)?;
        Ok(previous)
    }

    /// The `alias` configured for exactly this subject (`AliasFilter`).
    pub fn alias_of(&self, subject: &str) -> Option<String> {
        let r = &self.reader();
        if !r.has_aliases() {
            return None;
        }
        let q = QualifiedSubject::parse(subject).ok()?;
        r.get_config(&Self::scope_for(&q)).ok()??.alias
    }

    // ---------------- admin views ----------------

    /// One row per subject for the admin UI: how many versions it has, which
    /// compatibility level and mode apply, and where those come from.
    /// `visible` decides which subjects the caller is shown: the admin UI
    /// lists exactly the subjects that caller administers.
    pub fn admin_overview(
        &self,
        prefix: Option<&str>,
        deleted: bool,
        limit: usize,
        visible: &dyn Fn(&str, &str) -> bool,
    ) -> ApiResult<Value> {
        let r = &self.reader();
        let p = Self::parse_prefix(prefix)?;
        let contexts = self.contexts_matching(r, &p.context)?;
        let mut rows = Vec::new();
        let (mut total, mut total_versions) = (0usize, 0usize);
        for ctx in &contexts {
            for name in r.list_subject_names(ctx)? {
                if !name.starts_with(&p.subject) {
                    continue;
                }
                if !visible(ctx, &name) {
                    continue;
                }
                let q = QualifiedSubject::new(ctx, &name);
                let versions = r.list_versions(ctx, &name)?;
                let live: Vec<&(u32, VersionRecord)> = versions.iter().filter(|(_, v)| !v.deleted).collect();
                if live.is_empty() && !deleted {
                    continue;
                }
                total += 1;
                total_versions += versions.len();
                if rows.len() >= limit {
                    continue;
                }
                let own = r.get_config(&Self::scope_for(&q))?;
                let cfg = self.config_in_scope(r, &q)?;
                let scope = if own.is_some() {
                    "subject"
                } else if ctx != DEFAULT_CONTEXT && r.get_config(&Scope::Context(ctx.clone()))?.is_some() {
                    "context"
                } else if r.get_config(&Scope::Global)?.is_some() {
                    "global"
                } else {
                    "default"
                };
                let own_mode = r.get_mode(&Self::scope_for(&q))?;
                let latest = live.last().copied().or_else(|| versions.last());
                let schema_type = match latest {
                    Some((_, vr)) => r.get_schema(ctx, vr.id)?.map(|rec| rec.schema_type.as_str()),
                    None => None,
                };
                rows.push(json!({
                    "subject": q.qualified(),
                    "context": ctx,
                    "versions": versions.len(),
                    "deletedVersions": versions.len() - live.len(),
                    "deleted": live.is_empty(),
                    "latestVersion": latest.map(|(v, _)| *v),
                    "latestId": latest.map(|(_, vr)| vr.id),
                    "schemaType": schema_type,
                    "compatibility": cfg.compatibility_level.unwrap_or(self.default_compatibility).as_str(),
                    "compatibilityFrom": scope,
                    "normalize": cfg.normalize == Some(true),
                    "mode": self.mode_in_scope(r, &q)?.as_str(),
                    "modeFrom": if own_mode.is_some() { "subject" } else { "inherited" },
                    "alias": own.and_then(|c| c.alias),
                }));
            }
        }
        rows.sort_by(|a, b| a["subject"].as_str().cmp(&b["subject"].as_str()));
        let exporters: Vec<Value> = self
            .store
            .list_exporters()?
            .into_iter()
            .map(|e| {
                json!({
                    "name": e.info.name,
                    "state": e.state,
                    "offset": e.offset,
                    "ts": e.ts,
                    "trace": e.trace,
                    "subjects": e.info.subjects,
                    "contextType": e.info.context_type,
                    "context": e.info.context,
                    "subjectRenameFormat": e.info.subject_rename_format,
                    "config": e.info.config,
                })
            })
            .collect();
        Ok(json!({
            "clusterId": self.cluster_id,
            "version": env!("CARGO_PKG_VERSION"),
            "contexts": self.admin_contexts(r, visible)?,
            "global": {
                "compatibility": self.config_of(r, None)?.and_then(|c| c.compatibility_level).unwrap_or(self.default_compatibility).as_str(),
                "normalize": self.normalize_default,
                "mode": self.global_mode_in(r)?.as_str(),
            },
            "counts": { "subjects": total, "versions": total_versions, "shown": rows.len() },
            "exporters": exporters,
            "subjects": rows,
        }))
    }

    /// One row per context: how much it holds and what is configured on it.
    fn admin_contexts(&self, r: &Reader<'_>, visible: &dyn Fn(&str, &str) -> bool) -> ApiResult<Vec<Value>> {
        let global_mode = self.global_mode_in(r)?;
        let mut out = Vec::new();
        for ctx in r.list_contexts()? {
            let (mut subjects, mut deleted_subjects, mut versions) = (0usize, 0usize, 0usize);
            for name in r.list_subject_names(&ctx)? {
                if !visible(&ctx, &name) {
                    continue;
                }
                let vs = r.list_versions(&ctx, &name)?;
                versions += vs.len();
                if vs.iter().any(|(_, v)| !v.deleted) {
                    subjects += 1;
                } else {
                    deleted_subjects += 1;
                }
            }
            // The scope a context's own settings live in: the default context
            // has none of its own - it reads the global ones.
            let own_scope = if ctx == DEFAULT_CONTEXT { Scope::Global } else { Scope::Context(ctx.clone()) };
            let cfg = r.get_config(&own_scope)?;
            let mode = r.get_mode(&own_scope)?;
            // A context without configuration of its own falls back to the
            // server default, not to the global config: settings are resolved
            // by first match, never merged.
            let effective = cfg.as_ref().and_then(|c| c.compatibility_level).unwrap_or(self.default_compatibility);
            // A context the caller can see nothing in is not listed - unless
            // they administer the whole context, which includes the empty ones.
            if subjects + deleted_subjects == 0 && !visible(&ctx, "") {
                continue;
            }
            out.push(json!({
                "name": ctx,
                "subjects": subjects,
                "deletedSubjects": deleted_subjects,
                "versions": versions,
                "compatibility": effective.as_str(),
                "compatibilityFrom": if cfg.is_some() { "own" } else { "default" },
                "normalize": cfg.as_ref().map(|c| c.normalize.unwrap_or(self.normalize_default)).unwrap_or(self.normalize_default),
                "mode": if global_mode == Mode::ReadonlyOverride {
                    global_mode.as_str()
                } else {
                    mode.unwrap_or(Mode::Readwrite).as_str()
                },
                "modeFrom": if mode.is_some() { "own" } else { "default" },
            }));
        }
        Ok(out)
    }

    /// Every version of one subject, with the schema itself, for the admin UI.
    pub fn admin_subject(&self, subject: &str) -> ApiResult<Value> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let versions = r.list_versions(&q.context, &q.subject)?;
        if versions.is_empty() {
            return Err(ApiError::subject_not_found(&q.qualified()));
        }
        let mut out = Vec::new();
        for (v, vr) in &versions {
            let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            out.push(json!({
                "version": v,
                "id": vr.id,
                "deleted": vr.deleted,
                "registeredAt": vr.ts,
                "schemaType": rec.schema_type.as_str(),
                "schema": rec.schema,
                "references": rec.references,
                "metadata": rec.metadata,
                "ruleSet": rec.rule_set,
                "referencedBy": self.live_referrers(r, &q.context, &q.subject, *v)?,
            }));
        }
        let cfg = self.config_in_scope(r, &q)?;
        Ok(json!({
            "subject": q.qualified(),
            "config": r.get_config(&Self::scope_for(&q))?,
            "effectiveCompatibility": cfg.compatibility_level.unwrap_or(self.default_compatibility).as_str(),
            "mode": self.mode_in_scope(r, &q)?.as_str(),
            "versions": out,
        }))
    }

    // ---------------- contexts ----------------

    pub fn list_contexts(&self) -> ApiResult<Vec<String>> {
        let r = &self.reader();
        r.list_contexts()
    }

    pub fn delete_context(&self, ctx: &str) -> ApiResult<()> {
        let ctx = crate::context::normalize_context(ctx).ok_or_else(|| ApiError::invalid_subject(ctx))?;
        let _guard = self.write_lock();
        let r = &self.reader();
        if ctx == DEFAULT_CONTEXT || ctx == crate::context::WILDCARD_CONTEXT {
            return Err(ApiError::operation_not_permitted("The default context cannot be deleted"));
        }
        if !r.list_subject_names(&ctx)?.is_empty() {
            return Err(ApiError::context_not_empty(&ctx));
        }
        let mut tx = self.store.tx()?;
        // An empty context and its settings: no schema state, so no mode gate
        // (Confluent does not gate this either).
        let allowed = crate::modegate::Allowed::not_schema_state();
        tx.delete_context(&ctx, &allowed);
        tx.delete_config(&Scope::Context(ctx.clone()), &allowed);
        tx.delete_mode(&Scope::Context(ctx.clone()), &allowed);
        self.commit(tx)?;
        Ok(())
    }

    // ---------------- exporters ----------------

    fn exporter_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.exporter_lock.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn validate_exporter(info: &ExporterInfo) -> ApiResult<()> {
        let valid_name = !info.name.is_empty()
            && info.name.len() <= 256
            && info.name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
        if !valid_name {
            return Err(ApiError::invalid_exporter(format!("Invalid exporter name '{}'", info.name)));
        }
        match info.context_type.as_str() {
            "AUTO" | "NONE" | "DEFAULT" => {}
            "CUSTOM" => {
                if info.context.as_deref().is_none_or(|c| crate::context::normalize_context(c).is_none()) {
                    return Err(ApiError::invalid_exporter("Context type CUSTOM requires a valid 'context'"));
                }
            }
            other => return Err(ApiError::invalid_exporter(format!("Invalid context type '{other}'"))),
        }
        if !info.config.get("schema.registry.url").and_then(Value::as_str).is_some_and(|s| !s.is_empty()) {
            return Err(ApiError::invalid_exporter("Missing required config 'schema.registry.url'"));
        }
        if info.subjects.is_empty() {
            return Err(ApiError::invalid_exporter("Exporter 'subjects' must not be empty"));
        }
        // CUSTOM and DEFAULT map every source context onto one destination
        // context. Since ids are per context (each starts at 1), two source
        // contexts would collide there and the import could not keep ids.
        if matches!(info.context_type.as_str(), "CUSTOM" | "DEFAULT") {
            let mut contexts: Vec<String> = Vec::new();
            for p in &info.subjects {
                let q = QualifiedSubject::parse(p)?;
                if q.is_wildcard() {
                    return Err(ApiError::invalid_exporter(format!(
                        "Exporter '{}' with contextType {} cannot use the context wildcard ':*:': ids are per context and could not be preserved in '{}'",
                        info.name,
                        info.context_type,
                        info.context.as_deref().unwrap_or(DEFAULT_CONTEXT)
                    )));
                }
                if !contexts.contains(&q.context) {
                    contexts.push(q.context);
                }
            }
            if contexts.len() > 1 {
                return Err(ApiError::invalid_exporter(format!(
                    "Exporter '{}' with contextType {} would merge contexts {} into one destination context; ids are per context and could not be preserved",
                    info.name,
                    info.context_type,
                    contexts.join(", ")
                )));
            }
        }
        Ok(())
    }

    pub fn create_exporter(&self, req: ExporterUpdateRequest) -> ApiResult<String> {
        let _g = self.exporter_lock();
        let name = req.name.clone().unwrap_or_default();
        if self.store.get_exporter(&name)?.is_some() {
            return Err(ApiError::exporter_exists(&name));
        }
        let info = ExporterInfo {
            name: name.clone(),
            subjects: req.subjects.unwrap_or_else(|| vec!["*".into()]),
            context_type: req.context_type.map(|c| c.to_ascii_uppercase()).unwrap_or_else(|| "AUTO".into()),
            context: req.context,
            subject_rename_format: req.subject_rename_format,
            config: req.config.unwrap_or_default(),
        };
        Self::validate_exporter(&info)?;
        self.store.put_exporter(
            &ExporterRecord { info, state: ExporterState::Running, offset: 0, ts: now_millis(), trace: String::new() },
            &crate::modegate::Allowed::not_schema_state(),
        )?;
        self.changes.notify_waiters();
        Ok(name)
    }

    pub fn update_exporter(&self, name: &str, req: ExporterUpdateRequest) -> ApiResult<String> {
        let _g = self.exporter_lock();
        let mut rec = self.store.get_exporter(name)?.ok_or_else(|| ApiError::exporter_not_found(name))?;
        if let Some(s) = req.subjects {
            rec.info.subjects = s;
        }
        if let Some(c) = req.context_type {
            rec.info.context_type = c.to_ascii_uppercase();
        }
        if req.context.is_some() {
            rec.info.context = req.context;
        }
        if req.subject_rename_format.is_some() {
            rec.info.subject_rename_format = req.subject_rename_format;
        }
        if let Some(cfg) = req.config {
            rec.info.config.extend(cfg);
        }
        Self::validate_exporter(&rec.info)?;
        rec.ts = now_millis();
        self.store.put_exporter(&rec, &crate::modegate::Allowed::not_schema_state())?;
        self.changes.notify_waiters();
        Ok(name.to_string())
    }

    pub fn update_exporter_config(&self, name: &str, cfg: serde_json::Map<String, Value>) -> ApiResult<String> {
        self.update_exporter(name, ExporterUpdateRequest { config: Some(cfg), ..Default::default() })
    }

    pub fn get_exporter(&self, name: &str) -> ApiResult<ExporterRecord> {
        self.store.get_exporter(name)?.ok_or_else(|| ApiError::exporter_not_found(name))
    }

    pub fn list_exporters(&self) -> ApiResult<Vec<String>> {
        Ok(self.store.list_exporters()?.into_iter().map(|e| e.info.name).collect())
    }

    pub fn delete_exporter(&self, name: &str) -> ApiResult<()> {
        let _g = self.exporter_lock();
        self.get_exporter(name)?;
        self.store.delete_exporter(name, &crate::modegate::Allowed::not_schema_state())
    }

    /// Pause / resume / reset.
    pub fn exporter_transition(&self, name: &str, action: &str) -> ApiResult<String> {
        let _g = self.exporter_lock();
        let mut rec = self.get_exporter(name)?;
        match action {
            "pause" => rec.state = ExporterState::Paused,
            "resume" => {
                // A failed exporter conflicts with what the destination holds;
                // picking up where it stopped would hit the same wall.
                if rec.state == ExporterState::Failed {
                    return Err(ApiError::operation_not_permitted(format!(
                        "Exporter {name} has failed and cannot be resumed; reset it to start over. Last error: {}",
                        rec.trace
                    )));
                }
                rec.state = ExporterState::Running;
                rec.trace.clear();
            }
            "reset" => {
                // Start over from the beginning of the log, whatever state it
                // was in: this is the way out of Failed.
                rec.offset = 0;
                rec.trace.clear();
                if rec.state != ExporterState::Paused {
                    rec.state = ExporterState::Running;
                }
            }
            _ => return Err(ApiError::unprocessable(format!("unknown action {action}"))),
        }
        rec.ts = now_millis();
        self.store.put_exporter(&rec, &crate::modegate::Allowed::not_schema_state())?;
        self.changes.notify_waiters();
        Ok(name.to_string())
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

    /// Every (context, subject, version) currently stored, oldest version
    /// first: what an exporter replays when the change log no longer reaches
    /// back far enough.
    pub fn all_subject_versions(&self) -> ApiResult<Vec<(String, String, u32)>> {
        let r = &self.reader();
        let mut out = Vec::new();
        for ctx in r.list_contexts()? {
            for subject in r.list_subject_names(&ctx)? {
                for (v, _) in r.list_versions(&ctx, &subject)? {
                    out.push((ctx.clone(), subject.clone(), v));
                }
            }
        }
        Ok(out)
    }

    /// Everything the exporter needs to replay one subject-version.
    pub fn export_payload(&self, ctx: &str, subject: &str, version: u32) -> ApiResult<Option<(VersionRecord, Arc<SchemaRecord>)>> {
        let r = &self.reader();
        let Some(vr) = r.get_version(ctx, subject, version)? else { return Ok(None) };
        let rec = r.get_schema(ctx, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        Ok(Some((vr, rec)))
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
