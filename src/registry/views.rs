//! The shapes a request arrives in and an answer goes back in, and the small
//! comparisons Confluent makes between them.
//!
//! `Draft` is a registration as sent; `Canon` is that schema once it has been
//! canonicalized (or normalized); `Entity` is a version as stored. Telling one
//! from another matters, because Confluent decides "is this the same schema"
//! differently at each stage - see `can_lookup` and `equivalent`.

use super::*;

pub(crate) fn is_avro(t: &Option<&'static str>) -> bool {
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
    pub(crate) fn id(id: u32) -> Self {
        Self { id, version: None, schema_type: None, references: Vec::new(), metadata: None, rule_set: None, schema: None }
    }

    /// `new RegisterSchemaResponse(schema)` for a "modified" or replayed schema.
    pub(crate) fn full(e: &Entity) -> Self {
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
pub(crate) struct Draft {
    pub(crate) schema: Option<String>,
    /// As sent; `None` is AVRO.
    pub(crate) schema_type: Option<String>,
    pub(crate) refs: Vec<RefIn>,
    pub(crate) metadata: Option<Value>,
    pub(crate) rule_set: Option<Value>,
    /// 0 when not given.
    pub(crate) version: i32,
    /// -1 when not given.
    pub(crate) id: i32,
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
    pub(crate) fn refs_string(&self) -> String {
        format!("[{}]", self.refs.iter().map(|r| r.to_string()).collect::<Vec<_>>().join(", "))
    }

    /// `Schema#toString`.
    pub(crate) fn entity_string(&self, q: &QualifiedSubject) -> String {
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
    pub(crate) fn invalid(&self, q: &QualifiedSubject, kind: schema::ErrorKind, detail: &str) -> ApiError {
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
pub(crate) fn java_string(v: &Value) -> String {
    match v {
        Value::Object(o) => format!("{{{}}}", o.iter().map(|(k, x)| format!("{k}={}", java_string(x))).collect::<Vec<_>>().join(", ")),
        Value::Array(a) => format!("[{}]", a.iter().map(java_string).collect::<Vec<_>>().join(", ")),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `Metadata#toString`.
pub(crate) fn java_metadata(m: &Option<Value>) -> String {
    let Some(m) = m else { return "null".into() };
    let part = |k: &str, empty: &str| m.get(k).map(java_string).unwrap_or_else(|| empty.into());
    format!("Metadata{{tags={}, properties={}, sensitive={}}}", part("tags", "{}"), part("properties", "{}"), part("sensitive", "[]"))
}

/// `RuleSet#toString` (encoding rules are not printed).
pub(crate) fn java_rule_set(r: &Option<Value>) -> String {
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
pub(crate) struct Canon {
    pub(crate) schema_type: SchemaType,
    /// Canonical, or normalized when normalizing.
    pub(crate) text: String,
    pub(crate) refs: Vec<SchemaReference>,
    pub(crate) targets: Vec<(QualifiedSubject, u32)>,
    pub(crate) metadata: Option<Value>,
    pub(crate) rule_set: Option<Value>,
    pub(crate) parsed: Arc<ParsedSchema>,
}

impl Canon {
    /// Confluent's `MD5.ofSchema` (plus the type: identical text under two
    /// types is two schemas here; Confluent rejects the second with 42205).
    pub(crate) fn fingerprint(&self) -> String {
        schema::fingerprint(self.schema_type, &self.text, &self.refs, self.metadata.as_ref(), self.rule_set.as_ref())
    }
}

/// A stored or matched `Schema` entity: what lookups and version reads answer.
#[derive(Debug, Clone)]
pub(crate) struct Entity {
    pub(crate) subject: String,
    pub(crate) version: u32,
    pub(crate) id: u32,
    pub(crate) schema_type: SchemaType,
    pub(crate) references: Vec<SchemaReference>,
    pub(crate) metadata: Option<Value>,
    pub(crate) rule_set: Option<Value>,
    pub(crate) schema: String,
}

impl Entity {
    pub(crate) fn stored(q: &QualifiedSubject, version: u32, id: u32, rec: &SchemaRecord) -> Self {
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

    pub(crate) fn from_canon(q: &QualifiedSubject, version: u32, id: u32, c: &Canon) -> Self {
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

    pub(crate) fn fingerprint(&self) -> String {
        schema::fingerprint(self.schema_type, &self.schema, &self.references, self.metadata.as_ref(), self.rule_set.as_ref())
    }

    pub(crate) fn view(self) -> SchemaView {
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

pub(crate) fn confluent_version(m: &Option<Value>) -> Option<String> {
    metadata_property(m, "confluent:version").and_then(Value::as_str).map(String::from)
}

/// `Metadata.removeConfluentVersion`, in canonical form.
pub(crate) fn without_confluent_version(m: &Option<Value>) -> Option<Value> {
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
pub(crate) fn equivalent(c: &Canon, c_meta: &Option<Value>, p: &Entity, p_meta: &Option<Value>) -> bool {
    c.schema_type == p.schema_type && c.text == p.schema && c_meta == p_meta && c.rule_set == p.rule_set
}

/// `AbstractSchemaProvider.canLookupIgnoringVersion`.
pub(crate) fn can_lookup_ignoring_version(c: &Canon, p: &Entity) -> bool {
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
pub(crate) fn can_lookup(c: &Canon, p: &Entity) -> bool {
    if c.refs.is_empty() && !p.references.is_empty() && can_lookup_ignoring_version(c, p) {
        return true;
    }
    if confluent_version(&c.metadata).is_some() || confluent_version(&p.metadata).is_some() {
        return c.refs == p.references && can_lookup_ignoring_version(c, p);
    }
    false
}

/// Set `metadata.properties["confluent:version"]`.
pub(crate) fn with_confluent_version(metadata: Option<Value>, version: u32) -> Option<Value> {
    let mut m = metadata.unwrap_or_else(|| json!({}));
    if !m.get("properties").is_some_and(Value::is_object) {
        m["properties"] = json!({});
    }
    m["properties"]["confluent:version"] = Value::String(version.to_string());
    Some(m)
}

/// A live version considered for a compatibility check.
pub(crate) struct OldVersion {
    pub(crate) version: u32,
    pub(crate) id: u32,
    pub(crate) rec: Arc<SchemaRecord>,
}

