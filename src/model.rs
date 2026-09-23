//! Wire types (the JSON the Confluent REST API speaks) and persisted records.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    #[default]
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    /// Confluent looks the provider up by exact name; `null` means AVRO.
    pub fn parse(s: Option<&str>) -> Result<Self, ApiError> {
        match s {
            None | Some("AVRO") => Ok(Self::Avro),
            Some("JSON") => Ok(Self::Json),
            Some("PROTOBUF") => Ok(Self::Protobuf),
            Some(other) => Err(ApiError::new(42201, format!("Invalid schema type {other}"))),
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
    pub fn is_avro(&self) -> bool {
        matches!(self, Self::Avro)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    None,
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
}

impl CompatibilityLevel {
    pub fn parse(s: &str) -> Result<Self, ApiError> {
        Ok(match s.to_ascii_uppercase().as_str() {
            "NONE" => Self::None,
            "BACKWARD" => Self::Backward,
            "BACKWARD_TRANSITIVE" => Self::BackwardTransitive,
            "FORWARD" => Self::Forward,
            "FORWARD_TRANSITIVE" => Self::ForwardTransitive,
            "FULL" => Self::Full,
            "FULL_TRANSITIVE" => Self::FullTransitive,
            _ => return Err(ApiError::invalid_compatibility(s)),
        })
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Backward => "BACKWARD",
            Self::BackwardTransitive => "BACKWARD_TRANSITIVE",
            Self::Forward => "FORWARD",
            Self::ForwardTransitive => "FORWARD_TRANSITIVE",
            Self::Full => "FULL",
            Self::FullTransitive => "FULL_TRANSITIVE",
        }
    }
    pub fn transitive(&self) -> bool {
        matches!(self, Self::BackwardTransitive | Self::ForwardTransitive | Self::FullTransitive)
    }
    /// new schema must be able to read data written with old schemas
    pub fn backward(&self) -> bool {
        matches!(self, Self::Backward | Self::BackwardTransitive | Self::Full | Self::FullTransitive)
    }
    /// old schemas must be able to read data written with the new schema
    pub fn forward(&self) -> bool {
        matches!(self, Self::Forward | Self::ForwardTransitive | Self::Full | Self::FullTransitive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Mode {
    Readwrite,
    Readonly,
    ReadonlyOverride,
    Import,
}

impl Mode {
    pub fn parse(s: &str) -> Result<Self, ApiError> {
        Ok(match s.to_ascii_uppercase().as_str() {
            "READWRITE" => Self::Readwrite,
            "READONLY" => Self::Readonly,
            "READONLY_OVERRIDE" => Self::ReadonlyOverride,
            "IMPORT" => Self::Import,
            _ => return Err(ApiError::invalid_mode(s)),
        })
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Readwrite => "READWRITE",
            Self::Readonly => "READONLY",
            Self::ReadonlyOverride => "READONLY_OVERRIDE",
            Self::Import => "IMPORT",
        }
    }
}

// ---------------------------------------------------------------------------
// Schema references / requests
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// A reference as sent by a client: any member may be missing, the element may be `null`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RefIn {
    pub null: bool,
    pub name: Option<String>,
    pub subject: Option<String>,
    pub version: Option<i32>,
}

impl From<SchemaReference> for RefIn {
    fn from(r: SchemaReference) -> Self {
        Self { null: false, name: Some(r.name), subject: Some(r.subject), version: Some(r.version) }
    }
}

impl std::fmt::Display for RefIn {
    /// `SchemaReference#toString`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.null {
            return f.write_str("null");
        }
        let v = self.version.map(|v| v.to_string()).unwrap_or_else(|| "null".into());
        write!(
            f,
            "{{name='{}', subject='{}', version={v}}}",
            self.name.as_deref().unwrap_or("null"),
            self.subject.as_deref().unwrap_or("null")
        )
    }
}

/// Body of `POST /subjects/{subject}/versions/{version}/tags`.
#[derive(Debug, Clone, Default)]
pub struct TagSchemaRequest {
    pub tags_to_add: Vec<crate::schema::tags::TagEdit>,
    pub tags_to_remove: Vec<crate::schema::tags::TagEdit>,
    pub new_version: Option<i32>,
    pub metadata: Option<Value>,
    pub rule_set: Option<Value>,
    pub rules_to_merge: Option<Value>,
    pub rules_to_remove: Vec<String>,
}

impl TagSchemaRequest {
    pub fn from_json(v: &Value) -> Result<Self, ApiError> {
        use crate::api::jackson as j;
        let o = j::object(v, "TagSchemaRequest")?;
        let edits = |key: &str| -> Result<Vec<crate::schema::tags::TagEdit>, ApiError> {
            let mut out = Vec::new();
            for x in o.get(key).and_then(Value::as_array).into_iter().flatten() {
                let e = j::object(x, "SchemaTags")?;
                let entity = j::opt_object(e.get("schemaEntity"), "SchemaEntity")?;
                let path = entity.and_then(|s| s.get("entityPath")).and_then(Value::as_str).unwrap_or_default();
                let ty = entity.and_then(|s| s.get("entityType")).and_then(Value::as_str).unwrap_or("sr_field");
                let tags = e
                    .get("tags")
                    .and_then(Value::as_array)
                    .map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect())
                    .unwrap_or_default();
                // `SchemaEntity` drops a leading dot from the path.
                let path = path.strip_prefix('.').unwrap_or(path).to_string();
                out.push(crate::schema::tags::TagEdit { path, record: ty.eq_ignore_ascii_case("sr_record"), tags });
            }
            Ok(out)
        };
        Ok(Self {
            tags_to_add: edits("tagsToAdd")?,
            tags_to_remove: edits("tagsToRemove")?,
            new_version: j::int(o.get("newVersion"))?,
            metadata: j::metadata(o.get("metadata"))?,
            rule_set: j::rule_set(o.get("ruleSet"))?,
            rules_to_merge: j::rule_set(o.get("rulesToMerge"))?,
            rules_to_remove: o
                .get("rulesToRemove")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect())
                .unwrap_or_default(),
        })
    }
}

/// Body of `POST /subjects/{subject}/versions`, `POST /subjects/{subject}` and
/// `POST /compatibility/...`, read with Jackson's coercions (see `api::jackson`).
#[derive(Debug, Clone, Default)]
pub struct RegisterSchemaRequest {
    pub schema: Option<String>,
    pub schema_type: Option<String>,
    pub references: Option<Vec<RefIn>>,
    /// Canonical `Metadata` (see `api::jackson::metadata`).
    pub metadata: Option<Value>,
    pub rule_set: Option<Value>,
    pub version: Option<i32>,
    pub id: Option<i32>,
}

impl RegisterSchemaRequest {
    pub fn from_json(v: &Value) -> Result<Self, ApiError> {
        use crate::api::jackson as j;
        let o = j::object(v, "RegisterSchemaRequest")?;
        let references = match o.get("references") {
            None | Some(Value::Null) => None,
            Some(Value::Array(a)) => Some(
                a.iter()
                    .map(|r| {
                        Ok(match j::opt_object(Some(r), "SchemaReference")? {
                            None => RefIn { null: true, ..Default::default() },
                            Some(r) => RefIn {
                                null: false,
                                name: j::string(r.get("name"))?,
                                subject: j::string(r.get("subject"))?,
                                version: j::int(r.get("version"))?,
                            },
                        })
                    })
                    .collect::<Result<Vec<_>, ApiError>>()?,
            ),
            Some(x) => {
                let what = if x.is_string() { "String value (token `JsonToken.VALUE_STRING`)" } else { "Object value (token `JsonToken.START_OBJECT`)" };
                return Err(j::bad(format!("Cannot deserialize value of type `SchemaReference>` from {what}")));
            }
        };
        Ok(Self {
            schema: j::string(o.get("schema"))?,
            schema_type: j::string(o.get("schemaType"))?,
            references,
            metadata: j::metadata(o.get("metadata"))?,
            rule_set: j::rule_set(o.get("ruleSet"))?,
            version: j::int(o.get("version"))?,
            id: j::int(o.get("id"))?,
        })
    }
}

// ---------------------------------------------------------------------------
// Persisted records
// ---------------------------------------------------------------------------

/// Immutable schema content, keyed by (context, id).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRecord {
    #[serde(rename = "t")]
    pub schema_type: SchemaType,
    #[serde(rename = "s")]
    pub schema: String,
    #[serde(rename = "r", default, skip_serializing_if = "Vec::is_empty")]
    pub references: Vec<SchemaReference>,
    #[serde(rename = "m", default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    #[serde(rename = "rs", default, skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<Value>,
    /// Fingerprint of the stored (as-registered) form.
    #[serde(rename = "fp")]
    pub fingerprint: String,
    /// Fingerprint of (type, normalized text, references) only - no metadata or
    /// rules. Used for `normalize=true` matching and metadata-less lookups.
    #[serde(rename = "sfp")]
    pub schema_fingerprint: String,
    #[serde(rename = "g", default)]
    pub guid: String,
}

/// A (context, subject, version) entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionRecord {
    pub id: u32,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub ts: i64,
}

/// Subject/context/global configuration. Every field is optional so that
/// partial updates merge and lookups can fall back level by level.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConfigRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_level: Option<CompatibilityLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_fields: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_rules: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_rule_set: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_rule_set: Option<Value>,
}

impl ConfigRecord {
    /// `Config#hasDefaultsOrOverrides`.
    pub fn has_defaults_or_overrides(&self) -> bool {
        self.default_metadata.is_some()
            || self.override_metadata.is_some()
            || self.default_rule_set.is_some()
            || self.override_rule_set.is_some()
    }

    /// Fields set in `other` override fields in `self`.
    pub fn merged_with(&self, other: &ConfigRecord) -> ConfigRecord {
        ConfigRecord {
            compatibility_level: other.compatibility_level.or(self.compatibility_level),
            alias: other.alias.clone().or_else(|| self.alias.clone()),
            normalize: other.normalize.or(self.normalize),
            validate_fields: other.validate_fields.or(self.validate_fields),
            validate_rules: other.validate_rules.or(self.validate_rules),
            compatibility_group: other.compatibility_group.clone().or_else(|| self.compatibility_group.clone()),
            default_metadata: other.default_metadata.clone().or_else(|| self.default_metadata.clone()),
            override_metadata: other.override_metadata.clone().or_else(|| self.override_metadata.clone()),
            default_rule_set: other.default_rule_set.clone().or_else(|| self.default_rule_set.clone()),
            override_rule_set: other.override_rule_set.clone().or_else(|| self.override_rule_set.clone()),
        }
    }
}

/// `PUT /config` body (`ConfigUpdateRequest`). The level is `compatibility` on
/// the way in and `compatibilityLevel` on the way out. The response echoes the
/// request as read (after coercion, before validation).
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigUpdateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_fields: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validate_rules: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compatibility_group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_rule_set: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub override_rule_set: Option<Value>,
}

impl ConfigUpdateRequest {
    pub fn from_json(v: &Value) -> Result<Self, ApiError> {
        use crate::api::jackson as j;
        let o = j::object(v, "ConfigUpdateRequest")?;
        Ok(Self {
            alias: j::string(o.get("alias"))?,
            normalize: j::boolean(o.get("normalize"))?,
            validate_fields: j::boolean(o.get("validateFields"))?,
            validate_rules: j::boolean(o.get("validateRules"))?,
            compatibility: j::string(o.get("compatibility"))?,
            compatibility_group: j::string(o.get("compatibilityGroup"))?,
            default_metadata: j::metadata(o.get("defaultMetadata"))?,
            override_metadata: j::metadata(o.get("overrideMetadata"))?,
            default_rule_set: j::rule_set(o.get("defaultRuleSet"))?,
            override_rule_set: j::rule_set(o.get("overrideRuleSet"))?,
        })
    }

    pub fn into_record(self) -> Result<ConfigRecord, ApiError> {
        Ok(ConfigRecord {
            compatibility_level: self.compatibility.as_deref().map(CompatibilityLevel::parse).transpose()?,
            alias: self.alias,
            normalize: self.normalize,
            validate_fields: self.validate_fields,
            validate_rules: self.validate_rules,
            compatibility_group: self.compatibility_group,
            default_metadata: self.default_metadata,
            override_metadata: self.override_metadata,
            default_rule_set: self.default_rule_set,
            override_rule_set: self.override_rule_set,
        })
    }
}

/// `PUT /mode` body. The response echoes `mode` as sent (`"import"` stays lower case).
#[derive(Debug, Clone, Serialize)]
pub struct ModeUpdateRequest {
    pub mode: Option<String>,
}

impl ModeUpdateRequest {
    pub fn from_json(v: &Value) -> Result<Self, ApiError> {
        let o = crate::api::jackson::object(v, "ModeUpdateRequest")?;
        Ok(Self { mode: crate::api::jackson::string(o.get("mode"))? })
    }
}

/// Append-only change log entry. The exporter tails this log, the same way
/// Confluent's exporter tails the `_schemas` topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent {
    pub ctx: String,
    pub subject: String,
    pub version: u32,
    pub id: u32,
    pub kind: LogEventKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogEventKind {
    Register,
    SoftDelete,
    HardDelete,
}

// ---------------------------------------------------------------------------
// Exporters (schema linking)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExporterInfo {
    pub name: String,
    #[serde(default = "default_exporter_subjects")]
    pub subjects: Vec<String>,
    #[serde(default = "default_context_type")]
    pub context_type: String,
    #[serde(default)]
    pub context: Option<String>,
    #[serde(default)]
    pub subject_rename_format: Option<String>,
    #[serde(default)]
    pub config: serde_json::Map<String, Value>,
}

fn default_exporter_subjects() -> Vec<String> {
    vec!["*".to_string()]
}
fn default_context_type() -> String {
    "AUTO".to_string()
}

/// Body of `POST /exporters` and `PUT /exporters/{name}` (all fields optional on PUT).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExporterUpdateRequest {
    pub name: Option<String>,
    pub subjects: Option<Vec<String>>,
    pub context_type: Option<String>,
    pub context: Option<String>,
    pub subject_rename_format: Option<String>,
    pub config: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ExporterState {
    Starting,
    Running,
    Paused,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExporterRecord {
    pub info: ExporterInfo,
    pub state: ExporterState,
    /// Next log sequence number to export.
    pub offset: u64,
    pub ts: i64,
    #[serde(default)]
    pub trace: String,
}

pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
