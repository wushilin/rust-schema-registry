//! Format-specific parsing, canonicalization and compatibility checking.
//!
//! Every format exposes the same three things:
//!
//! * `parse(text, refs)` - validate the schema (with its resolved references)
//! * a canonical string (what we store and hand back) and a normalized string
//!   (what `normalize=true` dedups on)
//! * `can_read(reader, writer)` - can a consumer using `reader` decode data
//!   produced with `writer`? Returns human-readable incompatibility messages.
//!
//! Compatibility levels are then just combinations of `can_read`:
//! BACKWARD = new reads old, FORWARD = old reads new, FULL = both.

pub mod avro;
pub mod java_order;
mod avro_java;
pub mod json;
pub mod proto_print;
pub mod proto_wire;
pub mod tags;
pub mod protobuf;

use sha2::{Digest, Sha256};

use crate::model::{CompatibilityLevel, SchemaReference, SchemaType};

/// A referenced schema, already fetched from the store.
#[derive(Debug, Clone)]
pub struct ResolvedRef {
    pub name: String,
    pub schema: String,
}

pub enum Parsed {
    Avro(avro::AvroSchema),
    Json(json::JsonSchema),
    Protobuf(protobuf::ProtoSchema),
}

pub struct ParsedSchema {
    pub schema_type: SchemaType,
    /// As-registered form (whitespace-compacted for JSON based formats).
    pub canonical: String,
    /// Normalized form, used when `normalize=true`.
    pub normalized: String,
    /// Why normalizing fails, when it does (reported only if requested).
    pub normalize_error: Option<String>,
    pub inner: Parsed,
}

/// Which of Confluent's two failure points rejected a schema. They produce
/// different 42201 messages: a parse failure is reported with the references
/// and type (`Invalid schema {..} with refs [..] of type AVRO, details: ..`),
/// a failed `ParsedSchema#validate` without (`Invalid schema {..}, details: ..`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Parse,
    Validate,
}

#[derive(Debug, Clone)]
pub struct SchemaError {
    pub kind: ErrorKind,
    pub message: String,
}

impl SchemaError {
    pub fn parse(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Parse, message: message.into() }
    }
    pub fn validate(message: impl Into<String>) -> Self {
        Self { kind: ErrorKind::Validate, message: message.into() }
    }
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(test)]
/// Parse and validate a new schema the way Confluent's Oracle path does
/// (Avro defaults validated). `refs` must contain every transitively
/// referenced schema, dependencies first.
pub fn parse(schema_type: SchemaType, text: &str, refs: &[ResolvedRef]) -> Result<ParsedSchema, SchemaError> {
    parse_with(schema_type, text, refs, true)
}

/// `validate_defaults`: Confluent's `(avro.validate.defaults || normalize) && isNew`.
pub fn parse_with(
    schema_type: SchemaType,
    text: &str,
    refs: &[ResolvedRef],
    validate_defaults: bool,
) -> Result<ParsedSchema, SchemaError> {
    match schema_type {
        SchemaType::Avro => {
            let s = avro::AvroSchema::parse(text, refs, validate_defaults)?;
            Ok(ParsedSchema {
                schema_type,
                canonical: s.canonical.clone(),
                normalized: s.normalized.clone(),
                normalize_error: s.normalize_error.clone(),
                inner: Parsed::Avro(s),
            })
        }
        SchemaType::Json => {
            // Confluent: unparseable JSON fails while parsing; anything the
            // everit loader rejects fails `validate()` with a fixed message.
            if serde_json::from_str::<serde_json::Value>(text).is_err() {
                return Err(SchemaError::parse(format!("Invalid JSON {text}")));
            }
            let s = json::JsonSchema::parse(text, refs).map_err(|_| SchemaError::validate("Invalid JSON Schema"))?;
            Ok(ParsedSchema {
                schema_type,
                canonical: s.canonical.clone(),
                normalized: s.normalized.clone(),
                normalize_error: None,
                inner: Parsed::Json(s),
            })
        }
        SchemaType::Protobuf => {
            let s = protobuf::ProtoSchema::parse(text, refs)?;
            Ok(ParsedSchema {
                schema_type,
                canonical: s.canonical.clone(),
                normalized: s.normalized.clone(),
                normalize_error: None,
                inner: Parsed::Protobuf(s),
            })
        }
    }
}

/// Labels used in messages ("new"/"old" depending on direction).
#[derive(Clone, Copy)]
pub struct Labels {
    pub reader: &'static str,
    pub writer: &'static str,
}

/// Can `reader` read data written with `writer`? Empty result means yes.
pub fn can_read(reader: &ParsedSchema, writer: &ParsedSchema, labels: Labels) -> Vec<String> {
    match (&reader.inner, &writer.inner) {
        (Parsed::Avro(r), Parsed::Avro(w)) => avro::can_read(r, w, labels),
        (Parsed::Json(r), Parsed::Json(w)) => json::can_read(r, w, labels),
        (Parsed::Protobuf(r), Parsed::Protobuf(w)) => protobuf::can_read(r, w, labels),
        _ => vec![format!(
            "{{errorType:'SCHEMA_TYPE_CHANGED', description:'The {} schema is of type {} but the {} schema is of type {}'}}",
            labels.reader,
            reader.schema_type.as_str(),
            labels.writer,
            writer.schema_type.as_str()
        )],
    }
}

/// Identity of a schema within a context. Two registrations with the same
/// fingerprint share an ID (Confluent semantics: same schema text, type,
/// references and metadata/rules => same ID, across subjects).
pub fn fingerprint(
    schema_type: SchemaType,
    schema: &str,
    references: &[SchemaReference],
    metadata: Option<&serde_json::Value>,
    rule_set: Option<&serde_json::Value>,
) -> String {
    let mut h = Sha256::new();
    h.update(schema_type.as_str().as_bytes());
    h.update([0]);
    h.update(schema.as_bytes());
    h.update([0]);
    for r in references {
        h.update(r.name.as_bytes());
        h.update([1]);
        h.update(r.subject.as_bytes());
        h.update([1]);
        h.update(r.version.to_be_bytes());
        h.update([0]);
    }
    h.update([0]);
    if let Some(m) = metadata.filter(|m| !m.is_null()) {
        h.update(m.to_string().as_bytes());
    }
    h.update([0]);
    if let Some(r) = rule_set.filter(|r| !r.is_null()) {
        h.update(r.to_string().as_bytes());
    }
    hex::encode(h.finalize())
}

/// An earlier version to check against.
pub struct Previous<'a> {
    pub version: u32,
    pub schema: &'a ParsedSchema,
}

const MAX_SCHEMA_SIZE_FOR_LOGGING: usize = 10240;

/// Confluent's `SchemaValidatorBuilder#formatErrorMessages`: after a failing
/// check, append the old version and schema (truncated past 10 KiB).
fn with_trailer(mut msgs: Vec<String>, p: &Previous<'_>, append: bool) -> Vec<String> {
    if !msgs.is_empty() && append {
        msgs.push(format!("{{oldSchemaVersion: {}}}", p.version));
        let old = &p.schema.canonical;
        if old.len() <= MAX_SCHEMA_SIZE_FOR_LOGGING {
            msgs.push(format!("{{oldSchema: '{old}'}}"));
        } else {
            let mut cut = MAX_SCHEMA_SIZE_FOR_LOGGING;
            while !old.is_char_boundary(cut) {
                cut -= 1;
            }
            msgs.push(format!("{{oldSchema: <truncated> '{}...'}}", &old[..cut]));
        }
    }
    msgs
}

/// Check `new` against earlier versions (newest first) at `level`, exactly like
/// Confluent's `CompatibilityChecker`: non-transitive levels look at the latest
/// version only; transitive levels walk newest to oldest and stop at the first
/// failure. FULL reports the forward problems first, then the backward ones.
pub fn check_level(new: &ParsedSchema, previous_newest_first: &[Previous<'_>], level: CompatibilityLevel) -> Vec<String> {
    if level == CompatibilityLevel::None {
        return Vec::new();
    }
    let candidates = if level.transitive() { previous_newest_first } else { &previous_newest_first[..previous_newest_first.len().min(1)] };
    let new_reads_old = Labels { reader: "new", writer: "old" };
    let old_reads_new = Labels { reader: "old", writer: "new" };
    for p in candidates {
        let msgs = if p.schema.schema_type != new.schema_type {
            vec!["Incompatible because of different schema type".to_string()]
        } else {
            match (level.backward(), level.forward()) {
                (true, false) => with_trailer(can_read(new, p.schema, new_reads_old), p, true),
                (false, true) => with_trailer(can_read(p.schema, new, old_reads_new), p, true),
                _ => {
                    let mut m = with_trailer(can_read(p.schema, new, old_reads_new), p, false);
                    m.extend(with_trailer(can_read(new, p.schema, new_reads_old), p, true));
                    m
                }
            }
        };
        if !msgs.is_empty() {
            return msgs;
        }
    }
    Vec::new()
}
