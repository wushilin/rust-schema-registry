//! Protobuf: compile with `protox` (pure Rust `protoc`) against an in-memory
//! file set made of the schema, its references (keyed by reference name, which
//! is the import path) and the well-known / Confluent built-in imports.
//!
//! Compatibility follows Confluent's `ProtobufSchemaDiff` rules: messages are
//! matched by full name, fields by number. Incompatible changes are
//! PACKAGE_CHANGED, MESSAGE_REMOVED, FIELD_KIND_CHANGED,
//! FIELD_SCALAR_KIND_CHANGED (outside wire-compatible groups),
//! FIELD_NAMED_TYPE_CHANGED, REQUIRED_FIELD_ADDED/REMOVED, ONEOF_FIELD_REMOVED,
//! MULTIPLE_FIELDS_MOVED_TO_ONEOF and FIELD_MOVED_TO_EXISTING_ONEOF.

use std::collections::HashMap;

use base64::Engine;
use prost::Message;
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor};
use prost_types::FileDescriptorProto;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

use super::SchemaError;
use super::{Labels, ResolvedRef};

const ROOT: &str = "__root__.proto";

const CONFLUENT_META: &str = r#"syntax = "proto3";
package confluent;
import "google/protobuf/descriptor.proto";
option go_package = "../confluent";
option java_package = "io.confluent.protobuf";
option java_outer_classname = "MetaProto";
message Meta {
  string doc = 1;
  map<string, string> params = 2;
  repeated string tags = 3;
}
extend google.protobuf.FileOptions { Meta file_meta = 1088; }
extend google.protobuf.MessageOptions { Meta message_meta = 1088; }
extend google.protobuf.FieldOptions { Meta field_meta = 1088; }
extend google.protobuf.EnumOptions { Meta enum_meta = 1088; }
extend google.protobuf.EnumValueOptions { Meta enum_value_meta = 1088; }
"#;

const CONFLUENT_DECIMAL: &str = r#"syntax = "proto3";
package confluent.type;
option go_package = "../type";
option java_package = "io.confluent.protobuf.type";
option java_outer_classname = "DecimalProto";
message Decimal {
  bytes value = 1;
  uint32 precision = 2;
  int32 scale = 3;
}
"#;

enum Source {
    Text(String),
    Descriptor(FileDescriptorProto),
}

struct MemResolver {
    files: HashMap<String, Source>,
}

impl FileResolver for MemResolver {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.files.get(name) {
            Some(Source::Text(src)) => File::from_source(name, src),
            Some(Source::Descriptor(fd)) => {
                let mut fd = fd.clone();
                fd.name = Some(name.to_string());
                Ok(File::from_file_descriptor_proto(fd))
            }
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

/// Recognize a base64-encoded `FileDescriptorProto` (how non-Java clients send schemas).
/// Unambiguous: `.proto` source always contains whitespace or `;`, base64 never does.
fn decode_descriptor(text: &str) -> Option<FileDescriptorProto> {
    let t = text.trim();
    if t.is_empty() || !t.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=')) {
        return None;
    }
    let bytes = base64::engine::general_purpose::STANDARD.decode(t).ok()?;
    let fd = FileDescriptorProto::decode(bytes.as_slice()).ok()?;
    let declares_something = !fd.message_type.is_empty() || !fd.enum_type.is_empty() || !fd.service.is_empty();
    (declares_something || fd.syntax.is_some() || fd.package.is_some()).then_some(fd)
}

fn source_of(text: &str) -> Source {
    match decode_descriptor(text) {
        Some(fd) => Source::Descriptor(fd),
        None => Source::Text(text.to_string()),
    }
}

/// Files Confluent treats as always available (its `dependenciesWithLogicalTypes`).
const BUILTINS: &[(&str, &str)] = &[
    ("confluent/meta.proto", CONFLUENT_META),
    ("confluent/type/decimal.proto", CONFLUENT_DECIMAL),
    ("google/type/calendar_period.proto", include_str!("protos/google/type/calendar_period.proto")),
    ("google/type/color.proto", include_str!("protos/google/type/color.proto")),
    ("google/type/date.proto", include_str!("protos/google/type/date.proto")),
    ("google/type/datetime.proto", include_str!("protos/google/type/datetime.proto")),
    ("google/type/dayofweek.proto", include_str!("protos/google/type/dayofweek.proto")),
    ("google/type/expr.proto", include_str!("protos/google/type/expr.proto")),
    ("google/type/fraction.proto", include_str!("protos/google/type/fraction.proto")),
    ("google/type/latlng.proto", include_str!("protos/google/type/latlng.proto")),
    ("google/type/money.proto", include_str!("protos/google/type/money.proto")),
    ("google/type/month.proto", include_str!("protos/google/type/month.proto")),
    ("google/type/postal_address.proto", include_str!("protos/google/type/postal_address.proto")),
    ("google/type/quaternion.proto", include_str!("protos/google/type/quaternion.proto")),
    ("google/type/timeofday.proto", include_str!("protos/google/type/timeofday.proto")),
];

fn compile(root: Source, refs: &[ResolvedRef]) -> Result<DescriptorPool, String> {
    let mut files = HashMap::new();
    for (name, src) in BUILTINS {
        files.insert(name.to_string(), Source::Text(src.to_string()));
    }
    for r in refs {
        files.insert(r.name.clone(), source_of(&r.schema));
    }
    files.insert(ROOT.to_string(), root);
    let mut chain = ChainFileResolver::new();
    chain.add(MemResolver { files });
    chain.add(GoogleFileResolver::new());
    let mut compiler = protox::Compiler::with_file_resolver(chain);
    compiler.include_imports(true);
    compiler.open_file(ROOT).map_err(|e| e.to_string().replace(ROOT, "schema"))?;
    Ok(compiler.descriptor_pool())
}

/// Confluent parses `.proto` text with Wire, which never resolves custom
/// options and doesn't require unused imports to exist. When a strict compile
/// fails, retry with unresolvable imports dropped and custom options stripped;
/// types that genuinely can't be resolved still fail.
fn relax(text: &str, refs: &[ResolvedRef]) -> String {
    let available = |name: &str| {
        name.starts_with("google/protobuf/")
            || BUILTINS.iter().any(|(n, _)| *n == name)
            || refs.iter().any(|r| r.name == name)
    };
    let src = normalize_text(text);
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    let mut dropped_import = false;
    let mut imported: Vec<String> = Vec::new();
    let starts = |i: usize, kw: &str| src[char_offset(&b, i)..].starts_with(kw);
    // Skip a statement up to its terminating ';' at brace depth 0.
    let skip_statement = |mut i: usize| -> usize {
        let mut depth = 0i32;
        let mut in_str: Option<char> = None;
        while i < b.len() {
            let c = b[i];
            if let Some(q) = in_str {
                if c == '\\' {
                    i += 1;
                } else if c == q {
                    in_str = None;
                }
            } else {
                match c {
                    '"' | '\'' => in_str = Some(c),
                    '{' => depth += 1,
                    '}' => depth -= 1,
                    ';' if depth <= 0 => return i + 1,
                    _ => {}
                }
            }
            i += 1;
        }
        i
    };
    while i < b.len() {
        let c = b[i];
        if c == '"' || c == '\'' {
            // copy string literal verbatim
            let q = c;
            out.push(c);
            i += 1;
            while i < b.len() {
                out.push(b[i]);
                if b[i] == '\\' && i + 1 < b.len() {
                    i += 1;
                    out.push(b[i]);
                } else if b[i] == q {
                    break;
                }
                i += 1;
            }
            i += 1;
            continue;
        }
        let at_word_start = i == 0 || !(b[i - 1].is_alphanumeric() || b[i - 1] == '_');
        if at_word_start && starts(i, "import") {
            let end = skip_statement(i);
            let stmt: String = b[i..end].iter().collect();
            let name = stmt.split('"').nth(1).unwrap_or("");
            if !available(name) {
                dropped_import = true;
                i = end;
                continue;
            }
            imported.push(name.to_string());
        }
        if at_word_start && (starts(i, "option(") || starts(i, "option (")) {
            i = skip_statement(i);
            continue;
        }
        if c == '[' {
            // Field options: drop custom `(ext) = value` entries.
            let mut j = i + 1;
            let mut depth = 0i32;
            let mut in_str: Option<char> = None;
            let mut entries: Vec<String> = Vec::new();
            let mut cur = String::new();
            while j < b.len() {
                let d = b[j];
                if let Some(q) = in_str {
                    cur.push(d);
                    if d == '\\' && j + 1 < b.len() {
                        j += 1;
                        cur.push(b[j]);
                    } else if d == q {
                        in_str = None;
                    }
                } else {
                    match d {
                        '"' | '\'' => {
                            in_str = Some(d);
                            cur.push(d);
                        }
                        '{' | '[' => {
                            depth += 1;
                            cur.push(d);
                        }
                        '}' => {
                            depth -= 1;
                            cur.push(d);
                        }
                        ']' if depth == 0 => break,
                        ']' => {
                            depth -= 1;
                            cur.push(d);
                        }
                        ',' if depth == 0 => entries.push(std::mem::take(&mut cur)),
                        _ => cur.push(d),
                    }
                }
                j += 1;
            }
            entries.push(cur);
            let kept: Vec<String> = entries.into_iter().filter(|e| !e.trim_start().starts_with('(')).collect();
            let kept: Vec<String> = kept.into_iter().filter(|e| !e.trim().is_empty()).collect();
            if !kept.is_empty() {
                out.push('[');
                out.push_str(&kept.join(","));
                out.push(']');
            }
            i = j + 1;
            continue;
        }
        out.push(c);
        i += 1;
    }
    if dropped_import {
        // Wire links against every dependency it was given, whatever the
        // import statements say: make all references visible.
        let extra: String = refs
            .iter()
            .filter(|r| !imported.contains(&r.name))
            .map(|r| format!("import \"{}\";\n", r.name))
            .collect();
        let at = out.find("syntax").and_then(|s| out[s..].find(';').map(|e| s + e + 1)).unwrap_or(0);
        out.insert_str(at, &format!("\n{extra}"));
    }
    out
}

fn char_offset(chars: &[char], i: usize) -> usize {
    chars[..i].iter().map(|c| c.len_utf8()).sum()
}

pub struct ProtoSchema {
    pool: DescriptorPool,
    /// Referenced files by import name, to tell same-reference types apart.
    deps: HashMap<String, String>,
    pub canonical: String,
    pub normalized: String,
}

impl ProtoSchema {
    pub fn parse(text: &str, refs: &[ResolvedRef]) -> Result<Self, SchemaError> {
        let deps: HashMap<String, String> = refs.iter().map(|r| (r.name.clone(), r.schema.clone())).collect();
        match decode_descriptor(text) {
            None => {
                // Confluent stores Wire's rendering of the source (types as
                // written, options kept, comments dropped).
                let wire = super::proto_wire::canonical(text);
                let pool = match compile(Source::Text(text.to_string()), refs) {
                    Ok(p) => p,
                    Err(strict) => {
                        // Lenient, Confluent-style acceptance (see `relax`).
                        match compile(Source::Text(relax(text, refs)), refs) {
                            Ok(p) => p,
                            Err(relaxed) => return Err(classify(wire.is_some(), &strict, &relaxed)),
                        }
                    }
                };
                let canonical = wire.unwrap_or_else(|| text.to_string());
                let normalized = normalized_for(&canonical, &pool);
                Ok(Self { pool, deps, normalized, canonical })
            }
            Some(fd) => {
                // Store the text form like Confluent does, provided it compiles
                // (custom options aren't rendered); otherwise keep the base64 form.
                let rendered = super::proto_print::render(&fd);
                if let Ok(pool) = compile(Source::Text(rendered.clone()), refs) {
                    let canonical = super::proto_wire::canonical(&rendered).unwrap_or(rendered);
                    let normalized = normalized_for(&canonical, &pool);
                    return Ok(Self { pool, deps, normalized, canonical });
                }
                let pool = compile(Source::Descriptor(fd), refs).map_err(SchemaError::parse)?;
                Ok(Self { pool, deps, canonical: text.trim().to_string(), normalized: text.trim().to_string() })
            }
        }
    }

    /// `format=serialized`: base64 `FileDescriptorProto`, built from the
    /// Wire model like Confluent does (see `proto_wire::serialized`).
    pub fn serialized(&self) -> Option<String> {
        let bytes = super::proto_wire::serialized(&self.canonical, &self.pool, "default").or_else(|| {
            let f = self.pool.get_file_by_name(ROOT)?;
            let mut fd = f.file_descriptor_proto().clone();
            fd.name = Some("default".to_string());
            Some(fd.encode_to_vec())
        })?;
        Some(base64::engine::general_purpose::STANDARD.encode(bytes))
    }

}

/// Map a compile failure onto Confluent's two failure points: syntax errors
/// and duplicate definitions fail Wire's parser; unresolved types fail
/// `validate()` (descriptor building) with "Could not resolve type".
fn classify(wire_parses: bool, strict: &str, relaxed: &str) -> SchemaError {
    let undefined = [relaxed, strict].into_iter().find_map(|e| {
        let rest = &e[e.find("name '")? + 6..];
        let name = &rest[..rest.find('\'')?];
        e.contains("is not defined").then(|| name.to_string())
    });
    if let Some(name) = undefined {
        return SchemaError::validate(format!("Could not resolve type: {name}"));
    }
    if !wire_parses || strict.contains("defined twice") {
        return SchemaError::parse(format!("Could not parse Protobuf - {strict}"));
    }
    SchemaError::validate(strict.to_string())
}

/// Confluent's normalized text, falling back to whitespace normalization.
fn normalized_for(canonical: &str, pool: &DescriptorPool) -> String {
    let file_pool = pool.get_file_by_name(ROOT).map(|_| pool);
    file_pool.and_then(|p| super::proto_wire::normalized(canonical, p)).unwrap_or_else(|| normalize_text(canonical))
}

/// Strip comments and collapse whitespace, so formatting-only edits dedup under `normalize=true`.
fn normalize_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    let mut in_str: Option<char> = None;
    let mut pending_space = false;
    while let Some(c) = chars.next() {
        if let Some(q) = in_str {
            out.push(c);
            if c == '\\' {
                if let Some(n) = chars.next() {
                    out.push(n);
                }
            } else if c == q {
                in_str = None;
            }
            continue;
        }
        match c {
            '/' if chars.peek() == Some(&'/') => {
                for n in chars.by_ref() {
                    if n == '\n' {
                        break;
                    }
                }
                pending_space = true;
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = ' ';
                for n in chars.by_ref() {
                    if prev == '*' && n == '/' {
                        break;
                    }
                    prev = n;
                }
                pending_space = true;
            }
            c if c.is_whitespace() => pending_space = true,
            c => {
                let punct = |x: char| "{}();=,<>[]".contains(x);
                if pending_space && !out.is_empty() && !punct(c) && !out.ends_with(punct) {
                    out.push(' ');
                }
                pending_space = false;
                if c == '"' || c == '\'' {
                    in_str = Some(c);
                }
                out.push(c);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Compatibility: a port of Confluent's `protobuf.diff` package.
//
// Differences are collected while walking the *writer* ("original") and
// *reader* ("update") schemas the way Confluent's `SchemaDiff` does, then
// every difference not in `COMPATIBLE` is reported. Iteration follows Java
// HashSet order so messages come out in Confluent's order.
// ---------------------------------------------------------------------------

use super::java_order::{JavaSet, map_order, union_order};
use prost_reflect::Syntax;
use prost_types::field_descriptor_proto::Label;

const COMPATIBLE: &[&str] = &[
    "MESSAGE_ADDED",
    "MESSAGE_MOVED",
    "ENUM_ADDED",
    "ENUM_REMOVED",
    "ENUM_CONST_ADDED",
    "ENUM_CONST_CHANGED",
    "ENUM_CONST_REMOVED",
    "FIELD_ADDED",
    "FIELD_REMOVED",
    "FIELD_NAME_CHANGED",
    "FIELD_STRING_OR_BYTES_LABEL_CHANGED",
    "ONEOF_ADDED",
    "ONEOF_REMOVED",
    "ONEOF_FIELD_ADDED",
];

struct Diff {
    kind: &'static str,
    path: String,
}

impl Diff {
    fn incompatible(&self) -> bool {
        !COMPATIBLE.contains(&self.kind)
    }

    /// Confluent's `Difference#toString`, with its two `%s` filled in as
    /// (reader, writer) labels.
    fn render(&self, rl: &str, wl: &str) -> String {
        let p = &self.path;
        let d = match self.kind {
            "PACKAGE_CHANGED" => format!("The package at '{p}' in the {rl} schema does not match the package in the {wl} schema"),
            "MESSAGE_REMOVED" => format!("The {rl} schema is missing a MESSAGE type at path '{p}' in the {wl} schema"),
            "FIELD_KIND_CHANGED" => format!("The type of a field at path '{p}' in the {rl} schema does not match the {wl} schema"),
            "FIELD_SCALAR_KIND_CHANGED" => {
                format!("The kind of a SCALAR field at path '{p}' in the {rl} schema does not match its kind in the {wl} schema")
            }
            "FIELD_NAMED_TYPE_CHANGED" => {
                format!("The type of a MESSAGE field at path '{p}' in the {rl} schema does not match its type in the {wl} schema")
            }
            "FIELD_NUMERIC_LABEL_CHANGED" => {
                format!("The label for a NUMERIC field at path '{p}' in the {rl} schema does not match its label in the {wl} schema")
            }
            "REQUIRED_FIELD_ADDED" => format!("A required field  at path '{p}' in the {rl} schema is missing in the {wl} schema"),
            "REQUIRED_FIELD_REMOVED" => format!("The {rl} schema is missing a required field at path: '{p}' in the {wl} schema"),
            "ONEOF_FIELD_REMOVED" => format!("The {rl} schema is missing a oneof field at path '{p}' in the {wl} schema"),
            "MULTIPLE_FIELDS_MOVED_TO_ONEOF" => {
                format!("Multiple fields in the oneof at path '{p}' in the {rl} schema are outside a oneof in the {wl} schema")
            }
            "FIELD_MOVED_TO_EXISTING_ONEOF" => {
                format!("A field in the oneof at path '{p}' in the {rl} schema is outside an existing oneof in the {wl} schema")
            }
            _ => String::new(),
        };
        format!("{{errorType:\"{}\", description:\"{d}\"}}", self.kind)
    }
}

struct Ctx<'a> {
    orig: &'a ProtoSchema,
    upd: &'a ProtoSchema,
    path: Vec<String>,
    diffs: Vec<Diff>,
    /// Messages currently being compared (Confluent's `enterSchema` guard).
    stack: Vec<String>,
}

impl Ctx<'_> {
    fn add(&mut self, kind: &'static str) {
        self.diffs.push(Diff { kind, path: format!("#/{}", self.path.join("/")) });
    }
    fn enter(&mut self, p: impl Into<String>) {
        self.path.push(p.into());
    }
    fn leave(&mut self) {
        self.path.pop();
    }
}

/// Field type as Confluent's diff sees it.
enum FieldType {
    Scalar(&'static str),
    Enum,
    Message(MessageDescriptor),
    Map(Box<FieldType>, Box<FieldType>),
}

#[derive(PartialEq)]
enum TypeKind {
    Scalar,
    Map,
    Message,
}

impl FieldType {
    fn of(f: &FieldDescriptor) -> FieldType {
        if f.is_map()
            && let Kind::Message(entry) = f.kind()
        {
            return FieldType::Map(
                Box::new(FieldType::of(&entry.map_entry_key_field())),
                Box::new(FieldType::of(&entry.map_entry_value_field())),
            );
        }
        match f.kind() {
            Kind::Message(m) => FieldType::Message(m),
            Kind::Enum(_) => FieldType::Enum,
            Kind::Double => FieldType::Scalar("double"),
            Kind::Float => FieldType::Scalar("float"),
            Kind::Int32 => FieldType::Scalar("int32"),
            Kind::Int64 => FieldType::Scalar("int64"),
            Kind::Uint32 => FieldType::Scalar("uint32"),
            Kind::Uint64 => FieldType::Scalar("uint64"),
            Kind::Sint32 => FieldType::Scalar("sint32"),
            Kind::Sint64 => FieldType::Scalar("sint64"),
            Kind::Fixed32 => FieldType::Scalar("fixed32"),
            Kind::Fixed64 => FieldType::Scalar("fixed64"),
            Kind::Sfixed32 => FieldType::Scalar("sfixed32"),
            Kind::Sfixed64 => FieldType::Scalar("sfixed64"),
            Kind::Bool => FieldType::Scalar("bool"),
            Kind::String => FieldType::Scalar("string"),
            Kind::Bytes => FieldType::Scalar("bytes"),
        }
    }

    fn kind(&self) -> TypeKind {
        match self {
            FieldType::Scalar(_) | FieldType::Enum => TypeKind::Scalar,
            FieldType::Map(..) => TypeKind::Map,
            FieldType::Message(_) => TypeKind::Message,
        }
    }

    /// Confluent's `ScalarKind`: wire-compatible groups; enums are plain numbers.
    fn scalar_kind(&self) -> &'static str {
        match self {
            FieldType::Enum => "GENERAL_NUMBER",
            FieldType::Scalar(s) => match *s {
                "int32" | "int64" | "uint32" | "uint64" | "bool" => "GENERAL_NUMBER",
                "sint32" | "sint64" => "SIGNED_NUMBER",
                "string" | "bytes" => "STRING_OR_BYTES",
                "fixed32" | "sfixed32" => "FIXED32",
                "fixed64" | "sfixed64" => "FIXED64",
                "float" => "FLOAT",
                _ => "DOUBLE",
            },
            _ => "ANY",
        }
    }
}

/// The label as written in the source (`None` when implicit), which is what
/// Confluent's text-based diff compares.
fn explicit_label(f: &FieldDescriptor) -> Option<Label> {
    if oneof_of(f).is_some() || f.is_map() {
        return None;
    }
    let fdp = f.field_descriptor_proto();
    match f.parent_file().syntax() {
        Syntax::Proto3 => {
            if fdp.label() == Label::Repeated {
                Some(Label::Repeated)
            } else if fdp.proto3_optional() {
                Some(Label::Optional)
            } else {
                None
            }
        }
        _ => Some(fdp.label()),
    }
}

fn is_required(f: &FieldDescriptor) -> bool {
    explicit_label(f) == Some(Label::Required)
}

/// Real (non-synthetic) oneof name of a field.
fn oneof_of(f: &FieldDescriptor) -> Option<String> {
    f.containing_oneof().filter(|o| !o.is_synthetic()).map(|o| o.name().to_string())
}

fn local_name(m: &MessageDescriptor) -> String {
    let pkg = m.parent_file().package_name().to_string();
    let full = m.full_name();
    if !pkg.is_empty() && full.starts_with(&format!("{pkg}.")) { full[pkg.len() + 1..].to_string() } else { full.to_string() }
}

/// Messages to compare at one level (map entries don't exist in Confluent's text model).
fn named(msgs: impl Iterator<Item = MessageDescriptor>) -> Vec<MessageDescriptor> {
    msgs.filter(|m| !m.is_map_entry()).collect()
}

fn compare_types(ctx: &mut Ctx<'_>, orig: &[MessageDescriptor], upd: &[MessageDescriptor]) {
    let on: Vec<String> = orig.iter().map(|m| m.name().to_string()).collect();
    let un: Vec<String> = upd.iter().map(|m| m.name().to_string()).collect();
    for name in union_order(&on, &un) {
        ctx.enter(name.clone());
        let oi = on.iter().position(|n| *n == name);
        let ui = un.iter().position(|n| *n == name);
        match (oi, ui) {
            (Some(_), None) => ctx.add("MESSAGE_REMOVED"),
            (None, Some(_)) => ctx.add("MESSAGE_ADDED"),
            (Some(o), Some(u)) => {
                compare_message(ctx, &orig[o], &upd[u]);
                if o != u {
                    ctx.add("MESSAGE_MOVED");
                }
            }
            (None, None) => {}
        }
        ctx.leave();
    }
}

fn compare_message(ctx: &mut Ctx<'_>, o: &MessageDescriptor, u: &MessageDescriptor) {
    let key = o.full_name().to_string();
    if !ctx.stack.contains(&key) {
        ctx.stack.push(key);
        compare_message_body(ctx, o, u);
        ctx.stack.pop();
    }
    compare_types(ctx, &named(o.child_messages()), &named(u.child_messages()));
}

fn compare_message_body(ctx: &mut Ctx<'_>, o: &MessageDescriptor, u: &MessageDescriptor) {
    let regular = |m: &MessageDescriptor| -> Vec<FieldDescriptor> { m.fields().filter(|f| oneof_of(f).is_none()).collect() };
    let oneofs = |m: &MessageDescriptor| -> Vec<(String, Vec<FieldDescriptor>)> {
        m.oneofs().filter(|x| !x.is_synthetic()).map(|x| (x.name().to_string(), x.fields().collect())).collect()
    };
    let mut orig_regular = regular(o);
    let upd_regular = regular(u);
    let orig_oneofs = oneofs(o);
    let upd_oneofs = oneofs(u);
    let orig_oneof_tags: Vec<u32> = orig_oneofs.iter().flat_map(|(_, fs)| fs.iter().map(|f| f.number())).collect();
    // HashMap key order of the original regular fields, fixed before removals.
    let orig_tag_order = map_order(&orig_regular.iter().map(|f| f.number()).collect::<Vec<_>>());

    for (name, fields) in &upd_oneofs {
        ctx.enter(name.clone());
        let (mut moved, mut existing) = (0, 0);
        for f in fields {
            if let Some(i) = orig_regular.iter().position(|x| x.number() == f.number()) {
                orig_regular.remove(i);
                moved += 1;
            } else if orig_oneof_tags.contains(&f.number()) {
                existing += 1;
            }
        }
        if moved > 1 {
            ctx.add("MULTIPLE_FIELDS_MOVED_TO_ONEOF");
        } else if moved == 1 && existing > 0 {
            ctx.add("FIELD_MOVED_TO_EXISTING_ONEOF");
        }
        ctx.leave();
    }

    let oon: Vec<String> = orig_oneofs.iter().map(|(n, _)| n.clone()).collect();
    let uon: Vec<String> = upd_oneofs.iter().map(|(n, _)| n.clone()).collect();
    for name in union_order(&oon, &uon) {
        ctx.enter(name.clone());
        let of = orig_oneofs.iter().find(|(n, _)| *n == name).map(|(_, f)| f);
        let uf = upd_oneofs.iter().find(|(n, _)| *n == name).map(|(_, f)| f);
        match (of, uf) {
            (Some(_), None) => ctx.add("ONEOF_REMOVED"),
            (None, Some(_)) => ctx.add("ONEOF_ADDED"),
            (Some(of), Some(uf)) => {
                let ot: Vec<u32> = of.iter().map(|f| f.number()).collect();
                let ut: Vec<u32> = uf.iter().map(|f| f.number()).collect();
                for tag in union_order(&ot, &ut) {
                    ctx.enter(tag.to_string());
                    match (of.iter().find(|f| f.number() == tag), uf.iter().find(|f| f.number() == tag)) {
                        (Some(_), None) => ctx.add("ONEOF_FIELD_REMOVED"),
                        (None, Some(_)) => ctx.add("ONEOF_FIELD_ADDED"),
                        (Some(a), Some(b)) => compare_field(ctx, a, b),
                        (None, None) => {}
                    }
                    ctx.leave();
                }
            }
            (None, None) => {}
        }
        ctx.leave();
    }

    // `new HashSet(originalByTag.keySet())` after the moved fields were removed, then addAll(update).
    let remaining: Vec<u32> = orig_tag_order.into_iter().filter(|t| orig_regular.iter().any(|f| f.number() == *t)).collect();
    let mut all_tags = JavaSet::from_collection(&remaining);
    all_tags.extend(&map_order(&upd_regular.iter().map(|f| f.number()).collect::<Vec<_>>()));
    for tag in all_tags.ordered() {
        ctx.enter(tag.to_string());
        let of = orig_regular.iter().find(|f| f.number() == tag);
        let uf = upd_regular.iter().find(|f| f.number() == tag);
        match (of, uf) {
            (Some(f), None) => ctx.add(if is_required(f) { "REQUIRED_FIELD_REMOVED" } else { "FIELD_REMOVED" }),
            (None, Some(f)) => ctx.add(if is_required(f) { "REQUIRED_FIELD_ADDED" } else { "FIELD_ADDED" }),
            (Some(a), Some(b)) => compare_field(ctx, a, b),
            (None, None) => {}
        }
        ctx.leave();
    }
}

fn compare_field(ctx: &mut Ctx<'_>, o: &FieldDescriptor, u: &FieldDescriptor) {
    if o.name() != u.name() {
        ctx.add("FIELD_NAME_CHANGED");
    }
    compare_labels_and_types(ctx, explicit_label(o), explicit_label(u), &FieldType::of(o), &FieldType::of(u));
}

fn compare_labels_and_types(ctx: &mut Ctx<'_>, ol: Option<Label>, ul: Option<Label>, ot: &FieldType, ut: &FieldType) {
    if ot.kind() != ut.kind() {
        ctx.add("FIELD_KIND_CHANGED");
        return;
    }
    match (ot, ut) {
        (FieldType::Map(ok, ov), FieldType::Map(uk, uv)) => {
            compare_labels_and_types(ctx, None, None, ok, uk);
            compare_labels_and_types(ctx, None, None, ov, uv);
        }
        (FieldType::Message(om), FieldType::Message(um)) => compare_message_types(ctx, om, um),
        _ => {
            let kind = ot.scalar_kind();
            if kind != ut.scalar_kind() {
                ctx.add("FIELD_SCALAR_KIND_CHANGED");
            } else if let (Some(a), Some(b)) = (ol, ul)
                && a != b
            {
                ctx.add(if kind == "STRING_OR_BYTES" { "FIELD_STRING_OR_BYTES_LABEL_CHANGED" } else { "FIELD_NUMERIC_LABEL_CHANGED" });
            }
        }
    }
}

fn compare_message_types(ctx: &mut Ctx<'_>, om: &MessageDescriptor, um: &MessageDescriptor) {
    let (ol, ul) = (local_name(om), local_name(um));
    if ol != ul {
        ctx.add("FIELD_NAMED_TYPE_CHANGED");
        return;
    }
    // Confluent skips types that come from the same reference (subject and
    // version); types of the schema itself carry an identical dummy reference.
    let (of, uf) = (om.parent_file(), um.parent_file());
    let same_origin = (of.name() == ROOT && uf.name() == ROOT)
        || (of.name() == uf.name()
            && ctx.orig.deps.get(of.name()).is_some_and(|a| ctx.upd.deps.get(uf.name()) == Some(a)));
    if same_origin {
        return;
    }
    let saved_path = std::mem::replace(&mut ctx.path, ol.split('.').map(String::from).collect());
    let before = ctx.diffs.len();
    compare_message(ctx, om, um);
    ctx.path = saved_path;
    if ctx.diffs[before..].iter().any(Diff::incompatible) {
        ctx.add("FIELD_NAMED_TYPE_CHANGED");
    }
}

/// Can `reader` read data written with `writer`? (Confluent: original = writer, update = reader.)
pub fn can_read(reader: &ProtoSchema, writer: &ProtoSchema, labels: Labels) -> Vec<String> {
    let (Some(wf), Some(rf)) = (writer.pool.get_file_by_name(ROOT), reader.pool.get_file_by_name(ROOT)) else {
        return Vec::new();
    };
    let mut ctx = Ctx { orig: writer, upd: reader, path: Vec::new(), diffs: Vec::new(), stack: Vec::new() };
    if wf.package_name() != rf.package_name() {
        ctx.add("PACKAGE_CHANGED");
    }
    compare_types(&mut ctx, &named(wf.messages()), &named(rf.messages()));
    ctx.diffs.iter().filter(|d| d.incompatible()).map(|d| d.render(labels.reader, labels.writer)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Labels = Labels { reader: "new", writer: "old" };

    fn p(s: &str) -> ProtoSchema {
        ProtoSchema::parse(s, &[]).unwrap()
    }

    #[test]
    fn field_rules() {
        let old = p("syntax = \"proto3\"; package x; message A { string a = 1; int32 b = 2; }");
        let added = p("syntax = \"proto3\"; package x; message A { string a = 1; int32 b = 2; bool c = 3; }");
        assert!(can_read(&added, &old, L).is_empty());
        let widened = p("syntax = \"proto3\"; package x; message A { string a = 1; int64 b = 2; }");
        assert!(can_read(&widened, &old, L).is_empty());
        let changed = p("syntax = \"proto3\"; package x; message A { int32 a = 1; int32 b = 2; }");
        assert!(can_read(&changed, &old, L)[0].contains("FIELD_SCALAR_KIND_CHANGED"));
        let removed_msg = p("syntax = \"proto3\"; package x; message B { string a = 1; }");
        assert!(can_read(&removed_msg, &old, L).iter().any(|m| m.contains("MESSAGE_REMOVED")));
    }

    #[test]
    fn imports() {
        let dep = ResolvedRef {
            name: "dep.proto".into(),
            schema: "syntax = \"proto3\"; package d; message D { string x = 1; }".into(),
        };
        let s = ProtoSchema::parse(
            "syntax = \"proto3\"; package m; import \"dep.proto\"; import \"google/protobuf/timestamp.proto\"; \
             message M { d.D d = 1; google.protobuf.Timestamp ts = 2; }",
            &[dep],
        )
        .unwrap();
        assert!(can_read(&s, &s, L).is_empty());
        // Like Confluent: an unused missing import is tolerated, a used one is not.
        assert!(ProtoSchema::parse("syntax = \"proto3\"; import \"missing.proto\";", &[]).is_ok());
        assert!(ProtoSchema::parse("syntax = \"proto3\"; import \"missing.proto\"; message A { m.X x = 1; }", &[]).is_err());
    }

    #[test]
    fn base64_descriptor_round_trip() {
        let text = "syntax = \"proto3\";\npackage acme;\nimport \"google/protobuf/timestamp.proto\";\n\
                    message Order {\n  string id = 1;\n  repeated int32 qty = 2;\n  map<string, int64> tags = 3;\n  \
                    optional string note = 4;\n  oneof pay { string card = 5; string cash = 6; }\n  \
                    google.protobuf.Timestamp ts = 7;\n  Kind kind = 8;\n  enum Kind { A = 0; B = 1; }\n  reserved 20 to 25;\n}\n";
        let s = p(text);
        let b64 = s.serialized().unwrap();
        let from_b64 = ProtoSchema::parse(&b64, &[]).unwrap();
        assert!(from_b64.canonical.contains("message Order"), "{}", from_b64.canonical);
        assert!(from_b64.canonical.contains("map<string, int64> tags = 3;"), "{}", from_b64.canonical);
        assert!(from_b64.canonical.contains("oneof pay"), "{}", from_b64.canonical);
        assert!(from_b64.canonical.contains("optional string note = 4;"), "{}", from_b64.canonical);
        assert!(can_read(&from_b64, &s, L).is_empty());
        assert!(can_read(&s, &from_b64, L).is_empty());
        // The rendered text must itself parse (it's what we store).
        assert!(ProtoSchema::parse(&from_b64.canonical, &[]).is_ok());
    }

    #[test]
    fn explicit_map_entry_is_kept_like_confluent() {
        // Confluent stores the source shape (Wire), not a `map<>` rewrite.
        let java_style = "syntax = \"proto3\";\npackage j;\nmessage Ev {\n  repeated mEntry m = 3;\n  message mEntry {\n    option map_entry = true;\n    string key = 1;\n    int32 value = 2;\n  }\n}\n";
        let s = p(java_style);
        assert!(s.canonical.contains("repeated mEntry m = 3;"), "{}", s.canonical);
        assert!(s.canonical.contains("option map_entry = true;"), "{}", s.canonical);
    }

    #[test]
    fn custom_options_are_kept_in_wire_format() {
        let text = "syntax = \"proto3\";\nimport \"confluent/meta.proto\";\nmessage A {\n  string a = 1 [(confluent.field_meta).tags = \"PII\"];\n}\n";
        let c = p(text).canonical;
        assert!(c.contains("string a = 1 [(confluent.field_meta).tags = \"PII\"];"), "{c}");
    }

    #[test]
    fn normalization() {
        assert_eq!(
            normalize_text("syntax = \"proto3\";\n// c\nmessage A {\n  string a = 1; /* x */\n}\n"),
            normalize_text("syntax=\"proto3\"; message A { string a=1; }")
        );
    }
}
