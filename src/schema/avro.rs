//! Avro: parsing via `apache-avro`, compatibility via our own implementation of
//! the Avro schema-resolution rules (mirroring Java's `SchemaCompatibility`,
//! which is what Confluent uses).
//!
//! We don't use `apache_avro::schema_compatibility` because it compares
//! `Schema::Ref` nodes by name only, so it can't see through references to
//! other subjects or named types reused later in the same schema. Here every
//! `Ref` is resolved through a name table built from the schema and its
//! references.

use std::collections::HashMap;

use apache_avro::Schema;
use apache_avro::schema::{EnumSchema, FixedSchema, InnerDecimalSchema, Name, RecordSchema, UuidSchema};

use super::{Labels, ResolvedRef};

pub struct AvroSchema {
    pub schema: Schema,
    names: HashMap<String, Schema>,
    pub canonical: String,
    pub normalized: String,
    /// Normalizing throws in Confluent (e.g. a default whose list holds a
    /// value of the wrong type).
    pub normalize_error: Option<String>,
}

impl AvroSchema {
    /// All failures, rejected defaults included, are parse failures (Java
    /// Avro checks defaults while parsing).
    pub fn parse(text: &str, refs: &[ResolvedRef], validate_defaults: bool) -> Result<Self, super::SchemaError> {
        Self::parse_str(text, refs, validate_defaults).map_err(super::SchemaError::parse)
    }

    fn parse_str(text: &str, refs: &[ResolvedRef], validate_defaults: bool) -> Result<Self, String> {
        let value: serde_json::Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(e) => {
                // Java reads one value and complains about what follows it.
                let mut stream = serde_json::Deserializer::from_str(text).into_iter::<serde_json::Value>();
                if let Some(Ok(_)) = stream.next() {
                    let rest = text[stream.byte_offset()..].trim();
                    if !rest.is_empty() {
                        return Err(format!("dangling content after end of schema: {rest}"));
                    }
                }
                return Err(format!("Invalid JSON: {e}"));
            }
        };
        // Parsing still uses the compact input; what we *store* is Java's
        // `Schema.toString()` rendering, like Confluent.
        // Java Avro's checks first, for Confluent's messages.
        // Defaults Java tolerates (not validated) would still stop the crate's
        // parser: it gets a copy with type-correct stand-ins; we store the original.
        let mut java = super::avro_java::Parser::new(validate_defaults);
        let mut dep_texts: Vec<String> = Vec::with_capacity(refs.len());
        for r in refs {
            match serde_json::from_str::<serde_json::Value>(&r.schema) {
                Ok(mut v) => {
                    for (ptr, stand_in) in java.parse(&v)?.lenient {
                        if let Some(slot) = v.pointer_mut(&ptr) {
                            *slot = stand_in;
                        }
                    }
                    dep_texts.push(serde_json::to_string(&v).unwrap_or_else(|_| r.schema.clone()));
                }
                Err(_) => dep_texts.push(r.schema.clone()),
            }
        }
        let mut model_value = value.clone();
        let root = java.parse(&value)?;
        // Textual float/double defaults become numbers while parsing (Double.valueOf).
        let mut value = value;
        for (ptr, raw) in &root.numbers {
            if let Some(slot) = value.pointer_mut(ptr) {
                *slot = serde_json::Value::String(format!("{}{raw}", super::avro_java::RAW_NUMBER));
            }
        }
        for (ptr, stand_in) in &root.lenient {
            if let Some(slot) = model_value.pointer_mut(ptr) {
                *slot = stand_in.clone();
            }
        }
        // Confluent's normalized form renders each default through the field's type.
        let mut normalized_value = value.clone();
        let mut normalize_error = None;
        for (ptr, out) in root.normalized {
            match out {
                Ok(Some(v)) => {
                    if let Some(slot) = normalized_value.pointer_mut(&ptr) {
                        *slot = v;
                    }
                }
                Ok(None) => {
                    let parent = &ptr[..ptr.rfind('/').unwrap_or(0)];
                    if let Some(serde_json::Value::Object(o)) = normalized_value.pointer_mut(parent) {
                        o.shift_remove("default");
                    }
                }
                Err(e) => {
                    normalize_error.get_or_insert(e);
                }
            }
        }
        super::avro_java::model_safe_namespaces(&mut model_value);
        let compact = serde_json::to_string(&model_value).map_err(|e| e.to_string())?;
        let (schema, schemata) = if refs.is_empty() {
            (Schema::parse(&model_value).map_err(|e| e.to_string())?, Vec::new())
        } else {
            let deps: Vec<&str> = dep_texts.iter().map(String::as_str).collect();
            Schema::parse_str_with_list(&compact, deps).map_err(|e| e.to_string())?
        };
        check_redefinitions(&value, refs)?;
        let mut names = HashMap::new();
        collect_names(&schema, &mut names);
        for s in &schemata {
            collect_names(s, &mut names);
        }

        let canonical = java_to_string(&value, refs);
        // Confluent's normalized Avro: the Java rendering with properties sorted.
        let normalized = java_render(&normalized_value, refs, true);
        Ok(Self { schema, names, canonical, normalized, normalize_error })
    }
}

const PRIMITIVES: &[&str] = &["null", "boolean", "int", "long", "float", "double", "bytes", "string"];

/// Render a schema the way Java Avro's `Schema.toString()` does (which is what
/// Confluent stores and returns): fixed key order per schema kind, `namespace`
/// only where it differs from the enclosing one, names qualified relative to
/// the current namespace, bare primitives without properties collapsed to
/// `"type"`, and custom properties after the standard attributes.
/// Types defined by references are known up front and printed by name.
/// Java's `Schema#toString` of a schema given as JSON (for messages).
pub fn java_string_of(value: &serde_json::Value) -> String {
    java_render(value, &[], false)
}

fn java_to_string(value: &serde_json::Value, refs: &[ResolvedRef]) -> String {
    java_render(value, refs, false)
}

fn java_render(value: &serde_json::Value, refs: &[ResolvedRef], sort_props: bool) -> String {
    SORT_PROPS.with(|s| s.set(sort_props));
    let mut known: Vec<String> = Vec::new();
    for r in refs {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&r.schema) {
            collect_defined(&v, "", &mut known);
        }
    }
    let mut out = String::new();
    render_schema(value, &mut None, &mut known, &mut out);
    out
}

fn collect_defined(v: &serde_json::Value, ns: &str, out: &mut Vec<String>) {
    match v {
        serde_json::Value::Array(a) => a.iter().for_each(|x| collect_defined(x, ns, out)),
        serde_json::Value::Object(o) => {
            let mut inner = ns.to_string();
            if let (Some(t), Some(name)) = (o.get("type").and_then(|t| t.as_str()), o.get("name").and_then(|n| n.as_str()))
                && matches!(t, "record" | "error" | "enum" | "fixed")
            {
                let (full, space) = split_name(name, o.get("namespace").and_then(|n| n.as_str()), ns);
                out.push(full);
                inner = space.unwrap_or_default();
            }
            for k in ["type", "fields", "items", "values"] {
                if let Some(x) = o.get(k).filter(|x| !x.is_string()) {
                    collect_defined(x, &inner, out);
                }
            }
        }
        _ => {}
    }
}

/// (full name, namespace) per the Avro naming rules.
fn split_name(name: &str, namespace: Option<&str>, enclosing: &str) -> (String, Option<String>) {
    if let Some((space, _)) = name.rsplit_once('.') {
        return (name.to_string(), Some(space.to_string()));
    }
    let space = namespace.unwrap_or(enclosing);
    if space.is_empty() { (name.to_string(), None) } else { (format!("{space}.{name}"), Some(space.to_string())) }
}

fn json_str(s: &str) -> String {
    serde_json::to_string(s).unwrap_or_default()
}

/// Name as Java prints a reference: simple when in the current namespace.
fn qualified(full: &str, space: &Option<String>) -> String {
    match (full.rsplit_once('.'), space) {
        (Some((ns, simple)), Some(cur)) if ns == cur => simple.to_string(),
        _ => full.to_string(),
    }
}

thread_local! {
    static SORT_PROPS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn write_props(o: &serde_json::Map<String, serde_json::Value>, reserved: &[&str], out: &mut String) {
    let mut props: Vec<(&String, &serde_json::Value)> = o.iter().collect();
    if SORT_PROPS.with(|s| s.get()) {
        props.sort_by(|a, b| a.0.cmp(b.0));
    }
    for (k, v) in props {
        if !reserved.contains(&k.as_str()) {
            out.push(',');
            out.push_str(&json_str(k));
            out.push(':');
            out.push_str(&serde_json::to_string(v).unwrap_or_default());
        }
    }
}

fn write_aliases(o: &serde_json::Map<String, serde_json::Value>, out: &mut String) {
    if let Some(serde_json::Value::Array(a)) = o.get("aliases")
        && !a.is_empty()
    {
        out.push_str(",\"aliases\":");
        out.push_str(&serde_json::to_string(a).unwrap_or_default());
    }
}

fn render_schema(v: &serde_json::Value, space: &mut Option<String>, known: &mut Vec<String>, out: &mut String) {
    use serde_json::Value as J;
    match v {
        J::String(t) => {
            if PRIMITIVES.contains(&t.as_str()) {
                out.push_str(&json_str(t));
            } else {
                let full = if t.contains('.') {
                    t.clone()
                } else {
                    match space {
                        Some(ns) if !ns.is_empty() => format!("{ns}.{t}"),
                        _ => t.clone(),
                    }
                };
                // An unqualified name may also refer to a type in the null namespace.
                let full = if known.contains(&full) || !known.contains(t) { full } else { t.clone() };
                out.push_str(&json_str(&qualified(&full, space)));
            }
        }
        J::Array(branches) => {
            out.push('[');
            for (i, b) in branches.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                render_schema(b, space, known, out);
            }
            out.push(']');
        }
        J::Object(o) => {
            let ty = o.get("type");
            match ty.and_then(|t| t.as_str()) {
                Some(t @ ("record" | "error" | "enum" | "fixed")) => {
                    let name = o.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let enclosing = space.clone().unwrap_or_default();
                    let (full, ns) = split_name(name, o.get("namespace").and_then(|n| n.as_str()), &enclosing);
                    if known.contains(&full) {
                        out.push_str(&json_str(&qualified(&full, space)));
                        return;
                    }
                    known.push(full.clone());
                    let simple = full.rsplit('.').next().unwrap_or(&full).to_string();
                    out.push_str(&format!("{{\"type\":{},\"name\":{}", json_str(t), json_str(&simple)));
                    match (&ns, &*space) {
                        (Some(n), Some(cur)) if n == cur => {}
                        (Some(n), _) => out.push_str(&format!(",\"namespace\":{}", json_str(n))),
                        (None, Some(cur)) if !cur.is_empty() => out.push_str(",\"namespace\":\"\""),
                        _ => {}
                    }
                    if let Some(d) = o.get("doc").and_then(|d| d.as_str()) {
                        out.push_str(&format!(",\"doc\":{}", json_str(d)));
                    }
                    let saved = std::mem::replace(space, ns.clone());
                    match t {
                        "enum" => {
                            out.push_str(",\"symbols\":");
                            out.push_str(&serde_json::to_string(o.get("symbols").unwrap_or(&J::Array(vec![]))).unwrap_or_default());
                            if let Some(d) = o.get("default") {
                                out.push_str(&format!(",\"default\":{}", serde_json::to_string(d).unwrap_or_default()));
                            }
                            write_props(o, &["type", "name", "namespace", "doc", "symbols", "default", "aliases"], out);
                        }
                        "fixed" => {
                            out.push_str(&format!(",\"size\":{}", o.get("size").map(|s| s.to_string()).unwrap_or_default()));
                            write_props(o, &["type", "name", "namespace", "doc", "size", "aliases"], out);
                        }
                        _ => {
                            out.push_str(",\"fields\":[");
                            let fields = o.get("fields").and_then(|f| f.as_array()).cloned().unwrap_or_default();
                            for (i, f) in fields.iter().enumerate() {
                                if i > 0 {
                                    out.push(',');
                                }
                                render_field(f, space, known, out);
                            }
                            out.push(']');
                            write_props(o, &["type", "name", "namespace", "doc", "fields", "aliases"], out);
                        }
                    }
                    write_aliases(o, out);
                    out.push('}');
                    *space = saved;
                }
                Some("array") => {
                    out.push_str("{\"type\":\"array\",\"items\":");
                    render_schema(o.get("items").unwrap_or(&J::Null), space, known, out);
                    write_props(o, &["type", "items"], out);
                    out.push('}');
                }
                Some("map") => {
                    out.push_str("{\"type\":\"map\",\"values\":");
                    render_schema(o.get("values").unwrap_or(&J::Null), space, known, out);
                    write_props(o, &["type", "values"], out);
                    out.push('}');
                }
                Some(p) if PRIMITIVES.contains(&p) => {
                    if o.len() == 1 {
                        out.push_str(&json_str(p));
                    } else {
                        out.push_str(&format!("{{\"type\":{}", json_str(p)));
                        write_props(o, &["type"], out);
                        out.push('}');
                    }
                }
                Some(other) => render_schema(&J::String(other.to_string()), space, known, out),
                None => match ty {
                    Some(inner) => render_schema(inner, space, known, out),
                    None => out.push_str(&serde_json::to_string(v).unwrap_or_default()),
                },
            }
        }
        other => out.push_str(&serde_json::to_string(other).unwrap_or_default()),
    }
}

fn render_field(f: &serde_json::Value, space: &mut Option<String>, known: &mut Vec<String>, out: &mut String) {
    let Some(o) = f.as_object() else { return };
    out.push_str(&format!("{{\"name\":{},\"type\":", json_str(o.get("name").and_then(|n| n.as_str()).unwrap_or(""))));
    render_schema(o.get("type").unwrap_or(&serde_json::Value::Null), space, known, out);
    if let Some(d) = o.get("doc").and_then(|d| d.as_str()) {
        out.push_str(&format!(",\"doc\":{}", json_str(d)));
    }
    if let Some(d) = o.get("default") {
        // A default Java holds as a double node (converted from text) prints bare.
        let text = match d.as_str().and_then(|s| s.strip_prefix(super::avro_java::RAW_NUMBER)) {
            Some(raw) => raw.to_string(),
            None => serde_json::to_string(d).unwrap_or_default(),
        };
        out.push_str(&format!(",\"default\":{text}"));
    }
    if let Some(order) = o.get("order").and_then(|x| x.as_str())
        && order != "ascending"
    {
        out.push_str(&format!(",\"order\":{}", json_str(order)));
    }
    write_aliases(o, out);
    write_props(o, &["name", "type", "doc", "default", "order", "aliases"], out);
    out.push('}');
}

/// Java's parser rejects a second definition of a named type ("Can't
/// redefine: X"), including one that a reference already defined.
fn check_redefinitions(value: &serde_json::Value, refs: &[ResolvedRef]) -> Result<(), String> {
    fn walk(v: &serde_json::Value, ns: &str, seen: &mut Vec<String>) -> Result<(), String> {
        match v {
            serde_json::Value::Array(a) => a.iter().try_for_each(|x| walk(x, ns, seen)),
            serde_json::Value::Object(o) => {
                let ty = o.get("type").and_then(|t| t.as_str()).unwrap_or("");
                let mut inner_ns = ns.to_string();
                if matches!(ty, "record" | "error" | "enum" | "fixed")
                    && let Some(name) = o.get("name").and_then(|n| n.as_str())
                {
                    let full = if name.contains('.') {
                        name.to_string()
                    } else {
                        let space = o.get("namespace").and_then(|n| n.as_str()).unwrap_or(ns);
                        if space.is_empty() { name.to_string() } else { format!("{space}.{name}") }
                    };
                    if seen.contains(&full) {
                        return Err(format!("Can't redefine: {full}"));
                    }
                    inner_ns = full.rsplit_once('.').map(|(n, _)| n.to_string()).unwrap_or_default();
                    seen.push(full);
                }
                if let Some(t) = o.get("type").filter(|t| !t.is_string()) {
                    walk(t, &inner_ns, seen)?;
                }
                for key in ["fields", "items", "values"] {
                    if let Some(x) = o.get(key) {
                        walk(x, &inner_ns, seen)?;
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    let mut seen = Vec::new();
    for r in refs {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&r.schema) {
            walk(&v, "", &mut seen)?;
        }
    }
    walk(value, "", &mut seen)
}

/// Java's `Schema.isValidDefault`, applied to every field default (Confluent
/// parses new schemas with default validation on).


fn collect_names(s: &Schema, out: &mut HashMap<String, Schema>) {
    match s {
        Schema::Record(r) => {
            out.entry(fullname(&r.name)).or_insert_with(|| s.clone());
            for f in &r.fields {
                collect_names(&f.schema, out);
            }
        }
        Schema::Enum(e) => {
            out.entry(fullname(&e.name)).or_insert_with(|| s.clone());
        }
        Schema::Fixed(f) | Schema::Duration(f) => {
            out.entry(fullname(&f.name)).or_insert_with(|| s.clone());
        }
        Schema::Decimal(d) => {
            if let InnerDecimalSchema::Fixed(f) = &d.inner {
                out.entry(fullname(&f.name)).or_insert_with(|| Schema::Fixed(f.clone()));
            }
        }
        Schema::Uuid(UuidSchema::Fixed(f)) => {
            out.entry(fullname(&f.name)).or_insert_with(|| Schema::Fixed(f.clone()));
        }
        Schema::Array(a) => collect_names(&a.items, out),
        Schema::Map(m) => collect_names(&m.types, out),
        Schema::Union(u) => u.variants().iter().for_each(|v| collect_names(v, out)),
        _ => {}
    }
}

fn fullname(n: &Name) -> String {
    n.fullname(None)
}

fn short_name(n: &Name) -> String {
    let full = fullname(n);
    full.rsplit('.').next().unwrap_or(&full).to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Prim {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
}

/// A schema with logical types stripped, the way Java's compatibility checker sees it.
enum Lowered<'a> {
    Prim(Prim),
    Array(&'a Schema),
    Map(&'a Schema),
    Union(&'a [Schema]),
    Record(&'a RecordSchema),
    Enum(&'a EnumSchema),
    Fixed(&'a FixedSchema),
    Unresolved(String),
}

impl Lowered<'_> {
    fn type_name(&self) -> &'static str {
        match self {
            Lowered::Prim(p) => match p {
                Prim::Null => "null",
                Prim::Boolean => "boolean",
                Prim::Int => "int",
                Prim::Long => "long",
                Prim::Float => "float",
                Prim::Double => "double",
                Prim::Bytes => "bytes",
                Prim::String => "string",
            },
            Lowered::Array(_) => "array",
            Lowered::Map(_) => "map",
            Lowered::Union(_) => "union",
            Lowered::Record(_) => "record",
            Lowered::Enum(_) => "enum",
            Lowered::Fixed(_) => "fixed",
            Lowered::Unresolved(_) => "unresolved reference",
        }
    }
}

fn lower<'a>(s: &'a Schema, names: &'a HashMap<String, Schema>) -> Lowered<'a> {
    match s {
        Schema::Null => Lowered::Prim(Prim::Null),
        Schema::Boolean => Lowered::Prim(Prim::Boolean),
        Schema::Int | Schema::Date | Schema::TimeMillis => Lowered::Prim(Prim::Int),
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => Lowered::Prim(Prim::Long),
        Schema::Float => Lowered::Prim(Prim::Float),
        Schema::Double => Lowered::Prim(Prim::Double),
        Schema::Bytes | Schema::BigDecimal => Lowered::Prim(Prim::Bytes),
        Schema::String => Lowered::Prim(Prim::String),
        Schema::Uuid(UuidSchema::String) => Lowered::Prim(Prim::String),
        Schema::Uuid(UuidSchema::Bytes) => Lowered::Prim(Prim::Bytes),
        Schema::Uuid(UuidSchema::Fixed(f)) | Schema::Duration(f) | Schema::Fixed(f) => Lowered::Fixed(f),
        Schema::Decimal(d) => match &d.inner {
            InnerDecimalSchema::Bytes => Lowered::Prim(Prim::Bytes),
            InnerDecimalSchema::Fixed(f) => Lowered::Fixed(f),
        },
        Schema::Array(a) => Lowered::Array(&a.items),
        Schema::Map(m) => Lowered::Map(&m.types),
        Schema::Union(u) => Lowered::Union(u.variants()),
        Schema::Record(r) => Lowered::Record(r),
        Schema::Enum(e) => Lowered::Enum(e),
        Schema::Ref { name } => match names.get(&fullname(name)) {
            Some(def) => lower(def, names),
            None => Lowered::Unresolved(fullname(name)),
        },
    }
}

/// Writer primitive can be promoted to reader primitive (Avro spec "schema resolution").
fn promotable(writer: Prim, reader: Prim) -> bool {
    use Prim::*;
    writer == reader
        || matches!(
            (writer, reader),
            (Int, Long | Float | Double) | (Long, Float | Double) | (Float, Double) | (String, Bytes) | (Bytes, String)
        )
}

/// Memoized result for a (reader, writer) pair of named types. Java's
/// checker memoizes by schema identity, and a named type referenced twice is
/// the same object, so its incompatibilities are reported again on reuse.
enum Memo {
    InProgress,
    Done(Vec<String>),
}

/// A port of Avro 1.11's `SchemaCompatibility.ReaderWriterCompatibilityChecker`
/// (the checker Confluent's `AvroSchema.isBackwardCompatible` uses), including
/// its quirks: union branches are checked with a fresh root location, and a
/// missing enum field with a default is compared against the whole writer.
struct Checker<'a> {
    rn: &'a HashMap<String, Schema>,
    wn: &'a HashMap<String, Schema>,
    labels: Labels,
    memo: HashMap<(String, String), Memo>,
}

fn msg(error_type: &str, description: String, info: &str) -> String {
    format!("{{errorType:'{error_type}', description:'{description}', additionalInfo:'{info}'}}")
}

fn path_of(loc: &[String]) -> String {
    format!("/{}", loc.join("/"))
}

fn named_key(l: &Lowered<'_>) -> Option<String> {
    match l {
        Lowered::Record(r) => Some(fullname(&r.name)),
        Lowered::Enum(e) => Some(fullname(&e.name)),
        Lowered::Fixed(f) => Some(fullname(&f.name)),
        _ => None,
    }
}

fn prim_of(l: &Lowered<'_>) -> Option<Prim> {
    match l {
        Lowered::Prim(p) => Some(*p),
        _ => None,
    }
}

impl Checker<'_> {
    fn names_match(&self, reader: &Name, reader_aliases: Option<&Vec<apache_avro::schema::Alias>>, writer: &Name) -> bool {
        short_name(reader) == short_name(writer)
            || reader_aliases.is_some_and(|a| {
                a.iter().any(|a| {
                    let ns = reader.namespace();
                    a.fullname(None) == fullname(writer) || a.fullname(ns.as_deref()) == fullname(writer)
                })
            })
    }

    /// `getCompatibility(token, reader, writer, location)`.
    fn get(&mut self, token: Option<&str>, reader: &Schema, writer: &Schema, loc: &mut Vec<String>) -> Vec<String> {
        if let Some(t) = token {
            loc.push(t.to_string());
        }
        let key = match (named_key(&lower(reader, self.rn)), named_key(&lower(writer, self.wn))) {
            (Some(r), Some(w)) => Some((r, w)),
            _ => None,
        };
        let result = match key.as_ref().and_then(|k| self.memo.get(k)) {
            Some(Memo::InProgress) => Vec::new(),
            Some(Memo::Done(m)) => m.clone(),
            None => {
                if let Some(k) = &key {
                    self.memo.insert(k.clone(), Memo::InProgress);
                }
                let r = self.calc(reader, writer, loc);
                if let Some(k) = key {
                    self.memo.insert(k, Memo::Done(r.clone()));
                }
                r
            }
        };
        if token.is_some() {
            loc.pop();
        }
        result
    }

    /// `getCompatibility(reader, writer)`: a fresh root location.
    fn fresh(&mut self, reader: &Schema, writer: &Schema) -> Vec<String> {
        self.get(None, reader, writer, &mut Vec::new())
    }

    fn type_mismatch(&self, r: &Lowered<'_>, w: &Lowered<'_>, loc: &[String]) -> String {
        let Labels { reader: rl, writer: wl } = self.labels;
        msg(
            "TYPE_MISMATCH",
            format!("The type (path '{}') of a field in the {rl} schema does not match with the {wl} schema", path_of(loc)),
            &format!("reader type: {} not compatible with writer type: {}", r.type_name().to_uppercase(), w.type_name().to_uppercase()),
        )
    }

    fn missing_union_branch(&self, writer_type: &str, loc: &[String]) -> String {
        let Labels { reader: rl, writer: wl } = self.labels;
        msg(
            "MISSING_UNION_BRANCH",
            format!("The {rl} schema is missing a type inside a union field at path '{}' in the {wl} schema", path_of(loc)),
            &format!("reader union lacking writer type: {}", writer_type.to_uppercase()),
        )
    }

    fn check_names(&self, reader: &Name, aliases: Option<&Vec<apache_avro::schema::Alias>>, writer: &Name, loc: &mut Vec<String>) -> Vec<String> {
        loc.push("name".into());
        let out = if self.names_match(reader, aliases, writer) {
            Vec::new()
        } else {
            vec![msg(
                "NAME_MISMATCH",
                format!("The name of the schema has changed (path '{}')", path_of(loc)),
                &format!("expected: {}", fullname(writer)),
            )]
        };
        loc.pop();
        out
    }

    fn calc(&mut self, reader: &Schema, writer: &Schema, loc: &mut Vec<String>) -> Vec<String> {
        let (r, w) = (lower(reader, self.rn), lower(writer, self.wn));
        let Labels { reader: rl, writer: wl } = self.labels;
        if let Lowered::Unresolved(n) = &r {
            return vec![msg("TYPE_MISMATCH", format!("Unresolved named type '{n}' at path '{}'", path_of(loc)), n)];
        }
        if let Lowered::Unresolved(n) = &w {
            return vec![msg("TYPE_MISMATCH", format!("Unresolved named type '{n}' at path '{}'", path_of(loc)), n)];
        }
        let same_type = match (&r, &w) {
            (Lowered::Prim(a), Lowered::Prim(b)) => a == b,
            (a, b) => std::mem::discriminant(a) == std::mem::discriminant(b),
        };
        if same_type {
            return match (&r, &w) {
                (Lowered::Prim(_), _) => Vec::new(),
                (Lowered::Array(ri), Lowered::Array(wi)) => self.get(Some("items"), ri, wi, loc),
                (Lowered::Map(rv), Lowered::Map(wv)) => self.get(Some("values"), rv, wv, loc),
                (Lowered::Fixed(rf), Lowered::Fixed(wf)) => {
                    let mut out = self.check_names(&rf.name, rf.aliases.as_ref(), &wf.name, loc);
                    loc.push("size".into());
                    if rf.size != wf.size {
                        out.push(msg(
                            "FIXED_SIZE_MISMATCH",
                            format!("The size of FIXED type field at path '{}' in the {rl} schema does not match with the {wl} schema", path_of(loc)),
                            &format!("expected: {}, found: {}", wf.size, rf.size),
                        ));
                    }
                    loc.pop();
                    out
                }
                (Lowered::Enum(re), Lowered::Enum(we)) => {
                    let mut out = self.check_names(&re.name, re.aliases.as_ref(), &we.name, loc);
                    loc.push("symbols".into());
                    // TreeSet(writer symbols) - reader symbols, in sorted order.
                    let mut missing: Vec<&String> = we.symbols.iter().filter(|s| !re.symbols.contains(s)).collect();
                    missing.sort();
                    missing.dedup();
                    let reader_default_ok = re.default.as_ref().is_some_and(|d| re.symbols.contains(d));
                    if !missing.is_empty() && !reader_default_ok {
                        let list = format!("[{}]", missing.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
                        out.push(msg(
                            "MISSING_ENUM_SYMBOLS",
                            format!("The {rl} schema is missing enum symbols '{list}' at path '{}' in the {wl} schema", path_of(loc)),
                            &list,
                        ));
                    }
                    loc.pop();
                    out
                }
                (Lowered::Record(rr), Lowered::Record(wr)) => {
                    let mut out = self.check_names(&rr.name, rr.aliases.as_ref(), &wr.name, loc);
                    loc.push("fields".into());
                    for (pos, rf) in rr.fields.iter().enumerate() {
                        loc.push(pos.to_string());
                        let wf = wr
                            .fields
                            .iter()
                            .find(|wf| wf.name == rf.name)
                            .or_else(|| wr.fields.iter().find(|wf| rf.aliases.contains(&wf.name)));
                        match wf {
                            Some(wf) => out.extend(self.get(Some("type"), &rf.schema, &wf.schema, loc)),
                            None if rf.default.is_none() => {
                                let enum_with_default = matches!(lower(&rf.schema, self.rn), Lowered::Enum(e) if e.default.is_some());
                                if enum_with_default {
                                    // Java compares the field's enum with the whole writer record here.
                                    out.extend(self.get(Some("type"), &rf.schema, writer, loc));
                                } else {
                                    out.push(msg(
                                        "READER_FIELD_MISSING_DEFAULT_VALUE",
                                        format!(
                                            "The field '{}' at path '{}' in the {rl} schema has no default value and is missing in the {wl} schema",
                                            rf.name,
                                            path_of(loc)
                                        ),
                                        &rf.name,
                                    ));
                                }
                            }
                            None => {}
                        }
                        loc.pop();
                    }
                    loc.pop();
                    out
                }
                (Lowered::Union(_), Lowered::Union(ws)) => {
                    let mut out = Vec::new();
                    for (i, wb) in ws.iter().enumerate() {
                        loc.push(i.to_string());
                        if !self.fresh(reader, wb).is_empty() {
                            out.push(self.missing_union_branch(lower(wb, self.wn).type_name(), loc));
                        }
                        loc.pop();
                    }
                    out
                }
                _ => Vec::new(),
            };
        }
        // Different types. A writer union: every branch must be readable
        // (each checked from a fresh root location, as in Java).
        if let Lowered::Union(ws) = &w {
            let mut out = Vec::new();
            for wb in *ws {
                out.extend(self.fresh(reader, wb));
            }
            return out;
        }
        match (&r, prim_of(&w)) {
            (Lowered::Prim(rp), Some(wp)) if promotable(wp, *rp) => Vec::new(),
            (Lowered::Union(rs), _) => {
                for rb in *rs {
                    if self.fresh(rb, writer).is_empty() {
                        return Vec::new();
                    }
                }
                vec![self.missing_union_branch(w.type_name(), loc)]
            }
            _ => vec![self.type_mismatch(&r, &w, loc)],
        }
    }
}

pub fn can_read(reader: &AvroSchema, writer: &AvroSchema, labels: Labels) -> Vec<String> {
    let mut c = Checker { rn: &reader.names, wn: &writer.names, labels, memo: HashMap::new() };
    c.fresh(&reader.schema, &writer.schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Labels = Labels { reader: "new", writer: "old" };

    fn p(s: &str) -> AvroSchema {
        AvroSchema::parse(s, &[], true).map_err(|e| e.message).unwrap()
    }

    #[test]
    fn adding_field_with_default_is_backward_compatible() {
        let old = p(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#);
        let new = p(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"x"}]}"#);
        assert!(can_read(&new, &old, L).is_empty());
        let bad = p(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#);
        let m = can_read(&bad, &old, L);
        assert!(m[0].contains("READER_FIELD_MISSING_DEFAULT_VALUE"), "{m:?}");
    }

    #[test]
    fn promotion_and_unions() {
        let int = p(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#);
        let long = p(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}"#);
        assert!(can_read(&long, &int, L).is_empty());
        assert!(!can_read(&int, &long, L).is_empty());
        let opt = p(r#"{"type":"record","name":"R","fields":[{"name":"a","type":["null","int"]}]}"#);
        assert!(can_read(&opt, &int, L).is_empty());
        assert!(!can_read(&int, &opt, L).is_empty());
    }

    #[test]
    fn recursive_and_reused_named_types() {
        let s = r#"{"type":"record","name":"Node","fields":[{"name":"next","type":["null","Node"]},{"name":"v","type":{"type":"fixed","name":"F","size":4}},{"name":"w","type":"F"}]}"#;
        assert!(can_read(&p(s), &p(s), L).is_empty());
    }

    #[test]
    fn references() {
        let addr = ResolvedRef {
            name: "Address".into(),
            schema: r#"{"type":"record","name":"Address","namespace":"a","fields":[{"name":"street","type":"string"}]}"#.into(),
        };
        let s = AvroSchema::parse(
            r#"{"type":"record","name":"Person","namespace":"a","fields":[{"name":"home","type":"Address"}]}"#,
            &[addr],
            true,
        )
        .map_err(|e| e.message)
        .unwrap();
        assert!(can_read(&s, &s, L).is_empty());
    }

    #[test]
    fn enums() {
        let a = p(r#"{"type":"enum","name":"E","symbols":["A","B"]}"#);
        let b = p(r#"{"type":"enum","name":"E","symbols":["A"]}"#);
        assert!(can_read(&a, &b, L).is_empty());
        assert!(!can_read(&b, &a, L).is_empty());
        let c = p(r#"{"type":"enum","name":"E","symbols":["A"],"default":"A"}"#);
        assert!(can_read(&c, &a, L).is_empty());
    }
}
