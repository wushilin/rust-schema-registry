//! Java Avro's schema parser checks, with its messages.
//!
//! A port of `org.apache.avro.Schema#parse(JsonNode, Names)` and the checks
//! in the schema constructors (Avro 1.11.4, as bundled with Confluent 7.9):
//! the same checks in the same order, so an invalid schema is rejected with
//! the message Confluent returns (`Record has no fields: {...}`,
//! `Undefined name: "x"`, `Duplicate field a in record X: ...`). It only
//! validates; the schema model itself comes from the `apache_avro` crate.

use std::collections::HashMap;

use serde_json::Value;

const PRIMITIVES: &[&str] = &["null", "boolean", "int", "long", "float", "double", "bytes", "string"];

/// The parsed shape, as far as the checks need it.
#[derive(Debug, Clone)]
enum J {
    Prim(&'static str),
    Record { full: String, fields: Vec<(String, J, Option<Value>)> },
    Enum { full: String, first: Option<String>, symbols: Vec<String> },
    Fixed { full: String, size: usize },
    Array(Box<J>),
    Map(Box<J>),
    Union(Vec<J>),
    /// A named type, by full name (records may refer to themselves).
    Named(String),
}

impl J {
    /// `Schema#getFullName`.
    fn full_name(&self) -> String {
        match self {
            J::Prim(p) => p.to_string(),
            J::Record { full, .. } | J::Enum { full, .. } | J::Fixed { full, .. } | J::Named(full) => full.clone(),
            J::Array(_) => "array".into(),
            J::Map(_) => "map".into(),
            J::Union(_) => "union".into(),
        }
    }
}

/// `Schema.Name`: validated simple name plus namespace.
struct Name {
    space: Option<String>,
    full: String,
}

fn is_letter(c: char) -> bool {
    c.is_alphabetic()
}

/// `Schema.validateName`.
fn validate_name(name: Option<&str>) -> Result<String, String> {
    let Some(name) = name else { return Err("Null name".into()) };
    let mut chars = name.chars();
    let Some(first) = chars.next() else { return Err("Empty name".into()) };
    if !is_letter(first) && first != '_' {
        return Err(format!("Illegal initial character: {name}"));
    }
    if chars.any(|c| !(c.is_alphanumeric() || c == '_')) {
        return Err(format!("Illegal character in: {name}"));
    }
    Ok(name.to_string())
}

impl Name {
    fn new(name: &str, space: Option<&str>) -> Result<Self, String> {
        let (space, simple) = match name.rfind('.') {
            Some(dot) => (Some(name[..dot].to_string()), validate_name(Some(&name[dot + 1..]))?),
            None => (space.map(String::from), validate_name(Some(name))?),
        };
        let space = space.filter(|s| !s.is_empty());
        let full = match &space {
            Some(s) => format!("{s}.{simple}"),
            None => simple,
        };
        Ok(Self { space, full })
    }
}

/// `Double.valueOf` for the inputs that matter here.
fn java_double(s: &str) -> Option<f64> {
    let t = s.trim();
    match t {
        "NaN" | "+NaN" | "-NaN" => return Some(f64::NAN),
        "Infinity" | "+Infinity" => return Some(f64::INFINITY),
        "-Infinity" => return Some(f64::NEG_INFINITY),
        _ => {}
    }
    let t = t.strip_suffix(['d', 'D', 'f', 'F']).unwrap_or(t);
    if t.chars().any(|c| c.is_ascii_alphabetic() && c != 'e' && c != 'E') {
        return None;
    }
    t.parse::<f64>().ok()
}

/// Jackson's `JsonNode#toString` (compact, keys in document order).
fn node(v: &Value) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

fn text<'a>(o: &'a Value, key: &str) -> Option<&'a str> {
    o.get(key).and_then(Value::as_str)
}

fn required<'a>(o: &'a Value, key: &str, error: &str) -> Result<&'a str, String> {
    text(o, key).ok_or_else(|| format!("{error}: {}", node(o)))
}

pub struct Parser {
    names: HashMap<String, J>,
    space: Option<String>,
    validate_defaults: bool,
    /// JSON pointer of the node being parsed.
    path: Vec<String>,
    /// Invalid defaults Java accepted (defaults not validated): pointer and a
    /// valid stand-in of the field's type.
    lenient: Vec<(String, Value)>,
    /// Every field default as Confluent's normalization renders it
    /// (`toJsonNode(field.defaultVal())`): converted, dropped (`None`), or the
    /// exception that conversion throws.
    normalized: Vec<(String, Result<Option<Value>, String>)>,
    numbers: Vec<(String, String)>,
}

/// The result of parsing one schema.
pub struct Parsed {
    pub lenient: Vec<(String, Value)>,
    pub normalized: Vec<(String, Result<Option<Value>, String>)>,
    /// Textual float/double defaults, as Java prints the converted number.
    pub numbers: Vec<(String, String)>,
}

/// Marks a default string that must be printed as a bare number.
pub const RAW_NUMBER: &str = "\u{0}raw-number:";

/// `Double.toString` / `Float.toString`.
fn java_fp(f: f64, is_float: bool) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    let digits = if is_float { format!("{:e}", f as f32) } else { format!("{f:e}") };
    let (mant, exp) = digits.split_once('e').unwrap_or((&digits, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let a = f.abs();
    if a == 0.0 || (1e-3..1e7).contains(&a) {
        let plain = if is_float { format!("{}", f as f32) } else { format!("{f}") };
        if plain.contains('.') { plain } else { format!("{plain}.0") }
    } else {
        let mant = if mant.contains('.') { mant.to_string() } else { format!("{mant}.0") };
        format!("{mant}E{exp}")
    }
}

/// Java doesn't validate namespaces; the `apache_avro` model does. Rewrite
/// the segments it would reject (consistently, so names still match).
pub fn model_safe_namespaces(v: &mut Value) {
    fn fix(ns: &str) -> String {
        ns.split('.')
            .map(|seg| {
                let ok = seg.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                    && seg.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
                if ok { seg.to_string() } else { format!("_{}", seg.chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '_' }).collect::<String>()) }
            })
            .collect::<Vec<_>>()
            .join(".")
    }
    match v {
        Value::Object(o) => {
            if let Some(Value::String(ns)) = o.get_mut("namespace") {
                *ns = fix(ns);
            }
            let named = matches!(o.get("type").and_then(Value::as_str), Some("record" | "error" | "enum" | "fixed"));
            if named && let Some(Value::String(n)) = o.get_mut("name") && let Some(dot) = n.rfind('.') {
                *n = format!("{}.{}", fix(&n[..dot]), &n[dot + 1..]);
            }
            for (_, x) in o.iter_mut() {
                model_safe_namespaces(x);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(model_safe_namespaces),
        Value::String(s) if s.contains('.') => {
            if let Some(dot) = s.rfind('.') {
                *s = format!("{}.{}", fix(&s[..dot]), &s[dot + 1..]);
            }
        }
        _ => {}
    }
}

/// A Java datum from `JacksonUtils.toObject`; `None` stands for Java `null`.
#[derive(Debug, Clone)]
enum D {
    NullValue,
    Bool(bool),
    Int(i64),
    Long(i64),
    Float(f32),
    Double(f64),
    Str(String),
    Bytes(String),
    List(Vec<Option<D>>),
    Map(Vec<(String, Option<D>)>),
}

const NPE_DATUM: &str = "Cannot invoke \"Object.getClass()\" because \"datum\" is null";

/// `AvroSchemaUtils.genJson` into a JSON value.
fn gen_json(d: &Option<D>) -> Result<Value, String> {
    let Some(d) = d else { return Err(NPE_DATUM.into()) };
    Ok(match d {
        D::NullValue => Value::Null,
        D::Bool(b) => Value::Bool(*b),
        D::Int(i) | D::Long(i) => Value::from(*i),
        D::Float(f) => format!("{f}").parse::<f64>().ok().and_then(serde_json::Number::from_f64).map(Value::Number).unwrap_or(Value::Null),
        D::Double(f) => serde_json::Number::from_f64(*f).map(Value::Number).unwrap_or(Value::Null),
        D::Str(s) | D::Bytes(s) => Value::String(s.clone()),
        D::List(l) => Value::Array(l.iter().map(gen_json).collect::<Result<_, _>>()?),
        D::Map(m) => Value::Object(m.iter().map(|(k, v)| Ok((k.clone(), gen_json(v)?))).collect::<Result<_, String>>()?),
    })
}

impl Parser {
    pub fn new(validate_defaults: bool) -> Self {
        Self {
            names: HashMap::new(),
            space: None,
            validate_defaults,
            path: Vec::new(),
            lenient: Vec::new(),
            normalized: Vec::new(),
            numbers: Vec::new(),
        }
    }

    /// `Parser#parse` for one (reference or root) schema; names accumulate.
    pub fn parse(&mut self, v: &Value) -> Result<Parsed, String> {
        self.space = None;
        self.path.clear();
        self.lenient.clear();
        self.normalized.clear();
        self.numbers.clear();
        self.schema(v)?;
        Ok(Parsed {
            lenient: std::mem::take(&mut self.lenient),
            normalized: std::mem::take(&mut self.normalized),
            numbers: std::mem::take(&mut self.numbers),
        })
    }

    /// `JacksonUtils.toObject(node, schema)`.
    fn to_object(&self, node: &Value, j: Option<&J>) -> Result<Option<D>, String> {
        let j = j.map(|j| self.resolve(j));
        if let Some(J::Union(types)) = j {
            return self.to_object(node, types.first());
        }
        let prim = |p: &str| matches!(j, Some(J::Prim(x)) if *x == p);
        Ok(match node {
            Value::Null => Some(D::NullValue),
            Value::Bool(b) => Some(D::Bool(*b)),
            Value::Number(n) if n.is_f64() => {
                let f = n.as_f64().unwrap_or(0.0);
                if j.is_none() || prim("double") {
                    Some(D::Double(f))
                } else if prim("float") {
                    Some(D::Float(f as f32))
                } else {
                    None
                }
            }
            Value::Number(n) => {
                let Some(i) = n.as_i64() else { return Ok(None) }; // BigInteger
                let is_int = i32::try_from(i).is_ok();
                if prim("float") {
                    Some(D::Float(i as f32))
                } else if prim("double") {
                    Some(D::Double(i as f64))
                } else if is_int && (j.is_none() || prim("int")) {
                    Some(D::Int(i))
                } else if prim("long") || (!is_int && (j.is_none() || prim("int"))) {
                    Some(D::Long(i))
                } else {
                    None
                }
            }
            Value::String(t) => match j {
                None | Some(J::Prim("string")) | Some(J::Enum { .. }) => Some(D::Str(t.clone())),
                Some(J::Prim("bytes")) | Some(J::Fixed { .. }) => {
                    Some(D::Bytes(t.chars().map(|c| if (c as u32) < 256 { c } else { '?' }).collect()))
                }
                _ => None,
            },
            Value::Array(items) => {
                let elem = match j {
                    None => None,
                    Some(J::Array(e)) => Some(&**e),
                    Some(other) => return Err(format!("Not an array: {}", self.type_json(other))),
                };
                Some(D::List(items.iter().map(|x| self.to_object(x, elem)).collect::<Result<_, _>>()?))
            }
            Value::Object(o) => {
                let mut m = Vec::with_capacity(o.len());
                for (k, x) in o {
                    let s = match j {
                        Some(J::Map(v)) => Some(&**v),
                        Some(J::Record { fields, .. }) => match fields.iter().find(|(n, ..)| n == k) {
                            Some((_, t, _)) => Some(t),
                            None => {
                                return Err("Cannot invoke \"org.apache.avro.Schema$Field.schema()\" because the return value of \"org.apache.avro.Schema.getField(String)\" is null".into());
                            }
                        },
                        _ => None,
                    };
                    m.push((k.clone(), self.to_object(x, s)?));
                }
                Some(D::Map(m))
            }
        })
    }

    /// A short type description for messages.
    fn type_json(&self, j: &J) -> String {
        match self.resolve(j) {
            J::Prim(p) => format!("\"{p}\""),
            other => format!("\"{}\"", other.full_name()),
        }
    }

    fn at<T>(&mut self, seg: impl ToString, f: impl FnOnce(&mut Self) -> Result<T, String>) -> Result<T, String> {
        self.path.push(seg.to_string());
        let r = f(self);
        self.path.pop();
        r
    }

    fn pointer(&self) -> String {
        self.path.iter().map(|s| format!("/{s}")).collect()
    }

    /// Some value of this type (for tolerated invalid defaults).
    fn stand_in(&self, j: &J) -> Value {
        match self.resolve(j) {
            J::Prim("null") => Value::Null,
            J::Prim("boolean") => Value::Bool(false),
            J::Prim("int") | J::Prim("long") => Value::from(0),
            J::Prim("float") | J::Prim("double") => Value::from(0.0),
            J::Prim(_) => Value::from(""),
            J::Enum { first, .. } => Value::from(first.clone().unwrap_or_default()),
            J::Fixed { size, .. } => Value::from("\u{0}".repeat(*size)),
            J::Array(_) => Value::Array(Vec::new()),
            J::Map(_) => Value::Object(Default::default()),
            J::Union(types) => types.first().map(|t| self.stand_in(t)).unwrap_or(Value::Null),
            J::Record { fields, .. } => {
                Value::Object(fields.iter().map(|(n, t, _)| (n.clone(), self.stand_in(t))).collect())
            }
            J::Named(_) => Value::Null,
        }
    }

    /// `Names#get`.
    fn get(&self, o: &str) -> Result<Option<J>, String> {
        if let Some(p) = PRIMITIVES.iter().find(|p| **p == o) {
            return Ok(Some(J::Prim(p)));
        }
        let n = Name::new(o, self.space.as_deref())?;
        if self.names.contains_key(&n.full) {
            return Ok(Some(J::Named(n.full)));
        }
        let n = Name::new(o, None)?;
        Ok(self.names.contains_key(&n.full).then_some(J::Named(n.full)))
    }

    /// `Names#add`.
    fn add(&mut self, full: &str, j: J) -> Result<(), String> {
        if self.names.contains_key(full) {
            return Err(format!("Can't redefine: {full}"));
        }
        self.names.insert(full.to_string(), j);
        Ok(())
    }

    fn resolve<'a>(&'a self, j: &'a J) -> &'a J {
        match j {
            J::Named(n) => self.names.get(n).map(|x| self.resolve(x)).unwrap_or(j),
            other => other,
        }
    }

    /// `Schema.parseAliases`.
    fn aliases(v: &Value) -> Result<(), String> {
        match v.get("aliases") {
            None => Ok(()),
            Some(Value::Array(a)) => match a.iter().find(|x| !x.is_string()) {
                Some(bad) => Err(format!("alias not a string: {}", node(bad))),
                None => Ok(()),
            },
            Some(_) => Err(format!("aliases not an array: {}", node(v))),
        }
    }

    fn named_schema_check(full: &str) -> Result<(), String> {
        if PRIMITIVES.contains(&full) {
            return Err(format!("Schemas may not be named after primitives: {full}"));
        }
        Ok(())
    }

    fn schema(&mut self, v: &Value) -> Result<J, String> {
        match v {
            Value::String(s) => self.get(s)?.ok_or_else(|| format!("Undefined name: {}", node(v))),
            Value::Object(_) => self.object(v),
            Value::Array(items) => {
                let mut types = Vec::with_capacity(items.len());
                for (i, t) in items.iter().enumerate() {
                    types.push(self.at(i, |p| p.schema(t))?);
                }
                let mut seen: Vec<String> = Vec::new();
                for t in &types {
                    if matches!(self.resolve(t), J::Union(_)) {
                        return Err(format!("Nested union: {}", super::avro::java_string_of(v)));
                    }
                    let name = t.full_name();
                    if seen.contains(&name) {
                        return Err(format!("Duplicate in union:{name}"));
                    }
                    seen.push(name);
                }
                Ok(J::Union(types))
            }
            _ => Err(format!("Schema not yet supported: {}", node(v))),
        }
    }

    fn object(&mut self, v: &Value) -> Result<J, String> {
        let ty = required(v, "type", "No type")?.to_string();
        let saved = self.space.clone();
        let named = matches!(ty.as_str(), "record" | "error" | "enum" | "fixed");
        let mut name = None;
        if named {
            let space = text(v, "namespace").map(String::from).or_else(|| saved.clone());
            let n = Name::new(required(v, "name", "No name in schema")?, space.as_deref())?;
            self.space = n.space.clone();
            name = Some(n);
        }
        let result = if let Some(p) = PRIMITIVES.iter().find(|p| **p == ty) {
            J::Prim(p)
        } else if ty == "record" || ty == "error" {
            let full = name.as_ref().expect("named").full.clone();
            Self::named_schema_check(&full)?;
            self.add(&full, J::Record { full: full.clone(), fields: Vec::new() })?;
            let Some(Value::Array(fields_node)) = v.get("fields") else {
                return Err(format!("Record has no fields: {}", node(v)));
            };
            let mut fields: Vec<(String, J, Option<Value>)> = Vec::new();
            for (fi, field) in fields_node.iter().enumerate() {
                let fname = required(field, "name", "No field name")?.to_string();
                let Some(ftype) = field.get("type") else { return Err(format!("No field type: {}", node(field))) };
                if let Value::String(t) = ftype
                    && self.get(t)?.is_none()
                {
                    return Err(format!(
                        "{} is not a defined name. The type of the \"{fname}\" field must be a defined name or a {{\"type\": ...}} expression.",
                        node(ftype)
                    ));
                }
                let fschema = self.at("fields", |p| p.at(fi, |p| p.at("type", |p| p.schema(ftype))))?;
                if let Some(order) = field.get("order") {
                    let o = order.as_str().unwrap_or("").to_ascii_uppercase();
                    if !["ASCENDING", "DESCENDING", "IGNORE"].contains(&o.as_str()) {
                        return Err(format!("No enum constant org.apache.avro.Schema.Field.Order.{o}"));
                    }
                }
                // A textual float/double default is converted with Double.valueOf first.
                let mut default = field.get("default").cloned();
                let mut textual_number = false;
                if let Some(Value::String(t)) = &default
                    && matches!(self.resolve(&fschema), J::Prim("float") | J::Prim("double"))
                {
                    let d = java_double(t).ok_or_else(|| format!("For input string: \"{t}\""))?;
                    let ptr = format!("{}/fields/{fi}/default", self.pointer());
                    self.lenient.push((ptr.clone(), serde_json::Number::from_f64(d).map(Value::Number).unwrap_or(Value::from(0.0))));
                    self.numbers.push((ptr.clone(), java_fp(d, false)));
                    let is_float = matches!(self.resolve(&fschema), J::Prim("float"));
                    self.normalized.push((ptr, Ok(Some(Value::String(format!("{RAW_NUMBER}{}", java_fp(d, is_float)))))));
                    textual_number = true;
                    default = Some(Value::from(if d.is_finite() { d } else { 0.0 }));
                }
                // Field constructor: the name, then (optionally) the default.
                validate_name(Some(&fname))?;
                if let Some(d) = &default
                    && (!self.valid_default(&fschema, Some(d)) || !self.model_accepts(&fschema, d))
                {
                    if self.validate_defaults && !self.valid_default(&fschema, Some(d)) {
                        return Err(format!(
                            "Invalid default for field {fname}: {} not a {}",
                            node(d),
                            super::avro::java_string_of(ftype)
                        ));
                    }
                    let ptr = format!("{}/fields/{fi}/default", self.pointer());
                    let stand_in = self.stand_in(&fschema);
                    self.lenient.push((ptr, stand_in));
                }
                if let Some(d) = default.as_ref().filter(|_| !textual_number) {
                    let ptr = format!("{}/fields/{fi}/default", self.pointer());
                    let out = self.to_object(d, Some(&fschema)).and_then(|o| o.map(|o| gen_json(&Some(o))).transpose());
                    self.normalized.push((ptr, out));
                }
                Self::aliases(field)?;
                fields.push((fname, fschema, default));
            }
            // RecordSchema#setFields
            for (i, (n, s, _)) in fields.iter().enumerate() {
                if let Some(j) = fields[..i].iter().position(|(m, ..)| m == n) {
                    return Err(format!(
                        "Duplicate field {n} in record {full}: {n} type:{} pos:{i} and {n} type:{} pos:{j}.",
                        self.type_name(s),
                        self.type_name(&fields[j].1)
                    ));
                }
            }
            let rec = J::Record { full: full.clone(), fields };
            self.names.insert(full.clone(), rec.clone());
            rec
        } else if ty == "enum" {
            let full = name.as_ref().expect("named").full.clone();
            let Some(Value::Array(symbols)) = v.get("symbols") else {
                return Err(format!("Enum has no symbols: {}", node(v)));
            };
            Self::named_schema_check(&full)?;
            let mut seen: Vec<&str> = Vec::new();
            for s in symbols {
                let sym = validate_name(s.as_str())?;
                if seen.contains(&sym.as_str()) {
                    return Err(format!("Duplicate enum symbol: {sym}"));
                }
                seen.push(s.as_str().unwrap_or(""));
            }
            if let Some(d) = v.get("default").and_then(Value::as_str)
                && !seen.contains(&d)
            {
                let list: Vec<&str> = symbols.iter().filter_map(Value::as_str).collect();
                return Err(format!("The Enum Default: {d} is not in the enum symbol set: [{}]", list.join(", ")));
            }
            let e = J::Enum {
                full: full.clone(),
                first: symbols.first().and_then(Value::as_str).map(String::from),
                symbols: symbols.iter().filter_map(Value::as_str).map(String::from).collect(),
            };
            self.add(&full, e.clone())?;
            e
        } else if ty == "array" {
            let Some(items) = v.get("items") else { return Err(format!("Array has no items type: {}", node(v))) };
            J::Array(Box::new(self.at("items", |p| p.schema(items))?))
        } else if ty == "map" {
            let Some(values) = v.get("values") else { return Err(format!("Map has no values type: {}", node(v))) };
            J::Map(Box::new(self.at("values", |p| p.schema(values))?))
        } else if ty == "fixed" {
            let full = name.as_ref().expect("named").full.clone();
            let size = v.get("size").and_then(Value::as_i64).filter(|s| i32::try_from(*s).is_ok());
            if size.is_none() || v.get("size").is_some_and(|s| !s.is_i64()) {
                return Err(format!("Invalid or no size: {}", node(v)));
            }
            if let Some(n) = size.filter(|n| *n < 0) {
                return Err(format!("Malformed data. Length is negative: {n}"));
            }
            Self::named_schema_check(&full)?;
            let f = J::Fixed { full: full.clone(), size: size.unwrap_or(0) as usize };
            self.add(&full, f.clone())?;
            f
        } else {
            let n = Name::new(&ty, self.space.as_deref())?;
            match self.names.get(&n.full) {
                Some(_) => {
                    self.space = saved;
                    return Ok(J::Named(n.full));
                }
                None => return Err(format!("Type not supported: {ty}")),
            }
        };
        self.space = saved;
        if named {
            Self::aliases(v)?;
        }
        Ok(result)
    }

    /// `Schema.Type` name, as `Field#toString` prints it.
    fn type_name(&self, j: &J) -> &'static str {
        match self.resolve(j) {
            J::Prim(p) => match *p {
                "null" => "NULL",
                "boolean" => "BOOLEAN",
                "int" => "INT",
                "long" => "LONG",
                "float" => "FLOAT",
                "double" => "DOUBLE",
                "bytes" => "BYTES",
                _ => "STRING",
            },
            J::Record { .. } => "RECORD",
            J::Enum { .. } => "ENUM",
            J::Fixed { .. } => "FIXED",
            J::Array(_) => "ARRAY",
            J::Map(_) => "MAP",
            J::Union(_) => "UNION",
            J::Named(_) => "RECORD",
        }
    }

    /// What the `apache_avro` model additionally insists on (enum symbols,
    /// fixed sizes), so tolerated defaults can be swapped for a stand-in.
    fn model_accepts(&self, j: &J, d: &Value) -> bool {
        match self.resolve(j) {
            J::Enum { symbols, .. } => d.as_str().is_some_and(|s| symbols.iter().any(|x| x == s)),
            J::Fixed { size, .. } => d.as_str().is_some_and(|s| s.chars().count() == *size),
            J::Union(types) => types.first().is_none_or(|t| self.model_accepts(t, d)),
            J::Array(items) => d.as_array().is_none_or(|a| a.iter().all(|e| self.model_accepts(items, e))),
            J::Map(values) => d.as_object().is_none_or(|o| o.values().all(|e| self.model_accepts(values, e))),
            J::Record { fields, .. } => {
                d.as_object().is_none_or(|o| fields.iter().all(|(n, t, _)| o.get(n).is_none_or(|e| self.model_accepts(t, e))))
            }
            _ => true,
        }
    }

    /// `Schema.isValidDefault`.
    fn valid_default(&self, j: &J, d: Option<&Value>) -> bool {
        let Some(d) = d else { return false };
        match self.resolve(j) {
            J::Prim("string") | J::Prim("bytes") | J::Enum { .. } | J::Fixed { .. } => d.is_string(),
            J::Prim("int") => d.as_i64().is_some_and(|i| i32::try_from(i).is_ok()) && !d.is_f64(),
            J::Prim("long") => (d.is_i64() || d.as_u64().is_some_and(|u| i64::try_from(u).is_ok())) && !d.is_f64(),
            J::Prim("float") | J::Prim("double") => d.is_number(),
            J::Prim("boolean") => d.is_boolean(),
            J::Prim("null") => d.is_null(),
            J::Array(items) => d.as_array().is_some_and(|a| a.iter().all(|e| self.valid_default(items, Some(e)))),
            J::Map(values) => d.as_object().is_some_and(|o| o.values().all(|e| self.valid_default(values, Some(e)))),
            J::Union(types) => types.first().is_some_and(|t| self.valid_default(t, Some(d))),
            J::Record { fields, .. } => {
                d.is_object() && fields.iter().all(|(n, s, fd)| self.valid_default(s, d.get(n).or(fd.as_ref())))
            }
            _ => false,
        }
    }
}
