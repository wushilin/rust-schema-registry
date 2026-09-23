//! JSON Schema: structural compatibility in the spirit of Confluent's
//! `JsonSchemaDiff`.
//!
//! "reader can read writer" means every document valid under the writer schema
//! is also valid under the reader schema. We check the constructs that matter
//! for evolution: types, enums/consts, numeric/string/array bounds, object
//! properties under open/closed/partially-open content models, `required`,
//! `items`, and `oneOf`/`anyOf`/`allOf`. `$ref` is resolved within the
//! document and against schema references (by reference name or `$id`).

use std::collections::HashMap;

use serde_json::{Map, Value};

use super::{Labels, ResolvedRef};

pub struct JsonSchema {
    root: Value,
    refs: HashMap<String, Value>,
    pub canonical: String,
    pub normalized: String,
}

const TYPES: &[&str] = &["null", "boolean", "object", "array", "number", "string", "integer"];

impl JsonSchema {
    pub fn parse(text: &str, refs: &[ResolvedRef]) -> Result<Self, String> {
        let root: Value = serde_json::from_str(text).map_err(|e| format!("Invalid JSON: {e}"))?;
        let mut map = HashMap::new();
        for r in refs {
            let v: Value = serde_json::from_str(&r.schema).map_err(|e| format!("Invalid reference {}: {e}", r.name))?;
            if let Some(id) = v.get("$id").and_then(Value::as_str) {
                map.insert(id.to_string(), v.clone());
            }
            map.insert(r.name.clone(), v);
        }
        validate(&root, &map)?;
        let canonical = serde_json::to_string(&root).map_err(|e| e.to_string())?;
        let normalized = serde_json::to_string(&sorted(&root)).map_err(|e| e.to_string())?;
        Ok(Self { root, refs: map, canonical, normalized })
    }
}

fn validate(root: &Value, refs: &HashMap<String, Value>) -> Result<(), String> {
    match root {
        Value::Null => Err("Invalid JSON Schema".into()),
        Value::Object(_) => {
            let draft4 = root.get("$schema").and_then(Value::as_str).is_some_and(|s| s.contains("draft-04"));
            let mut v = Validator { root, refs, draft4, visited: Vec::new() };
            v.schema(root, root, "#")
        }
        _ => Ok(()),
    }
}

struct Validator<'a> {
    root: &'a Value,
    refs: &'a HashMap<String, Value>,
    draft4: bool,
    visited: Vec<*const Value>,
}

/// Java regexes that the Rust engine can't compile (look-around,
/// backreferences) are still valid for Confluent.
fn regex_ok(p: &str) -> bool {
    match regex::Regex::new(p) {
        Ok(_) => true,
        Err(e) => {
            let m = e.to_string();
            m.contains("look-around") || m.contains("backreferences")
        }
    }
}

/// Find a subschema whose `$id` (or draft-4 `id`) equals `anchor`.
fn find_anchor<'v>(v: &'v Value, anchor: &str) -> Option<&'v Value> {
    match v {
        Value::Object(o) => {
            if o.get("$id").or_else(|| o.get("id")).and_then(Value::as_str).is_some_and(|id| id == anchor || id.ends_with(anchor)) {
                return Some(v);
            }
            o.values().find_map(|x| find_anchor(x, anchor))
        }
        Value::Array(a) => a.iter().find_map(|x| find_anchor(x, anchor)),
        _ => None,
    }
}

/// Resolve a `$ref` against the current document or a supplied reference.
fn resolve_in<'v>(r: &str, doc: &'v Value, refs: &'v HashMap<String, Value>) -> Option<(&'v Value, &'v Value)> {
    let (base, frag) = match r.find('#') {
        Some(i) => (&r[..i], &r[i + 1..]),
        None => (r, ""),
    };
    let target_doc: &Value = if base.is_empty() {
        doc
    } else {
        refs.get(base).or_else(|| refs.iter().find(|(k, _)| base.ends_with(k.as_str())).map(|(_, v)| v))?
    };
    let frag = frag.replace("%25", "%").replace("%22", "\"");
    let target = if frag.is_empty() || frag == "/" {
        target_doc
    } else if frag.starts_with('/') {
        target_doc.pointer(&frag)?
    } else {
        find_anchor(target_doc, &format!("#{frag}"))?
    };
    Some((target_doc, target))
}

impl<'a> Validator<'a> {
    fn fail(&self, path: &str, why: &str) -> Result<(), String> {
        Err(format!("Invalid JSON Schema: {why} at {path}"))
    }

    fn sub(&mut self, v: &'a Value, doc: &'a Value, path: &str) -> Result<(), String> {
        match v {
            Value::Bool(_) => Ok(()),
            Value::Object(_) => self.schema(v, doc, path),
            _ => self.fail(path, "subschema must be an object or boolean"),
        }
    }

    fn schema(&mut self, v: &'a Value, doc: &'a Value, path: &str) -> Result<(), String> {
        let Value::Object(o) = v else { return Ok(()) };
        if self.visited.contains(&(v as *const Value)) {
            return Ok(());
        }
        self.visited.push(v as *const Value);
        if let Some(r) = o.get("$ref") {
            let Some(r) = r.as_str() else { return self.fail(path, "$ref must be a string") };
            let refs = self.refs;
            let Some((tdoc, target)) = resolve_in(r, doc, refs) else {
                return self.fail(path, &format!("cannot resolve $ref '{r}'"));
            };
            return self.sub(target, tdoc, path);
        }
        let _ = self.root;
        if let Some(t) = o.get("type") {
            let ok = match t {
                Value::String(s) => TYPES.contains(&s.as_str()),
                Value::Array(a) => a.iter().all(|x| x.as_str().is_some_and(|s| TYPES.contains(&s))),
                _ => false,
            };
            if !ok {
                return self.fail(path, &format!("invalid type {t}"));
            }
        }
        for k in ["minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties"] {
            if let Some(x) = o.get(k)
                && !(x.is_i64() || x.is_u64())
            {
                return self.fail(path, &format!("'{k}' must be an integer"));
            }
        }
        for k in ["minimum", "maximum"] {
            if o.get(k).is_some_and(|x| !x.is_number()) {
                return self.fail(path, &format!("'{k}' must be a number"));
            }
        }
        if let Some(m) = o.get("multipleOf") {
            if !m.is_number() || m.as_f64() == Some(0.0) {
                return self.fail(path, "multipleOf must be a non-zero number");
            }
        }
        for k in ["exclusiveMinimum", "exclusiveMaximum"] {
            if let Some(x) = o.get(k) {
                let ok = if self.draft4 { x.is_boolean() } else { x.is_number() };
                if !ok {
                    return self.fail(path, &format!("invalid '{k}'"));
                }
            }
        }
        if let Some(p) = o.get("pattern") {
            match p.as_str() {
                Some(p) if regex_ok(p) => {}
                _ => return self.fail(path, "invalid pattern"),
            }
        }
        if o.get("enum").is_some_and(|e| !e.is_array()) {
            return self.fail(path, "enum must be an array");
        }
        if let Some(r) = o.get("required")
            && !r.as_array().is_some_and(|a| a.iter().all(Value::is_string))
        {
            return self.fail(path, "required must be an array of strings");
        }
        if o.get("uniqueItems").is_some_and(|u| !u.is_boolean()) {
            return self.fail(path, "uniqueItems must be a boolean");
        }
        for key in ["properties", "patternProperties"] {
            if let Some(p) = o.get(key) {
                let Some(p) = p.as_object() else { return self.fail(path, &format!("'{key}' must be an object")) };
                for (k, s) in p {
                    if key == "patternProperties" && !regex_ok(k) {
                        return self.fail(path, "invalid patternProperties regex");
                    }
                    self.sub(s, doc, &format!("{path}/{key}/{k}"))?;
                }
            }
        }
        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(p) = o.get(key) {
                let Some(a) = p.as_array() else { return self.fail(path, &format!("'{key}' must be an array")) };
                for (i, s) in a.iter().enumerate() {
                    self.sub(s, doc, &format!("{path}/{key}/{i}"))?;
                }
            }
        }
        match o.get("items") {
            Some(Value::Array(a)) => {
                for (i, s) in a.iter().enumerate() {
                    self.sub(s, doc, &format!("{path}/items/{i}"))?;
                }
            }
            Some(s) => self.sub(s, doc, &format!("{path}/items"))?,
            None => {}
        }
        for key in ["additionalProperties", "additionalItems", "not", "contains", "propertyNames", "if", "then", "else"] {
            if let Some(s) = o.get(key) {
                self.sub(s, doc, &format!("{path}/{key}"))?;
            }
        }
        if let Some(d) = o.get("dependencies") {
            let Some(d) = d.as_object() else { return self.fail(path, "dependencies must be an object") };
            for (k, dep) in d {
                match dep {
                    Value::Array(a) if a.iter().all(Value::is_string) => {}
                    Value::Array(_) => return self.fail(path, "dependency arrays must contain strings"),
                    s => self.sub(s, doc, &format!("{path}/dependencies/{k}"))?,
                }
            }
        }
        Ok(())
    }
}

fn sorted(v: &Value) -> Value {
    match v {
        Value::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let mut m = Map::new();
            for k in keys {
                m.insert(k.clone(), sorted(&o[k]));
            }
            Value::Object(m)
        }
        Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// Compatibility: a port of Confluent's `json.diff` package, which runs on
// everit-json-schema's object model. We first load the JSON into the same
// model (see `Loader`: extractor order, keyword consumption and synthetic
// `allOf` wrapping as in everit's `SchemaLoader`), then diff it exactly like
// `SchemaDiff` does: original = writer, update = reader.
// ---------------------------------------------------------------------------

use super::java_order::{map_order, union_order, JavaSet};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Criterion {
    All,
    Any,
    One,
}

impl Criterion {
    fn name(self) -> &'static str {
        match self {
            Criterion::All => "allOf",
            Criterion::Any => "anyOf",
            Criterion::One => "oneOf",
        }
    }
}

#[derive(Default)]
struct StrS {
    min_length: Option<i64>,
    max_length: Option<i64>,
    pattern: Option<String>,
}

#[derive(Default)]
struct NumS {
    minimum: Option<serde_json::Number>,
    maximum: Option<serde_json::Number>,
    exclusive_minimum: Option<serde_json::Number>,
    exclusive_maximum: Option<serde_json::Number>,
    multiple_of: Option<serde_json::Number>,
    requires_integer: bool,
}

struct ArrS {
    all_items: Option<usize>,
    item_schemas: Option<Vec<usize>>,
    permits_additional_items: bool,
    schema_of_additional_items: Option<usize>,
    min_items: Option<i64>,
    max_items: Option<i64>,
    unique_items: bool,
}

struct ObjS {
    /// In Java HashMap iteration order.
    properties: Vec<(String, usize)>,
    required: Vec<String>,
    permits_additional: bool,
    schema_of_additional: Option<usize>,
    pattern_properties: Vec<(String, usize)>,
    property_dependencies: Vec<(String, Vec<String>)>,
    schema_dependencies: Vec<(String, usize)>,
    min_properties: Option<i64>,
    max_properties: Option<i64>,
}

enum Kind {
    True,
    False,
    Empty,
    Boolean,
    Null,
    Str(StrS),
    Num(NumS),
    Arr(ArrS),
    Obj(ObjS),
    Enum(Vec<Value>),
    Const(Value),
    Not(usize),
    Combined { crit: Criterion, subs: Vec<usize>, raw: Vec<Value> },
    Ref(usize),
    Conditional,
}

impl Kind {
    /// everit class identity, for `schemaTypesEqual`.
    fn class(&self) -> u8 {
        match self {
            Kind::True => 0,
            Kind::False => 1,
            Kind::Empty => 2,
            Kind::Boolean => 3,
            Kind::Null => 4,
            Kind::Str(_) => 5,
            Kind::Num(_) => 6,
            Kind::Arr(_) => 7,
            Kind::Obj(_) => 8,
            Kind::Enum(_) => 9,
            Kind::Const(_) => 10,
            Kind::Not(_) => 11,
            Kind::Combined { .. } => 12,
            Kind::Ref(_) => 13,
            Kind::Conditional => 14,
        }
    }
}

struct SNode {
    kind: Kind,
    id: Option<String>,
    title: Option<String>,
    description: Option<String>,
    default: Option<Value>,
}

impl SNode {
    fn bare(kind: Kind) -> Self {
        SNode { kind, id: None, title: None, description: None, default: None }
    }
}

const ARRAY_KEYWORDS: &[&str] = &["items", "additionalItems", "minItems", "maxItems", "uniqueItems", "contains"];
const OBJECT_KEYWORDS: &[&str] =
    &["properties", "required", "minProperties", "maxProperties", "dependencies", "patternProperties", "additionalProperties", "propertyNames"];
const NUMBER_KEYWORDS: &[&str] = &["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf"];
const STRING_KEYWORDS: &[&str] = &["minLength", "maxLength", "pattern", "format"];

/// Builds the everit-like model for one schema document plus its references.
struct Loader<'a> {
    nodes: Vec<SNode>,
    refs: &'a HashMap<String, Value>,
    /// (document key, JSON pointer) -> node, so `$ref` cycles share one node.
    ref_cache: HashMap<(String, String), usize>,
    draft4: bool,
}

fn int(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_f64().map(|f| f as i64))
}

impl<'a> Loader<'a> {
    fn push(&mut self, n: SNode) -> usize {
        self.nodes.push(n);
        self.nodes.len() - 1
    }

    fn load(&mut self, v: &Value, doc: &Value, doc_key: &str) -> usize {
        match v {
            Value::Bool(true) => self.push(SNode::bare(Kind::True)),
            Value::Bool(false) => self.push(SNode::bare(Kind::False)),
            Value::Object(o) => self.load_object(o, doc, doc_key),
            _ => self.push(SNode::bare(Kind::Empty)),
        }
    }

    fn resolve_ref(&mut self, r: &str, doc: &Value, doc_key: &str) -> usize {
        let (base, frag) = match r.find('#') {
            Some(i) => (&r[..i], &r[i + 1..]),
            None => (r, ""),
        };
        let (target_doc, key): (Value, String) = if base.is_empty() {
            (doc.clone(), doc_key.to_string())
        } else {
            match self.refs.get(base).or_else(|| self.refs.iter().find(|(k, _)| base.ends_with(k.as_str())).map(|(_, v)| v)) {
                Some(d) => (d.clone(), base.to_string()),
                None => return self.push(SNode::bare(Kind::Empty)),
            }
        };
        let cache_key = (key.clone(), frag.to_string());
        if let Some(i) = self.ref_cache.get(&cache_key) {
            return *i;
        }
        // Placeholder first, so recursive references resolve to this node.
        let idx = self.push(SNode::bare(Kind::Empty));
        self.ref_cache.insert(cache_key, idx);
        let frag = frag.replace("%25", "%").replace("%22", "\"");
        let target = if frag.is_empty() || frag == "/" {
            Some(target_doc.clone())
        } else if frag.starts_with('/') {
            target_doc.pointer(&frag).cloned()
        } else {
            find_anchor(&target_doc, &format!("#{frag}")).cloned()
        };
        if let Some(t) = target {
            let loaded = self.load(&t, &target_doc, &key);
            let node = std::mem::replace(&mut self.nodes[loaded], SNode::bare(Kind::Empty));
            self.nodes[idx] = node;
            self.nodes[loaded] = SNode::bare(Kind::Ref(idx));
        }
        idx
    }

    fn load_object(&mut self, o: &Map<String, Value>, doc: &Value, doc_key: &str) -> usize {
        let common = |o: &Map<String, Value>, draft4: bool| {
            let s = |k: &str| o.get(k).and_then(Value::as_str).map(String::from);
            (s(if draft4 { "id" } else { "$id" }), s("title"), s("description"), o.get("default").cloned())
        };
        if let Some(r) = o.get("$ref").and_then(Value::as_str) {
            let target = self.resolve_ref(r, doc, doc_key);
            let (id, title, description, default) = common(o, self.draft4);
            return self.push(SNode { kind: Kind::Ref(target), id, title, description, default });
        }
        let mut remaining: Vec<&str> = o.keys().map(String::as_str).collect();
        let consume = |remaining: &mut Vec<&str>, keys: &[&str]| remaining.retain(|k| !keys.contains(k));
        let mut extracted: Vec<usize> = Vec::new();

        if let Some(Value::Array(vals)) = o.get("enum") {
            let n = self.push(SNode::bare(Kind::Enum(vals.clone())));
            extracted.push(n);
            consume(&mut remaining, &["enum"]);
        }
        let present: Vec<String> = ["allOf", "anyOf", "oneOf"].iter().filter(|k| o.contains_key(**k)).map(|k| k.to_string()).collect();
        let mut present_set = JavaSet::new();
        present_set.extend(&present);
        for key in present_set.ordered() {
            let crit = match key.as_str() {
                "allOf" => Criterion::All,
                "anyOf" => Criterion::Any,
                _ => Criterion::One,
            };
            let raw: Vec<Value> = o[&key].as_array().cloned().unwrap_or_default();
            let subs = raw.iter().map(|s| self.load(s, doc, doc_key)).collect();
            let n = self.push(SNode::bare(Kind::Combined { crit, subs, raw }));
            extracted.push(n);
        }
        consume(&mut remaining, &["allOf", "anyOf", "oneOf"]);
        if let Some(not) = o.get("not") {
            let inner = self.load(not, doc, doc_key);
            let n = self.push(SNode::bare(Kind::Not(inner)));
            extracted.push(n);
            consume(&mut remaining, &["not"]);
        }
        if !self.draft4
            && let Some(c) = o.get("const")
        {
            let n = self.push(SNode::bare(Kind::Const(c.clone())));
            extracted.push(n);
            consume(&mut remaining, &["const"]);
        }
        match o.get("type") {
            Some(Value::String(t)) => {
                let n = self.typed(t, o, doc, doc_key, &mut remaining);
                extracted.push(n);
            }
            Some(Value::Array(ts)) => {
                let raw: Vec<Value> = ts.to_vec();
                let subs: Vec<usize> =
                    ts.iter().filter_map(Value::as_str).map(|t| self.typed(t, o, doc, doc_key, &mut remaining)).collect();
                let n = self.push(SNode::bare(Kind::Combined { crit: Criterion::Any, subs, raw }));
                extracted.push(n);
            }
            _ => {}
        }
        consume(&mut remaining, &["type"]);
        // Property sniffing on what's left.
        let has = |remaining: &Vec<&str>, keys: &[&str]| remaining.iter().any(|k| keys.contains(k));
        if has(&remaining, ARRAY_KEYWORDS) {
            let n = self.array(o, doc, doc_key);
            extracted.push(n);
            consume(&mut remaining, ARRAY_KEYWORDS);
        }
        if has(&remaining, OBJECT_KEYWORDS) {
            let n = self.object(o, doc, doc_key);
            extracted.push(n);
            consume(&mut remaining, OBJECT_KEYWORDS);
        }
        if has(&remaining, NUMBER_KEYWORDS) {
            let n = self.number(o, false);
            extracted.push(n);
            consume(&mut remaining, NUMBER_KEYWORDS);
        }
        if has(&remaining, STRING_KEYWORDS) {
            let n = self.string(o);
            extracted.push(n);
            consume(&mut remaining, STRING_KEYWORDS);
        }
        if !self.draft4 && has(&remaining, &["if", "then", "else"]) {
            let n = self.push(SNode::bare(Kind::Conditional));
            extracted.push(n);
        }

        let (id, title, description, default) = common(o, self.draft4);
        let kind = match extracted.len() {
            0 => Kind::Empty,
            1 => {
                let only = extracted[0];
                let node = std::mem::replace(&mut self.nodes[only], SNode::bare(Kind::Empty));
                node.kind
            }
            _ => Kind::Combined { crit: Criterion::All, subs: extracted, raw: Vec::new() },
        };
        self.push(SNode { kind, id, title, description, default })
    }

    fn typed(&mut self, t: &str, o: &Map<String, Value>, doc: &Value, doc_key: &str, remaining: &mut Vec<&str>) -> usize {
        let consume = |remaining: &mut Vec<&str>, keys: &[&str]| remaining.retain(|k| !keys.contains(k));
        match t {
            "string" => {
                consume(remaining, STRING_KEYWORDS);
                self.string(o)
            }
            "integer" => {
                consume(remaining, NUMBER_KEYWORDS);
                self.number(o, true)
            }
            "number" => {
                consume(remaining, NUMBER_KEYWORDS);
                self.number(o, false)
            }
            "boolean" => self.push(SNode::bare(Kind::Boolean)),
            "null" => self.push(SNode::bare(Kind::Null)),
            "array" => {
                consume(remaining, ARRAY_KEYWORDS);
                self.array(o, doc, doc_key)
            }
            "object" => {
                consume(remaining, OBJECT_KEYWORDS);
                self.object(o, doc, doc_key)
            }
            _ => self.push(SNode::bare(Kind::Empty)),
        }
    }

    fn string(&mut self, o: &Map<String, Value>) -> usize {
        let s = StrS {
            min_length: o.get("minLength").and_then(int),
            max_length: o.get("maxLength").and_then(int),
            pattern: o.get("pattern").and_then(Value::as_str).map(String::from),
        };
        self.push(SNode::bare(Kind::Str(s)))
    }

    fn number(&mut self, o: &Map<String, Value>, requires_integer: bool) -> usize {
        let n = |k: &str| o.get(k).and_then(|v| v.as_number().cloned());
        let s = NumS {
            minimum: n("minimum"),
            maximum: n("maximum"),
            // draft-04 exclusive limits are booleans: no numeric limit.
            exclusive_minimum: if self.draft4 { None } else { n("exclusiveMinimum") },
            exclusive_maximum: if self.draft4 { None } else { n("exclusiveMaximum") },
            multiple_of: n("multipleOf"),
            requires_integer,
        };
        self.push(SNode::bare(Kind::Num(s)))
    }

    fn array(&mut self, o: &Map<String, Value>, doc: &Value, doc_key: &str) -> usize {
        let (all_items, item_schemas) = match o.get("items") {
            Some(Value::Array(items)) => (None, Some(items.iter().map(|i| self.load(i, doc, doc_key)).collect())),
            Some(i @ (Value::Object(_) | Value::Bool(_))) => (Some(self.load(i, doc, doc_key)), None),
            _ => (None, None),
        };
        let (permits_additional_items, schema_of_additional_items) = match o.get("additionalItems") {
            Some(Value::Bool(b)) => (*b, None),
            Some(s @ Value::Object(_)) => (true, Some(self.load(s, doc, doc_key))),
            _ => (true, None),
        };
        let a = ArrS {
            all_items,
            item_schemas,
            permits_additional_items,
            schema_of_additional_items,
            min_items: o.get("minItems").and_then(int),
            max_items: o.get("maxItems").and_then(int),
            unique_items: o.get("uniqueItems").and_then(Value::as_bool).unwrap_or(false),
        };
        self.push(SNode::bare(Kind::Arr(a)))
    }

    fn object(&mut self, o: &Map<String, Value>, doc: &Value, doc_key: &str) -> usize {
        let id_kw = if self.draft4 { "id" } else { "$id" };
        let mut properties = Vec::new();
        if let Some(Value::Object(props)) = o.get("properties") {
            let keys: Vec<String> = props.keys().cloned().collect();
            for k in map_order(&keys) {
                let v = &props[&k];
                if k == id_kw && !v.is_object() {
                    continue;
                }
                let n = self.load(v, doc, doc_key);
                properties.push((k, n));
            }
        }
        let (permits_additional, schema_of_additional) = match o.get("additionalProperties") {
            Some(Value::Bool(b)) => (*b, None),
            Some(s @ Value::Object(_)) => (true, Some(self.load(s, doc, doc_key))),
            _ => (true, None),
        };
        let mut pattern_properties = Vec::new();
        if let Some(Value::Object(pp)) = o.get("patternProperties") {
            for (k, v) in pp {
                let n = self.load(v, doc, doc_key);
                pattern_properties.push((k.clone(), n));
            }
        }
        let mut property_dependencies = Vec::new();
        let mut schema_dependencies = Vec::new();
        if let Some(Value::Object(deps)) = o.get("dependencies") {
            for (k, v) in deps {
                match v {
                    Value::Array(a) => property_dependencies.push((k.clone(), a.iter().filter_map(|x| x.as_str().map(String::from)).collect())),
                    _ => {
                        let n = self.load(v, doc, doc_key);
                        schema_dependencies.push((k.clone(), n));
                    }
                }
            }
        }
        let s = ObjS {
            properties,
            required: o.get("required").and_then(Value::as_array).map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect()).unwrap_or_default(),
            permits_additional,
            schema_of_additional,
            pattern_properties,
            property_dependencies,
            schema_dependencies,
            min_properties: o.get("minProperties").and_then(int),
            max_properties: o.get("maxProperties").and_then(int),
        };
        self.push(SNode::bare(Kind::Obj(s)))
    }
}

struct Model {
    nodes: Vec<SNode>,
    root: usize,
}

fn build_model(schema: &JsonSchema) -> Model {
    let draft4 = schema.root.get("$schema").and_then(Value::as_str).is_some_and(|s| s.contains("draft-04") || s.ends_with("json-schema.org/schema") || s.ends_with("json-schema.org/schema#"));
    let mut l = Loader { nodes: Vec::new(), refs: &schema.refs, ref_cache: HashMap::new(), draft4 };
    let root = l.load(&schema.root, &schema.root, "");
    Model { nodes: l.nodes, root }
}

// ---------------- the diff ----------------

const COMPATIBLE: &[&str] = &[
    "ID_CHANGED", "DESCRIPTION_CHANGED", "TITLE_CHANGED", "DEFAULT_CHANGED", "SCHEMA_REMOVED", "TYPE_EXTENDED",
    "MAX_LENGTH_INCREASED", "MAX_LENGTH_REMOVED", "MIN_LENGTH_DECREASED", "MIN_LENGTH_REMOVED", "PATTERN_REMOVED",
    "MAXIMUM_INCREASED", "MAXIMUM_REMOVED", "MINIMUM_DECREASED", "MINIMUM_REMOVED", "EXCLUSIVE_MAXIMUM_INCREASED",
    "EXCLUSIVE_MAXIMUM_REMOVED", "EXCLUSIVE_MINIMUM_DECREASED", "EXCLUSIVE_MINIMUM_REMOVED", "MULTIPLE_OF_REDUCED",
    "MULTIPLE_OF_REMOVED", "REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED", "REQUIRED_ATTRIBUTE_REMOVED", "DEPENDENCY_ARRAY_NARROWED",
    "DEPENDENCY_ARRAY_REMOVED", "DEPENDENCY_SCHEMA_REMOVED", "MAX_PROPERTIES_INCREASED", "MAX_PROPERTIES_REMOVED",
    "MIN_PROPERTIES_DECREASED", "MIN_PROPERTIES_REMOVED", "ADDITIONAL_PROPERTIES_ADDED", "ADDITIONAL_PROPERTIES_EXTENDED",
    "PROPERTY_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL", "REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL",
    "OPTIONAL_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL", "PROPERTY_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL",
    "PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL", "PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL",
    "PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", "MAX_ITEMS_INCREASED", "MAX_ITEMS_REMOVED",
    "MIN_ITEMS_DECREASED", "MIN_ITEMS_REMOVED", "UNIQUE_ITEMS_REMOVED", "ADDITIONAL_ITEMS_ADDED", "ADDITIONAL_ITEMS_EXTENDED",
    "ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL", "ITEM_ADDED_TO_CLOSED_CONTENT_MODEL",
    "ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL", "ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL",
    "ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL", "ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL",
    "ENUM_ARRAY_EXTENDED", "COMBINED_TYPE_EXTENDED", "PRODUCT_TYPE_NARROWED", "SUM_TYPE_EXTENDED", "NOT_TYPE_NARROWED",
];

#[derive(Clone)]
struct Diff {
    kind: &'static str,
    path: String,
}

impl Diff {
    fn incompatible(&self) -> bool {
        !COMPATIBLE.contains(&self.kind)
    }

    /// Confluent's `Difference#toString` (including its mismatched closing quote).
    fn render(&self, a: &str, b: &str) -> String {
        const KEYWORD: &[&str] = &[
            "MAXIMUM_ADDED", "MINIMUM_ADDED", "EXCLUSIVE_MAXIMUM_ADDED", "EXCLUSIVE_MINIMUM_ADDED", "MULTIPLE_OF_ADDED",
            "MAX_LENGTH_ADDED", "MIN_LENGTH_ADDED", "PATTERN_ADDED", "REQUIRED_ATTRIBUTE_ADDED", "MAX_PROPERTIES_ADDED",
            "MIN_PROPERTIES_ADDED", "DEPENDENCY_ARRAY_ADDED", "DEPENDENCY_SCHEMA_ADDED", "MAX_ITEMS_ADDED", "MIN_ITEMS_ADDED",
            "UNIQUE_ITEMS_ADDED", "ADDITIONAL_ITEMS_REMOVED", "ADDITIONAL_PROPERTIES_REMOVED",
        ];
        const INCREASED: &[&str] = &["MIN_LENGTH_INCREASED", "MINIMUM_INCREASED", "EXCLUSIVE_MINIMUM_INCREASED", "MIN_PROPERTIES_INCREASED", "MULTIPLE_OF_EXPANDED", "MIN_ITEMS_INCREASED"];
        const DECREASED: &[&str] = &["MAX_LENGTH_DECREASED", "MAXIMUM_DECREASED", "MAX_ITEMS_DECREASED", "EXCLUSIVE_MAXIMUM_DECREASED", "MAX_PROPERTIES_DECREASED"];
        const CHANGED: &[&str] = &["PATTERN_CHANGED", "MULTIPLE_OF_CHANGED", "DEPENDENCY_ARRAY_CHANGED"];
        const NARROWED: &[&str] = &["ADDITIONAL_ITEMS_NARROWED", "ENUM_ARRAY_NARROWED", "SUM_TYPE_NARROWED", "ADDITIONAL_PROPERTIES_NARROWED"];
        const EXTENDED: &[&str] = &["DEPENDENCY_ARRAY_EXTENDED", "PRODUCT_TYPE_EXTENDED", "SUM_TYPE_EXTENDED", "NOT_TYPE_EXTENDED"];
        const TYPE: &[&str] = &["TYPE_CHANGED", "TYPE_NARROWED", "COMBINED_TYPE_CHANGED", "COMBINED_TYPE_SUBSCHEMAS_CHANGED", "ENUM_ARRAY_CHANGED"];
        let (k, p) = (self.kind, &self.path);
        let d = if KEYWORD.contains(&k) {
            format!("The keyword at path '{p}' in the {a} schema is not present in the {b} schema")
        } else if INCREASED.contains(&k) {
            format!("The value at path '{p}' in the {a} schema is more than its value in the {b} schema")
        } else if DECREASED.contains(&k) {
            format!("The value at path '{p}' in the {a} schema is less than its value in the {b} schema")
        } else if CHANGED.contains(&k) {
            format!("The value at path '{p}' is different between the {a} and {b} schema")
        } else if NARROWED.contains(&k) {
            format!("An array or combined type at path '{p}' has fewer elements in the {a} schema than the {b} schema")
        } else if EXTENDED.contains(&k) {
            format!("An array or combined type at path '{p}' has more elements in the {a} schema than the {b} schema")
        } else if TYPE.contains(&k) {
            format!("A type at path '{p}' is different between the {a} schema and the {b} schema")
        } else {
            match k {
                "PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL" | "ITEM_ADDED_TO_OPEN_CONTENT_MODEL" => format!(
                    "The {a} schema has an open content model and has a property or item at path '{p}' which is missing in the {b} schema"
                ),
                "REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL" => format!(
                    "The {a} schema has an unopen content model and has a required property at path '{p}' which is missing in the {b} schema"
                ),
                "PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL" | "ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL" => format!(
                    "The {a} has a closed content model and is missing a property or item present at path '{p}' in the {b} schema"
                ),
                "PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" | "ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" => format!(
                    "A property or item is missing in the {a} schema but present at path '{p}' in the {b} schema and is not covered by its partially open content model"
                ),
                "PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" | "ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" => format!(
                    "The {a} schema has a property or item at path '{p}' which is missing in the {b} schema and is not covered by its partially open content model"
                ),
                _ => String::new(),
            }
        };
        format!("{{errorType:\"{k}\", description:\"{d}'}}")
    }
}

struct Ctx<'a> {
    o: &'a Model,
    u: &'a Model,
    path: Vec<String>,
    diffs: Vec<Diff>,
    /// Original schemas currently being compared (everit identity), cycle guard.
    stack: Vec<usize>,
}

impl<'a> Ctx<'a> {
    fn sub(&self) -> Ctx<'a> {
        Ctx { o: self.o, u: self.u, path: self.path.clone(), diffs: Vec::new(), stack: self.stack.clone() }
    }
    fn compatible(&self) -> bool {
        !self.diffs.iter().any(Diff::incompatible)
    }
    fn add(&mut self, kind: &'static str) {
        self.diffs.push(Diff { kind, path: format!("#/{}", self.path.join("/")) });
    }
    fn add_at(&mut self, attr: &str, kind: &'static str) {
        self.path.push(attr.to_string());
        self.add(kind);
        self.path.pop();
    }
    fn on(&self, i: usize) -> &'a SNode {
        &self.o.nodes[i]
    }
    fn un(&self, i: usize) -> &'a SNode {
        &self.u.nodes[i]
    }
}

fn deref(m: &Model, i: usize) -> usize {
    match m.nodes[i].kind {
        Kind::Ref(t) => t,
        _ => i,
    }
}

fn compare(ctx: &mut Ctx<'_>, o: Option<usize>, u: Option<usize>) {
    let (o, u) = match (o, u) {
        (None, None) => return,
        (None, Some(_)) => return ctx.add("SCHEMA_ADDED"),
        (Some(_), None) => return ctx.add("SCHEMA_REMOVED"),
        (Some(o), Some(u)) => (deref(ctx.o, o), deref(ctx.u, u)),
    };
    let (on, un) = (ctx.on(o), ctx.un(u));
    match (&on.kind, &un.kind) {
        (k, Kind::Combined { crit, subs, .. }) if !matches!(k, Kind::Combined { .. }) => {
            if subs.len() == 1 {
                let mut sub = ctx.sub();
                compare(&mut sub, Some(o), Some(subs[0]));
                if sub.compatible() {
                    ctx.diffs.extend(sub.diffs);
                    return;
                }
            } else if matches!(crit, Criterion::Any | Criterion::One) {
                for s in subs {
                    let mut sub = ctx.sub();
                    compare(&mut sub, Some(o), Some(*s));
                    if sub.compatible() {
                        ctx.diffs.extend(sub.diffs);
                        ctx.add("SUM_TYPE_EXTENDED");
                        return;
                    }
                }
            }
        }
        (Kind::Combined { crit, subs, .. }, k) if !matches!(k, Kind::Combined { .. }) => {
            if subs.len() == 1 {
                let mut sub = ctx.sub();
                compare(&mut sub, Some(subs[0]), Some(u));
                if sub.compatible() {
                    ctx.diffs.extend(sub.diffs);
                    return;
                }
            } else if *crit == Criterion::All {
                for s in subs {
                    let mut sub = ctx.sub();
                    compare(&mut sub, Some(*s), Some(u));
                    if sub.compatible() {
                        ctx.diffs.extend(sub.diffs);
                        ctx.add("PRODUCT_TYPE_NARROWED");
                        return;
                    }
                }
            }
        }
        _ => {}
    }
    if on.kind.class() != un.kind.class() {
        // `update instanceof EmptySchema` is also true for TrueSchema.
        let update_accepts_all = matches!(un.kind, Kind::Empty | Kind::True);
        if !matches!(on.kind, Kind::False) && !update_accepts_all {
            ctx.add("TYPE_CHANGED");
        }
        return;
    }
    if ctx.stack.contains(&o) {
        return;
    }
    ctx.stack.push(o);
    if on.id != un.id {
        ctx.add("ID_CHANGED");
    }
    if on.title != un.title {
        ctx.add("TITLE_CHANGED");
    }
    if on.description != un.description {
        ctx.add("DESCRIPTION_CHANGED");
    }
    if on.default != un.default {
        ctx.add("DEFAULT_CHANGED");
    }
    match (&on.kind, &un.kind) {
        (Kind::Str(a), Kind::Str(b)) => compare_string(ctx, a, b),
        (Kind::Num(a), Kind::Num(b)) => compare_number(ctx, a, b),
        (Kind::Const(a), Kind::Const(b)) => {
            if a != b {
                ctx.add_at("const", "ENUM_ARRAY_CHANGED");
            }
        }
        (Kind::Enum(a), Kind::Enum(b)) => {
            let contains_all = |x: &Vec<Value>, y: &Vec<Value>| y.iter().all(|v| x.contains(v));
            let same = contains_all(a, b) && contains_all(b, a);
            if !same {
                if contains_all(b, a) {
                    ctx.add_at("enum", "ENUM_ARRAY_EXTENDED");
                } else if contains_all(a, b) {
                    ctx.add_at("enum", "ENUM_ARRAY_NARROWED");
                } else {
                    ctx.add_at("enum", "ENUM_ARRAY_CHANGED");
                }
            }
        }
        (Kind::Combined { crit: oc, subs: os, raw: oraw }, Kind::Combined { crit: uc, subs: us, raw: uraw }) => {
            compare_combined(ctx, (*oc, os, oraw), (*uc, us, uraw))
        }
        (Kind::Not(a), Kind::Not(b)) => {
            ctx.path.push("not".into());
            // Note the swap: `not` inverts the direction.
            let mut sub = Ctx { o: ctx.u, u: ctx.o, path: ctx.path.clone(), diffs: Vec::new(), stack: Vec::new() };
            compare(&mut sub, Some(*b), Some(*a));
            ctx.add(if sub.compatible() { "NOT_TYPE_NARROWED" } else { "NOT_TYPE_EXTENDED" });
            ctx.path.pop();
        }
        (Kind::Obj(a), Kind::Obj(b)) => compare_object(ctx, a, b),
        (Kind::Arr(a), Kind::Arr(b)) => compare_array(ctx, a, b),
        _ => {}
    }
    ctx.stack.pop();
}

fn compare_bound<T: PartialOrd + PartialEq + Copy>(
    ctx: &mut Ctx<'_>,
    key: &str,
    o: Option<T>,
    u: Option<T>,
    kinds: [&'static str; 4], // added, removed, increased, decreased
) {
    if o == u {
        return;
    }
    match (o, u) {
        (None, Some(_)) => ctx.add_at(key, kinds[0]),
        (Some(_), None) => ctx.add_at(key, kinds[1]),
        (Some(a), Some(b)) if a < b => ctx.add_at(key, kinds[2]),
        (Some(a), Some(b)) if a > b => ctx.add_at(key, kinds[3]),
        _ => {}
    }
}

fn compare_string(ctx: &mut Ctx<'_>, o: &StrS, u: &StrS) {
    compare_bound(ctx, "maxLength", o.max_length, u.max_length, ["MAX_LENGTH_ADDED", "MAX_LENGTH_REMOVED", "MAX_LENGTH_INCREASED", "MAX_LENGTH_DECREASED"]);
    compare_bound(ctx, "minLength", o.min_length, u.min_length, ["MIN_LENGTH_ADDED", "MIN_LENGTH_REMOVED", "MIN_LENGTH_INCREASED", "MIN_LENGTH_DECREASED"]);
    match (&o.pattern, &u.pattern) {
        (None, Some(_)) => ctx.add_at("pattern", "PATTERN_ADDED"),
        (Some(_), None) => ctx.add_at("pattern", "PATTERN_REMOVED"),
        (Some(a), Some(b)) if a != b => ctx.add_at("pattern", "PATTERN_CHANGED"),
        _ => {}
    }
}

fn compare_number(ctx: &mut Ctx<'_>, o: &NumS, u: &NumS) {
    // Java compares the Number objects for equality first (Integer 10 != Double 10.0),
    // then by doubleValue.
    let num = |n: &Option<serde_json::Number>| n.as_ref().map(|x| x.as_f64().unwrap_or(0.0));
    let bound = |ctx: &mut Ctx<'_>, key: &str, a: &Option<serde_json::Number>, b: &Option<serde_json::Number>, kinds: [&'static str; 4]| {
        if a != b {
            match (num(a), num(b)) {
                (None, Some(_)) => ctx.add_at(key, kinds[0]),
                (Some(_), None) => ctx.add_at(key, kinds[1]),
                (Some(x), Some(y)) if x < y => ctx.add_at(key, kinds[2]),
                (Some(x), Some(y)) if x > y => ctx.add_at(key, kinds[3]),
                _ => {}
            }
        }
    };
    bound(ctx, "maximum", &o.maximum, &u.maximum, ["MAXIMUM_ADDED", "MAXIMUM_REMOVED", "MAXIMUM_INCREASED", "MAXIMUM_DECREASED"]);
    bound(ctx, "minimum", &o.minimum, &u.minimum, ["MINIMUM_ADDED", "MINIMUM_REMOVED", "MINIMUM_INCREASED", "MINIMUM_DECREASED"]);
    // Confluent 7.9 compares a decreased exclusiveMaximum against `maximum`
    // and throws when that's absent; we report the evident intent instead.
    bound(ctx, "exclusiveMaximum", &o.exclusive_maximum, &u.exclusive_maximum, [
        "EXCLUSIVE_MAXIMUM_ADDED", "EXCLUSIVE_MAXIMUM_REMOVED", "EXCLUSIVE_MAXIMUM_INCREASED", "EXCLUSIVE_MAXIMUM_DECREASED",
    ]);
    bound(ctx, "exclusiveMinimum", &o.exclusive_minimum, &u.exclusive_minimum, [
        "EXCLUSIVE_MINIMUM_ADDED", "EXCLUSIVE_MINIMUM_REMOVED", "EXCLUSIVE_MINIMUM_INCREASED", "EXCLUSIVE_MINIMUM_DECREASED",
    ]);
    // multipleOf: BigDecimal equality, then Java int modulo checks.
    let dec = |n: &Option<serde_json::Number>| n.as_ref().map(|x| x.as_f64().unwrap_or(0.0));
    if dec(&o.multiple_of) != dec(&u.multiple_of) {
        match (dec(&o.multiple_of), dec(&u.multiple_of)) {
            (None, _) => ctx.add_at("multipleOf", "MULTIPLE_OF_ADDED"),
            (_, None) => ctx.add_at("multipleOf", "MULTIPLE_OF_REMOVED"),
            (Some(a), Some(b)) => {
                let (ai, bi) = (a as i64, b as i64);
                let kind = if ai != 0 && bi % ai == 0 {
                    "MULTIPLE_OF_EXPANDED"
                } else if bi != 0 && ai % bi == 0 {
                    "MULTIPLE_OF_REDUCED"
                } else {
                    "MULTIPLE_OF_CHANGED"
                };
                ctx.add_at("multipleOf", kind);
            }
        }
    }
    if o.requires_integer != u.requires_integer {
        ctx.add(if o.requires_integer { "TYPE_EXTENDED" } else { "TYPE_NARROWED" });
    }
}

fn compare_combined(ctx: &mut Ctx<'_>, o: (Criterion, &Vec<usize>, &Vec<Value>), u: (Criterion, &Vec<usize>, &Vec<Value>)) {
    let (oc, os, oraw) = o;
    let (uc, us, uraw) = u;
    if oc != uc {
        let single = |v: &Vec<usize>| v.len() == 1;
        let extended = uc == Criterion::Any
            || (single(os) && single(us))
            || (single(os) && uc == Criterion::One)
            || (single(us) && oc == Criterion::All);
        if extended {
            ctx.add("COMBINED_TYPE_EXTENDED");
        } else {
            ctx.add("COMBINED_TYPE_CHANGED");
            return;
        }
    }
    // LinkedHashSet of subschemas: identical subschemas collapse.
    let dedup = |subs: &Vec<usize>, raw: &Vec<Value>| -> Vec<usize> {
        let mut seen: Vec<&Value> = Vec::new();
        let mut out = Vec::new();
        for (i, s) in subs.iter().enumerate() {
            match raw.get(i) {
                Some(v) if seen.contains(&v) => {}
                Some(v) => {
                    seen.push(v);
                    out.push(*s);
                }
                None => out.push(*s),
            }
        }
        out
    };
    let (os, us) = (dedup(os, oraw), dedup(us, uraw));
    if os.len() < us.len() {
        ctx.add(if uc == Criterion::All { "PRODUCT_TYPE_EXTENDED" } else { "SUM_TYPE_EXTENDED" });
    } else if os.len() > us.len() {
        ctx.add(if matches!(oc, Criterion::Any | Criterion::One) { "SUM_TYPE_NARROWED" } else { "PRODUCT_TYPE_NARROWED" });
    }
    // Compatible pairings, then a maximum bipartite matching over them.
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); os.len()];
    for (i, o_sub) in os.iter().enumerate() {
        ctx.path.push(format!("{}/{i}", oc.name()));
        for (j, u_sub) in us.iter().enumerate() {
            let mut sub = ctx.sub();
            compare(&mut sub, Some(*o_sub), Some(*u_sub));
            if sub.compatible() {
                edges[i].push(j);
            }
        }
        ctx.path.pop();
    }
    let matched = max_matching(&edges, us.len());
    if matched < os.len().min(us.len()) {
        ctx.add("COMBINED_TYPE_SUBSCHEMAS_CHANGED");
    }
}

/// Size of a maximum bipartite matching (augmenting paths; sets are tiny).
fn max_matching(edges: &[Vec<usize>], right: usize) -> usize {
    fn augment(l: usize, edges: &[Vec<usize>], seen: &mut [bool], owner: &mut [Option<usize>]) -> bool {
        for &r in &edges[l] {
            if !seen[r] {
                seen[r] = true;
                if owner[r].is_none_or(|o| augment(o, edges, seen, owner)) {
                    owner[r] = Some(l);
                    return true;
                }
            }
        }
        false
    }
    let mut owner = vec![None; right];
    (0..edges.len()).filter(|&l| augment(l, edges, &mut vec![false; right], &mut owner)).count()
}

fn is_open_object(s: &ObjS) -> bool {
    s.pattern_properties.is_empty() && s.schema_of_additional.is_none() && s.permits_additional
}

fn partial_schema(s: &ObjS, key: &str) -> Option<usize> {
    for (pattern, schema) in &s.pattern_properties {
        if regex::Regex::new(pattern).is_ok_and(|r| r.is_match(key)) {
            return Some(*schema);
        }
    }
    s.schema_of_additional
}

fn compare_object(ctx: &mut Ctx<'_>, o: &ObjS, u: &ObjS) {
    let has_default = |ctx: &Ctx<'_>, idx: usize| ctx.un(deref(ctx.u, idx)).default.is_some() || ctx.un(idx).default.is_some();
    // required
    ctx.path.push("required".into());
    for (key, _) in &o.properties {
        let Some((_, uidx)) = u.properties.iter().find(|(k, _)| k == key) else { continue };
        let (or, ur) = (o.required.contains(key), u.required.contains(key));
        ctx.path.push(key.clone());
        if or && !ur {
            ctx.add("REQUIRED_ATTRIBUTE_REMOVED");
        } else if !or && ur {
            ctx.add(if has_default(ctx, *uidx) { "REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED" } else { "REQUIRED_ATTRIBUTE_ADDED" });
        }
        ctx.path.pop();
    }
    ctx.path.pop();

    // properties
    ctx.path.push("properties".into());
    let ok: Vec<String> = o.properties.iter().map(|(k, _)| k.clone()).collect();
    let uk: Vec<String> = u.properties.iter().map(|(k, _)| k.clone()).collect();
    let mut keys = JavaSet::from_collection(&ok);
    keys.extend(&uk);
    for key in keys.ordered() {
        ctx.path.push(key.clone());
        let os = o.properties.iter().find(|(k, _)| *k == key).map(|(_, s)| *s);
        let us = u.properties.iter().find(|(k, _)| *k == key).map(|(_, s)| *s);
        match (os, us) {
            (Some(os), None) => {
                if is_open_object(u) {
                    ctx.add("PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL");
                } else if let Some(partial) = partial_schema(u, &key) {
                    let mut sub = ctx.sub();
                    compare(&mut sub, Some(os), Some(partial));
                    let compatible = sub.compatible();
                    ctx.diffs.extend(sub.diffs);
                    ctx.add(if compatible {
                        "PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL"
                    } else {
                        "PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL"
                    });
                } else if matches!(ctx.on(deref(ctx.o, os)).kind, Kind::False) {
                    ctx.add("PROPERTY_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL");
                } else {
                    ctx.add("PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL");
                }
            }
            (None, Some(us)) => {
                if is_open_object(o) {
                    let empty = matches!(ctx.un(deref(ctx.u, us)).kind, Kind::Empty | Kind::True);
                    ctx.add(if empty { "PROPERTY_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL" } else { "PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL" });
                } else {
                    if let Some(partial) = partial_schema(o, &key) {
                        let mut sub = ctx.sub();
                        compare(&mut sub, Some(partial), Some(us));
                        let compatible = sub.compatible();
                        ctx.diffs.extend(sub.diffs);
                        ctx.add(if compatible {
                            "PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL"
                        } else {
                            "PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL"
                        });
                    }
                    if u.required.contains(&key) {
                        ctx.add(if has_default(ctx, us) {
                            "REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL"
                        } else {
                            "REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL"
                        });
                    } else {
                        ctx.add("OPTIONAL_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL");
                    }
                }
            }
            (Some(os), Some(us)) => compare(ctx, Some(os), Some(us)),
            (None, None) => {}
        }
        ctx.path.pop();
    }
    ctx.path.pop();

    // dependencies
    ctx.path.push("dependencies".into());
    let od: Vec<String> = o.property_dependencies.iter().map(|(k, _)| k.clone()).collect();
    let ud: Vec<String> = u.property_dependencies.iter().map(|(k, _)| k.clone()).collect();
    for key in union_order(&od, &ud) {
        ctx.path.push(key.clone());
        let a = o.property_dependencies.iter().find(|(k, _)| *k == key).map(|(_, v)| v);
        let b = u.property_dependencies.iter().find(|(k, _)| *k == key).map(|(_, v)| v);
        let contains_all = |x: &Vec<String>, y: &Vec<String>| y.iter().all(|v| x.contains(v));
        match (a, b) {
            (Some(_), None) => ctx.add("DEPENDENCY_ARRAY_REMOVED"),
            (None, Some(_)) => ctx.add("DEPENDENCY_ARRAY_ADDED"),
            (Some(a), Some(b)) if !(contains_all(a, b) && contains_all(b, a)) => {
                if contains_all(b, a) {
                    ctx.add("DEPENDENCY_ARRAY_EXTENDED");
                } else if contains_all(a, b) {
                    ctx.add("DEPENDENCY_ARRAY_NARROWED");
                } else {
                    ctx.add("DEPENDENCY_ARRAY_CHANGED");
                }
            }
            _ => {}
        }
        ctx.path.pop();
    }
    let os_: Vec<String> = o.schema_dependencies.iter().map(|(k, _)| k.clone()).collect();
    let us_: Vec<String> = u.schema_dependencies.iter().map(|(k, _)| k.clone()).collect();
    for key in union_order(&os_, &us_) {
        ctx.path.push(key.clone());
        let a = o.schema_dependencies.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
        let b = u.schema_dependencies.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
        match (a, b) {
            (Some(_), None) => ctx.add("DEPENDENCY_SCHEMA_REMOVED"),
            (None, Some(_)) => ctx.add("DEPENDENCY_SCHEMA_ADDED"),
            (Some(a), Some(b)) => compare(ctx, Some(a), Some(b)),
            _ => {}
        }
        ctx.path.pop();
    }
    ctx.path.pop();

    // additionalProperties
    ctx.path.push("additionalProperties".into());
    if o.permits_additional != u.permits_additional {
        ctx.add(if u.permits_additional { "ADDITIONAL_PROPERTIES_ADDED" } else { "ADDITIONAL_PROPERTIES_REMOVED" });
    } else if o.schema_of_additional.is_none() && u.schema_of_additional.is_some() {
        ctx.add("ADDITIONAL_PROPERTIES_NARROWED");
    } else if u.schema_of_additional.is_none() && o.schema_of_additional.is_some() {
        ctx.add("ADDITIONAL_PROPERTIES_EXTENDED");
    } else {
        compare(ctx, o.schema_of_additional, u.schema_of_additional);
    }
    ctx.path.pop();

    compare_bound(ctx, "maxProperties", o.max_properties, u.max_properties, [
        "MAX_PROPERTIES_ADDED", "MAX_PROPERTIES_REMOVED", "MAX_PROPERTIES_INCREASED", "MAX_PROPERTIES_DECREASED",
    ]);
    compare_bound(ctx, "minProperties", o.min_properties, u.min_properties, [
        "MIN_PROPERTIES_ADDED", "MIN_PROPERTIES_REMOVED", "MIN_PROPERTIES_INCREASED", "MIN_PROPERTIES_DECREASED",
    ]);
}

fn compare_array(ctx: &mut Ctx<'_>, o: &ArrS, u: &ArrS) {
    ctx.path.push("items".into());
    compare(ctx, o.all_items, u.all_items);
    ctx.path.pop();

    let empty = Vec::new();
    let oi = o.item_schemas.as_ref().unwrap_or(&empty);
    let ui = u.item_schemas.as_ref().unwrap_or(&empty);
    let common = oi.len().min(ui.len());
    for idx in 0..common {
        ctx.path.push(format!("items/{idx}"));
        compare(ctx, Some(oi[idx]), Some(ui[idx]));
        ctx.path.pop();
    }
    let open = |a: &ArrS| a.schema_of_additional_items.is_none() && a.permits_additional_items;
    for (idx, os) in oi.iter().enumerate().skip(common) {
        ctx.path.push(format!("items/{idx}"));
        if open(u) {
            ctx.add("ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL");
        } else if let Some(partial) = u.schema_of_additional_items {
            let mut sub = ctx.sub();
            compare(&mut sub, Some(*os), Some(partial));
            let compatible = sub.compatible();
            ctx.diffs.extend(sub.diffs);
            ctx.add(if compatible { "ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" } else { "ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" });
        } else if matches!(ctx.on(deref(ctx.o, *os)).kind, Kind::False) {
            ctx.add("ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL");
        } else {
            ctx.add("ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL");
        }
        ctx.path.pop();
    }
    for (idx, us) in ui.iter().enumerate().skip(common) {
        ctx.path.push(format!("items/{idx}"));
        if open(o) {
            let empty = matches!(ctx.un(deref(ctx.u, *us)).kind, Kind::Empty | Kind::True);
            ctx.add(if empty { "ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL" } else { "ITEM_ADDED_TO_OPEN_CONTENT_MODEL" });
        } else if let Some(partial) = o.schema_of_additional_items {
            let mut sub = ctx.sub();
            compare(&mut sub, Some(partial), Some(*us));
            let compatible = sub.compatible();
            ctx.diffs.extend(sub.diffs);
            ctx.add(if compatible { "ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" } else { "ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL" });
        } else {
            ctx.add("ITEM_ADDED_TO_CLOSED_CONTENT_MODEL");
        }
        ctx.path.pop();
    }

    ctx.path.push("additionalItems".into());
    if o.permits_additional_items != u.permits_additional_items {
        ctx.add(if o.permits_additional_items { "ADDITIONAL_ITEMS_REMOVED" } else { "ADDITIONAL_ITEMS_ADDED" });
    } else if o.schema_of_additional_items.is_none() && u.schema_of_additional_items.is_some() {
        ctx.add("ADDITIONAL_ITEMS_NARROWED");
    } else if u.schema_of_additional_items.is_none() && o.schema_of_additional_items.is_some() {
        ctx.add("ADDITIONAL_ITEMS_EXTENDED");
    } else {
        compare(ctx, o.schema_of_additional_items, u.schema_of_additional_items);
    }
    ctx.path.pop();

    compare_bound(ctx, "maxItems", o.max_items, u.max_items, ["MAX_ITEMS_ADDED", "MAX_ITEMS_REMOVED", "MAX_ITEMS_INCREASED", "MAX_ITEMS_DECREASED"]);
    compare_bound(ctx, "minItems", o.min_items, u.min_items, ["MIN_ITEMS_ADDED", "MIN_ITEMS_REMOVED", "MIN_ITEMS_INCREASED", "MIN_ITEMS_DECREASED"]);
    if o.unique_items != u.unique_items {
        ctx.add_at("uniqueItems", if o.unique_items { "UNIQUE_ITEMS_REMOVED" } else { "UNIQUE_ITEMS_ADDED" });
    }
}

/// Can `reader` read data written with `writer`? (Confluent: original = writer, update = reader.)
pub fn can_read(reader: &JsonSchema, writer: &JsonSchema, labels: Labels) -> Vec<String> {
    let (wm, rm) = (build_model(writer), build_model(reader));
    let mut ctx = Ctx { o: &wm, u: &rm, path: Vec::new(), diffs: Vec::new(), stack: Vec::new() };
    compare(&mut ctx, Some(wm.root), Some(rm.root));
    ctx.diffs.iter().filter(|d| d.incompatible()).map(|d| d.render(labels.reader, labels.writer)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const L: Labels = Labels { reader: "new", writer: "old" };

    fn p(s: &str) -> JsonSchema {
        JsonSchema::parse(s, &[]).unwrap()
    }

    #[test]
    fn open_model_property_rules() {
        let old = p(r#"{"type":"object","properties":{"a":{"type":"string"}}}"#);
        // Removing a property from an open model is backward compatible.
        let removed = p(r#"{"type":"object","properties":{}}"#);
        assert!(can_read(&removed, &old, L).is_empty());
        // Adding a typed property to an open model is not.
        let added = p(r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}"#);
        assert!(!can_read(&added, &old, L).is_empty());
    }

    #[test]
    fn closed_model_property_rules() {
        let old = p(r#"{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}"#);
        let added = p(r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},"additionalProperties":false}"#);
        assert!(can_read(&added, &old, L).is_empty());
        let removed = p(r#"{"type":"object","properties":{},"additionalProperties":false}"#);
        assert!(!can_read(&removed, &old, L).is_empty());
    }

    #[test]
    fn types_and_required() {
        let int = p(r#"{"type":"integer"}"#);
        let num = p(r#"{"type":"number"}"#);
        assert!(can_read(&num, &int, L).is_empty());
        assert!(!can_read(&int, &num, L).is_empty());
        let a = p(r#"{"type":"object","properties":{"x":{"type":"string"}}}"#);
        let b = p(r#"{"type":"object","properties":{"x":{"type":"string"}},"required":["x"]}"#);
        assert!(!can_read(&b, &a, L).is_empty());
        assert!(can_read(&a, &b, L).is_empty());
    }

    #[test]
    fn local_refs() {
        let s = r##"{"definitions":{"n":{"type":"object","properties":{"next":{"$ref":"#/definitions/n"}}}},"$ref":"#/definitions/n"}"##;
        assert!(can_read(&p(s), &p(s), L).is_empty());
    }
}
