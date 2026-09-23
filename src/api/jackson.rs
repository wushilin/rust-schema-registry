//! Request bodies read the way Confluent's Jersey + Jackson stack reads them.
//!
//! Observed against Confluent 7.9 (tests/http_conformance):
//! - `Content-Type` must be one of the registry's media types, `application/json`
//!   or `application/octet-stream` (parameters and case ignored); a missing
//!   header counts as `application/octet-stream`. Anything else is 415.
//! - An empty or `null` body fails bean validation: 422 `{method}.argN must not be null (was null)`.
//! - Malformed JSON or a wrong JSON shape is a 400 with Jackson's message.
//! - Scalars coerce: numbers and booleans become strings (`{"schema": 42}` is
//!   the Avro schema `42`), numeric strings and floats become integers
//!   (`"1"`, `1.7` -> 1), `""` becomes null, `"true"`/`"false"`/numbers become
//!   booleans. Unknown properties are ignored; a repeated key keeps the last value.

use serde_json::{Map, Value};

use crate::error::{ApiError, ApiResult};

pub fn bad(msg: impl Into<String>) -> ApiError {
    ApiError::new(400, msg)
}

/// Jackson's name for a JSON token, as used in its messages.
fn token(v: &Value) -> &'static str {
    match v {
        Value::Null => "JsonToken.VALUE_NULL",
        Value::Bool(true) => "JsonToken.VALUE_TRUE",
        Value::Bool(false) => "JsonToken.VALUE_FALSE",
        Value::Number(n) if n.is_f64() => "JsonToken.VALUE_NUMBER_FLOAT",
        Value::Number(_) => "JsonToken.VALUE_NUMBER_INT",
        Value::String(_) => "JsonToken.VALUE_STRING",
        Value::Array(_) => "JsonToken.START_ARRAY",
        Value::Object(_) => "JsonToken.START_OBJECT",
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "Null",
        Value::Bool(_) => "Boolean",
        Value::Number(_) => "Number",
        Value::String(_) => "String",
        Value::Array(_) => "Array",
        Value::Object(_) => "Object",
    }
}

/// Java's `Double.toString` for the values JSON can carry (close enough for coercion).
fn java_number(n: &serde_json::Number) -> String {
    match n.as_f64() {
        Some(f) if n.is_f64() => {
            if f.fract() == 0.0 && f.abs() < 1e7 {
                format!("{f:.1}")
            } else {
                f.to_string()
            }
        }
        _ => n.to_string(),
    }
}

/// A bean: the top level of a body, or a nested object property.
pub fn object<'a>(v: &'a Value, ty: &str) -> ApiResult<&'a Map<String, Value>> {
    match v {
        Value::Object(o) => Ok(o),
        Value::Array(_) => Err(bad(format!("Cannot deserialize value of type `{ty}` from Array value (token `{}`)", token(v)))),
        Value::String(s) => Err(bad(format!(
            "Cannot construct instance of `{ty}` (although at least one Creator exists): no String-argument constructor/factory method to deserialize from String value ('{s}')"
        ))),
        Value::Number(n) => Err(bad(format!(
            "Cannot construct instance of `{ty}` (although at least one Creator exists): no int/Int-argument constructor/factory method to deserialize from Number value ({n})"
        ))),
        _ => Err(bad(format!("Cannot deserialize value of type `{ty}` from {} value (token `{}`)", kind(v), token(v)))),
    }
}

/// An optional nested bean (`null` is absent).
pub fn opt_object<'a>(v: Option<&'a Value>, ty: &str) -> ApiResult<Option<&'a Map<String, Value>>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(v) => object(v, ty).map(Some),
    }
}

pub fn string(v: Option<&Value>) -> ApiResult<Option<String>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(Value::Bool(b)) => Ok(Some(b.to_string())),
        Some(Value::Number(n)) => Ok(Some(java_number(n))),
        Some(v) => Err(bad(format!("Cannot deserialize value of type `String` from {} value (token `{}`)", kind(v), token(v)))),
    }
}

pub fn int(v: Option<&Value>) -> ApiResult<Option<i32>> {
    let out_of_range = |s: &str| bad(format!("Numeric value ({s}) out of range of int (-2147483648 - 2147483647)"));
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i32::try_from(i).map(Some).map_err(|_| out_of_range(&n.to_string()))
            } else if let Some(u) = n.as_u64() {
                i32::try_from(u).map(Some).map_err(|_| out_of_range(&n.to_string()))
            } else {
                let f = n.as_f64().unwrap_or(0.0).trunc();
                if f < i32::MIN as f64 || f > i32::MAX as f64 {
                    return Err(out_of_range(&n.to_string()));
                }
                Ok(Some(f as i32))
            }
        }
        Some(Value::String(s)) if s.trim().is_empty() => Ok(None),
        Some(Value::String(s)) => s.trim().parse::<i32>().map(Some).map_err(|_| {
            bad(format!("Cannot deserialize value of type `Integer` from String \"{s}\": not a valid `java.lang.Integer` value"))
        }),
        Some(v) => Err(bad(format!("Cannot deserialize value of type `Integer` from {} value (token `{}`)", kind(v), token(v)))),
    }
}

pub fn boolean(v: Option<&Value>) -> ApiResult<Option<bool>> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(Value::Number(n)) => Ok(Some(n.as_f64() != Some(0.0))),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) if s == "true" || s == "True" || s == "TRUE" => Ok(Some(true)),
        Some(Value::String(s)) if s == "false" || s == "False" || s == "FALSE" => Ok(Some(false)),
        Some(Value::String(s)) => Err(bad(format!(
            "Cannot deserialize value of type `Boolean` from String \"{s}\": only \"true\" or \"false\" recognized"
        ))),
        Some(v) => Err(bad(format!("Cannot deserialize value of type `Boolean` from {} value (token `{}`)", kind(v), token(v)))),
    }
}

/// `Set<String>` (Jackson does not wrap a lone string into an array).
fn string_set(v: &Value) -> ApiResult<Vec<String>> {
    let Value::Array(a) = v else {
        return Err(match v {
            Value::String(s) => bad(format!(
                "Cannot construct instance of `HashSet` (although at least one Creator exists): no String-argument constructor/factory method to deserialize from String value ('{s}')"
            )),
            _ => bad(format!("Cannot deserialize value of type `HashSet` from {} value (token `{}`)", kind(v), token(v))),
        });
    };
    let mut out: Vec<String> = Vec::new();
    for x in a {
        if let Some(s) = string(Some(x))? {
            out.push(s);
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

/// Confluent's `Metadata` bean in canonical form: `tags` (sorted map of
/// sorted sets), `properties` (sorted map), `sensitive` (sorted set); empty
/// members are dropped (`@JsonInclude(NON_EMPTY)`), unknown keys ignored.
pub fn metadata(v: Option<&Value>) -> ApiResult<Option<Value>> {
    let Some(o) = opt_object(v, "Metadata")? else { return Ok(None) };
    let mut out = Map::new();
    if let Some(tags) = opt_object(o.get("tags"), "Map")? {
        let mut t = Map::new();
        for (k, x) in tags {
            if !x.is_null() {
                t.insert(k.clone(), Value::from(string_set(x)?));
            }
        }
        if !t.is_empty() {
            out.insert("tags".into(), sorted(t));
        }
    }
    if let Some(props) = opt_object(o.get("properties"), "Map")? {
        let mut p = Map::new();
        for (k, x) in props {
            match string(Some(x))? {
                Some(s) => p.insert(k.clone(), Value::String(s)),
                // Confluent: NullPointerException while sorting the map.
                None => return Err(ApiError::new(500, "Internal Server Error")),
            };
        }
        if !p.is_empty() {
            out.insert("properties".into(), sorted(p));
        }
    }
    match o.get("sensitive") {
        None | Some(Value::Null) => {}
        Some(s) => {
            let s = string_set(s)?;
            if !s.is_empty() {
                out.insert("sensitive".into(), Value::from(s));
            }
        }
    }
    Ok(Some(Value::Object(out)))
}

fn sorted(m: Map<String, Value>) -> Value {
    let mut v: Vec<(String, Value)> = m.into_iter().collect();
    v.sort_by(|a, b| a.0.cmp(&b.0));
    Value::Object(v.into_iter().collect())
}

const RULE_KINDS: &[&str] = &["CONDITION", "TRANSFORM"];
const RULE_MODES: &[&str] = &["UPGRADE", "DOWNGRADE", "UPDOWN", "WRITE", "READ", "WRITEREAD"];
const MIGRATION_MODES: &[&str] = &["UPGRADE", "DOWNGRADE", "UPDOWN"];
const DOMAIN_MODES: &[&str] = &["WRITE", "READ", "WRITEREAD"];

/// `Rule#validateName`.
fn rule_name(r: &Map<String, Value>) -> Result<String, String> {
    let Some(name) = r.get("name").and_then(Value::as_str) else { return Err("Missing rule name".into()) };
    let mut chars = name.chars();
    let Some(first) = chars.next() else { return Err("Empty rule name".into()) };
    if name.chars().count() > 64 {
        return Err("Rule name too long".into());
    }
    if !first.is_alphabetic() && first != '_' {
        return Err(format!("Illegal initial character in rule name: {name}"));
    }
    if chars.any(|c| !(c.is_alphanumeric() || c == '_' || c == '-')) {
        return Err(format!("Illegal character in rule name: {name}"));
    }
    Ok(name.to_string())
}

/// `RuleSet#validate` and `Rule#validate`: rule names are unique and
/// well-formed, a type is required, and each list only takes its own modes.
/// Confluent answers a failure with 42210.
pub fn validate_rule_set(rule_set: &Value) -> ApiResult<()> {
    let fail = |m: String| ApiError::new(42210, m);
    let Some(o) = rule_set.as_object() else { return Ok(()) };
    let mut names: Vec<String> = Vec::new();
    for (key, modes, only) in [
        ("migrationRules", MIGRATION_MODES, "Migration rules can only be UPGRADE, DOWNGRADE, UPDOWN"),
        ("domainRules", DOMAIN_MODES, "Domain rules can only be WRITE, READ, WRITEREAD"),
    ] {
        for rule in o.get(key).and_then(Value::as_array).into_iter().flatten() {
            let Some(r) = rule.as_object() else { continue };
            let name = rule_name(r).map_err(fail)?;
            if names.contains(&name) {
                return Err(fail(format!("Found rule with duplicate name '{name}'")));
            }
            names.push(name);
            if r.get("type").and_then(Value::as_str).is_none() {
                return Err(fail("Missing rule type".into()));
            }
            let mode = r.get("mode").and_then(Value::as_str);
            // `validateAction`: only WRITEREAD and UPDOWN may list several actions.
            if let Some(m) = mode.filter(|m| *m != "WRITEREAD" && *m != "UPDOWN") {
                let _ = m;
                for action in ["onSuccess", "onFailure"] {
                    if r.get(action).and_then(Value::as_str).is_some_and(|a| a.contains(',')) {
                        return Err(fail("Multiple actions only valid with WRITEREAD and UPDOWN".into()));
                    }
                }
            }
            if let Some(m) = mode
                && !modes.contains(&m)
            {
                return Err(fail(only.to_string()));
            }
        }
    }
    Ok(())
}

/// Confluent's `RuleSet` bean: shape and enum values are checked, the rest is kept as sent.
pub fn rule_set(v: Option<&Value>) -> ApiResult<Option<Value>> {
    let Some(o) = opt_object(v, "RuleSet")? else { return Ok(None) };
    for key in ["migrationRules", "domainRules", "encodingRules"] {
        match o.get(key) {
            None | Some(Value::Null) => {}
            Some(Value::Array(rules)) => {
                for r in rules {
                    let Some(r) = opt_object(Some(r), "Rule")? else { continue };
                    for (field, allowed, ty) in [("kind", RULE_KINDS, "RuleKind"), ("mode", RULE_MODES, "RuleMode")] {
                        if let Some(s) = string(r.get(field))?
                            && !allowed.contains(&s.as_str())
                        {
                            return Err(bad(format!(
                                "Cannot deserialize value of type `{ty}` from String \"{s}\": not one of the values accepted for Enum class: [{}]",
                                allowed.join(", ")
                            )));
                        }
                    }
                }
            }
            Some(x) => {
                return Err(bad(format!("Cannot deserialize value of type `ArrayList<Rule>` from {} value (token `{}`)", kind(x), token(x))));
            }
        }
    }
    Ok(Some(v.cloned().unwrap_or(Value::Null)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn err(rule_set: Value) -> String {
        validate_rule_set(&rule_set).unwrap_err().message
    }

    fn rule(name: &str, mode: &str) -> Value {
        json!({"name": name, "kind": "CONDITION", "type": "CEL", "mode": mode, "expr": "true"})
    }

    /// Community Confluent drops rule sets before validating them, so these
    /// follow Confluent's own `RuleSet#validate` / `Rule#validate` instead.
    #[test]
    fn rule_sets_are_validated_like_confluent() {
        assert!(validate_rule_set(&json!({"domainRules": [rule("r", "WRITE")]})).is_ok());
        assert!(validate_rule_set(&json!({"migrationRules": [rule("m", "UPGRADE")]})).is_ok());
        assert_eq!(err(json!({"domainRules": [rule("", "WRITE")]})), "Empty rule name");
        assert_eq!(err(json!({"domainRules": [{"kind": "CONDITION", "type": "CEL", "mode": "WRITE"}]})), "Missing rule name");
        assert_eq!(err(json!({"domainRules": [rule("1r", "WRITE")]})), "Illegal initial character in rule name: 1r");
        assert_eq!(err(json!({"domainRules": [rule("a b", "WRITE")]})), "Illegal character in rule name: a b");
        assert_eq!(err(json!({"domainRules": [rule(&"x".repeat(65), "WRITE")]})), "Rule name too long");
        assert_eq!(err(json!({"domainRules": [{"name": "r", "mode": "WRITE"}]})), "Missing rule type");
        assert_eq!(err(json!({"domainRules": [rule("r", "UPGRADE")]})), "Domain rules can only be WRITE, READ, WRITEREAD");
        assert_eq!(
            err(json!({"migrationRules": [rule("r", "WRITE")]})),
            "Migration rules can only be UPGRADE, DOWNGRADE, UPDOWN"
        );
        assert_eq!(
            err(json!({"domainRules": [rule("r", "WRITE"), rule("r", "READ")]})),
            "Found rule with duplicate name 'r'"
        );
        // A name is unique across both lists.
        assert_eq!(
            err(json!({"migrationRules": [rule("r", "UPGRADE")], "domainRules": [rule("r", "WRITE")]})),
            "Found rule with duplicate name 'r'"
        );
        // Several actions need a mode that runs in both directions.
        let with_actions = |mode: &str| {
            let mut r = rule("r", mode);
            r["onSuccess"] = json!("a,b");
            json!({"domainRules": [r]})
        };
        assert_eq!(err(with_actions("WRITE")), "Multiple actions only valid with WRITEREAD and UPDOWN");
        assert!(validate_rule_set(&with_actions("WRITEREAD")).is_ok());
        assert_eq!(validate_rule_set(&json!({"domainRules": [rule("", "WRITE")]})).unwrap_err().code, 42210);
    }
}
