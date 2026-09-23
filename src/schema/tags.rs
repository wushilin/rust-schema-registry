//! Schema tags: `POST /subjects/{subject}/versions/{version}/tags`.
//!
//! Confluent stores tags inside the schema itself and registers the result as
//! a new version, so tagging is a schema rewrite. Each schema type has its own
//! place and path syntax for them (paths as verified against Confluent 7.9):
//!
//! | type     | record path      | field path           | where the tags go               |
//! |----------|------------------|----------------------|---------------------------------|
//! | Avro     | `com.x.User`     | `com.x.User.name`    | `confluent:tags` property       |
//! | JSON     | `object`         | `object.a.object.b`  | `confluent:tags` keyword        |
//! | Protobuf | `M` / `M.N`      | `M.a` / `M.N.x`      | `(confluent.message_meta)` /
//!                                                          `(confluent.field_meta)` option  |

use serde_json::{Map, Value};

use crate::model::SchemaType;

pub const TAGS_KEY: &str = "confluent:tags";

/// One entry of `tagsToAdd` / `tagsToRemove`.
#[derive(Debug, Clone)]
pub struct TagEdit {
    pub path: String,
    /// `sr_record` (a named type) rather than `sr_field`.
    pub record: bool,
    pub tags: Vec<String>,
}

pub fn no_match(path: &str) -> String {
    format!("java.lang.IllegalArgumentException: No matching path '{path}' found in the schema")
}

/// Apply tag additions and removals, returning the rewritten schema text.
pub fn apply(schema_type: SchemaType, text: &str, add: &[TagEdit], remove: &[TagEdit]) -> Result<String, String> {
    match schema_type {
        SchemaType::Avro => {
            let mut v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
            for e in add.iter().chain(remove) {
                let adding = add.iter().any(|a| std::ptr::eq(a, e));
                if !avro_edit(&mut v, e, adding, None) {
                    return Err(no_match(&e.path));
                }
            }
            serde_json::to_string(&v).map_err(|e| e.to_string())
        }
        SchemaType::Json => {
            let mut v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
            for e in add.iter().chain(remove) {
                let adding = add.iter().any(|a| std::ptr::eq(a, e));
                let segments: Vec<&str> = e.path.split('.').collect();
                if !json_edit(&mut v, &segments, e, adding) {
                    return Err(no_match(&e.path));
                }
            }
            serde_json::to_string(&v).map_err(|e| e.to_string())
        }
        SchemaType::Protobuf => super::proto_wire::apply_tags(text, add, remove),
    }
}

/// Merge or remove tags on a node's `confluent:tags` property.
pub fn edit_tags(o: &mut Map<String, Value>, tags: &[String], adding: bool) {
    let mut current: Vec<String> =
        o.get(TAGS_KEY).and_then(Value::as_array).map(|a| a.iter().filter_map(Value::as_str).map(String::from).collect()).unwrap_or_default();
    if adding {
        for t in tags {
            if !current.contains(t) {
                current.push(t.clone());
            }
        }
    } else {
        current.retain(|t| !tags.contains(t));
    }
    if current.is_empty() {
        o.shift_remove(TAGS_KEY);
    } else {
        o.insert(TAGS_KEY.into(), Value::from(current));
    }
}

/// Walk an Avro schema, tagging the named type or field the path names.
/// `space` is the namespace inherited from the enclosing type.
fn avro_edit(v: &mut Value, edit: &TagEdit, adding: bool, space: Option<&str>) -> bool {
    match v {
        Value::Array(items) => items.iter_mut().any(|x| avro_edit(x, edit, adding, space)),
        Value::Object(o) => {
            let ty = o.get("type").and_then(Value::as_str).unwrap_or("").to_string();
            let named = matches!(ty.as_str(), "record" | "error" | "enum" | "fixed");
            let full = named.then(|| {
                let name = o.get("name").and_then(Value::as_str).unwrap_or("");
                match (name.contains('.'), o.get("namespace").and_then(Value::as_str).or(space)) {
                    (false, Some(ns)) if !ns.is_empty() => format!("{ns}.{name}"),
                    _ => name.to_string(),
                }
            });
            if let Some(full) = &full {
                // Confluent matches the full name or the bare record name.
                let simple = full.rsplit('.').next().unwrap_or(full).to_string();
                if edit.record && (*full == edit.path || simple == edit.path) {
                    edit_tags(o, &edit.tags, adding);
                    return true;
                }
                // A field of this type: "<name>.<field>".
                if !edit.record
                    && let Some(field) = edit
                        .path
                        .strip_prefix(&format!("{full}."))
                        .or_else(|| edit.path.strip_prefix(&format!("{simple}.")))
                    && let Some(fields) = o.get_mut("fields").and_then(Value::as_array_mut)
                    && let Some(f) = fields
                        .iter_mut()
                        .find(|f| f.get("name").and_then(Value::as_str) == Some(field))
                        .and_then(Value::as_object_mut)
                {
                    edit_tags(f, &edit.tags, adding);
                    return true;
                }
            }
            let inner = full
                .as_ref()
                .and_then(|f| f.rfind('.').map(|i| f[..i].to_string()))
                .or_else(|| space.map(String::from));
            for (k, x) in o.iter_mut() {
                if k == "name" || k == "namespace" {
                    continue;
                }
                if avro_edit(x, edit, adding, inner.as_deref()) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Walk a JSON Schema by alternating type and property names (`object.a.object.b`).
fn json_edit(v: &mut Value, segments: &[&str], edit: &TagEdit, adding: bool) -> bool {
    let Some(o) = v.as_object_mut() else { return false };
    let ty = o.get("type").and_then(Value::as_str).unwrap_or("").to_string();
    match segments {
        [] => false,
        [last] => {
            if *last != ty {
                return false;
            }
            edit_tags(o, &edit.tags, adding);
            true
        }
        [head, property, rest @ ..] => {
            if *head != ty {
                return false;
            }
            let Some(next) = o.get_mut("properties").and_then(Value::as_object_mut).and_then(|p| p.get_mut(*property)) else {
                return false;
            };
            if rest.is_empty() {
                let Some(target) = next.as_object_mut() else { return false };
                edit_tags(target, &edit.tags, adding);
                return true;
            }
            json_edit(next, rest, edit, adding)
        }
    }
}
