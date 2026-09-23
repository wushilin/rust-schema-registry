//! Golden tests: our schema handling vs Confluent's own libraries.
//!
//! `tests/golden/corpus.json` (from `make_corpus.py`) was run through the
//! Confluent 7.9.0 schema libraries by `tests/oracle` to produce
//! `tests/golden/golden.json`: for every schema, whether the server accepts it
//! and the canonical/normalized strings it stores; for every evolution pair,
//! the BACKWARD and FORWARD compatibility messages.
//!
//! Each category is its own test so a regression names what drifted. Known,
//! deliberate differences are listed in `ALLOWED` with a reason.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::model::SchemaType;
use crate::schema::{self, Labels, ResolvedRef};

const GOLDEN: &str = include_str!("../tests/golden/golden.json");
const CORPUS: &str = include_str!("../tests/golden/corpus.json");

/// (category, case id) pairs where we knowingly differ, with the reason.
const ALLOWED: &[(&str, &str, &str)] = &[
    (
        "validity",
        "proto-dup-number",
        "Confluent's Wire parser accepts two fields with the same number; protoc and protobuf-java reject \
         such a file, so no client could use it. We reject it.",
    ),
    (
        "validity",
        "proto-norm-order",
        "proto3 requires the first enum value to be 0; Confluent's Wire parser doesn't check, protoc does. We reject it.",
    ),
];

fn allowed(category: &str, id: &str) -> bool {
    ALLOWED.iter().any(|(c, i, _)| *c == category && *i == id)
}

fn schema_type(t: &str) -> SchemaType {
    SchemaType::parse(Some(t)).expect("schema type")
}

fn refs(v: &Value) -> Vec<ResolvedRef> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|r| ResolvedRef { name: r["name"].as_str().unwrap().into(), schema: r["schema"].as_str().unwrap().into() })
                .collect()
        })
        .unwrap_or_default()
}

fn corpus_by_id(section: &str) -> BTreeMap<String, Value> {
    let c: Value = serde_json::from_str(CORPUS).unwrap();
    c[section].as_array().unwrap().iter().map(|e| (e["id"].as_str().unwrap().to_string(), e.clone())).collect()
}

fn golden(section: &str) -> BTreeMap<String, Value> {
    let g: Value = serde_json::from_str(GOLDEN).unwrap();
    g[section].as_object().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Run `check` for every case, collect mismatches, and fail listing them all.
fn run(category: &str, section: &str, check: impl Fn(&Value, &Value) -> Option<String>) {
    let corpus = corpus_by_id(section);
    let mut failures = Vec::new();
    for (id, expected) in golden(section) {
        if let Some(diff) = check(&corpus[&id], &expected)
            && !allowed(category, &id)
        {
            failures.push(format!("  [{id}] {diff}"));
        }
    }
    assert!(failures.is_empty(), "{category}: {} mismatch(es) vs Confluent\n{}", failures.len(), failures.join("\n"));
}

fn parse(case: &Value, schema_key: &str, refs_key: &str) -> Result<schema::ParsedSchema, String> {
    schema::parse(schema_type(case["type"].as_str().unwrap()), case[schema_key].as_str().unwrap(), &refs(&case[refs_key]))
        .map_err(|e| e.message)
}

#[test]
fn golden_validity() {
    run("validity", "schemas", |case, exp| {
        let ours = parse(case, "schema", "refs");
        let theirs = exp["valid"].as_bool().unwrap();
        (ours.is_ok() != theirs).then(|| match ours {
            Ok(_) => format!("we accept; Confluent rejects: {}", exp["error"]),
            Err(e) => format!("we reject ({e}); Confluent accepts"),
        })
    });
}

#[test]
fn golden_canonical() {
    run("canonical", "schemas", |case, exp| {
        let ours = parse(case, "schema", "refs").ok()?;
        let theirs = exp["canonical"].as_str()?;
        (ours.canonical != theirs).then(|| format!("\n    ours:   {}\n    theirs: {}", ours.canonical, theirs))
    });
}

#[test]
fn golden_normalized() {
    run("normalized", "schemas", |case, exp| {
        let ours = parse(case, "schema", "refs").ok()?;
        let theirs = exp["normalized"].as_str()?;
        (ours.normalized != theirs).then(|| format!("\n    ours:   {}\n    theirs: {}", ours.normalized, theirs))
    });
}

fn our_compat(case: &Value) -> Result<(Vec<String>, Vec<String>), String> {
    let old = parse(case, "old", "old_refs")?;
    let new = parse(case, "new", "new_refs")?;
    let backward = schema::can_read(&new, &old, Labels { reader: "new", writer: "old" });
    let forward = schema::can_read(&old, &new, Labels { reader: "old", writer: "new" });
    Ok((backward, forward))
}

#[test]
fn golden_compatibility_verdicts() {
    run("compat", "compat", |case, exp| {
        if exp.get("error").is_some() {
            return None; // Confluent couldn't parse the pair; covered by validity
        }
        let (b, f) = match our_compat(case) {
            Ok(r) => r,
            Err(e) => return Some(format!("we fail to parse: {e}")),
        };
        let tb = exp["BACKWARD"].as_array().unwrap().is_empty();
        let tf = exp["FORWARD"].as_array().unwrap().is_empty();
        let mut d = Vec::new();
        let verdict = |ok: bool| if ok { "compatible" } else { "INCOMPATIBLE" };
        if b.is_empty() != tb {
            let why = if tb { format!("ours: {b:?}") } else { format!("theirs: {}", exp["BACKWARD"]) };
            d.push(format!("BACKWARD ours={} theirs={} {why}", verdict(b.is_empty()), verdict(tb)));
        }
        if f.is_empty() != tf {
            let why = if tf { format!("ours: {f:?}") } else { format!("theirs: {}", exp["FORWARD"]) };
            d.push(format!("FORWARD ours={} theirs={} {why}", verdict(f.is_empty()), verdict(tf)));
        }
        (!d.is_empty()).then(|| d.join("; "))
    });
}

/// The checker's differences, without the `{oldSchema: ...}` trailer that the
/// server-level caller adds (we append ours in `Registry::check_compatibility`).
fn their_messages(v: &Value) -> Vec<String> {
    v.as_array().unwrap().iter().map(|m| m.as_str().unwrap().to_string()).filter(|m| !m.starts_with("{oldSchema:")).collect()
}

#[test]
fn golden_compatibility_messages() {
    run("messages", "compat", |case, exp| {
        if exp.get("error").is_some() {
            return None;
        }
        let (b, f) = our_compat(case).ok()?;
        let tb = their_messages(&exp["BACKWARD"]);
        let tf = their_messages(&exp["FORWARD"]);
        let mut d = Vec::new();
        if b != tb {
            d.push(format!("\n    BACKWARD ours:   {b:?}\n    BACKWARD theirs: {tb:?}"));
        }
        if f != tf {
            d.push(format!("\n    FORWARD ours:   {f:?}\n    FORWARD theirs: {tf:?}"));
        }
        (!d.is_empty()).then(|| d.concat())
    });
}

const LEVELS: &[&str] = &["NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE"];

/// Every version chain at every compatibility level: same verdict and the same
/// messages (including the `{oldSchemaVersion}` / `{oldSchema}` trailers).
#[test]
fn golden_levels() {
    let corpus = corpus_by_id("chains");
    let mut failures = Vec::new();
    for (id, expected) in golden("chains") {
        let case = &corpus[&id];
        let versions: Vec<schema::ParsedSchema> = case["versions"]
            .as_array()
            .unwrap()
            .iter()
            .zip(case["types"].as_array().unwrap())
            .map(|(v, t)| schema::parse(schema_type(t.as_str().unwrap()), v.as_str().unwrap(), &[]).expect("chain schema parses"))
            .collect();
        let (newest, earlier) = versions.split_last().unwrap();
        let previous: Vec<schema::Previous<'_>> =
            earlier.iter().enumerate().rev().map(|(i, s)| schema::Previous { version: i as u32 + 1, schema: s }).collect();
        for level in LEVELS {
            let lvl = crate::model::CompatibilityLevel::parse(level).unwrap();
            let ours = schema::check_level(newest, &previous, lvl);
            let theirs: Vec<String> = expected[*level].as_array().unwrap().iter().map(|m| m.as_str().unwrap().to_string()).collect();
            if ours != theirs && !allowed("levels", &format!("{id}/{level}")) {
                failures.push(format!("  [{id} @ {level}]\n    ours:   {ours:?}\n    theirs: {theirs:?}"));
            }
        }
    }
    assert!(failures.is_empty(), "levels: {} mismatch(es) vs Confluent\n{}", failures.len(), failures.join("\n"));
}
