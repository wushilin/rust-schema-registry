//! Behaviour observed on a live Confluent Schema Registry 7.9.0 (single-node
//! KRaft Kafka), pinned as unit tests. Each test names the probe it came from
//! (`tests/conformance/*.sh` reproduce them over HTTP against both servers).
//!
//! If one of these fails, we have drifted from Confluent - don't "fix" the
//! test without re-checking the real server.
//!
//! These run with `normalize` off, Confluent's default; our server default
//! (normalize on) is covered at the end of the file.

use serde_json::json;
use tempfile::TempDir;

use crate::model::{CompatibilityLevel, Mode, RegisterSchemaRequest, SchemaReference};
use crate::registry::{Registry, SubjectVersion};
use crate::store::Store;

fn registry() -> (Registry, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(dir.path(), false).expect("open store");
    let snap = store.load_snapshot().expect("snapshot");
    (Registry::new(store, snap, CompatibilityLevel::Backward, "test".into(), 1000, false), dir)
}

/// A distinct Avro record per name.
fn rec(name: &str) -> String {
    json!({"type": "record", "name": name, "fields": [{"name": "a", "type": "int"}]}).to_string()
}

fn req(schema: String) -> RegisterSchemaRequest {
    RegisterSchemaRequest { schema: Some(schema), ..Default::default() }
}

fn import_req(schema: String, id: i32, version: Option<i32>) -> RegisterSchemaRequest {
    RegisterSchemaRequest { schema: Some(schema), id: Some(id), version, ..Default::default() }
}

fn register(r: &Registry, subject: &str, name: &str) -> u32 {
    r.register(subject, req(rec(name)), false).unwrap_or_else(|e| panic!("register {subject}: {e}")).id
}

fn import_mode(r: &Registry, subject: &str) {
    r.set_mode(Some(subject), Mode::Import, false).expect("set IMPORT");
}

/// Record name of the schema returned for `id` with the given subject hint.
fn resolve(r: &Registry, id: u32, hint: Option<&str>) -> Result<String, u32> {
    r.get_schema_by_id(id.into(), hint, false, None)
        .map(|v| serde_json::from_str::<serde_json::Value>(&v.schema).unwrap()["name"].as_str().unwrap().to_string())
        .map_err(|e| e.code)
}

// ===========================================================================
// ID allocation (tests/conformance/id_allocation.sh)
// ===========================================================================

#[test]
fn each_context_has_its_own_counter_starting_at_1() {
    let (r, _d) = registry();
    for i in 0..10 {
        register(&r, &format!("d{i}"), &format!("D{i}"));
    }
    // Confluent D2/D8: a new context starts at 1 no matter how far the default context is.
    assert_eq!(register(&r, ":.zz:one", "Z1"), 1);
    assert_eq!(register(&r, ":.zz:two", "Z2"), 2);
    assert_eq!(register(&r, ":.ww:one", "W1"), 1);
    // D4: the default context continues its own sequence, unaffected by .zz.
    assert_eq!(register(&r, "d-next", "DN"), 11);
}

#[test]
fn identical_schema_gets_independent_ids_per_context_but_shared_within_one() {
    let (r, _d) = registry();
    register(&r, "pad", "Pad");
    let a = register(&r, "s1", "Same");
    // Same schema, other subject, same context: same id (Confluent A1/probe dedup).
    assert_eq!(register(&r, "s2", "Same"), a);
    // Same schema in another context: that context's own id space (A2 -> 1).
    assert_eq!(register(&r, ":.ca:s1", "Same"), 1);
    assert_ne!(a, 1);
}

#[test]
fn imports_do_not_move_other_contexts_counters() {
    let (r, _d) = registry();
    register(&r, "d1", "D1");
    register(&r, ":.zz:one", "Z1");
    import_mode(&r, ":.yy:imp");
    assert_eq!(r.register(":.yy:imp", import_req(rec("Y1"), 900, Some(1)), false).unwrap().id, 900);
    // D6/D7: neither the default context nor .zz jump to 901.
    assert_eq!(register(&r, "d2", "D2"), 2);
    assert_eq!(register(&r, ":.zz:two", "Z2"), 2);
}

#[test]
fn import_advances_its_own_contexts_counter() {
    let (r, _d) = registry();
    import_mode(&r, ":.xa:s1");
    r.register(":.xa:s1", import_req(rec("A"), 777, Some(1)), false).unwrap();
    r.delete_mode(":.xa:s1").unwrap();
    // C9: next auto id in .xa is 778.
    assert_eq!(register(&r, ":.xa:s2", "D"), 778);
}

#[test]
fn same_id_may_mean_different_schemas_in_different_contexts() {
    let (r, _d) = registry();
    for s in [":.xa:s1", ":.xb:s1", "xdef-s1"] {
        import_mode(&r, s);
    }
    // C1-C3: all three imports of id 777 succeed with different content.
    assert_eq!(r.register(":.xa:s1", import_req(rec("A"), 777, Some(1)), false).unwrap().id, 777);
    assert_eq!(r.register(":.xb:s1", import_req(rec("B"), 777, Some(1)), false).unwrap().id, 777);
    assert_eq!(r.register("xdef-s1", import_req(rec("C"), 777, Some(1)), false).unwrap().id, 777);
    // C4-C6: each context answers with its own schema.
    assert_eq!(resolve(&r, 777, None), Ok("C".into()));
    assert_eq!(resolve(&r, 777, Some(":.xa:")), Ok("A".into()));
    assert_eq!(resolve(&r, 777, Some(":.xb:")), Ok("B".into()));
    // C7: unqualified subject `s1` prefers a context where `s1` uses the id
    // (.xa sorts before .xb) over the default context's unrelated subject.
    assert_eq!(resolve(&r, 777, Some("s1")), Ok("A".into()));
    // C8: without a hint, /versions is the default context only.
    assert_eq!(
        r.id_versions(777, None, false).unwrap().iter().map(|v| v.subject.clone()).collect::<Vec<_>>(),
        vec!["xdef-s1".to_string()]
    );
}

// ===========================================================================
// Resolving an id across contexts (probes against id 11 living only in .jdev)
// ===========================================================================

/// Default context has ids 1..=10 (subjects d0..d9); `.jdev` has 11 fillers
/// (ids 1..=11) where `thing-value` uses id 11. Returns the .jdev-only id (11).
fn jdev_fixture(r: &Registry) -> u32 {
    for i in 0..10 {
        register(r, &format!("d{i}"), &format!("D{i}"));
    }
    for i in 0..10 {
        register(r, &format!(":.jdev:filler-{i}"), &format!("F{i}"));
    }
    let id = register(r, ":.jdev:thing-value", "OnlyDev");
    assert_eq!(id, 11);
    id
}

#[test]
fn no_subject_hint_means_default_context_only() {
    let (r, _d) = registry();
    let id = jdev_fixture(&r);
    assert_eq!(resolve(&r, id, None), Err(40403));
}

#[test]
fn unqualified_hint_falls_back_to_context_where_that_subject_uses_the_id() {
    let (r, _d) = registry();
    let id = jdev_fixture(&r);
    assert_eq!(resolve(&r, id, Some("thing-value")), Ok("OnlyDev".into()));
    // `:.:name` is treated like an unqualified name.
    assert_eq!(resolve(&r, id, Some(":.:thing-value")), Ok("OnlyDev".into()));
}

#[test]
fn unqualified_hint_does_not_fall_back_for_other_subjects() {
    let (r, _d) = registry();
    let id = jdev_fixture(&r);
    // Subject exists nowhere: no blind search, even though .jdev has the id.
    assert_eq!(resolve(&r, id, Some("some-other-topic-value")), Err(40403));
    // Subject exists in .jdev but doesn't use this id.
    assert_eq!(resolve(&r, id, Some("filler-0")), Err(40403));
    // Subject exists in the default context but the id isn't there.
    assert_eq!(resolve(&r, id, Some("d0")), Err(40403));
}

#[test]
fn unqualified_hint_finds_other_contexts_id_even_when_default_has_same_number() {
    let (r, _d) = registry();
    jdev_fixture(&r);
    // id 1: default has D0 (subject d0), .jdev has F0 (subject filler-0).
    assert_eq!(resolve(&r, 1, Some("filler-0")), Ok("F0".into()));
    // An unrelated hint falls through to the default context's own id 1.
    assert_eq!(resolve(&r, 1, Some("unrelated-value")), Ok("D0".into()));
    assert_eq!(resolve(&r, 1, None), Ok("D0".into()));
}

#[test]
fn qualified_hints_are_strict() {
    let (r, _d) = registry();
    let id = jdev_fixture(&r);
    // Context-only hint: any subject in that context.
    assert_eq!(resolve(&r, id, Some(":.jdev:")), Ok("OnlyDev".into()));
    assert_eq!(resolve(&r, id, Some(":.jdev:thing-value")), Ok("OnlyDev".into()));
    // Qualified subject that doesn't use the id / doesn't exist.
    assert_eq!(resolve(&r, id, Some(":.jdev:filler-0")), Err(40403));
    assert_eq!(resolve(&r, id, Some(":.jdev:nonexistent")), Err(40403));
    // Unknown context.
    assert_eq!(resolve(&r, id, Some(":.nope:")), Err(40403));
}

#[test]
fn default_context_ids_resolve_with_any_hint() {
    let (r, _d) = registry();
    let addr = register(&r, "addr", "Addr");
    assert_eq!(resolve(&r, addr, Some("unrelated-value")), Ok("Addr".into()));
    assert_eq!(resolve(&r, addr, Some("person-value")), Ok("Addr".into()));
}

#[test]
fn ties_between_contexts_go_to_the_first_context_in_sorted_order() {
    let (r, _d) = registry();
    assert_eq!(register(&r, ":.zz:one", "Z1"), 1);
    assert_eq!(register(&r, ":.ww:one", "W1"), 1);
    // D10: both `:.ww:one` and `:.zz:one` use id 1; Confluent answered .ww.
    assert_eq!(resolve(&r, 1, Some("one")), Ok("W1".into()));
}

#[test]
fn subject_param_locates_but_does_not_filter_id_listings() {
    let (r, _d) = registry();
    let id = register(&r, "addr", "Addr");
    let expected = vec![SubjectVersion { subject: "addr".into(), version: 1 }];
    let got = r.id_versions(id.into(), Some("unrelated-value"), false).unwrap();
    assert_eq!(got.iter().map(|v| (v.subject.clone(), v.version)).collect::<Vec<_>>(),
               expected.iter().map(|v| (v.subject.clone(), v.version)).collect::<Vec<_>>());
    assert_eq!(r.id_subjects(id.into(), Some("unrelated-value"), false).unwrap(), vec!["addr".to_string()]);
    let jid = {
        let (r2, _d2) = registry();
        let id = jdev_fixture(&r2);
        assert_eq!(r2.id_subjects(id.into(), Some("thing-value"), false).unwrap(), vec![":.jdev:thing-value".to_string()]);
        assert_eq!(r2.id_subjects(id.into(), Some(":.jdev:"), false).unwrap(), vec![":.jdev:thing-value".to_string()]);
        id
    };
    assert_eq!(jid, 11);
}

// ===========================================================================
// IMPORT mode (tests/conformance/import_mode.sh)
// ===========================================================================

#[test]
fn explicit_id_outside_import_mode_is_rejected() {
    let (r, _d) = registry();
    let e = r.register("rw1", import_req(rec("R"), 500, None), false).unwrap_err();
    assert_eq!(e.code, 42205); // B0
}

#[test]
fn import_mode_requires_empty_scope_unless_forced() {
    let (r, _d) = registry();
    register(&r, "ida", "S");
    let e = r.set_mode(Some("ida"), Mode::Import, false).unwrap_err();
    assert_eq!((e.code, e.message.as_str()), (42205, "Cannot import since found existing subjects")); // B2
    let e = r.set_mode(None, Mode::Import, false).unwrap_err();
    assert_eq!((e.code, e.message.as_str()), (42205, "Cannot import since found existing subjects")); // B3
    r.set_mode(Some("ida"), Mode::Import, true).unwrap();
    r.set_mode(Some("empty"), Mode::Import, false).unwrap(); // B1
}

#[test]
fn import_register_replay_and_overwrite_rules() {
    let (r, _d) = registry();
    import_mode(&r, "imp1");
    // B4: first import answers {"id":100} only.
    let resp = r.register("imp1", import_req(rec("I1"), 100, Some(1)), false).unwrap();
    assert_eq!(serde_json::to_value(&resp).unwrap(), json!({"id": 100}));
    // B5: an identical replay answers {"id","version","schema"}.
    let resp = r.register("imp1", import_req(rec("I1"), 100, Some(1)), false).unwrap();
    assert_eq!(serde_json::to_value(&resp).unwrap(), json!({"id": 100, "version": 1, "schema": rec("I1")}));
    // B6: different content under an existing id.
    let e = r.register("imp1", import_req(rec("I2"), 100, Some(2)), false).unwrap_err();
    assert_eq!((e.code, e.message.as_str()), (42205, "Overwrite new schema with id 100 is not permitted."));
    // B7: IMPORT mode without an id.
    let e = r.register("imp1", req(rec("I3")), false).unwrap_err();
    assert_eq!((e.code, e.message.as_str()), (42205, "Subject imp1 is not in read-write mode"));
    // B8/B9: id without version takes the next version.
    r.register("imp1", import_req(rec("I4"), 101, None), false).unwrap();
    assert_eq!(r.list_versions("imp1", false, false).unwrap(), vec![1, 2]);
    // B10: version gaps are allowed.
    r.register("imp1", import_req(rec("I5"), 102, Some(10)), false).unwrap();
    // B11: an existing version is overwritten (last write wins).
    assert_eq!(r.register("imp1", import_req(rec("I6"), 103, Some(10)), false).unwrap().id, 103);
    assert_eq!(r.get_version("imp1", crate::registry::VersionSpec::Exact(10), false).unwrap().id, 103);
    // B12: compatibility is not checked in IMPORT mode (I1 int -> string).
    let incompatible = json!({"type": "record", "name": "I1", "fields": [{"name": "a", "type": "string"}]}).to_string();
    assert_eq!(r.register("imp1", import_req(incompatible, 104, Some(11)), false).unwrap().id, 104);
}

#[test]
fn import_same_content_under_a_new_id_elsewhere() {
    let (r, _d) = registry();
    import_mode(&r, "imp1");
    import_mode(&r, "imp2");
    r.register("imp1", import_req(rec("I1"), 100, Some(1)), false).unwrap();
    // B13/B14: identical content may be imported under a second id.
    assert_eq!(r.register("imp2", import_req(rec("I1"), 200, Some(1)), false).unwrap().id, 200);
    assert_eq!(resolve(&r, 200, None), Ok("I1".into()));
    assert_eq!(resolve(&r, 100, None), Ok("I1".into()));
}

#[test]
fn deletes_are_allowed_in_import_mode() {
    let (r, _d) = registry();
    import_mode(&r, "imp2");
    r.register("imp2", import_req(rec("I1"), 200, Some(1)), false).unwrap();
    assert_eq!(r.delete_version("imp2", crate::registry::VersionSpec::Exact(1), false).unwrap(), 1); // B20
}

#[test]
fn readwrite_idempotent_register_answers_id_only() {
    let (r, _d) = registry();
    let id = register(&r, "ida", "S");
    let resp = r.register("ida", req(rec("S")), false).unwrap();
    assert_eq!(serde_json::to_value(&resp).unwrap(), json!({ "id": id }));
    // Same schema, brand-new subject: shares the id, still {"id"} only.
    let resp = r.register("newsubj", req(rec("S")), false).unwrap();
    assert_eq!(serde_json::to_value(&resp).unwrap(), json!({ "id": id }));
}

// ===========================================================================
// Error formats
// ===========================================================================

#[test]
fn incompatible_error_message_format() {
    let (r, _d) = registry();
    register(&r, "imp1", "I1");
    let other = json!({"type": "record", "name": "I9", "fields": [{"name": "a", "type": "string"}]}).to_string();
    let e = r.register("imp1", req(other), false).unwrap_err();
    assert_eq!(e.code, 409);
    assert!(
        e.message.starts_with(
            "Schema being registered is incompatible with an earlier schema for subject \"imp1\", details: [{errorType:'NAME_MISMATCH', description:'The name of the schema has changed (path '/name')', additionalInfo:'expected: I1'}"
        ),
        "{}",
        e.message
    );
    assert!(e.message.contains("{oldSchemaVersion: 1}"), "{}", e.message);
    assert!(e.message.ends_with("{validateFields: 'false', compatibility: 'BACKWARD'}]"), "{}", e.message);
}

// ===========================================================================
// References across contexts
// ===========================================================================

#[test]
fn unqualified_references_resolve_in_the_referrers_context() {
    let (r, _d) = registry();
    register(&r, "address", "Address");
    let person = json!({"type": "record", "name": "P", "fields": [{"name": "a", "type": "Address"}]}).to_string();
    let refs = |subject: &str| {
        Some(vec![SchemaReference { name: "Address".into(), subject: subject.into(), version: 1 }.into()])
    };
    let in_dev = RegisterSchemaRequest { schema: Some(person.clone()), references: refs("address"), ..Default::default() };
    assert_eq!(r.register(":.dev:p", in_dev, false).unwrap_err().code, 42201);
    let qualified = RegisterSchemaRequest { schema: Some(person), references: refs(":.:address"), ..Default::default() };
    assert!(r.register(":.dev:p", qualified, false).is_ok());
}

// ===========================================================================
// Snapshot / cache consistency (our own guarantees)
// ===========================================================================

#[test]
fn state_survives_reopen_via_snapshot_loader() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(dir.path(), false).unwrap();
        let snap = store.load_snapshot().unwrap();
        let r = Registry::new(store, snap, CompatibilityLevel::Backward, "t".into(), 10, false);
        register(&r, "a", "A");
        register(&r, ":.c:b", "B");
        r.delete_version("a", crate::registry::VersionSpec::Exact(1), false).unwrap();
        register(&r, "a", "A2");
        r.set_config(Some("a"), crate::model::ConfigRecord { compatibility_level: Some(CompatibilityLevel::None), ..Default::default() }).unwrap();
    }
    let store = Store::open(dir.path(), false).unwrap();
    let snap = store.load_snapshot().unwrap();
    let r = Registry::new(store, snap, CompatibilityLevel::Backward, "t".into(), 10, false);
    assert_eq!(r.list_versions("a", true, false).unwrap(), vec![1, 2]);
    assert_eq!(r.list_versions("a", false, false).unwrap(), vec![2]);
    assert_eq!(r.list_contexts().unwrap(), vec![".".to_string(), ".c".to_string()]);
    assert_eq!(r.get_config(Some("a"), false).unwrap().compatibility_level, Some(CompatibilityLevel::None));
    // Counters survive: next id in the default context is 3.
    assert_eq!(register(&r, "z", "Z"), 3);
}

#[test]
fn tiny_cache_still_serves_everything() {
    // Cache bound of 2 entries: bodies must be re-read from RocksDB transparently.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path(), false).unwrap();
    let snap = store.load_snapshot().unwrap();
    let r = Registry::new(store, snap, CompatibilityLevel::Backward, "t".into(), 2, false);
    let ids: Vec<u32> = (0..50).map(|i| register(&r, &format!("s{i}"), &format!("R{i}"))).collect();
    for (i, id) in ids.iter().enumerate() {
        assert_eq!(resolve(&r, *id, None), Ok(format!("R{i}")));
    }
}

// ===========================================================================
// Logically equal schemas share an id (tests/conformance/logical_equality*.py)
//
// Confluent dedups on the *stored* string: the canonical form by default, the
// normalized form with `normalize=true`. So spellings that canonicalize the
// same always share an id; spellings that only normalize the same share an id
// when registered with normalization.
// ===========================================================================

fn reg_as(r: &Registry, subject: &str, schema_type: &str, schema: &str, normalize: bool) -> u32 {
    let req = RegisterSchemaRequest { schema: Some(schema.into()), schema_type: Some(schema_type.into()), ..Default::default() };
    r.register(subject, req, normalize).unwrap_or_else(|e| panic!("{subject}: {e}")).id
}

/// Register each variant under its own subject; return ids.
fn ids(r: &Registry, schema_type: &str, variants: &[&str], normalize: bool) -> Vec<u32> {
    variants.iter().enumerate().map(|(i, v)| reg_as(r, &format!("{schema_type}-{i}-{normalize}"), schema_type, v, normalize)).collect()
}

const AVRO_USER: &str = r#"{"type":"record","name":"User","namespace":"acme","fields":[{"name":"id","type":"long"},{"name":"tag","type":"string","x-a":1,"x-b":2}]}"#;

#[test]
fn avro_spellings_with_the_same_canonical_form_share_an_id() {
    let (r, _d) = registry();
    let got = ids(&r, "AVRO", &[
        AVRO_USER,
        // pretty-printed
        "{\n  \"type\": \"record\",\n  \"name\": \"User\",\n  \"namespace\": \"acme\",\n  \"fields\": [\n    {\"name\": \"id\", \"type\": \"long\"},\n    {\"name\": \"tag\", \"type\": \"string\", \"x-a\": 1, \"x-b\": 2}\n  ]\n}",
        // attribute order
        r#"{"fields":[{"type":"long","name":"id"},{"x-a":1,"x-b":2,"type":"string","name":"tag"}],"namespace":"acme","name":"User","type":"record"}"#,
        // full name instead of name + namespace
        r#"{"type":"record","name":"acme.User","fields":[{"name":"id","type":"long"},{"name":"tag","type":"string","x-a":1,"x-b":2}]}"#,
        // primitive written as an object
        r#"{"type":"record","name":"User","namespace":"acme","fields":[{"name":"id","type":{"type":"long"}},{"name":"tag","type":"string","x-a":1,"x-b":2}]}"#,
    ], false);
    assert!(got.iter().all(|i| *i == got[0]), "{got:?}");
}

#[test]
fn avro_differences_canonicalization_keeps_get_new_ids() {
    let (r, _d) = registry();
    let base = reg_as(&r, "a0", "AVRO", AVRO_USER, false);
    // Custom property order is part of the canonical form (Confluent: new id).
    let props = r#"{"type":"record","name":"User","namespace":"acme","fields":[{"name":"id","type":"long"},{"name":"tag","type":"string","x-b":2,"x-a":1}]}"#;
    assert_ne!(reg_as(&r, "a1", "AVRO", props, false), base);
    // ...but not of the normalized one.
    assert_eq!(reg_as(&r, "a2", "AVRO", props, true), reg_as(&r, "a3", "AVRO", AVRO_USER, true));
    // A doc is a real difference.
    let doc = r#"{"type":"record","name":"User","namespace":"acme","doc":"x","fields":[{"name":"id","type":"long"},{"name":"tag","type":"string","x-a":1,"x-b":2}]}"#;
    assert_ne!(reg_as(&r, "a4", "AVRO", doc, false), base);
}

const JSON_OBJ: &str = r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},"required":["a"]}"#;
const JSON_REORDERED: &str = r#"{"required":["a"],"properties":{"b":{"type":"integer"},"a":{"type":"string"}},"type":"object"}"#;

#[test]
fn json_whitespace_is_ignored_key_order_is_not_unless_normalized() {
    let (r, _d) = registry();
    let pretty = "{\n  \"type\": \"object\",\n  \"properties\": {\n    \"a\": {\"type\": \"string\"},\n    \"b\": {\"type\": \"integer\"}\n  },\n  \"required\": [\"a\"]\n}";
    let plain = ids(&r, "JSON", &[JSON_OBJ, pretty, JSON_REORDERED], false);
    assert_eq!(plain[0], plain[1], "whitespace");
    assert_ne!(plain[0], plain[2], "key order is significant without normalize (Confluent)");
    let norm = ids(&r, "JSON", &[JSON_OBJ, JSON_REORDERED, pretty], true);
    assert!(norm.iter().all(|i| *i == norm[0]), "normalized: {norm:?}");
    // A normalized registration stores the normalized text, so it does not
    // share the id of an earlier non-normalized one (Confluent behaves the same).
    assert_ne!(norm[0], plain[0]);
}

const PROTO_USER: &str = "syntax = \"proto3\";\npackage acme;\n\nmessage User {\n  int64 id = 1;\n  Tag tag = 2;\n}\nmessage Tag {\n  string v = 1;\n}\n";

#[test]
fn protobuf_formatting_and_comments_are_ignored() {
    let (r, _d) = registry();
    let got = ids(&r, "PROTOBUF", &[
        PROTO_USER,
        "syntax=\"proto3\";package acme; // c\nmessage User{int64 id=1;Tag tag=2;} /* x */ message Tag{string v=1;}",
    ], false);
    assert_eq!(got[0], got[1]);
}

#[test]
fn protobuf_structural_spellings_share_an_id_only_when_normalized() {
    let field_order = "syntax = \"proto3\";\npackage acme;\nmessage User { Tag tag = 2; int64 id = 1; }\nmessage Tag { string v = 1; }\n";
    let qualified = "syntax = \"proto3\";\npackage acme;\nmessage User { int64 id = 1; .acme.Tag tag = 2; }\nmessage Tag { string v = 1; }\n";
    let message_order = "syntax = \"proto3\";\npackage acme;\nmessage Tag { string v = 1; }\nmessage User { int64 id = 1; Tag tag = 2; }\n";
    let (r, _d) = registry();
    let plain = ids(&r, "PROTOBUF", &[PROTO_USER, field_order, qualified], false);
    assert!(plain[1] != plain[0] && plain[2] != plain[0], "{plain:?}");
    let norm = ids(&r, "PROTOBUF", &[PROTO_USER, field_order, qualified, message_order], true);
    assert_eq!(norm[0], norm[1], "field order");
    assert_eq!(norm[0], norm[2], "qualified type names");
    // Message order is kept by normalization, so this stays a different schema.
    assert_ne!(norm[0], norm[3], "message order");
}

#[test]
fn normalize_in_config_applies_to_every_registration() {
    let (r, _d) = registry();
    r.set_config(None, crate::model::ConfigRecord { normalize: Some(true), ..Default::default() }).unwrap();
    assert_eq!(reg_as(&r, "j1", "JSON", JSON_OBJ, false), reg_as(&r, "j2", "JSON", JSON_REORDERED, false));
}

#[test]
fn equal_schema_again_in_the_same_subject_is_the_same_version() {
    let (r, _d) = registry();
    let pretty = "{ \"type\" : \"object\", \"properties\" : { \"a\":{\"type\":\"string\"}, \"b\":{\"type\":\"integer\"} }, \"required\" : [\"a\"] }";
    let id = reg_as(&r, "s", "JSON", JSON_OBJ, false);
    assert_eq!(reg_as(&r, "s", "JSON", pretty, false), id);
    assert_eq!(r.list_versions("s", false, false).unwrap(), vec![1]);
}

#[test]
fn lookup_finds_a_differently_formatted_equal_schema() {
    let (r, _d) = registry();
    reg_as(&r, "p", "PROTOBUF", PROTO_USER, false);
    let compact = RegisterSchemaRequest {
        schema: Some("syntax=\"proto3\";package acme;message User{int64 id=1;Tag tag=2;}message Tag{string v=1;}".into()),
        schema_type: Some("PROTOBUF".into()),
        ..Default::default()
    };
    assert_eq!(r.lookup("p", compact, false, false, None).unwrap().version, 1);
    let spaced = RegisterSchemaRequest { schema: Some(format!("  {AVRO_USER}  ")), ..Default::default() };
    reg_as(&r, "a", "AVRO", AVRO_USER, false);
    assert_eq!(r.lookup("a", spaced, false, false, None).unwrap().version, 1);
}

#[test]
fn equal_schemas_in_different_contexts_get_independent_ids() {
    let (r, _d) = registry();
    reg_as(&r, "pad", "AVRO", r#""string""#, false);
    let default_ctx = reg_as(&r, "u", "AVRO", AVRO_USER, false);
    let other_ctx = reg_as(&r, ":.other:u", "AVRO", AVRO_USER, false);
    assert_eq!((default_ctx, other_ctx), (2, 1));
}

#[test]
fn metadata_is_part_of_schema_identity() {
    let (r, _d) = registry();
    let plain = reg_as(&r, "m1", "AVRO", AVRO_USER, false);
    let with_meta = RegisterSchemaRequest {
        schema: Some(AVRO_USER.into()),
        metadata: Some(serde_json::json!({"properties": {"owner": "team-a"}})),
        ..Default::default()
    };
    assert_ne!(r.register("m2", with_meta, false).unwrap().id, plain);
}

#[test]
fn invalid_schema_error_uses_confluents_envelope() {
    let (r, _d) = registry();
    let bad = RegisterSchemaRequest {
        schema: Some(r#"{"type":"record","name":"R","fields":[{"name":"a","type":"X"}]}"#.into()),
        references: Some(vec![SchemaReference { name: "X".into(), subject: "nope".into(), version: 1 }.into()]),
        ..Default::default()
    };
    let e = r.register("bad-ref", bad, false).unwrap_err();
    assert_eq!(e.code, 42201);
    assert_eq!(
        e.message,
        "Invalid schema {subject=bad-ref,version=0,id=-1,schemaType=AVRO,references=[{name='X', subject='nope', version=1}],\
         metadata=null,ruleSet=null,schema={\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"X\"}]},schemaTags=null} \
         with refs [{name='X', subject='nope', version=1}] of type AVRO, details: No schema reference found for subject \"nope\" and version 1"
    );
}

// ===========================================================================
// Our default: normalize on (a deliberate departure from Confluent's default)
// ===========================================================================

fn registry_normalizing() -> (Registry, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(dir.path(), false).expect("open store");
    let snap = store.load_snapshot().expect("snapshot");
    (Registry::new(store, snap, CompatibilityLevel::Backward, "test".into(), 1000, true), dir)
}

#[test]
fn by_default_logically_equal_schemas_share_an_id_in_every_format() {
    let (r, _d) = registry_normalizing();
    // JSON: key order and whitespace.
    let json = ids(&r, "JSON", &[JSON_OBJ, JSON_REORDERED, "{ \"required\":[\"a\"], \"type\":\"object\", \"properties\":{\"b\":{\"type\":\"integer\"},\"a\":{\"type\":\"string\"}} }"], false);
    assert!(json.iter().all(|i| *i == json[0]), "json {json:?}");
    // Protobuf: field order, qualified type names, comments, formatting.
    let proto = ids(&r, "PROTOBUF", &[
        PROTO_USER,
        "syntax = \"proto3\";\npackage acme;\nmessage User { Tag tag = 2; int64 id = 1; }\nmessage Tag { string v = 1; }\n",
        "syntax=\"proto3\";package acme;message User{int64 id=1;.acme.Tag tag=2;} // c\nmessage Tag{string v=1;}",
    ], false);
    assert!(proto.iter().all(|i| *i == proto[0]), "proto {proto:?}");
    // Avro: property order, attribute order, full names.
    let avro = ids(&r, "AVRO", &[
        AVRO_USER,
        r#"{"fields":[{"type":"long","name":"id"},{"x-b":2,"x-a":1,"type":"string","name":"tag"}],"name":"acme.User","type":"record"}"#,
    ], false);
    assert!(avro.iter().all(|i| *i == avro[0]), "avro {avro:?}");
    // Real differences still get their own ids.
    assert_ne!(reg_as(&r, "other", "JSON", r#"{"type":"object","properties":{"a":{"type":"string"}}}"#, false), json[0]);
}

#[test]
fn by_default_the_config_reports_normalization() {
    let (r, _d) = registry_normalizing();
    assert_eq!(r.get_config(None, false).unwrap().normalize, Some(true));
    // A subject can still opt out, which gives Confluent's default behaviour there.
    r.set_config(Some("plain-1"), crate::model::ConfigRecord { normalize: Some(false), ..Default::default() }).unwrap();
    r.set_config(Some("plain-2"), crate::model::ConfigRecord { normalize: Some(false), ..Default::default() }).unwrap();
    assert_ne!(reg_as(&r, "plain-1", "JSON", JSON_OBJ, false), reg_as(&r, "plain-2", "JSON", JSON_REORDERED, false));
}

#[test]
fn by_default_lookup_matches_any_equal_spelling() {
    let (r, _d) = registry_normalizing();
    reg_as(&r, "s", "JSON", JSON_OBJ, false);
    let found = r
        .lookup("s", RegisterSchemaRequest { schema: Some(JSON_REORDERED.into()), schema_type: Some("JSON".into()), ..Default::default() }, false, false, None)
        .unwrap();
    assert_eq!(found.version, 1);
}

// ===========================================================================
// Registration semantics (tests/conformance/register_semantics.py)
// ===========================================================================

fn with_meta(schema: Option<&str>, k: &str, v: &str) -> RegisterSchemaRequest {
    RegisterSchemaRequest {
        schema: schema.map(String::from),
        metadata: Some(json!({"properties": {k: v}})),
        ..Default::default()
    }
}

fn resp(r: &Registry, subject: &str, req: RegisterSchemaRequest) -> serde_json::Value {
    serde_json::to_value(r.register(subject, req, false).unwrap()).unwrap()
}

#[test]
fn register_response_shape_follows_confluent() {
    let (r, _d) = registry();
    let full = json!({"id": 1, "version": 1, "metadata": {"properties": {"k": "v"}}, "schema": "\"string\""});
    // New registration carrying metadata: the full entity.
    assert_eq!(resp(&r, "s", with_meta(Some(r#""string""#), "k", "v")), full);
    // Exact repeat: just the id.
    assert_eq!(resp(&r, "s", with_meta(Some(r#""string""#), "k", "v")), json!({"id": 1}));
    // Repeat without metadata inherits it, which counts as modified: full entity again.
    assert_eq!(resp(&r, "s", req(r#""string""#.into())), full);
    // Plain schema without any metadata anywhere: id only.
    assert_eq!(resp(&r, "plain", req(r#""double""#.into())), json!({"id": 2}));
}

#[test]
fn different_metadata_is_a_new_version_and_id() {
    let (r, _d) = registry();
    resp(&r, "s", with_meta(Some(r#""string""#), "k", "v"));
    assert_eq!(
        resp(&r, "s", with_meta(Some(r#""string""#), "k", "v2")),
        json!({"id": 2, "version": 2, "metadata": {"properties": {"k": "v2"}}, "schema": "\"string\""})
    );
}

#[test]
fn metadata_only_request_creates_a_version_from_the_previous_schema() {
    let (r, _d) = registry();
    resp(&r, "s", with_meta(Some(r#""string""#), "k", "v"));
    assert_eq!(
        resp(&r, "s", with_meta(None, "k", "v3")),
        json!({"id": 2, "version": 2, "metadata": {"properties": {"k": "v3"}}, "schema": "\"string\""})
    );
    let e = r.register("empty", with_meta(None, "k", "v"), false).unwrap_err();
    assert_eq!((e.code, e.message.as_str()), (42201, "Empty schema"));
}

#[test]
fn explicit_version_outside_import_must_be_the_next_one() {
    let (r, _d) = registry();
    r.set_config(Some("s"), crate::model::ConfigRecord { compatibility_level: Some(CompatibilityLevel::None), ..Default::default() }).unwrap();
    let v = |schema: &str, version: Option<i32>| RegisterSchemaRequest { schema: Some(schema.into()), version, ..Default::default() };
    assert_eq!(
        resp(&r, "s", v(r#""int""#, Some(1))),
        json!({"id": 1, "version": 1, "metadata": {"properties": {"confluent:version": "1"}}, "schema": "\"int\""})
    );
    let e = r.register("s", v(r#""long""#, Some(5)), false).unwrap_err();
    assert_eq!((e.code, e.message.as_str()), (42201, "Version is not one more than previous version"));
    assert_eq!(resp(&r, "s", v(r#""long""#, Some(2)))["metadata"], json!({"properties": {"confluent:version": "2"}}));
    // Once present, `confluent:version` keeps tracking the version.
    assert_eq!(resp(&r, "s", v(r#""float""#, None))["metadata"], json!({"properties": {"confluent:version": "3"}}));
}

#[test]
fn reregistering_a_soft_deleted_schema_purges_the_old_version() {
    let (r, _d) = registry();
    r.set_config(Some("s"), crate::model::ConfigRecord { compatibility_level: Some(CompatibilityLevel::None), ..Default::default() }).unwrap();
    let v1 = reg_as(&r, "s", "AVRO", r#""double""#, false);
    reg_as(&r, "s", "AVRO", r#""boolean""#, false);
    r.delete_version("s", crate::registry::VersionSpec::Exact(1), false).unwrap();
    assert_eq!(reg_as(&r, "s", "AVRO", r#""double""#, false), v1);
    assert_eq!(r.list_versions("s", true, false).unwrap(), vec![2, 3]);
}

#[test]
fn emptying_a_subject_drops_its_config_and_mode() {
    let (r, _d) = registry();
    let none = || crate::model::ConfigRecord { compatibility_level: Some(CompatibilityLevel::None), ..Default::default() };
    // Last live version soft-deleted through the version endpoint...
    r.set_config(Some("a"), none()).unwrap();
    r.set_mode(Some("a"), Mode::Readwrite, false).unwrap();
    reg_as(&r, "a", "AVRO", r#""int""#, false);
    reg_as(&r, "a", "AVRO", r#""string""#, false);
    r.delete_version("a", crate::registry::VersionSpec::Exact(1), false).unwrap();
    assert!(r.get_config(Some("a"), false).is_ok(), "a live version remains: config stays");
    r.delete_version("a", crate::registry::VersionSpec::Exact(2), false).unwrap();
    assert_eq!(r.get_config(Some("a"), false).unwrap_err().code, 40408);
    assert_eq!(r.get_mode(Some("a"), false).unwrap_err().code, 40409);
    // ...or through the subject endpoint.
    r.set_config(Some("b"), none()).unwrap();
    reg_as(&r, "b", "AVRO", r#""int""#, false);
    r.delete_subject("b", false).unwrap();
    assert_eq!(r.get_config(Some("b"), false).unwrap_err().code, 40408);
}

// ===========================================================================
// Listing (tests/conformance/listing.py)
// ===========================================================================

#[test]
fn schema_listing_with_aliases() {
    let (r, _d) = registry();
    reg_as(&r, "lst-a", "AVRO", r#""string""#, false);
    reg_as(&r, "other-target", "AVRO", r#""bytes""#, false);
    for s in ["lst-alias2", "lst-alias"] {
        r.set_config(Some(s), crate::model::ConfigRecord { alias: Some("other-target".into()), ..Default::default() }).unwrap();
    }
    let rows = |aliases: bool| -> Vec<(String, Option<Vec<String>>)> {
        r.list_schemas(Some("lst-"), false, false, aliases, None).unwrap().into_iter().map(|v| (v.subject, v.aliases)).collect()
    };
    // Alias targets outside the prefix are included, with their aliases sorted.
    assert_eq!(
        rows(true),
        vec![("lst-a".into(), None), ("other-target".into(), Some(vec!["lst-alias".into(), "lst-alias2".into()]))]
    );
    assert_eq!(rows(false), vec![("lst-a".into(), None)]);
}

#[test]
fn schema_listing_rule_type_filter_uses_domain_and_migration_rules() {
    let (r, _d) = registry();
    let with_rules = |key: &str, ty: &str| RegisterSchemaRequest {
        schema: Some(r#""string""#.into()),
        rule_set: Some(json!({key: [{"name": "r", "kind": "CONDITION", "mode": "WRITE", "type": ty, "expr": "true"}]})),
        ..Default::default()
    };
    r.register("lst-d", with_rules("domainRules", "CEL"), false).unwrap();
    r.register("lst-m", with_rules("migrationRules", "JSONATA"), false).unwrap();
    r.register("lst-e", with_rules("encodingRules", "ENCRYPT"), false).unwrap();
    let only = |t: &str| -> Vec<String> {
        r.list_schemas(Some("lst-"), false, false, false, Some(t)).unwrap().into_iter().map(|v| v.subject).collect()
    };
    assert_eq!(only("CEL"), vec!["lst-d".to_string()]);
    assert_eq!(only("JSONATA"), vec!["lst-m".to_string()]);
    assert!(only("ENCRYPT").is_empty(), "encoding rules are not searched");
}

#[test]
fn search_limits_follow_confluents_normalize_limit() {
    use crate::registry::SearchLimits;
    assert_eq!(SearchLimits::normalize(-1, 1000, 1000), 1000);
    assert_eq!(SearchLimits::normalize(0, 1000, 1000), 1000);
    assert_eq!(SearchLimits::normalize(2, 1000, 1000), 2);
    // Above the max falls back to the default, not to the max.
    assert_eq!(SearchLimits::normalize(5000, 20, 1000), 20);
}

#[test]
fn serialized_protobuf_root_file_is_named_default() {
    use base64::Engine;
    use prost::Message;
    let (r, _d) = registry();
    let id = reg_as(&r, "p", "PROTOBUF", "syntax = \"proto3\";\nmessage M { string a = 1; }\n", false);
    let b64 = r.get_schema_by_id(id.into(), None, false, Some("serialized")).unwrap().schema;
    let fd = prost_types::FileDescriptorProto::decode(base64::engine::general_purpose::STANDARD.decode(b64).unwrap().as_slice()).unwrap();
    assert_eq!(fd.name(), "default");
}

// ---------------- admin views ----------------

/// A backward-compatible second version of `rec(name)`.
fn evolve(r: &Registry, subject: &str, name: &str) -> u32 {
    let schema = json!({"type": "record", "name": name,
                        "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "int", "default": 0}]});
    r.register(subject, req(schema.to_string()), false).expect("evolve").id
}

#[test]
fn admin_overview_reports_subjects_contexts_and_where_settings_come_from() {
    let (r, _d) = registry();
    register(&r, "a-value", "A");
    evolve(&r, "a-value", "A");
    register(&r, ":.eu:b-value", "B");
    register(&r, "gone-value", "G");
    r.delete_subject("gone-value", false).unwrap();
    let level = |l| crate::model::ConfigRecord { compatibility_level: Some(l), ..Default::default() };
    r.set_config(Some("a-value"), level(CompatibilityLevel::Full)).unwrap();
    r.set_config(Some(":.eu:"), level(CompatibilityLevel::None)).unwrap();

    let v = r.admin_overview(None, true, 100, &|_, _| true).unwrap();
    let rows = v["subjects"].as_array().unwrap();
    let row = |s: &str| rows.iter().find(|x| x["subject"] == s).unwrap_or_else(|| panic!("{s} missing"));
    assert_eq!(v["counts"]["subjects"], 3);
    assert_eq!(v["counts"]["versions"], 4);
    assert_eq!(row("a-value")["versions"], 2);
    assert_eq!(row("a-value")["compatibility"], "FULL");
    assert_eq!(row("a-value")["compatibilityFrom"], "subject");
    // A context's own config is where its subjects read theirs from.
    assert_eq!(row(":.eu:b-value")["compatibility"], "NONE");
    assert_eq!(row(":.eu:b-value")["compatibilityFrom"], "context");
    assert_eq!(row("gone-value")["deleted"], true);
    assert_eq!(row("gone-value")["deletedVersions"], 1);

    // Live subjects only, unless deleted rows are asked for.
    let live = r.admin_overview(None, false, 100, &|_, _| true).unwrap();
    assert!(live["subjects"].as_array().unwrap().iter().all(|x| x["subject"] != "gone-value"));

    let contexts = v["contexts"].as_array().unwrap();
    let eu = contexts.iter().find(|c| c["name"] == ".eu").unwrap();
    assert_eq!(eu["subjects"], 1);
    assert_eq!(eu["compatibility"], "NONE");
    assert_eq!(eu["compatibilityFrom"], "own");
    let default_ctx = contexts.iter().find(|c| c["name"] == ".").unwrap();
    assert_eq!(default_ctx["subjects"], 1, "the soft-deleted subject is counted separately");
    assert_eq!(default_ctx["deletedSubjects"], 1);

    // `limit` caps the rows, not the counts.
    let capped = r.admin_overview(None, true, 1, &|_, _| true).unwrap();
    assert_eq!(capped["counts"]["shown"], 1);
    assert_eq!(capped["counts"]["subjects"], 3);
}

#[test]
fn admin_subject_shows_every_version_and_404s_for_unknown_subjects() {
    let (r, _d) = registry();
    register(&r, "s-value", "S");
    evolve(&r, "s-value", "S");
    r.delete_version("s-value", crate::registry::VersionSpec::Exact(1), false).unwrap();

    let v = r.admin_subject("s-value").unwrap();
    let versions = v["versions"].as_array().unwrap();
    assert_eq!(versions.len(), 2, "soft-deleted versions are shown too");
    assert_eq!(versions[0]["version"], 1);
    assert_eq!(versions[0]["deleted"], true);
    assert_eq!(versions[1]["deleted"], false);
    assert!(versions[1]["schema"].as_str().unwrap().contains("\"b\""), "v2 is the evolved schema");

    assert_eq!(r.admin_subject("nope").unwrap_err().code, 40401);
}
