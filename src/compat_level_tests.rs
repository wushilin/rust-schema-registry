//! Compatibility levels end to end, one readable example per behaviour.
//!
//! Each case is a version history (oldest first). The earlier versions are
//! registered with the subject at NONE; then the subject is switched to the
//! level under test and the last version is (1) checked with the
//! compatibility endpoint and (2) registered. Both must agree with the
//! expected verdict, which was taken from a live Confluent 7.9.0 checker
//! (the same chains are pinned message-for-message in `golden_tests`).
//!
//! Expectation strings list the 7 levels in this order, `Y` = accepted:
//!
//!   NONE  BACKWARD  BACKWARD_TRANSITIVE  FORWARD  FORWARD_TRANSITIVE  FULL  FULL_TRANSITIVE

use tempfile::TempDir;

use crate::model::{CompatibilityLevel, ConfigRecord, RegisterSchemaRequest};
use crate::registry::Registry;
use crate::store::Store;

const LEVELS: [CompatibilityLevel; 7] = [
    CompatibilityLevel::None,
    CompatibilityLevel::Backward,
    CompatibilityLevel::BackwardTransitive,
    CompatibilityLevel::Forward,
    CompatibilityLevel::ForwardTransitive,
    CompatibilityLevel::Full,
    CompatibilityLevel::FullTransitive,
];

fn registry() -> (Registry, TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::open(dir.path(), false).expect("open store");
    let snap = store.load_snapshot().expect("snapshot");
    (Registry::new(store, snap, CompatibilityLevel::Backward, "test".into(), 100, false), dir)
}

fn req(schema_type: &str, schema: &str) -> RegisterSchemaRequest {
    RegisterSchemaRequest {
        schema: Some(schema.to_string()),
        schema_type: Some(schema_type.to_string()),
        ..Default::default()
    }
}

fn set_level(r: &Registry, subject: &str, level: CompatibilityLevel) {
    r.set_config(Some(subject), ConfigRecord { compatibility_level: Some(level), ..Default::default() }).expect("set config");
}

/// Run one history at every level and compare with `expect` (7 × Y/N).
fn check(name: &str, versions: &[(&str, &str)], expect: &str) {
    let expect: Vec<bool> = expect.split_whitespace().map(|x| x == "Y").collect();
    assert_eq!(expect.len(), 7, "{name}: expectation needs 7 entries");
    let (last_type, last) = versions.last().unwrap();
    for (level, want) in LEVELS.iter().zip(&expect) {
        let (r, _dir) = registry();
        let subject = "s";
        set_level(&r, subject, CompatibilityLevel::None);
        for (t, v) in &versions[..versions.len() - 1] {
            r.register(subject, req(t, v), false).unwrap_or_else(|e| panic!("{name}: history rejected: {e}"));
        }
        set_level(&r, subject, *level);

        let msgs = r.test_compatibility(subject, None, req(last_type, last), false, false).expect("compatibility check runs");
        assert_eq!(msgs.is_empty(), *want, "{name} @ {}: compatibility endpoint said {msgs:?}", level.as_str());

        let registered = r.register(subject, req(last_type, last), false);
        match (registered, want) {
            (Ok(_), true) => {}
            (Err(e), false) => {
                assert_eq!(e.code, 409, "{name} @ {}: wrong error {e}", level.as_str());
                assert!(e.message.contains(&format!("compatibility: '{}'", level.as_str())), "{name}: {}", e.message);
            }
            (Ok(_), false) => panic!("{name} @ {}: registration should have been rejected", level.as_str()),
            (Err(e), true) => panic!("{name} @ {}: registration should have succeeded: {e}", level.as_str()),
        }
    }
}

// ---------------------------------------------------------------------------
// Avro
// ---------------------------------------------------------------------------

const AVRO_A_INT: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}"#;

#[test]
fn avro_add_optional_field_is_compatible_everywhere() {
    check(
        "avro add optional field",
        &[("AVRO", AVRO_A_INT), ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string","default":"x"}]}"#)],
        "Y Y Y Y Y Y Y",
    );
}

#[test]
fn avro_add_required_field_breaks_backward_only() {
    // New readers can't fill `b` from old data; old readers just ignore it.
    check(
        "avro add required field",
        &[("AVRO", AVRO_A_INT), ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#)],
        "Y N N Y Y N N",
    );
}

#[test]
fn avro_remove_required_field_breaks_forward_only() {
    // Old readers require `b`, which new data no longer has.
    check(
        "avro remove required field",
        &[("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#), ("AVRO", AVRO_A_INT)],
        "Y Y Y N N N N",
    );
}

#[test]
fn avro_int_to_long_is_a_one_way_promotion() {
    check("avro int -> long", &[("AVRO", AVRO_A_INT), ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}"#)], "Y Y Y N N N N");
}

#[test]
fn avro_int_to_string_only_passes_none() {
    check("avro int -> string", &[("AVRO", AVRO_A_INT), ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}"#)], "Y N N N N N N");
}

#[test]
fn avro_enum_symbol_added_needs_a_reader_default_for_forward() {
    check(
        "avro enum add symbol",
        &[
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}}]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"]}}]}"#),
        ],
        "Y Y Y N N N N",
    );
    check(
        "avro enum add symbol, old default",
        &[
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}}]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"e","type":{"type":"enum","name":"E","symbols":["A","B","C"],"default":"A"}}]}"#),
        ],
        "Y Y Y Y Y Y Y",
    );
}

#[test]
fn avro_backward_transitive_sees_the_whole_history() {
    // v1 a:int -> v2 drops a -> v3 brings a back as string (with a default).
    // v3 reads v2 fine, but can't read v1's int.
    check(
        "avro backward gap",
        &[
            ("AVRO", AVRO_A_INT),
            ("AVRO", r#"{"type":"record","name":"R","fields":[]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string","default":""}]}"#),
        ],
        "Y Y N Y N Y N",
    );
}

#[test]
fn avro_forward_transitive_sees_the_whole_history() {
    // v2 (no fields) can read v3; v1 (x:int) can't read v3's string x.
    check(
        "avro forward gap",
        &[
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":0}]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"x","type":"string"}]}"#),
        ],
        "Y N N Y N N N",
    );
}

#[test]
fn avro_full_transitive_sees_the_whole_history() {
    check(
        "avro full gap",
        &[
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":0}]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[]}"#),
            ("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"x","type":"string","default":""}]}"#),
        ],
        "Y Y N Y N Y N",
    );
}

#[test]
fn changing_schema_type_is_incompatible_except_under_none() {
    check("avro -> json", &[("AVRO", AVRO_A_INT), ("JSON", r#"{"type":"object","properties":{"a":{"type":"integer"}}}"#)], "Y N N N N N N");
}

// ---------------------------------------------------------------------------
// JSON Schema
// ---------------------------------------------------------------------------

#[test]
fn json_closed_model_add_property_breaks_forward() {
    // Old readers are closed and reject the new property.
    check(
        "json closed add property",
        &[
            ("JSON", r#"{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}"#),
            ("JSON", r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},"additionalProperties":false}"#),
        ],
        "Y Y Y N N N N",
    );
}

#[test]
fn json_open_model_add_property_breaks_backward() {
    // Old data may already carry `b` with any value.
    check(
        "json open add property",
        &[
            ("JSON", r#"{"type":"object","properties":{"a":{"type":"string"}}}"#),
            ("JSON", r#"{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}}}"#),
        ],
        "Y N N Y Y N N",
    );
}

#[test]
fn json_new_required_property_breaks_backward() {
    check(
        "json add required",
        &[
            ("JSON", r#"{"type":"object","properties":{"a":{"type":"string"}}}"#),
            ("JSON", r#"{"type":"object","properties":{"a":{"type":"string"}},"required":["a"]}"#),
        ],
        "Y N N Y Y N N",
    );
}

#[test]
fn json_integer_to_number_widens() {
    check("json integer -> number", &[("JSON", r#"{"type":"integer"}"#), ("JSON", r#"{"type":"number"}"#)], "Y Y Y N N N N");
}

#[test]
fn json_description_change_is_always_fine() {
    check(
        "json description only",
        &[("JSON", r#"{"type":"string","description":"v1"}"#), ("JSON", r#"{"type":"string","description":"v2"}"#)],
        "Y Y Y Y Y Y Y",
    );
}

#[test]
fn json_transitive_levels_see_the_whole_history() {
    let a = |t: &str| format!(r#"{{"type":"object","properties":{{"a":{{"type":"{t}"}}}}}}"#);
    let (s, i, n) = (a("string"), a("integer"), a("number"));
    check("json backward gap", &[("JSON", &s), ("JSON", &i), ("JSON", &n)], "Y Y N N N N N");
    check("json forward gap", &[("JSON", &s), ("JSON", &n), ("JSON", &i)], "Y N N Y N N N");
    let i2 = r#"{"type":"object","properties":{"a":{"type":"integer","description":"still an integer"}}}"#;
    check("json full gap", &[("JSON", &s), ("JSON", &i), ("JSON", i2)], "Y Y N Y N Y N");
}

// ---------------------------------------------------------------------------
// Protobuf
// ---------------------------------------------------------------------------

fn pb(body: &str) -> String {
    format!("syntax = \"proto3\";\npackage p;\n{body}\n")
}

#[test]
fn proto_add_and_remove_field_are_compatible_everywhere() {
    let one = pb("message A { string a = 1; }");
    let two = pb("message A { string a = 1; int32 b = 2; }");
    check("proto add field", &[("PROTOBUF", &one), ("PROTOBUF", &two)], "Y Y Y Y Y Y Y");
    check("proto remove field", &[("PROTOBUF", &two), ("PROTOBUF", &one)], "Y Y Y Y Y Y Y");
}

#[test]
fn proto_wire_compatible_scalars_pass_incompatible_ones_fail() {
    let i32_ = pb("message A { int32 a = 1; }");
    check("proto int32 -> int64", &[("PROTOBUF", &i32_), ("PROTOBUF", &pb("message A { int64 a = 1; }"))], "Y Y Y Y Y Y Y");
    check("proto int32 -> string", &[("PROTOBUF", &i32_), ("PROTOBUF", &pb("message A { string a = 1; }"))], "Y N N N N N N");
}

#[test]
fn proto_removing_a_message_breaks_backward() {
    check(
        "proto remove message",
        &[("PROTOBUF", &pb("message A { string a = 1; }\nmessage B { string b = 1; }")), ("PROTOBUF", &pb("message A { string a = 1; }"))],
        "Y N N Y Y N N",
    );
}

#[test]
fn proto2_new_required_field_breaks_both_directions() {
    check(
        "proto2 add required",
        &[
            ("PROTOBUF", "syntax = \"proto2\";\npackage p;\nmessage A { optional string a = 1; }\n"),
            ("PROTOBUF", "syntax = \"proto2\";\npackage p;\nmessage A { optional string a = 1; required int32 b = 2; }\n"),
        ],
        "Y N N N N N N",
    );
}

#[test]
fn proto_moving_several_fields_into_a_new_oneof_breaks_backward() {
    check(
        "proto multiple into oneof",
        &[("PROTOBUF", &pb("message A { string a = 1; int32 b = 2; }")), ("PROTOBUF", &pb("message A { oneof k { string a = 1; int32 b = 2; } }"))],
        "Y N N Y Y N N",
    );
}

#[test]
fn proto_package_change_only_passes_none() {
    check(
        "proto package change",
        &[
            ("PROTOBUF", "syntax = \"proto3\";\npackage p;\nmessage A { string a = 1; }\n"),
            ("PROTOBUF", "syntax = \"proto3\";\npackage q;\nmessage A { string a = 1; }\n"),
        ],
        "Y N N N N N N",
    );
}

#[test]
fn proto_transitive_levels_see_the_whole_history() {
    // string -> int32 happened under NONE; int32 -> int64 is fine on its own.
    check(
        "proto transitive gap",
        &[
            ("PROTOBUF", &pb("message A { string x = 1; }")),
            ("PROTOBUF", &pb("message A { int32 x = 1; }")),
            ("PROTOBUF", &pb("message A { int64 x = 1; }")),
        ],
        "Y Y N Y N Y N",
    );
}

// ---------------------------------------------------------------------------
// History selection
// ---------------------------------------------------------------------------

#[test]
fn soft_deleted_versions_are_ignored_by_transitive_checks() {
    let (r, _dir) = registry();
    set_level(&r, "s", CompatibilityLevel::None);
    r.register("s", req("AVRO", AVRO_A_INT), false).unwrap();
    r.register("s", req("AVRO", r#"{"type":"record","name":"R","fields":[]}"#), false).unwrap();
    set_level(&r, "s", CompatibilityLevel::BackwardTransitive);
    let v3 = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string","default":""}]}"#;
    assert!(!r.test_compatibility("s", None, req("AVRO", v3), false, false).unwrap().is_empty(), "v1 still counts");
    r.delete_version("s", crate::registry::VersionSpec::Exact(1), false).unwrap();
    assert!(r.test_compatibility("s", None, req("AVRO", v3), false, false).unwrap().is_empty(), "soft-deleted v1 no longer counts");
}

#[test]
fn checking_against_a_specific_version_ignores_transitivity() {
    let (r, _dir) = registry();
    set_level(&r, "s", CompatibilityLevel::None);
    r.register("s", req("AVRO", AVRO_A_INT), false).unwrap();
    r.register("s", req("AVRO", r#"{"type":"record","name":"R","fields":[]}"#), false).unwrap();
    set_level(&r, "s", CompatibilityLevel::BackwardTransitive);
    let v3 = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string","default":""}]}"#;
    let against = |v: u32| r.test_compatibility("s", Some(crate::registry::VersionSpec::Exact(v)), req("AVRO", v3), false, false).unwrap();
    assert!(!against(1).is_empty());
    assert!(against(2).is_empty());
}

#[test]
fn incompatible_message_carries_confluent_trailers() {
    let (r, _dir) = registry();
    r.register("s", req("AVRO", AVRO_A_INT), false).unwrap();
    let bad = r#"{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}"#;
    let msgs = r.test_compatibility("s", None, req("AVRO", bad), false, false).unwrap();
    assert!(msgs[0].contains("READER_FIELD_MISSING_DEFAULT_VALUE"), "{msgs:?}");
    assert_eq!(msgs[msgs.len() - 3], "{oldSchemaVersion: 1}");
    assert!(msgs[msgs.len() - 2].starts_with("{oldSchema: '"));
    assert_eq!(msgs[msgs.len() - 1], "{validateFields: 'false', compatibility: 'BACKWARD'}");
}

#[test]
fn full_reports_forward_problems_before_backward_ones() {
    // Changing int -> string fails both ways; Confluent lists the forward
    // (old reads new) problem first and only the backward part gets trailers.
    let (r, _dir) = registry();
    set_level(&r, "s", CompatibilityLevel::Full);
    r.register("s", req("AVRO", AVRO_A_INT), false).unwrap();
    let msgs = r
        .test_compatibility("s", None, req("AVRO", r#"{"type":"record","name":"R","fields":[{"name":"a","type":"string"}]}"#), false, false)
        .unwrap();
    assert!(msgs[0].contains("in the old schema does not match with the new schema"), "{msgs:?}");
    assert!(msgs[1].contains("in the new schema does not match with the old schema"), "{msgs:?}");
    assert_eq!(msgs[2], "{oldSchemaVersion: 1}");
}
