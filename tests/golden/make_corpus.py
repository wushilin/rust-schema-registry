#!/usr/bin/env python3
"""Generate tests/golden/corpus.json: schemas and evolution pairs to run through
Confluent's libraries (tests/oracle) and then through ours (src/golden_tests.rs).

Regenerate the golden file after editing:
    python3 tests/golden/make_corpus.py
    (cd tests/oracle && mvn -q compile dependency:build-classpath -Dmdep.outputFile=cp.txt \
        && java -cp target/classes:$(cat cp.txt) oracle.Oracle ../golden/corpus.json ../golden/golden.json)
"""

import json
import os

j = json.dumps
schemas = []
compat = []
chains = []


def S(id, type, schema, refs=None):
    schemas.append({"id": id, "type": type, "schema": schema if isinstance(schema, str) else j(schema), "refs": refs or []})


def CH(id, type, *versions, types=None):
    """A version history (oldest first); the last version is checked against the rest at every level."""
    chains.append({"id": id, "types": types or [type] * len(versions),
                   "versions": [v if isinstance(v, str) else j(v) for v in versions]})


def C(id, type, old, new, old_refs=None, new_refs=None):
    compat.append({
        "id": id, "type": type,
        "old": old if isinstance(old, str) else j(old),
        "new": new if isinstance(new, str) else j(new),
        "old_refs": old_refs or [], "new_refs": new_refs or [],
    })


# ---------------------------------------------------------------------------
# Avro: canonical form & validity
# ---------------------------------------------------------------------------
rec = lambda name, fields, **kw: {"type": "record", "name": name, "fields": fields, **kw}
f = lambda name, type, **kw: {"name": name, "type": type, **kw}

S("avro-prim-string", "AVRO", '"string"')
S("avro-prim-object", "AVRO", {"type": "string"})
S("avro-prim-spaces", "AVRO", '  "long"  ')
S("avro-prim-logical", "AVRO", {"type": "int", "logicalType": "date"})
S("avro-prim-unknown-logical", "AVRO", {"type": "string", "logicalType": "my-thing"})
S("avro-record-pretty", "AVRO", json.dumps(rec("R", [f("a", "int")]), indent=4))
S("avro-record-key-order", "AVRO", '{"fields":[{"type":"int","name":"a"}],"name":"R","type":"record"}')
S("avro-record-namespace", "AVRO", rec("R", [f("a", "int")], namespace="com.acme"))
S("avro-record-fullname", "AVRO", rec("com.acme.R", [f("a", "int")]))
S("avro-record-fullname-and-namespace", "AVRO", rec("com.acme.R", [f("a", "int")], namespace="org.other"))
S("avro-nested-inherits-namespace", "AVRO", rec("Outer", [f("inner", rec("Inner", [f("x", "int")]))], namespace="a.b"))
S("avro-nested-other-namespace", "AVRO", rec("Outer", [f("inner", rec("Inner", [f("x", "int")], namespace="c.d"))], namespace="a.b"))
S("avro-nested-reuse-by-name", "AVRO", rec("Outer", [f("a", rec("Inner", [f("x", "int")])), f("b", "Inner")], namespace="n"))
S("avro-doc-aliases-order", "AVRO", rec("R", [f("a", "int", doc="field doc", aliases=["old_a"], order="descending")], doc="record doc", aliases=["OldR"]))
S("avro-defaults", "AVRO", rec("R", [
    f("i", "int", default=1), f("s", "string", default="x"), f("n", ["null", "string"], default=None),
    f("arr", {"type": "array", "items": "int"}, default=[1, 2]), f("m", {"type": "map", "values": "long"}, default={"k": 1}),
    f("b", "bytes", default="ÿ"), f("d", "double", default=1.5), f("bo", "boolean", default=True)]))
S("avro-record-default", "AVRO", rec("R", [f("inner", rec("I", [f("x", "int")]), default={"x": 3})]))
S("avro-custom-props", "AVRO", rec("R", [f("a", "int", **{"x-prop": "v", "connect.name": "z"})], **{"custom": {"k": [1, 2]}}))
S("avro-enum", "AVRO", {"type": "enum", "name": "E", "symbols": ["A", "B"], "doc": "d", "default": "A"})
S("avro-fixed", "AVRO", {"type": "fixed", "name": "F", "size": 16, "namespace": "n"})
S("avro-array-map", "AVRO", {"type": "array", "items": {"type": "map", "values": ["null", "int"]}})
S("avro-union-top", "AVRO", ["null", "string", rec("R", [f("a", "int")])])
S("avro-decimal-bytes", "AVRO", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2})
S("avro-decimal-fixed", "AVRO", {"type": "fixed", "name": "Dec", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 4})
S("avro-uuid", "AVRO", {"type": "string", "logicalType": "uuid"})
S("avro-timestamps", "AVRO", rec("T", [f("a", {"type": "long", "logicalType": "timestamp-millis"}),
                                        f("b", {"type": "long", "logicalType": "local-timestamp-micros"}),
                                        f("c", {"type": "int", "logicalType": "time-millis"})]))
S("avro-duration", "AVRO", {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"})
S("avro-recursive", "AVRO", rec("Node", [f("v", "int"), f("next", ["null", "Node"], default=None)]))
S("avro-unicode", "AVRO", rec("R", [f("a", "string", doc="café ☃")]))
S("avro-number-formats", "AVRO", rec("R", [f("d", "double", default=1.0), f("l", "long", default=10000000000)]))
# invalid
S("avro-bad-json", "AVRO", '{"type":')
S("avro-unknown-type", "AVRO", '"nosuchtype"')
S("avro-dup-field", "AVRO", rec("R", [f("a", "int"), f("a", "string")]))
S("avro-bad-enum-symbol", "AVRO", {"type": "enum", "name": "E", "symbols": ["1A"]})
S("avro-dup-enum-symbol", "AVRO", {"type": "enum", "name": "E", "symbols": ["A", "A"]})
S("avro-bad-default-int", "AVRO", rec("R", [f("a", "int", default="x")]))
S("avro-bad-default-union", "AVRO", rec("R", [f("a", ["null", "int"], default=1)]))
S("avro-union-dup", "AVRO", ["int", "int"])
S("avro-union-nested", "AVRO", ["null", ["int"]])
S("avro-record-no-name", "AVRO", {"type": "record", "fields": []})
S("avro-fields-not-array", "AVRO", {"type": "record", "name": "R", "fields": {}})
S("avro-bad-name", "AVRO", rec("1R", [f("a", "int")]))
S("avro-fixed-no-size", "AVRO", {"type": "fixed", "name": "F"})
S("avro-redefine-name", "AVRO", rec("R", [f("a", rec("X", [])), f("b", rec("X", []))]))
S("avro-unknown-ref", "AVRO", rec("R", [f("a", "Missing")]))
S("avro-enum-bad-default", "AVRO", {"type": "enum", "name": "E", "symbols": ["A"], "default": "Z"})
S("avro-empty-record", "AVRO", rec("R", []))
S("avro-bad-decimal", "AVRO", {"type": "bytes", "logicalType": "decimal", "precision": 2, "scale": 5})
S("avro-with-ref", "AVRO", rec("P", [f("addr", "com.acme.Address")], namespace="com.acme"),
  refs=[{"name": "com.acme.Address", "subject": "addr", "version": 1,
         "schema": j(rec("Address", [f("street", "string")], namespace="com.acme"))}])

# ---------------------------------------------------------------------------
# Avro: compatibility pairs (old -> new)
# ---------------------------------------------------------------------------
A = lambda *fields, name="R", **kw: rec(name, list(fields), **kw)
base = A(f("a", "int"), f("b", "string"))

C("avro-add-field-default", "AVRO", base, A(f("a", "int"), f("b", "string"), f("c", "int", default=0)))
C("avro-add-field-nodefault", "AVRO", base, A(f("a", "int"), f("b", "string"), f("c", "int")))
C("avro-add-field-null-union", "AVRO", base, A(f("a", "int"), f("b", "string"), f("c", ["null", "int"], default=None)))
C("avro-remove-field-had-default", "AVRO", A(f("a", "int"), f("b", "string", default="x")), A(f("a", "int")))
C("avro-remove-field-nodefault", "AVRO", base, A(f("a", "int")))
C("avro-field-order", "AVRO", base, A(f("b", "string"), f("a", "int")))
C("avro-doc-only", "AVRO", base, A(f("a", "int", doc="x"), f("b", "string"), doc="y"))
C("avro-default-change", "AVRO", A(f("a", "int", default=1)), A(f("a", "int", default=2)))
for old_t, new_t in [("int", "long"), ("long", "int"), ("int", "float"), ("int", "double"), ("long", "float"),
                     ("long", "double"), ("float", "double"), ("double", "float"), ("string", "bytes"),
                     ("bytes", "string"), ("int", "string"), ("boolean", "int"), ("null", "int")]:
    C(f"avro-prom-{old_t}-{new_t}", "AVRO", A(f("a", old_t)), A(f("a", new_t)))
C("avro-rename-field-alias", "AVRO", A(f("a", "int")), A(f("z", "int", aliases=["a"])))
C("avro-rename-field-noalias", "AVRO", A(f("a", "int")), A(f("z", "int")))
C("avro-rename-record", "AVRO", A(f("a", "int"), name="R"), A(f("a", "int"), name="S"))
C("avro-rename-record-alias", "AVRO", A(f("a", "int"), name="R"), A(f("a", "int"), name="S", aliases=["R"]))
C("avro-change-namespace", "AVRO", A(f("a", "int"), namespace="x"), A(f("a", "int"), namespace="y"))
E = lambda syms, **kw: {"type": "enum", "name": "E", "symbols": syms, **kw}
C("avro-enum-add", "AVRO", A(f("e", E(["A", "B"]))), A(f("e", E(["A", "B", "C"]))))
C("avro-enum-remove", "AVRO", A(f("e", E(["A", "B"]))), A(f("e", E(["A"]))))
C("avro-enum-remove-default", "AVRO", A(f("e", E(["A", "B"]))), A(f("e", E(["A"], default="A"))))
C("avro-enum-reorder", "AVRO", A(f("e", E(["A", "B"]))), A(f("e", E(["B", "A"]))))
C("avro-enum-top-remove", "AVRO", E(["A", "B"]), E(["A"]))
FX = lambda size, name="F": {"type": "fixed", "name": name, "size": size}
C("avro-fixed-size", "AVRO", A(f("x", FX(4))), A(f("x", FX(8))))
C("avro-fixed-rename", "AVRO", A(f("x", FX(4))), A(f("x", FX(4, name="G"))))
C("avro-fixed-to-bytes", "AVRO", A(f("x", FX(4))), A(f("x", "bytes")))
C("avro-union-add-branch", "AVRO", A(f("u", ["int", "string"])), A(f("u", ["int", "string", "long"])))
C("avro-union-remove-branch", "AVRO", A(f("u", ["int", "string"])), A(f("u", ["int"])))
C("avro-to-nullable", "AVRO", A(f("u", "int")), A(f("u", ["null", "int"])))
C("avro-from-nullable", "AVRO", A(f("u", ["null", "int"])), A(f("u", "int")))
C("avro-union-reorder", "AVRO", A(f("u", ["int", "string"])), A(f("u", ["string", "int"])))
C("avro-union-promote", "AVRO", A(f("u", ["null", "int"])), A(f("u", ["null", "long"])))
C("avro-union-to-single-member", "AVRO", A(f("u", ["string"])), A(f("u", "string")))
C("avro-top-union-add", "AVRO", ["int", "string"], ["int", "string", "null"])
C("avro-top-union-remove", "AVRO", ["int", "string"], ["int"])
C("avro-array-items-promote", "AVRO", A(f("x", {"type": "array", "items": "int"})), A(f("x", {"type": "array", "items": "long"})))
C("avro-array-items-narrow", "AVRO", A(f("x", {"type": "array", "items": "long"})), A(f("x", {"type": "array", "items": "int"})))
C("avro-map-values-promote", "AVRO", A(f("x", {"type": "map", "values": "int"})), A(f("x", {"type": "map", "values": "long"})))
C("avro-array-to-map", "AVRO", A(f("x", {"type": "array", "items": "int"})), A(f("x", {"type": "map", "values": "int"})))
nested = lambda *fs: A(f("n", rec("N", list(fs))))
C("avro-nested-add-nodefault", "AVRO", nested(f("x", "int")), nested(f("x", "int"), f("y", "int")))
C("avro-nested-add-default", "AVRO", nested(f("x", "int")), nested(f("x", "int"), f("y", "int", default=1)))
C("avro-nested-type-change", "AVRO", nested(f("x", "int")), nested(f("x", "string")))
C("avro-logical-int-date", "AVRO", A(f("x", "int")), A(f("x", {"type": "int", "logicalType": "date"})))
C("avro-logical-date-int", "AVRO", A(f("x", {"type": "int", "logicalType": "date"})), A(f("x", "int")))
C("avro-logical-ts-millis-micros", "AVRO", A(f("x", {"type": "long", "logicalType": "timestamp-millis"})),
  A(f("x", {"type": "long", "logicalType": "timestamp-micros"})))
C("avro-decimal-precision", "AVRO", A(f("x", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2})),
  A(f("x", {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2})))
C("avro-decimal-scale", "AVRO", A(f("x", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2})),
  A(f("x", {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 3})))
C("avro-record-to-primitive", "AVRO", A(f("a", "int")), '"int"')
C("avro-primitive-to-record", "AVRO", '"int"', A(f("a", "int")))
C("avro-string-to-enum", "AVRO", A(f("x", "string")), A(f("x", E(["A"]))))
C("avro-enum-to-string", "AVRO", A(f("x", E(["A"]))), A(f("x", "string")))
C("avro-union-to-string", "AVRO", A(f("x", ["int", "string"])), A(f("x", "string")))
C("avro-recursive-add-default", "AVRO", rec("Node", [f("v", "int"), f("next", ["null", "Node"], default=None)]),
  rec("Node", [f("v", "int"), f("w", "int", default=0), f("next", ["null", "Node"], default=None)]))
C("avro-recursive-add-nodefault", "AVRO", rec("Node", [f("v", "int"), f("next", ["null", "Node"], default=None)]),
  rec("Node", [f("v", "int"), f("w", "int"), f("next", ["null", "Node"], default=None)]))
C("avro-reused-type-change", "AVRO",
  A(f("a", rec("I", [f("x", "int")])), f("b", "I")),
  A(f("a", rec("I", [f("x", "long")])), f("b", "I")))
C("avro-add-then-union-default", "AVRO", base, A(f("a", "int"), f("b", "string"), f("c", ["string", "null"], default="x")))
C("avro-identical", "AVRO", base, base)
C("avro-ref-change", "AVRO",
  rec("P", [f("addr", "com.acme.Address")], namespace="com.acme"),
  rec("P", [f("addr", "com.acme.Address")], namespace="com.acme"),
  old_refs=[{"name": "com.acme.Address", "schema": j(rec("Address", [f("street", "string")], namespace="com.acme"))}],
  new_refs=[{"name": "com.acme.Address", "schema": j(rec("Address", [f("street", "string"), f("zip", "string")], namespace="com.acme"))}])

# ---------------------------------------------------------------------------
# JSON Schema: canonical & validity
# ---------------------------------------------------------------------------
obj = lambda props, **kw: {"type": "object", "properties": props, **kw}
S("json-pretty", "JSON", json.dumps(obj({"a": {"type": "string"}}), indent=2))
S("json-key-order", "JSON", '{"properties":{"b":{"type":"integer"},"a":{"type":"string"}},"type":"object"}')
S("json-draft7", "JSON", {"$schema": "http://json-schema.org/draft-07/schema#", "title": "T", **obj({"a": {"type": "string"}})})
S("json-2020", "JSON", {"$schema": "https://json-schema.org/draft/2020-12/schema", **obj({"a": {"type": "string"}})})
S("json-numbers", "JSON", {"type": "number", "minimum": 1.0, "maximum": 1e3, "multipleOf": 0.5})
S("json-unicode", "JSON", {"type": "string", "description": "café"})
S("json-boolean-true", "JSON", "true")
S("json-empty", "JSON", {})
S("json-defs-ref", "JSON", {"definitions": {"x": {"type": "string"}}, **obj({"a": {"$ref": "#/definitions/x"}})})
S("json-bad-json", "JSON", '{"type":')
S("json-bad-type", "JSON", {"type": "nosuchtype"})
S("json-bad-required", "JSON", {"type": "object", "required": "a"})
S("json-bad-ref", "JSON", obj({"a": {"$ref": "#/definitions/missing"}}))
S("json-array-schema", "JSON", '[1,2]')
S("json-number", "JSON", '5')
S("json-string", "JSON", '"x"')
S("json-null", "JSON", 'null')
S("json-false", "JSON", 'false')
S("json-bad-external-ref", "JSON", obj({"a": {"$ref": "http://example.invalid/nope.json"}}))
S("json-bad-pattern", "JSON", {"type": "string", "pattern": "("})
S("json-bad-min-type", "JSON", {"type": "string", "minLength": "x"})
S("json-unknown-keyword", "JSON", {"type": "string", "x-custom": 1})
S("json-draft4", "JSON", {"$schema": "http://json-schema.org/draft-04/schema#", "type": "number", "maximum": 5, "exclusiveMaximum": True})
S("json-unknown-format", "JSON", {"type": "string", "format": "my-format"})
S("json-prop-not-schema", "JSON", obj({"a": 5}))
S("json-items-not-schema", "JSON", {"type": "array", "items": 5})
S("json-allof-empty", "JSON", {"allOf": []})
S("json-oneof-not-array", "JSON", {"oneOf": {"type": "string"}})
S("json-enum-not-array", "JSON", {"enum": "a"})
S("json-deps-bad", "JSON", {"type": "object", "dependencies": {"a": [1]}})
S("json-defs2019-ref", "JSON", {"$defs": {"x": {"type": "string"}}, **obj({"a": {"$ref": "#/$defs/x"}})})
S("json-id-ref", "JSON", {"$id": "http://example.com/root.json", "definitions": {"x": {"$id": "#foo", "type": "string"}}, **obj({"a": {"$ref": "#foo"}})})
S("json-lookahead-pattern", "JSON", {"type": "string", "pattern": "^(?!x).*$"})
S("json-min-float", "JSON", {"type": "string", "minLength": 1.5})
S("json-multipleof-zero", "JSON", {"type": "number", "multipleOf": 0})
S("json-required-not-strings", "JSON", {"type": "object", "required": [1]})
S("json-additional-bad", "JSON", {"type": "object", "additionalProperties": 5})
S("json-type-array-bad", "JSON", {"type": ["string", "nope"]})
S("json-uniqueitems-bad", "JSON", {"type": "array", "uniqueItems": "yes"})
S("json-ref-sibling", "JSON", {"definitions": {"x": {"type": "string"}}, **obj({"a": {"$ref": "#/definitions/x", "maxLength": 3}})})
S("json-with-ref", "JSON", obj({"c": {"$ref": "customer.json"}}),
  refs=[{"name": "customer.json", "subject": "customer", "version": 1, "schema": j(obj({"id": {"type": "integer"}}))}])

# ---------------------------------------------------------------------------
# JSON Schema: compatibility pairs
# ---------------------------------------------------------------------------
open_a = obj({"a": {"type": "string"}})
closed_a = obj({"a": {"type": "string"}}, additionalProperties=False)
C("json-add-prop-open", "JSON", open_a, obj({"a": {"type": "string"}, "b": {"type": "integer"}}))
C("json-remove-prop-open", "JSON", obj({"a": {"type": "string"}, "b": {"type": "integer"}}), open_a)
C("json-add-prop-closed", "JSON", closed_a, obj({"a": {"type": "string"}, "b": {"type": "integer"}}, additionalProperties=False))
C("json-remove-prop-closed", "JSON", obj({"a": {"type": "string"}, "b": {"type": "integer"}}, additionalProperties=False), closed_a)
C("json-open-to-closed", "JSON", open_a, closed_a)
C("json-closed-to-open", "JSON", closed_a, open_a)
C("json-add-prop-partial", "JSON", obj({"a": {"type": "string"}}, additionalProperties={"type": "integer"}),
  obj({"a": {"type": "string"}, "b": {"type": "integer"}}, additionalProperties={"type": "integer"}))
C("json-add-prop-partial-mismatch", "JSON", obj({"a": {"type": "string"}}, additionalProperties={"type": "integer"}),
  obj({"a": {"type": "string"}, "b": {"type": "string"}}, additionalProperties={"type": "integer"}))
C("json-additional-schema-change", "JSON", obj({}, additionalProperties={"type": "integer"}), obj({}, additionalProperties={"type": "number"}))
C("json-additional-schema-narrow", "JSON", obj({}, additionalProperties={"type": "number"}), obj({}, additionalProperties={"type": "integer"}))
C("json-add-required", "JSON", open_a, obj({"a": {"type": "string"}}, required=["a"]))
C("json-remove-required", "JSON", obj({"a": {"type": "string"}}, required=["a"]), open_a)
C("json-add-required-with-default", "JSON", open_a, obj({"a": {"type": "string", "default": "x"}}, required=["a"]))
C("json-type-change", "JSON", {"type": "string"}, {"type": "integer"})
C("json-int-to-number", "JSON", {"type": "integer"}, {"type": "number"})
C("json-number-to-int", "JSON", {"type": "number"}, {"type": "integer"})
C("json-type-widen-array", "JSON", {"type": "string"}, {"type": ["string", "null"]})
C("json-type-narrow-array", "JSON", {"type": ["string", "null"]}, {"type": "string"})
C("json-enum-add", "JSON", {"type": "string", "enum": ["a", "b"]}, {"type": "string", "enum": ["a", "b", "c"]})
C("json-enum-remove", "JSON", {"type": "string", "enum": ["a", "b"]}, {"type": "string", "enum": ["a"]})
C("json-enum-added", "JSON", {"type": "string"}, {"type": "string", "enum": ["a"]})
C("json-enum-removed", "JSON", {"type": "string", "enum": ["a"]}, {"type": "string"})
for k, a, b in [("maxLength", 10, 5), ("maxLength", 5, 10), ("minLength", 1, 3), ("minLength", 3, 1),
                ("maximum", 10, 5), ("maximum", 5, 10), ("minimum", 1, 3), ("minimum", 3, 1),
                ("exclusiveMaximum", 10, 5), ("exclusiveMinimum", 3, 1),
                ("maxItems", 10, 5), ("minItems", 1, 3), ("maxProperties", 10, 5), ("minProperties", 1, 3)]:
    t = "string" if "Length" in k else "array" if "Items" in k else "object" if "Properties" in k else "number"
    C(f"json-{k}-{a}-{b}", "JSON", {"type": t, k: a}, {"type": t, k: b})
    if a == 10 or k.startswith("min"):
        C(f"json-{k}-added-{b}", "JSON", {"type": t}, {"type": t, k: b})
C("json-maxLength-removed", "JSON", {"type": "string", "maxLength": 5}, {"type": "string"})
C("json-pattern-added", "JSON", {"type": "string"}, {"type": "string", "pattern": "^a"})
C("json-pattern-changed", "JSON", {"type": "string", "pattern": "^a"}, {"type": "string", "pattern": "^b"})
C("json-pattern-removed", "JSON", {"type": "string", "pattern": "^a"}, {"type": "string"})
C("json-multipleOf-added", "JSON", {"type": "number"}, {"type": "number", "multipleOf": 2})
C("json-multipleOf-changed", "JSON", {"type": "number", "multipleOf": 4}, {"type": "number", "multipleOf": 2})
C("json-uniqueItems-added", "JSON", {"type": "array"}, {"type": "array", "uniqueItems": True})
C("json-items-type-change", "JSON", {"type": "array", "items": {"type": "string"}}, {"type": "array", "items": {"type": "integer"}})
C("json-items-widen", "JSON", {"type": "array", "items": {"type": "integer"}}, {"type": "array", "items": {"type": "number"}})
C("json-oneof-add", "JSON", {"oneOf": [{"type": "string"}, {"type": "integer"}]}, {"oneOf": [{"type": "string"}, {"type": "integer"}, {"type": "boolean"}]})
C("json-oneof-remove", "JSON", {"oneOf": [{"type": "string"}, {"type": "integer"}]}, {"oneOf": [{"type": "string"}]})
C("json-anyof-add", "JSON", {"anyOf": [{"type": "string"}]}, {"anyOf": [{"type": "string"}, {"type": "integer"}]})
C("json-allof-add", "JSON", {"allOf": [{"type": "object"}]}, {"allOf": [{"type": "object"}, {"required": ["a"]}]})
C("json-to-oneof", "JSON", {"type": "string"}, {"oneOf": [{"type": "string"}, {"type": "integer"}]})
C("json-from-oneof", "JSON", {"oneOf": [{"type": "string"}, {"type": "integer"}]}, {"type": "string"})
C("json-nested-type-change", "JSON", obj({"n": obj({"x": {"type": "string"}})}), obj({"n": obj({"x": {"type": "integer"}})}))
C("json-ref-target-change", "JSON",
  {"definitions": {"x": {"type": "string"}}, **obj({"a": {"$ref": "#/definitions/x"}})},
  {"definitions": {"x": {"type": "integer"}}, **obj({"a": {"$ref": "#/definitions/x"}})})
C("json-const-change", "JSON", {"const": "a"}, {"const": "b"})
C("json-description-only", "JSON", {"type": "string", "description": "x"}, {"type": "string", "description": "y"})
C("json-true-to-false", "JSON", "true", "false")
C("json-empty-to-typed", "JSON", {}, {"type": "string"})
C("json-typed-to-empty", "JSON", {"type": "string"}, {})
C("json-add-object-type", "JSON", {"properties": {"a": {"type": "string"}}}, obj({"a": {"type": "string"}}))
C("json-not-added", "JSON", {"type": "string"}, {"type": "string", "not": {"const": "x"}})
C("json-format-added", "JSON", {"type": "string"}, {"type": "string", "format": "email"})
C("json-dependencies-added", "JSON", obj({"a": {"type": "string"}, "b": {"type": "string"}}),
  obj({"a": {"type": "string"}, "b": {"type": "string"}}, dependencies={"a": ["b"]}))
C("json-identical", "JSON", open_a, open_a)

# ---------------------------------------------------------------------------
# Protobuf: canonical & validity
# ---------------------------------------------------------------------------
S("proto-basic", "PROTOBUF", 'syntax = "proto3";\npackage acme;\n\nmessage Order {\n  string id = 1;\n  int32 qty = 2;\n}\n')
S("proto-compact", "PROTOBUF", 'syntax="proto3";package acme;message Order{string id=1;int32 qty=2;}')
S("proto-comments", "PROTOBUF", 'syntax = "proto3";\n// leading\npackage acme;\n/* block */\nmessage A { // trailing\n  string a = 1; /* x */\n}\n')
S("proto-no-package", "PROTOBUF", 'syntax = "proto3";\nmessage A { string a = 1; }\n')
S("proto-proto2", "PROTOBUF", 'syntax = "proto2";\npackage p;\nmessage A {\n  required string a = 1;\n  optional int32 b = 2 [default = 5];\n  repeated string c = 3;\n  optional string d = 4 [default = "x"];\n}\n')
S("proto-no-syntax", "PROTOBUF", 'package p;\nmessage A { optional string a = 1; }\n')
S("proto-nested", "PROTOBUF", 'syntax = "proto3";\npackage p;\nmessage A {\n  message B { string x = 1; }\n  enum K { K0 = 0; K1 = 1; }\n  B b = 1;\n  K k = 2;\n}\n')
S("proto-enum-top", "PROTOBUF", 'syntax = "proto3";\npackage p;\nenum Color { RED = 0; GREEN = 1; }\nmessage A { Color c = 1; }\n')
S("proto-enum-alias", "PROTOBUF", 'syntax = "proto3";\nenum E {\n  option allow_alias = true;\n  A = 0;\n  B = 0;\n}\n')
S("proto-oneof", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  oneof k {\n    string s = 1;\n    int64 n = 2;\n  }\n  string other = 3;\n}\n')
S("proto-map", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  map<string, int64> m = 1;\n  map<int32, B> mb = 2;\n}\nmessage B { string x = 1; }\n')
S("proto-optional3", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  optional string a = 1;\n  string b = 2;\n}\n')
S("proto-reserved", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  reserved 2, 15, 9 to 11;\n  reserved "foo", "bar";\n  string a = 1;\n}\n')
S("proto-options", "PROTOBUF", 'syntax = "proto3";\npackage p;\noption java_package = "com.acme";\noption java_multiple_files = true;\noption go_package = "acme/p";\nmessage A {\n  option deprecated = true;\n  string a = 1 [deprecated = true];\n  repeated int32 b = 2 [packed = false];\n  string c = 3 [json_name = "cee"];\n}\n')
S("proto-service", "PROTOBUF", 'syntax = "proto3";\npackage p;\nmessage Req { string q = 1; }\nmessage Resp { string r = 1; }\nservice S {\n  rpc Get(Req) returns (Resp);\n  rpc Stream(stream Req) returns (stream Resp);\n}\n')
S("proto-wkt", "PROTOBUF", 'syntax = "proto3";\nimport "google/protobuf/timestamp.proto";\nimport "google/protobuf/wrappers.proto";\nmessage A {\n  google.protobuf.Timestamp ts = 1;\n  google.protobuf.StringValue s = 2;\n}\n')
S("proto-confluent-meta", "PROTOBUF", 'syntax = "proto3";\nimport "confluent/meta.proto";\nmessage A {\n  string ssn = 1 [(confluent.field_meta) = { tags: "PII" }];\n}\n')
S("proto-decimal", "PROTOBUF", 'syntax = "proto3";\nimport "confluent/type/decimal.proto";\nmessage A {\n  confluent.type.Decimal amount = 1 [(confluent.field_meta) = { params: [ { key: "precision" value: "8" } ] }];\n}\n')
S("proto-extensions", "PROTOBUF", 'syntax = "proto2";\npackage p;\nmessage A {\n  extensions 100 to 199;\n  optional string a = 1;\n}\nextend A {\n  optional int32 x = 100;\n}\n')
S("proto-explicit-map-entry", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  repeated MEntry m = 1;\n  message MEntry {\n    option map_entry = true;\n    string key = 1;\n    int32 value = 2;\n  }\n}\n')
S("proto-fq-types", "PROTOBUF", 'syntax = "proto3";\npackage a.b;\nmessage X { .a.b.Y y = 1; }\nmessage Y { string s = 1; }\n')
S("proto-google-type-date", "PROTOBUF", 'syntax = "proto3";\nimport "google/type/date.proto";\nimport "google/type/money.proto";\nmessage A {\n  google.type.Date d = 1;\n  google.type.Money m = 2;\n}\n')
S("proto-unknown-option", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  string a = 1 [(foo.bar) = 3];\n}\n')
S("proto-unknown-file-option", "PROTOBUF", 'syntax = "proto3";\noption (foo.bar) = "x";\nmessage A { string a = 1; }\n')
S("proto-meta-without-import", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  string ssn = 1 [(confluent.field_meta) = { tags: "PII" }];\n}\n')
S("proto-missing-import-used", "PROTOBUF", 'syntax = "proto3";\nimport "nope.proto";\nmessage A { nope.X x = 1; }\n')
S("proto-nested-enum-first", "PROTOBUF", 'syntax = "proto3";\nmessage A {\n  enum K { K0 = 0; }\n  message B { string x = 1; }\n  K k = 1;\n  B b = 2;\n}\nenum Top { T0 = 0; }\nmessage C { string c = 1; }\n')
S("proto-options-order", "PROTOBUF", 'syntax = "proto3";\npackage p;\nimport "google/protobuf/timestamp.proto";\noption java_package = "x";\nmessage A {\n  reserved 5;\n  option deprecated = true;\n  oneof k { string s = 1; }\n  string a = 2 [deprecated = true, json_name = "aa"];\n  message N { string n = 1; }\n}\nservice S { rpc M (A) returns (A) { option deprecated = true; } }\n')
S("proto-norm-order", "PROTOBUF", 'syntax = "proto3";\npackage p;\noption java_package = "x";\nmessage Z {\n  option deprecated = true;\n  option (foo.bar) = 1;\n  int32 b = 2 [json_name = "bb", deprecated = true];\n  string a = 1;\n  reserved "zz", "aa";\n  reserved 20, 10;\n  oneof o { string y = 4; string x = 3; }\n  message Y { string y = 1; }\n  message X { string x = 1; }\n  enum E { option allow_alias = true; E1 = 1; E0 = 0; E2 = 1; }\n}\nmessage A { Z z = 1; }\nenum Q { Q1 = 1; Q0 = 0; }\nservice S { rpc B (A) returns (A); rpc A (A) returns (A); }\n')
S("proto-norm-list-options", "PROTOBUF", 'syntax = "proto3";\nimport "confluent/meta.proto";\nmessage A {\n  string a = 1 [(confluent.field_meta) = { tags: ["x", "y"], params: [ { key: "k1" value: "v1" }, { key: "k2" value: "v2" } ] }];\n}\n')
S("proto-norm-public-import", "PROTOBUF", 'syntax = "proto3";\nimport public "google/protobuf/timestamp.proto";\nimport "google/protobuf/duration.proto";\nmessage A { google.protobuf.Timestamp t = 1; google.protobuf.Duration d = 2; }\n')
S("proto-norm-options-imports", "PROTOBUF", 'syntax = "proto3";\nimport "google/protobuf/wrappers.proto";\nimport "google/protobuf/duration.proto";\noption java_package = "x";\noption (zed.opt) = 1;\noption cc_enable_arenas = true;\nmessage A {\n  option (zz.m) = 2;\n  option deprecated = true;\n  option (aa.m) = 3;\n  google.protobuf.Duration d = 1 [json_name = "dd", (x.y) = 1, deprecated = true];\n  google.protobuf.StringValue s = 2;\n}\n')
S("proto-bad-syntax", "PROTOBUF", 'message {')
S("proto-bad-type", "PROTOBUF", 'syntax = "proto3";\nmessage A { Missing m = 1; }\n')
S("proto-dup-number", "PROTOBUF", 'syntax = "proto3";\nmessage A { string a = 1; string b = 1; }\n')
S("proto-missing-import", "PROTOBUF", 'syntax = "proto3";\nimport "nope.proto";\nmessage A { string a = 1; }\n')
S("proto3-required", "PROTOBUF", 'syntax = "proto3";\nmessage A { required string a = 1; }\n')
S("proto-with-ref", "PROTOBUF", 'syntax = "proto3";\npackage m;\nimport "dep.proto";\nmessage M { d.D d = 1; }\n',
  refs=[{"name": "dep.proto", "subject": "dep", "version": 1, "schema": 'syntax = "proto3";\npackage d;\nmessage D { string x = 1; }\n'}])

# ---------------------------------------------------------------------------
# Protobuf: compatibility pairs
# ---------------------------------------------------------------------------
PB = lambda body, pkg="p", syntax="proto3": f'syntax = "{syntax}";\npackage {pkg};\n{body}\n'
base_pb = PB("message A { string a = 1; int32 b = 2; }")
C("proto-add-field", "PROTOBUF", base_pb, PB("message A { string a = 1; int32 b = 2; bool c = 3; }"))
C("proto-remove-field", "PROTOBUF", base_pb, PB("message A { string a = 1; }"))
C("proto-rename-field", "PROTOBUF", base_pb, PB("message A { string a = 1; int32 bee = 2; }"))
C("proto-renumber-field", "PROTOBUF", base_pb, PB("message A { string a = 1; int32 b = 3; }"))
for old_t, new_t in [("int32", "int64"), ("int32", "uint32"), ("int32", "bool"), ("int64", "int32"),
                     ("sint32", "sint64"), ("sint32", "int32"), ("fixed32", "sfixed32"), ("fixed64", "sfixed64"),
                     ("fixed32", "fixed64"), ("float", "double"), ("int32", "string"), ("string", "bytes"),
                     ("bytes", "string"), ("int32", "float")]:
    C(f"proto-type-{old_t}-{new_t}", "PROTOBUF", PB(f"message A {{ {old_t} x = 1; }}"), PB(f"message A {{ {new_t} x = 1; }}"))
C("proto-scalar-to-message", "PROTOBUF", PB("message A { string x = 1; }\nmessage B { string y = 1; }"),
  PB("message A { B x = 1; }\nmessage B { string y = 1; }"))
C("proto-message-type-change", "PROTOBUF", PB("message A { B x = 1; }\nmessage B { string y = 1; }\nmessage C { string y = 1; }"),
  PB("message A { C x = 1; }\nmessage B { string y = 1; }\nmessage C { string y = 1; }"))
C("proto-enum-to-int", "PROTOBUF", PB("message A { E x = 1; }\nenum E { E0 = 0; }"), PB("message A { int32 x = 1; }\nenum E { E0 = 0; }"))
C("proto-remove-message", "PROTOBUF", PB("message A { string a = 1; }\nmessage B { string b = 1; }"), PB("message A { string a = 1; }"))
C("proto-add-message", "PROTOBUF", PB("message A { string a = 1; }"), PB("message A { string a = 1; }\nmessage B { string b = 1; }"))
C("proto-rename-message", "PROTOBUF", PB("message A { string a = 1; }"), PB("message Z { string a = 1; }"))
C("proto-package-change", "PROTOBUF", PB("message A { string a = 1; }", pkg="p"), PB("message A { string a = 1; }", pkg="q"))
C("proto-into-new-oneof-single", "PROTOBUF", PB("message A { string a = 1; int32 b = 2; }"),
  PB("message A { oneof k { string a = 1; } int32 b = 2; }"))
C("proto-into-new-oneof-multi", "PROTOBUF", PB("message A { string a = 1; int32 b = 2; }"),
  PB("message A { oneof k { string a = 1; int32 b = 2; } }"))
C("proto-into-existing-oneof", "PROTOBUF", PB("message A { oneof k { string a = 1; } int32 b = 2; }"),
  PB("message A { oneof k { string a = 1; int32 b = 2; } }"))
C("proto-remove-oneof-field", "PROTOBUF", PB("message A { oneof k { string a = 1; int32 b = 2; } }"),
  PB("message A { oneof k { string a = 1; } }"))
C("proto-add-oneof-field", "PROTOBUF", PB("message A { oneof k { string a = 1; } }"),
  PB("message A { oneof k { string a = 1; int32 b = 2; } }"))
C("proto-out-of-oneof", "PROTOBUF", PB("message A { oneof k { string a = 1; } }"), PB("message A { string a = 1; }"))
C("proto2-optional-to-required", "PROTOBUF", PB("message A { optional string a = 1; }", syntax="proto2"),
  PB("message A { required string a = 1; }", syntax="proto2"))
C("proto2-required-to-optional", "PROTOBUF", PB("message A { required string a = 1; }", syntax="proto2"),
  PB("message A { optional string a = 1; }", syntax="proto2"))
C("proto2-add-required", "PROTOBUF", PB("message A { optional string a = 1; }", syntax="proto2"),
  PB("message A { optional string a = 1; required int32 b = 2; }", syntax="proto2"))
C("proto2-remove-required", "PROTOBUF", PB("message A { optional string a = 1; required int32 b = 2; }", syntax="proto2"),
  PB("message A { optional string a = 1; }", syntax="proto2"))
C("proto-singular-to-repeated-int", "PROTOBUF", PB("message A { int32 x = 1; }"), PB("message A { repeated int32 x = 1; }"))
C("proto-singular-to-repeated-string", "PROTOBUF", PB("message A { string x = 1; }"), PB("message A { repeated string x = 1; }"))
C("proto-repeated-to-singular", "PROTOBUF", PB("message A { repeated string x = 1; }"), PB("message A { string x = 1; }"))
C("proto-enum-add-value", "PROTOBUF", PB("message A { E e = 1; }\nenum E { E0 = 0; }"), PB("message A { E e = 1; }\nenum E { E0 = 0; E1 = 1; }"))
C("proto-enum-remove-value", "PROTOBUF", PB("message A { E e = 1; }\nenum E { E0 = 0; E1 = 1; }"), PB("message A { E e = 1; }\nenum E { E0 = 0; }"))
C("proto-enum-rename", "PROTOBUF", PB("message A { E e = 1; }\nenum E { E0 = 0; }"), PB("message A { F e = 1; }\nenum F { E0 = 0; }"))
C("proto-remove-enum", "PROTOBUF", PB("message A { string a = 1; }\nenum E { E0 = 0; }"), PB("message A { string a = 1; }"))
C("proto-map-value-change", "PROTOBUF", PB("message A { map<string, int32> m = 1; }"), PB("message A { map<string, string> m = 1; }"))
C("proto-map-value-widen", "PROTOBUF", PB("message A { map<string, int32> m = 1; }"), PB("message A { map<string, int64> m = 1; }"))
C("proto-nested-add-field", "PROTOBUF", PB("message A { message N { string x = 1; } N n = 1; }"),
  PB("message A { message N { string x = 1; int32 y = 2; } N n = 1; }"))
C("proto-nested-type-change", "PROTOBUF", PB("message A { message N { string x = 1; } N n = 1; }"),
  PB("message A { message N { int32 x = 1; } N n = 1; }"))
C("proto-syntax-2-to-3", "PROTOBUF", PB("message A { optional string a = 1; }", syntax="proto2"), PB("message A { string a = 1; }"))
C("proto-reserved-added", "PROTOBUF", base_pb, PB("message A { reserved 2; string a = 1; }"))
C("proto-json-name", "PROTOBUF", base_pb, PB('message A { string a = 1 [json_name = "aa"]; int32 b = 2; }'))
C("proto-move-to-nested", "PROTOBUF", PB("message A { B b = 1; }\nmessage B { string x = 1; }"),
  PB("message A { message B { string x = 1; } B b = 1; }"))
C("proto-identical", "PROTOBUF", base_pb, base_pb)
C("proto-optional3-added", "PROTOBUF", base_pb, PB("message A { string a = 1; int32 b = 2; optional string c = 3; }"))
C("proto-to-optional3", "PROTOBUF", PB("message A { string a = 1; }"), PB("message A { optional string a = 1; }"))

# ---------------------------------------------------------------------------
# Version chains: every compatibility level, positive and negative.
# ---------------------------------------------------------------------------
R = lambda *fields: rec("R", list(fields))
CH("avro-add-optional-field", "AVRO", R(f("a", "int")), R(f("a", "int"), f("b", "string", default="x")))
CH("avro-add-required-field", "AVRO", R(f("a", "int")), R(f("a", "int"), f("b", "string")))
CH("avro-remove-optional-field", "AVRO", R(f("a", "int"), f("b", "string", default="x")), R(f("a", "int")))
CH("avro-remove-required-field", "AVRO", R(f("a", "int"), f("b", "string")), R(f("a", "int")))
CH("avro-promote-int-long", "AVRO", R(f("a", "int")), R(f("a", "long")))
CH("avro-change-int-string", "AVRO", R(f("a", "int")), R(f("a", "string")))
CH("avro-enum-add-symbol", "AVRO", R(f("e", E(["A", "B"]))), R(f("e", E(["A", "B", "C"]))))
CH("avro-enum-add-symbol-old-default", "AVRO", R(f("e", E(["A", "B"], default="A"))), R(f("e", E(["A", "B", "C"], default="A"))))
CH("avro-union-widen", "AVRO", R(f("a", ["null", "int"], default=None)), R(f("a", ["null", "int", "string"], default=None)))
CH("avro-rename-with-alias", "AVRO", R(f("a", "int")), R(f("b", "int", aliases=["a"])))
CH("avro-three-compatible", "AVRO", R(f("a", "int")), R(f("a", "int"), f("b", "string", default="")),
   R(f("a", "int"), f("b", "string", default=""), f("c", "long", default=0)))
CH("avro-backward-gap", "AVRO", R(f("a", "int")), R(), R(f("a", "string", default="")))
CH("avro-forward-gap", "AVRO", R(f("x", "int", default=0)), R(), R(f("x", "string")))
CH("avro-full-gap", "AVRO", R(f("x", "int", default=0)), R(), R(f("x", "string", default="")))
CH("avro-to-json", "AVRO", R(f("a", "int")), obj({"a": {"type": "integer"}}), types=["AVRO", "JSON"])

closed = lambda props, **kw: obj(props, additionalProperties=False, **kw)
CH("json-closed-add-property", "JSON", closed({"a": {"type": "string"}}), closed({"a": {"type": "string"}, "b": {"type": "integer"}}))
CH("json-open-add-property", "JSON", obj({"a": {"type": "string"}}), obj({"a": {"type": "string"}, "b": {"type": "integer"}}))
CH("json-open-remove-property", "JSON", obj({"a": {"type": "string"}, "b": {"type": "integer"}}), obj({"a": {"type": "string"}}))
CH("json-add-required", "JSON", obj({"a": {"type": "string"}}), obj({"a": {"type": "string"}}, required=["a"]))
CH("json-widen-integer-number", "JSON", {"type": "integer"}, {"type": "number"})
CH("json-change-type", "JSON", {"type": "string"}, {"type": "integer"})
CH("json-relax-max-length", "JSON", {"type": "string", "maxLength": 5}, {"type": "string", "maxLength": 10})
CH("json-enum-extend", "JSON", {"enum": ["a", "b"]}, {"enum": ["a", "b", "c"]})
CH("json-description-only", "JSON", {"type": "string", "description": "v1"}, {"type": "string", "description": "v2"})
CH("json-backward-gap", "JSON", obj({"a": {"type": "string"}}), obj({"a": {"type": "integer"}}), obj({"a": {"type": "number"}}))
CH("json-forward-gap", "JSON", obj({"a": {"type": "string"}}), obj({"a": {"type": "number"}}), obj({"a": {"type": "integer"}}))
CH("json-full-gap", "JSON", obj({"a": {"type": "string"}}), obj({"a": {"type": "integer"}}),
   obj({"a": {"type": "integer", "description": "still an integer"}}))

CH("proto-add-field", "PROTOBUF", PB("message A { string a = 1; }"), PB("message A { string a = 1; int32 b = 2; }"))
CH("proto-remove-field", "PROTOBUF", PB("message A { string a = 1; int32 b = 2; }"), PB("message A { string a = 1; }"))
CH("proto-int32-int64", "PROTOBUF", PB("message A { int32 a = 1; }"), PB("message A { int64 a = 1; }"))
CH("proto-int32-string", "PROTOBUF", PB("message A { int32 a = 1; }"), PB("message A { string a = 1; }"))
CH("proto-remove-message", "PROTOBUF", PB("message A { string a = 1; }\nmessage B { string b = 1; }"), PB("message A { string a = 1; }"))
CH("proto2-add-required", "PROTOBUF", PB("message A { optional string a = 1; }", syntax="proto2"),
   PB("message A { optional string a = 1; required int32 b = 2; }", syntax="proto2"))
CH("proto-multiple-into-oneof", "PROTOBUF", PB("message A { string a = 1; int32 b = 2; }"), PB("message A { oneof k { string a = 1; int32 b = 2; } }"))
CH("proto-package-change", "PROTOBUF", PB("message A { string a = 1; }", pkg="p"), PB("message A { string a = 1; }", pkg="q"))
CH("proto-transitive-gap", "PROTOBUF", PB("message A { string x = 1; }"), PB("message A { int32 x = 1; }"), PB("message A { int64 x = 1; }"))

out = os.path.join(os.path.dirname(__file__), "corpus.json")
with open(out, "w") as fh:
    json.dump({"schemas": schemas, "compat": compat, "chains": chains}, fh, indent=1, ensure_ascii=False)
print(f"wrote {out}: {len(schemas)} schemas, {len(compat)} compat pairs, {len(chains)} chains")
