#!/usr/bin/env python3
"""HTTP conformance scenarios: request sequences replayed against Confluent and us.

    python3 tests/http_conformance/scenarios.py            # writes scenarios.json

Every scenario is a list of HTTP steps. `run.py record` sends them to a real
Confluent 7.9 and stores status + body per step in golden.json;
`src/http_conformance_tests.rs` replays them against a fresh in-process server
and requires identical results (modulo the documented ALLOWED list).

Conventions
- Every subject, context and alias is prefixed with the scenario name, so
  scenarios never see each other's data even on a shared Confluent.
- Server-assigned ids are compared symbolically: the n-th distinct id seen in a
  scenario becomes "$idn" (in any "id"/"maxId" field, or in `ids` list steps).
  Paths and bodies may use "{$idn}" to refer back to one.
- Global state (top-level config/mode) is always restored in `teardown`
  (sent but not compared).
- A step may set `cmp`: "exact" (default), "keys" (same JSON keys, e.g. server
  version), or "status".
"""

import json
import os
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

AVRO_USER = '{"type":"record","name":"User","fields":[{"name":"name","type":"string"}]}'
AVRO_USER_AGE = ('{"type":"record","name":"User","fields":[{"name":"name","type":"string"},'
                 '{"name":"age","type":"int","default":0}]}')
AVRO_USER_AGE_NODEFAULT = ('{"type":"record","name":"User","fields":[{"name":"name","type":"string"},'
                           '{"name":"age","type":"int"}]}')
AVRO_USER_INT = '{"type":"record","name":"User","fields":[{"name":"name","type":"int"}]}'
AVRO_USER_SPACED = ('{ "type" : "record", "name" : "User",\n  "fields" : [ { "name" : "name", '
                    '"type" : "string" } ] }')
AVRO_USER_REORDERED = '{"name":"User","fields":[{"type":"string","name":"name"}],"type":"record"}'
AVRO_USER_DOC = '{"type":"record","name":"User","doc":"a user","fields":[{"name":"name","type":"string"}]}'
AVRO_STRING = '"string"'
AVRO_STRING_OBJ = '{"type":"string"}'
AVRO_INT = '"int"'
AVRO_LONG = '"long"'
AVRO_ENUM = '{"type":"enum","name":"Color","symbols":["RED","GREEN"]}'
AVRO_ENUM_MORE = '{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"]}'
AVRO_DEP = '{"type":"record","name":"Dep","namespace":"com.x","fields":[{"name":"v","type":"int"}]}'
AVRO_USES_DEP = ('{"type":"record","name":"Holder","namespace":"com.x","fields":'
                 '[{"name":"d","type":"com.x.Dep"}]}')
AVRO_BAD_MISSING_FIELDS = '{"type":"record","name":"X"}'
AVRO_BAD_TYPE = '{"type":"nosuchtype"}'
AVRO_BAD_JSON = '{"type":"record",'
AVRO_BAD_DEFAULT = '{"type":"record","name":"X","fields":[{"name":"a","type":"int","default":"x"}]}'
AVRO_BAD_NAME = '{"type":"record","name":"1X","fields":[]}'
AVRO_DUP_FIELD = ('{"type":"record","name":"X","fields":[{"name":"a","type":"int"},'
                  '{"name":"a","type":"int"}]}')
AVRO_UNION = '["null","string"]'
AVRO_NESTED_UNION = '["null",["int"]]'

JSON_A = '{"type":"object","properties":{"a":{"type":"string"}}}'
JSON_A_SPACED = '{ "type": "object", "properties": { "a": { "type": "string" } } }'
JSON_A_REORDERED = '{"properties":{"a":{"type":"string"}},"type":"object"}'
JSON_A_CLOSED = '{"type":"object","properties":{"a":{"type":"string"}},"additionalProperties":false}'
JSON_AB_CLOSED = ('{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},'
                  '"additionalProperties":false}')
JSON_A_INT = '{"type":"object","properties":{"a":{"type":"integer"}}}'
JSON_BAD_TYPE = '{"type":12}'
JSON_BAD_JSON = '{"type":'
JSON_BAD_REF = '{"$ref":"#/definitions/missing"}'
JSON_DEP = '{"type":"object","properties":{"v":{"type":"integer"}}}'
JSON_USES_DEP = '{"type":"object","properties":{"d":{"$ref":"dep.json"}}}'
JSON_TRUE = 'true'

PROTO_M = 'syntax = "proto3";\nmessage M {\n  string a = 1;\n}\n'
PROTO_M_SPACED = 'syntax = "proto3";\n\n\nmessage   M { string a = 1; }\n'
PROTO_M_B = 'syntax = "proto3";\nmessage M {\n  string a = 1;\n  int32 b = 2;\n}\n'
PROTO_M_INT = 'syntax = "proto3";\nmessage M {\n  int32 a = 1;\n}\n'
PROTO_M_PKG = 'syntax = "proto3";\npackage p.q;\nmessage M {\n  string a = 1;\n}\n'
PROTO_TWO = 'syntax = "proto3";\nmessage A {\n  string a = 1;\n}\nmessage B {\n  A a = 1;\n}\n'
PROTO_BAD = 'message {'
PROTO_BAD_TYPE = 'syntax = "proto3";\nmessage M {\n  Missing a = 1;\n}\n'
PROTO_DEP = 'syntax = "proto3";\npackage dep;\nmessage D {\n  int32 v = 1;\n}\n'
PROTO_USES_DEP = ('syntax = "proto3";\nimport "dep.proto";\nmessage H {\n  dep.D d = 1;\n}\n')
PROTO_WKT = ('syntax = "proto3";\nimport "google/protobuf/timestamp.proto";\n'
             'message T {\n  google.protobuf.Timestamp ts = 1;\n}\n')
PROTO_ENUM = 'syntax = "proto3";\nenum E {\n  E_UNKNOWN = 0;\n  E_A = 1;\n}\nmessage M {\n  E e = 1;\n}\n'

LEVELS = ["NONE", "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL",
          "FULL_TRANSITIVE"]
MODES = ["READWRITE", "READONLY", "READONLY_OVERRIDE", "IMPORT"]

# ---------------------------------------------------------------------------
# DSL
# ---------------------------------------------------------------------------

SCENARIOS = []


def q(s):
    return urllib.parse.quote(s, safe="")


class Sc:
    def __init__(self, name, doc=""):
        assert all(sc["name"] != name for sc in SCENARIOS), name
        self.name = name
        self.steps = []
        self.teardown = []
        SCENARIOS.append({"name": name, "doc": doc, "steps": self.steps, "teardown": self.teardown})

    # names -----------------------------------------------------------------
    def s(self, n="s"):
        """A subject in the default context."""
        return f"{self.name}.{n}"

    def ctx(self, n="c"):
        return f".{self.name}-{n}"

    def cs(self, n="s", c="c"):
        """A context-qualified subject."""
        return f":{self.ctx(c)}:{self.name}.{n}"

    # raw steps ---------------------------------------------------------------
    def req(self, m, p, body=None, raw=None, ct=None, cmp=None, ids=False, keep=None, note=None):
        st = {"m": m, "p": p}
        if body is not None:
            st["b"] = body
        if raw is not None:
            st["raw"] = raw
        if ct is not None:
            st["ct"] = ct
        if cmp:
            st["cmp"] = cmp
        if ids:
            st["ids"] = True
        if keep is not None:
            st["keep"] = keep
        if note:
            st["note"] = note
        self.steps.append(st)
        return self

    def get(self, p, **kw):
        return self.req("GET", p, **kw)

    def post(self, p, body=None, **kw):
        return self.req("POST", p, body, **kw)

    def put(self, p, body=None, **kw):
        return self.req("PUT", p, body, **kw)

    def delete(self, p, **kw):
        return self.req("DELETE", p, **kw)

    def restore_global(self):
        self.teardown.append({"m": "DELETE", "p": "/config"})
        self.teardown.append({"m": "PUT", "p": "/mode?force=true", "b": {"mode": "READWRITE"}})
        return self

    # helpers ---------------------------------------------------------------
    @staticmethod
    def body(schema, st=None, refs=None, **extra):
        b = {}
        if schema is not None:
            b["schema"] = schema
        if st is not None:
            b["schemaType"] = st
        if refs is not None:
            b["references"] = refs
        b.update(extra)
        return b

    def reg(self, subj, schema, st=None, refs=None, qs="", **extra):
        return self.post(f"/subjects/{q(subj)}/versions{qs}", self.body(schema, st, refs, **extra))

    def look(self, subj, schema, st=None, refs=None, qs="", **extra):
        return self.post(f"/subjects/{q(subj)}{qs}", self.body(schema, st, refs, **extra))

    def compat(self, subj, schema, st=None, version=None, qs="", refs=None):
        p = f"/compatibility/subjects/{q(subj)}/versions" + (f"/{version}" if version is not None else "")
        return self.post(p + qs, self.body(schema, st, refs))

    def ver(self, subj, v, qs=""):
        return self.get(f"/subjects/{q(subj)}/versions/{v}{qs}")

    def vers(self, subj, qs=""):
        return self.get(f"/subjects/{q(subj)}/versions{qs}")

    def del_ver(self, subj, v, qs=""):
        return self.delete(f"/subjects/{q(subj)}/versions/{v}{qs}")

    def del_subj(self, subj, qs=""):
        return self.delete(f"/subjects/{q(subj)}{qs}")

    def cfg(self, subj, body=None, qs=""):
        p = "/config" if subj is None else f"/config/{q(subj)}"
        return self.get(p + qs) if body is None else self.put(p + qs, body)

    def mode(self, subj, body=None, qs=""):
        p = "/mode" if subj is None else f"/mode/{q(subj)}"
        return self.get(p + qs) if body is None else self.put(p + qs, body)

    def subjects(self, qs=""):
        # Other scenarios' subjects are filtered out.
        return self.get(f"/subjects{qs}", keep=[self.name + ".", f":.{self.name}-"])

    def schemas(self, qs=""):
        return self.get(f"/schemas{qs}", keep=[self.name + ".", f":.{self.name}-"])

    def by_id(self, sym, suffix="", qs=""):
        return self.get(f"/schemas/ids/{sym}{suffix}{qs}")

    def none_compat(self, subj):
        return self.cfg(subj, {"compatibility": "NONE"})


TYPES = [
    # tag, schemaType, schema, compatible evolution, incompatible evolution, invalid, same-but-spaced
    ("avro", None, AVRO_USER, AVRO_USER_AGE, AVRO_USER_INT, AVRO_BAD_MISSING_FIELDS, AVRO_USER_SPACED),
    ("json", "JSON", JSON_A, JSON_A_REORDERED, JSON_A_INT, JSON_BAD_TYPE, JSON_A_SPACED),
    ("proto", "PROTOBUF", PROTO_M, PROTO_M_B, PROTO_M_INT, PROTO_BAD, PROTO_M_SPACED),
]

# ---------------------------------------------------------------------------
# Root, metadata, routing
# ---------------------------------------------------------------------------

sc = Sc("root", "root resource, metadata, schema types")
sc.get("/")
sc.post("/", {})
sc.post("/", {"a": "b"})
sc.post("/", raw="not json")
sc.get("/v1/metadata/id", cmp="keys")
sc.get("/v1/metadata/version", cmp="keys")
sc.get("/schemas/types")

sc = Sc("routing", "unknown paths and methods")
sc.get("/nope")
sc.get("/subjects/x/nope")
sc.get("/schemas/ids")
sc.put("/subjects", {})
sc.delete("/schemas/types")
sc.post("/config", {"compatibility": "NONE"})
sc.delete("/mode")
sc.post("/contexts")
sc.get("/subjects/")
sc.get("/schemas/")

# ---------------------------------------------------------------------------
# Registration: per schema type
# ---------------------------------------------------------------------------

for tag, st, a, b, bad_evo, invalid, spaced in TYPES:
    s = Sc(f"reg-{tag}", f"{tag}: register, repeat, lookup, evolution")
    S, T = s.s("a"), s.s("b")
    s.reg(S, a, st)
    s.reg(S, a, st)
    s.vers(S)
    s.ver(S, 1)
    s.ver(S, "latest")
    s.ver(S, -1)
    s.ver(S, 1, "/schema")
    s.reg(T, a, st)                      # same schema, other subject: same id
    s.by_id("{$id1}")
    s.by_id("{$id1}", "/schema")
    s.by_id("{$id1}", "/subjects")
    s.by_id("{$id1}", "/versions")
    s.look(S, a, st)
    s.look(S, b, st)                     # not registered here
    s.look(s.s("missing"), a, st)
    s.reg(S, b, st)                      # compatible evolution
    s.reg(S, bad_evo, st)                # incompatible -> 409
    s.compat(S, bad_evo, st, "latest")
    s.compat(S, bad_evo, st, "latest", "?verbose=true")
    s.compat(S, b, st)
    s.compat(S, b, st, None, "?verbose=true")
    s.vers(S)
    s.reg(S, a, st)                      # old schema again: existing version 1
    s.reg(s.s("inv"), invalid, st)
    s.look(S, invalid, st)
    s.compat(S, invalid, st, "latest")
    s.reg(s.s("sp"), spaced, st)         # differently spaced: new id when normalize=false?
    s.look(S, spaced, st)
    s.look(S, spaced, st, qs="?normalize=true")
    s.reg(s.s("sp"), spaced, st, qs="?normalize=true")
    s.subjects()
    s.schemas(f"?subjectPrefix={q(s.name)}")

for tag, st, a, b, bad_evo, invalid, spaced in TYPES:
    s = Sc(f"reg-{tag}-normalize", f"{tag}: normalize=true on register/lookup/config")
    S = s.s("a")
    s.reg(S, a, st, qs="?normalize=true")
    s.reg(S, spaced, st, qs="?normalize=true")
    s.reg(S, spaced, st)
    s.look(S, spaced, st, qs="?normalize=true")
    s.cfg(s.s("n"), {"normalize": True})
    s.reg(s.s("n"), a, st)
    s.reg(s.s("n"), spaced, st)
    s.look(s.s("n"), spaced, st)
    s.cfg(s.s("n"))

s = Sc("reg-schema-type", "schemaType spelling and mismatch")
S = s.s("a")
for t in ["AVRO", "avro", "Avro", "json", "PROTOBUF", "protobuf", "XML", "", None]:
    s.reg(s.s(f"t-{t}"), AVRO_STRING, t) if t is not None else s.post(
        f"/subjects/{q(s.s('t-null'))}/versions", {"schema": AVRO_STRING, "schemaType": None})
s.reg(S, JSON_A, "JSON")
s.reg(S, AVRO_STRING, None)               # different type, BACKWARD: incompatible?
s.none_compat(S)
s.reg(S, AVRO_STRING, None)
s.reg(S, PROTO_M, "PROTOBUF")
s.vers(S)
s.reg(s.s("proto-as-avro"), PROTO_M, None)
s.reg(s.s("avro-as-json"), AVRO_USER, "JSON")
s.reg(s.s("json-as-proto"), JSON_A, "PROTOBUF")

s = Sc("reg-body", "malformed and odd request bodies")
S = s.s("a")
s.post(f"/subjects/{q(S)}/versions", raw="")
s.post(f"/subjects/{q(S)}/versions", raw="   ")
s.post(f"/subjects/{q(S)}/versions", raw="{")
s.post(f"/subjects/{q(S)}/versions", raw="null")
s.post(f"/subjects/{q(S)}/versions", raw="[]")
s.post(f"/subjects/{q(S)}/versions", raw='"string"')
s.post(f"/subjects/{q(S)}/versions", raw="42")
s.post(f"/subjects/{q(S)}/versions", {})
s.post(f"/subjects/{q(S)}/versions", {"schema": None})
s.post(f"/subjects/{q(S)}/versions", {"schema": ""})
s.post(f"/subjects/{q(S)}/versions", {"schema": "   "})
s.post(f"/subjects/{q(S)}/versions", {"schema": 42})
s.post(f"/subjects/{q(S)}/versions", {"schema": {"type": "string"}})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "unknownField": 1})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "references": None})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "references": []})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "references": {}})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "metadata": None, "ruleSet": None})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "version": "x"})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "id": "x"})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "schemaType": 5})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "metadata": {"properties": {"k": 1}}})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "metadata": {"tags": {"f": ["PII"]}}})
s.post(f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING, "metadata": {"sensitive": ["k"]}})
s.vers(S)

s = Sc("reg-content-type", "Content-Type variants")
S = s.s("a")
for i, ct in enumerate(["application/vnd.schemaregistry.v1+json", "application/vnd.schemaregistry+json",
                        "application/json", "application/octet-stream", "text/plain", ""]):
    s.req("POST", f"/subjects/{q(S)}/versions", {"schema": AVRO_STRING}, ct=ct)
s.req("POST", f"/subjects/{q(S)}", {"schema": AVRO_STRING}, ct="text/plain")
s.req("PUT", f"/config/{q(S)}", {"compatibility": "NONE"}, ct="text/plain")

s = Sc("reg-avro-invalid", "invalid Avro schemas")
for i, bad in enumerate([AVRO_BAD_MISSING_FIELDS, AVRO_BAD_TYPE, AVRO_BAD_JSON, AVRO_BAD_DEFAULT,
                         AVRO_BAD_NAME, AVRO_DUP_FIELD, AVRO_NESTED_UNION, '""', '[]', '{}', 'nul',
                         '{"type":"array"}', '{"type":"map","values":"nope"}',
                         '{"type":"fixed","name":"F"}', '{"type":"enum","name":"E","symbols":["A","A"]}']):
    s.reg(s.s(f"b{i}"), bad)
s.subjects()

s = Sc("reg-avro-valid", "valid Avro shapes and their canonical text")
for i, good in enumerate([AVRO_STRING, AVRO_STRING_OBJ, AVRO_INT, AVRO_ENUM, AVRO_UNION, AVRO_USER_DOC,
                          '{"type":"array","items":"int"}', '{"type":"map","values":"long"}',
                          '{"type":"fixed","name":"F","size":4}',
                          '{"type":"bytes","logicalType":"decimal","precision":4,"scale":2}',
                          '{"type":"int","logicalType":"date"}',
                          '{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":1,'
                          '"doc":"d","aliases":["b"],"order":"descending"}],"aliases":["Q"],"custom":"x"}']):
    s.reg(s.s(f"g{i}"), good)
    s.ver(s.s(f"g{i}"), 1)

s = Sc("reg-json-invalid", "invalid JSON schemas")
for i, bad in enumerate([JSON_BAD_TYPE, JSON_BAD_JSON, JSON_BAD_REF, '[]', '12', '"x"',
                         '{"type":"object","properties":{"a":{"type":"nope"}}}',
                         '{"type":"object","required":"a"}', '{"minimum":"x"}',
                         '{"$schema":"http://example.com/unknown","type":"object"}']):
    s.reg(s.s(f"b{i}"), bad, "JSON")

s = Sc("reg-json-valid", "valid JSON schemas and drafts")
for i, good in enumerate([JSON_A, JSON_TRUE, '{}', 'false', JSON_A_CLOSED,
                          '{"$schema":"http://json-schema.org/draft-07/schema#","type":"string"}',
                          '{"$schema":"http://json-schema.org/draft-04/schema#","type":"string"}',
                          '{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}',
                          '{"type":"object","definitions":{"x":{"type":"string"}},'
                          '"properties":{"a":{"$ref":"#/definitions/x"}}}']):
    s.reg(s.s(f"g{i}"), good, "JSON")
    s.ver(s.s(f"g{i}"), 1)

s = Sc("reg-proto-invalid", "invalid Protobuf schemas")
for i, bad in enumerate([PROTO_BAD, PROTO_BAD_TYPE, '', 'syntax = "proto4";',
                         'syntax = "proto3";\nimport "missing.proto";\nmessage M { string a = 1; }\n',
                         'syntax = "proto3";\nmessage M { string a = 0; }\n',
                         'syntax = "proto3";\nmessage M { string a = 1; }\nmessage M { string b = 1; }\n']):
    s.reg(s.s(f"b{i}"), bad, "PROTOBUF")

s = Sc("reg-proto-valid", "valid Protobuf schemas and their canonical text")
for i, good in enumerate([PROTO_M, PROTO_M_PKG, PROTO_TWO, PROTO_WKT, PROTO_ENUM,
                          'syntax = "proto2";\nmessage M {\n  optional string a = 1 [default = "x"];\n}\n',
                          'message M {\n  optional string a = 1;\n}\n',
                          'syntax = "proto3";\nmessage M {\n  map<string, int32> m = 1;\n  oneof o {\n'
                          '    string x = 2;\n    int32 y = 3;\n  }\n  reserved 5, 7 to 9;\n'
                          '  reserved "z";\n}\n',
                          'syntax = "proto3";\noption java_package = "a.b";\nmessage M {\n'
                          '  string a = 1 [deprecated = true, json_name = "A"];\n}\n'
                          'service S {\n  rpc Get (M) returns (M);\n}\n']):
    s.reg(s.s(f"g{i}"), good, "PROTOBUF")
    s.ver(s.s(f"g{i}"), 1)
    s.ver(s.s(f"g{i}"), 1, "/schema")
    s.ver(s.s(f"g{i}"), 1, "/schema?format=serialized")
    s.by_id("{$id%d}" % (i + 1), "", "?format=serialized")

# ---------------------------------------------------------------------------
# Registration: explicit id/version, metadata, rule sets
# ---------------------------------------------------------------------------

s = Sc("reg-explicit-id", "id/version in the body outside IMPORT mode")
S = s.s("a")
s.reg(S, AVRO_STRING)
s.reg(S, AVRO_STRING, id="{$id1}")         # same schema, its own id
s.reg(S, AVRO_STRING, id=987654)            # same schema, other id
s.reg(s.s("b"), AVRO_INT, id=987655)        # new schema with an id
s.reg(S, AVRO_STRING, version=1)
s.reg(S, AVRO_STRING, version=2)
s.reg(S, AVRO_STRING, version=0)
s.reg(S, AVRO_STRING, version=-1)
s.none_compat(S)
s.reg(S, AVRO_INT, version=3)
s.reg(S, AVRO_INT, version=2)
s.ver(S, 2)
s.reg(S, AVRO_LONG, version=3)
s.ver(S, 3)
s.vers(S)

s = Sc("reg-metadata", "metadata and rule sets on register")
S = s.s("a")
s.reg(S, AVRO_STRING, metadata={"properties": {"owner": "x"}})
s.reg(S, AVRO_STRING, metadata={"properties": {"owner": "x"}})
s.reg(S, AVRO_STRING)
s.ver(S, "latest")
s.reg(S, AVRO_STRING, metadata={"properties": {"owner": "y"}})
s.reg(S, None, metadata={"properties": {"owner": "z"}})
s.vers(S)
s.ver(S, 3)
s.look(S, AVRO_STRING)
s.look(S, AVRO_STRING, metadata={"properties": {"owner": "x"}})
s.get(f"/subjects/{q(S)}/metadata?key=owner&value=x")
s.get(f"/subjects/{q(S)}/metadata?key=owner&value=nope")
s.get(f"/subjects/{q(S)}/metadata")
s.get(f"/subjects/{q(s.s('missing'))}/metadata?key=owner&value=x")
s.reg(s.s("r"), AVRO_STRING, ruleSet={"domainRules": [{"name": "r", "kind": "CONDITION", "type": "CEL",
                                                        "mode": "WRITE", "expr": "true"}]})
s.ver(s.s("r"), 1)
s.reg(s.s("m"), AVRO_STRING, metadata={"tags": {"name": ["PII"]}, "properties": {"a": "b"},
                                        "sensitive": ["a"]})
s.ver(s.s("m"), 1)
s.reg(s.s("m"), AVRO_STRING, metadata={})
s.reg(s.s("m"), AVRO_STRING, metadata={"properties": {}})
s.vers(s.s("m"))

s = Sc("reg-config-metadata", "defaultMetadata / overrideMetadata from config")
S = s.s("a")
s.cfg(S, {"defaultMetadata": {"properties": {"d": "1"}}, "overrideMetadata": {"properties": {"o": "1"}}})
s.cfg(S)
s.reg(S, AVRO_STRING)
s.ver(S, 1)
s.reg(S, AVRO_STRING, metadata={"properties": {"d": "2", "o": "2"}})
s.ver(S, "latest")
s.reg(S, AVRO_STRING)

# ---------------------------------------------------------------------------
# References
# ---------------------------------------------------------------------------

s = Sc("refs-avro", "Avro references: register, lookup, referencedby, delete protection")
D, H = s.s("dep"), s.s("holder")
s.reg(D, AVRO_DEP)
ref = [{"name": "com.x.Dep", "subject": D, "version": 1}]
s.reg(H, AVRO_USES_DEP, refs=ref)
s.reg(H, AVRO_USES_DEP, refs=ref)
s.look(H, AVRO_USES_DEP, refs=ref)
s.reg(s.s("noref"), AVRO_USES_DEP)
s.ver(H, 1)
s.by_id("{$id2}")
s.get(f"/subjects/{q(D)}/versions/1/referencedby", ids=True)
s.get(f"/subjects/{q(D)}/versions/latest/referencedby", ids=True)
s.get(f"/subjects/{q(H)}/versions/1/referencedby", ids=True)
s.get(f"/subjects/{q(s.s('missing'))}/versions/1/referencedby", ids=True)
s.get(f"/subjects/{q(D)}/versions/9/referencedby", ids=True)
s.del_ver(D, 1)
s.del_subj(D)
s.del_ver(H, 1)
s.del_ver(D, 1)
s.del_ver(H, 1, "?permanent=true")
s.del_ver(D, 1)
s.get(f"/subjects/{q(D)}/versions/1/referencedby", ids=True)

s = Sc("refs-bad", "invalid references")
D = s.s("dep")
s.reg(D, AVRO_DEP)
for i, r in enumerate([
    [{"name": "com.x.Dep", "subject": s.s("nope"), "version": 1}],
    [{"name": "com.x.Dep", "subject": D, "version": 5}],
    [{"name": "com.x.Dep", "subject": D, "version": -1}],
    [{"name": "com.x.Dep", "subject": D, "version": 0}],
    [{"name": "com.x.Dep", "subject": D}],
    [{"name": "com.x.Dep", "version": 1}],
    [{"subject": D, "version": 1}],
    [{"name": "", "subject": D, "version": 1}],
    [{"name": "com.x.Dep", "subject": D, "version": 1}, {"name": "com.x.Dep", "subject": D, "version": 1}],
    [{"name": "wrong.Name", "subject": D, "version": 1}],
]):
    s.reg(s.s(f"h{i}"), AVRO_USES_DEP, refs=r)

s = Sc("refs-json", "JSON references")
D, H = s.s("dep"), s.s("holder")
s.reg(D, JSON_DEP, "JSON")
ref = [{"name": "dep.json", "subject": D, "version": 1}]
s.reg(H, JSON_USES_DEP, "JSON", refs=ref)
s.reg(H, JSON_USES_DEP, "JSON", refs=ref)
s.reg(s.s("noref"), JSON_USES_DEP, "JSON")
s.ver(H, 1)
s.get(f"/subjects/{q(D)}/versions/1/referencedby", ids=True)

s = Sc("refs-proto", "Protobuf references")
D, H = s.s("dep"), s.s("holder")
s.reg(D, PROTO_DEP, "PROTOBUF")
ref = [{"name": "dep.proto", "subject": D, "version": 1}]
s.reg(H, PROTO_USES_DEP, "PROTOBUF", refs=ref)
s.reg(H, PROTO_USES_DEP, "PROTOBUF", refs=ref)
s.reg(s.s("noref"), PROTO_USES_DEP, "PROTOBUF")
s.reg(s.s("wrongname"), PROTO_USES_DEP, "PROTOBUF", refs=[{"name": "other.proto", "subject": D, "version": 1}])
s.ver(H, 1)
s.ver(H, 1, "/schema?format=serialized")
s.get(f"/subjects/{q(D)}/versions/1/referencedby", ids=True)
s.del_subj(D)
s.del_subj(H)
s.del_subj(D)
s.del_subj(D, "?permanent=true")
s.del_subj(H, "?permanent=true")
s.del_subj(D, "?permanent=true")

# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------

s = Sc("versions", "version path parameter forms")
S = s.s("a")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER_AGE)
for v in ["1", "2", "3", "latest", "LATEST", "Latest", "-1", "-2", "0", "abc", "1.0", "01", "+1",
          "2147483647", "2147483648", "99999999999", "%20", "1%20"]:
    s.ver(S, v)
for v in ["1", "latest", "-1", "0", "abc", "3"]:
    s.ver(S, v, "/schema")
s.vers(s.s("missing"))
s.ver(s.s("missing"), 1)
s.ver(s.s("missing"), "latest")
s.ver(s.s("missing"), "abc")
s.vers(S, "?deleted=true")
s.vers(S, "?deletedOnly=true")
s.vers(S, "?deleted=false")
s.vers(S, "?deleted=yes")

s = Sc("versions-deleted", "reading soft-deleted versions")
S = s.s("a")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER_AGE)
s.del_ver(S, 1)
s.vers(S)
s.vers(S, "?deleted=true")
s.vers(S, "?deletedOnly=true")
s.ver(S, 1)
s.ver(S, 1, "?deleted=true")
s.ver(S, 1, "/schema")
s.ver(S, 1, "/schema?deleted=true")
s.ver(S, "latest", "?deleted=true")
s.by_id("{$id1}")
s.by_id("{$id1}", "/subjects")
s.by_id("{$id1}", "/subjects", "?deleted=true")
s.by_id("{$id1}", "/versions")
s.by_id("{$id1}", "/versions", "?deleted=true")
s.look(S, AVRO_USER)
s.look(S, AVRO_USER, qs="?deleted=true")
s.del_ver(S, 2)
s.vers(S)
s.ver(S, "latest")
s.ver(S, "latest", "?deleted=true")
s.subjects()
s.subjects("?deleted=true")
s.subjects("?deletedOnly=true")

# ---------------------------------------------------------------------------
# Deletes: every order, repeated
# ---------------------------------------------------------------------------

s = Sc("delete-version", "version delete state machine")
S = s.s("a")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER_AGE)
s.reg(S, AVRO_USER_DOC)
s.del_ver(S, 1, "?permanent=true")          # not soft-deleted first
s.del_ver(S, 1)
s.del_ver(S, 1)                             # again
s.del_ver(S, 1, "?permanent=true")
s.del_ver(S, 1, "?permanent=true")          # again
s.del_ver(S, 1)
s.del_ver(S, "latest")
s.del_ver(S, "latest")
s.del_ver(S, -1)
s.del_ver(S, 0)
s.del_ver(S, "abc")
s.del_ver(S, 9)
s.del_ver(s.s("missing"), 1)
s.del_ver(s.s("missing"), "latest")
s.vers(S, "?deleted=true")
s.del_ver(S, 3)
s.vers(S)
s.subjects()
s.subjects("?deleted=true")
s.reg(S, AVRO_USER_AGE)                     # re-register after all soft-deleted
s.vers(S, "?deleted=true")
s.del_ver(S, "latest", "?permanent=true")
s.del_ver(S, 2, "?permanent=false")

s = Sc("delete-subject", "subject delete state machine")
S = s.s("a")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER_AGE)
s.del_subj(S, "?permanent=true")
s.del_subj(S)
s.del_subj(S)
s.vers(S)
s.vers(S, "?deleted=true")
s.ver(S, 1)
s.ver(S, 1, "?deleted=true")
s.by_id("{$id1}")
s.by_id("{$id1}", "/subjects")
s.by_id("{$id1}", "/subjects", "?deleted=true")
s.look(S, AVRO_USER)
s.look(S, AVRO_USER, qs="?deleted=true")
s.subjects()
s.subjects("?deleted=true")
s.subjects("?deletedOnly=true")
s.reg(S, AVRO_USER_INT)                     # after soft delete: compat not checked against deleted
s.vers(S, "?deleted=true")
s.del_subj(S)
s.del_subj(S, "?permanent=true")
s.del_subj(S, "?permanent=true")
s.vers(S, "?deleted=true")
s.by_id("{$id1}")
s.by_id("{$id3}")
s.reg(S, AVRO_USER)                         # after hard delete: versions restart
s.vers(S)
s.del_subj(s.s("missing"))
s.del_subj(s.s("missing"), "?permanent=true")

s = Sc("delete-shared-id", "an id shared by two subjects survives one hard delete")
A, B = s.s("a"), s.s("b")
s.reg(A, AVRO_STRING)
s.reg(B, AVRO_STRING)
s.del_subj(A)
s.del_subj(A, "?permanent=true")
s.by_id("{$id1}")
s.by_id("{$id1}", "/subjects")
s.by_id("{$id1}", "/versions")
s.del_subj(B)
s.del_subj(B, "?permanent=true")
s.by_id("{$id1}")
s.reg(A, AVRO_STRING)

# ---------------------------------------------------------------------------
# Schemas by id, listings
# ---------------------------------------------------------------------------

s = Sc("schemas-by-id", "id path parameter forms and query options")
S = s.s("a")
s.reg(S, AVRO_USER)
for sfx in ["", "/schema", "/subjects", "/versions"]:
    for bad in ["0", "-1", "abc", "1.5", "2147483648", "99999999"]:
        s.get(f"/schemas/ids/{bad}{sfx}")
s.by_id("{$id1}", "", f"?subject={q(S)}")
s.by_id("{$id1}", "", f"?subject={q(s.s('other'))}")
s.by_id("{$id1}", "", "?fetchMaxId=true")
s.by_id("{$id1}", "", "?format=resolved")
s.by_id("{$id1}", "", "?format=nope")
s.by_id("{$id1}", "/schema", "?format=nope")
s.by_id("{$id1}", "/subjects", f"?subject={q(S)}")
s.by_id("{$id1}", "/subjects", f"?subject={q(s.s('other'))}")
s.by_id("{$id1}", "/versions", f"?subject={q(S)}")
s.by_id("{$id1}", "/versions", f"?subject={q(s.s('other'))}")

s = Sc("schemas-list", "GET /schemas filters and paging")
for i, sch in enumerate([AVRO_STRING, AVRO_INT, AVRO_LONG]):
    s.reg(s.s(f"s{i}"), sch)
s.none_compat(s.s("s0"))
s.reg(s.s("s0"), AVRO_ENUM)
s.del_ver(s.s("s1"), 1)
P = q(s.name)
for qs in ["", "&deleted=true", "&latestOnly=true", "&latestOnly=true&deleted=true", "&offset=1",
           "&limit=2", "&offset=1&limit=1", "&offset=100", "&limit=0", "&limit=-5", "&offset=-1",
           "&limit=abc", "&offset=abc", "&aliases=true", "&ruleType=CEL"]:
    s.get(f"/schemas?subjectPrefix={P}{qs}")
s.get(f"/schemas?subjectPrefix={P}.s1")
s.get(f"/schemas?subjectPrefix={P}.s1&deleted=true")
s.get("/schemas?subjectPrefix=no-such-prefix")

s = Sc("subjects-list", "GET /subjects filters and paging")
for i in range(4):
    s.reg(s.s(f"s{i}"), AVRO_STRING)
s.del_subj(s.s("s1"))
s.del_ver(s.s("s2"), 1)
P = q(s.name)
for qs in ["", "&deleted=true", "&deletedOnly=true", "&deleted=true&deletedOnly=true", "&offset=1",
           "&limit=2", "&offset=1&limit=1", "&offset=100", "&limit=0", "&limit=-1", "&offset=-3",
           "&limit=x"]:
    s.get(f"/subjects?subjectPrefix={P}{qs}")
s.get(f"/subjects?subjectPrefix={P}.s3")
s.get("/subjects?subjectPrefix=no-such-prefix")

# ---------------------------------------------------------------------------
# Subject names
# ---------------------------------------------------------------------------

s = Sc("subject-names", "unusual subject names")
for i, n in enumerate(["with space", "slash/inside", "unicode-é中", "dots.and-dashes_", "UPPER",
                       "%percent", "plus+sign", "q?mark", "hash#tag", "a" * 300, "tab\there",
                       "trailing ", "semi;colon", "amp&er", "eq=al", "quote\"d", "back\\slash",
                       "star*", "colon:mid"]):
    S = s.s(n)
    s.reg(S, AVRO_STRING)
    s.vers(S)
    s.ver(S, 1)
    s.cfg(S, {"compatibility": "NONE"})
    s.cfg(S)
s.subjects()

s = Sc("subject-names-raw", "raw (unencoded) path forms")
s.post(f"/subjects/{s.name}.plain/versions", {"schema": AVRO_STRING})
s.get(f"/subjects/{s.name}.plain/versions")
s.get(f"/subjects/{s.name}.plain/versions/1/")
s.get(f"/subjects/{s.name}.plain//versions")
s.get(f"/subjects/:.{s.name}-c:{s.name}.x/versions")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

s = Sc("config-global", "top-level config")
s.reg(s.s("a"), AVRO_STRING)
s.cfg(None)
for lvl in LEVELS:
    s.cfg(None, {"compatibility": lvl})
    s.cfg(None)
s.cfg(None, {"compatibility": "backward"})
s.cfg(None, {"compatibility": "Full"})
s.cfg(None, {"compatibility": "NOPE"})
s.cfg(None, {"compatibility": ""})
s.cfg(None, {"compatibility": None})
s.cfg(None, {"compatibilityLevel": "FORWARD"})
s.cfg(None, {})
s.cfg(None, {"normalize": True})
s.cfg(None)
s.cfg(None, {"normalize": "yes"})
s.cfg(None, {"compatibility": 5})
s.put("/config", raw="{")
s.put("/config", raw="")
s.cfg(None, {"compatibility": "FULL", "unknownKey": 1})
s.cfg(None, {"validateFields": True})
s.cfg(None, {"compatibilityGroup": "application.major.version"})
s.cfg(None)
s.cfg(None, qs="?defaultToGlobal=true")
s.delete("/config")
s.cfg(None)
s.delete("/config")
s.restore_global()

s = Sc("config-subject", "subject-level config")
S = s.s("a")
s.cfg(S)
s.cfg(S, qs="?defaultToGlobal=true")
s.cfg(S, {"compatibility": "FULL"})
s.cfg(S, {"compatibility": "FULL"})
s.cfg(S)
s.cfg(S, qs="?defaultToGlobal=true")
s.cfg(S, {"compatibility": "nope"})
s.cfg(S, {})
s.cfg(S)
s.cfg(S, {"normalize": True})
s.cfg(S)
s.cfg(S, {"alias": s.s("target")})
s.cfg(S)
s.cfg(S, {"compatibility": "NONE", "normalize": False, "validateFields": False})
s.cfg(S)
s.delete(f"/config/{q(S)}")
s.cfg(S)
s.delete(f"/config/{q(S)}")
s.delete(f"/config/{q(s.s('never'))}")
s.cfg(s.s("never"), {"compatibility": "NONE"})
s.vers(s.s("never"))
s.subjects()

s = Sc("config-levels-enforced", "each level applied to a 3-version history")
for lvl in LEVELS:
    S = s.s(lvl.lower())
    s.cfg(S, {"compatibility": "NONE"})
    s.reg(S, AVRO_USER)
    s.reg(S, AVRO_USER_INT)
    s.reg(S, AVRO_USER_AGE)
    s.cfg(S, {"compatibility": lvl})
    s.reg(S, AVRO_USER_AGE_NODEFAULT)
    s.compat(S, AVRO_USER_AGE_NODEFAULT, None, None, "?verbose=true")
    s.compat(S, AVRO_USER_AGE_NODEFAULT, None, 1, "?verbose=true")
    s.compat(S, AVRO_USER, None, None, "?verbose=true")
    s.reg(S, AVRO_USER)

s = Sc("config-alias", "subject aliases")
T, A = s.s("target"), s.s("alias")
s.reg(T, AVRO_STRING)
s.cfg(A, {"alias": T})
s.vers(A)
s.ver(A, 1)
s.ver(A, "latest")
s.look(A, AVRO_STRING)
s.reg(A, AVRO_INT)
s.vers(T)
s.get(f"/schemas?subjectPrefix={q(s.name)}&aliases=true")
s.cfg(A, {"alias": s.s("nowhere")})
s.vers(A)

s = Sc("config-context", "context-level config and inheritance")
C, S = s.ctx(), s.cs("a")
s.cfg(f":{C}:", {"compatibility": "NONE"})
s.cfg(f":{C}:")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER_INT)
s.cfg(S)
s.cfg(S, qs="?defaultToGlobal=true")
s.delete(f"/config/{q(':' + C + ':')}")
s.reg(S, AVRO_USER_AGE_NODEFAULT)
s.cfg(f":{C}:")

# ---------------------------------------------------------------------------
# Mode
# ---------------------------------------------------------------------------

s = Sc("mode-global", "top-level mode")
s.reg(s.s("a"), AVRO_STRING)
s.mode(None)
s.mode(None, {"mode": "READONLY"})
s.mode(None)
s.reg(s.s("a"), AVRO_INT)
s.look(s.s("a"), AVRO_STRING)
s.del_subj(s.s("a"))
s.cfg(s.s("a"), {"compatibility": "NONE"})
s.mode(None, {"mode": "READONLY_OVERRIDE"})
s.reg(s.s("a"), AVRO_INT)
s.mode(None, {"mode": "IMPORT"})
s.mode(None, {"mode": "IMPORT"}, "?force=true")
s.mode(None)
s.mode(None, {"mode": "READWRITE"})
s.mode(None, {"mode": "readwrite"})
s.mode(None, {"mode": "NOPE"})
s.mode(None, {"mode": ""})
s.mode(None, {"mode": None})
s.mode(None, {})
s.put("/mode", raw="{")
s.mode(None, {"mode": "READWRITE", "extra": 1})
s.mode(None, qs="?defaultToGlobal=true")
s.mode(None)
s.restore_global()

s = Sc("mode-subject", "subject-level mode")
S = s.s("a")
s.reg(S, AVRO_STRING)
s.mode(S)
s.mode(S, qs="?defaultToGlobal=true")
s.mode(S, {"mode": "READONLY"})
s.mode(S, {"mode": "READONLY"})
s.mode(S)
s.reg(S, AVRO_INT)
s.reg(s.s("other"), AVRO_INT)
s.del_ver(S, 1)
s.del_subj(S)
s.cfg(S, {"compatibility": "NONE"})
s.ver(S, 1)
s.look(S, AVRO_STRING)
s.compat(S, AVRO_INT)
s.mode(S, {"mode": "IMPORT"})
s.mode(S, {"mode": "IMPORT"}, "?force=true")
s.mode(S)
s.mode(S, {"mode": "READWRITE"})
s.delete(f"/mode/{q(S)}")
s.mode(S)
s.delete(f"/mode/{q(S)}")
s.delete(f"/mode/{q(s.s('never'))}")
s.mode(s.s("never"), {"mode": "IMPORT"})
s.mode(s.s("never"))
s.mode(S, {"mode": "bogus"})

s = Sc("mode-context", "context-level mode")
C, S = s.ctx(), s.cs("a")
s.mode(f":{C}:", {"mode": "IMPORT"})
s.mode(f":{C}:")
s.mode(S, qs="?defaultToGlobal=true")
s.reg(S, AVRO_STRING, id=300001, version=1)
s.by_id("300001", "", f"?subject={q(S)}")
s.mode(f":{C}:", {"mode": "READWRITE"})
s.reg(S, AVRO_INT)
s.vers(S)

# ---------------------------------------------------------------------------
# Import mode
# ---------------------------------------------------------------------------

s = Sc("import", "IMPORT mode on a fresh subject")
S, T = s.s("a"), s.s("b")
s.mode(S, {"mode": "IMPORT"})
s.mode(T, {"mode": "IMPORT"})
s.reg(S, AVRO_STRING)                          # no id
s.reg(S, AVRO_STRING, id=400001)               # no version
s.reg(S, AVRO_STRING, id=400001, version=1)
s.reg(S, AVRO_STRING, id=400001, version=1)    # replay
s.reg(S, AVRO_INT, id=400001, version=2)       # id already used for other schema
s.reg(S, AVRO_INT, id=400002, version=5)       # version gap
s.reg(S, AVRO_LONG, id=400003, version=3)      # lower than latest
s.reg(S, AVRO_USER_INT, id=400004, version=5)  # version already exists
s.reg(T, AVRO_STRING, id=400001, version=1)    # same schema+id, other subject
s.reg(T, AVRO_STRING, id=400009, version=2)    # same schema, other id
s.reg(T, AVRO_USER, id=0, version=3)
s.reg(T, AVRO_USER, id=-1, version=3)
s.reg(T, AVRO_USER, id=400010, version=0)
s.vers(S)
s.vers(T)
s.by_id("400001", "/versions")
s.by_id("400002")
s.mode(S, {"mode": "READWRITE"})
s.reg(S, AVRO_USER_INT)
s.vers(S)
s.reg(S, AVRO_STRING, id=400001, version=9)
s.reg(S, AVRO_STRING, id=400001)

s = Sc("import-existing", "switching a non-empty subject to IMPORT")
S = s.s("a")
s.reg(S, AVRO_STRING)
s.mode(S, {"mode": "IMPORT"})
s.mode(S)
s.mode(S, {"mode": "IMPORT"}, "?force=true")
s.reg(S, AVRO_INT, id=410001, version=2)
s.reg(S, AVRO_INT, version=3)
s.vers(S)

# ---------------------------------------------------------------------------
# Compatibility endpoint
# ---------------------------------------------------------------------------

s = Sc("compat-endpoint", "POST /compatibility edge cases")
S = s.s("a")
s.compat(s.s("missing"), AVRO_STRING)
s.compat(s.s("missing"), AVRO_STRING, None, "latest")
s.compat(s.s("missing"), AVRO_STRING, None, 1)
s.reg(S, AVRO_USER)
for v in ["1", "latest", "-1", "2", "0", "abc"]:
    s.compat(S, AVRO_USER_INT, None, v)
s.compat(S, AVRO_USER_INT, None, None, "?verbose=false")
s.compat(S, AVRO_USER_INT, None, None, "?verbose=true")
s.compat(S, AVRO_BAD_JSON)
s.compat(S, AVRO_BAD_JSON, None, "latest", "?verbose=true")
s.compat(S, JSON_A, "JSON")
s.compat(S, JSON_A, "JSON", None, "?verbose=true")
s.compat(S, PROTO_M, "PROTOBUF", "latest", "?verbose=true")
s.post(f"/compatibility/subjects/{q(S)}/versions", raw="")
s.post(f"/compatibility/subjects/{q(S)}/versions", {})
s.post(f"/compatibility/subjects/{q(S)}/versions/latest", {"schema": AVRO_USER, "unknown": 1})
s.compat(S, AVRO_USER_AGE, None, None, "?normalize=true")
s.del_ver(S, 1)
s.compat(S, AVRO_USER_INT)
s.compat(S, AVRO_USER_INT, None, "latest")
s.compat(S, AVRO_USER_INT, None, 1)

s = Sc("compat-types", "compatibility verdicts with messages for each type")
for tag, st, a, b, bad_evo, invalid, spaced in TYPES:
    S = s.s(tag)
    s.reg(S, a, st)
    for lvl in ["BACKWARD", "FORWARD", "FULL"]:
        s.cfg(S, {"compatibility": lvl})
        s.compat(S, b, st, "latest", "?verbose=true")
        s.compat(S, bad_evo, st, "latest", "?verbose=true")
    s.cfg(S, {"compatibility": "NONE"})
    s.compat(S, bad_evo, st, "latest", "?verbose=true")

# ---------------------------------------------------------------------------
# Contexts
# ---------------------------------------------------------------------------

s = Sc("contexts", "context-qualified subjects and id resolution")
A, B, D = s.cs("a", "x"), s.cs("a", "y"), s.s("a")
s.reg(A, AVRO_STRING)
s.reg(B, AVRO_STRING)
s.reg(D, AVRO_STRING)
s.reg(A, AVRO_STRING)
s.get("/contexts", keep=["=.", f"={s.ctx('x')}", f"={s.ctx('y')}"])
s.subjects()
s.get(f"/subjects?subjectPrefix={q(':' + s.ctx('x') + ':')}")
s.get(f"/subjects?subjectPrefix={q(':*:')}", keep=[s.name + ".", f":.{s.name}-"])
s.vers(A)
s.ver(A, 1)
s.ver(f":{s.ctx('x')}:{s.name}.missing", 1)
s.by_id("{$id1}", "", f"?subject={q(A)}")
s.by_id("{$id2}", "", f"?subject={q(B)}")
s.by_id("{$id1}", "/subjects", f"?subject={q(A)}")
s.by_id("{$id1}", "/versions", f"?subject={q(A)}")
s.get(f"/contexts/{s.ctx('x')}/subjects/{q(s.name + '.a')}/versions")
s.get(f"/contexts/{s.ctx('x')}/subjects", keep=[s.name + ".", f":.{s.name}-"])
s.get(f"/contexts/{s.ctx('x')}/schemas/ids/{{$id1}}")
s.post(f"/contexts/{s.ctx('x')}/subjects/{q(s.name + '.b')}/versions", {"schema": AVRO_INT})
s.vers(s.cs("b", "x"))
s.get(f"/subjects/{q(':.' + s.name + '-x:')}/versions")
s.reg(f":{s.ctx('bad')}", AVRO_STRING)
s.reg(":.:" + s.name + ".dflt", AVRO_STRING)
s.vers(s.name + ".dflt")
s.del_subj(A)
s.del_subj(A, "?permanent=true")
s.get("/contexts", keep=["=.", f"={s.ctx('x')}", f"={s.ctx('y')}", f"={s.ctx('bad')}"])

s = Sc("ctxnames", "unusual context names")
for i, c in enumerate(["UPPER", "with-dash", "with_underscore", "1digit", "dot.inside"]):
    s.reg(f":.{s.name}-{c}:x", AVRO_STRING)
s.get("/contexts", keep=["=.", f".{s.name}-"])

# Confluent's content hash ignores the schema type: the same text under a
# second type fails with 42205. The colliding step is last (ALLOWED).
s = Sc("type-collision", "the same text as AVRO, then as JSON")
s.reg(s.s("avro"), AVRO_STRING)
s.reg(s.s("json"), AVRO_STRING, "JSON")
s = Sc("type-collision-2", "the same text as JSON, then as AVRO")
s.reg(s.s("json"), '{"type":"string"}', "JSON")
s.reg(s.s("avro"), '{"type":"string"}')

# ---------------------------------------------------------------------------
# Repeated mutations
# ---------------------------------------------------------------------------

s = Sc("twice", "every mutation applied twice")
S = s.s("a")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER)
s.cfg(S, {"compatibility": "FULL"})
s.cfg(S, {"compatibility": "FULL"})
s.mode(S, {"mode": "READWRITE"})
s.mode(S, {"mode": "READWRITE"})
s.delete(f"/config/{q(S)}")
s.delete(f"/config/{q(S)}")
s.delete(f"/mode/{q(S)}")
s.delete(f"/mode/{q(S)}")
s.del_ver(S, 1)
s.del_ver(S, 1)
s.del_ver(S, 1, "?permanent=true")
s.del_ver(S, 1, "?permanent=true")
s.reg(S, AVRO_USER)
s.reg(S, AVRO_USER)
s.del_subj(S)
s.del_subj(S)
s.del_subj(S, "?permanent=true")
s.del_subj(S, "?permanent=true")
s.vers(S, "?deleted=true")


# ---------------------------------------------------------------------------
# Second wave: Jackson coercion, scoping, aliases, cross-context reads, corners
# ---------------------------------------------------------------------------

s = Sc("jackson-coercion", "scalar coercions of request bodies")
for i, b in enumerate([
    {"schema": True}, {"schema": 1.5}, {"schema": AVRO_INT, "version": "1"}, {"schema": AVRO_INT, "version": 1.7},
    {"schema": AVRO_INT, "version": True}, {"schema": AVRO_INT, "version": ""}, {"schema": AVRO_INT, "version": 99999999999},
    {"schema": AVRO_INT, "id": "-1"}, {"schema": AVRO_INT, "schemaType": True}, {"schema": AVRO_INT, "schemaType": None},
    {"schema": AVRO_INT, "metadata": {"properties": {"k": {}}}},
    {"schema": AVRO_INT, "metadata": {"tags": {"f": "PII"}}}, {"schema": AVRO_INT, "metadata": {"sensitive": "k"}},
    {"schema": AVRO_INT, "metadata": "x"}, {"schema": AVRO_INT, "metadata": {"unknown": 1}},
    {"schema": AVRO_INT, "ruleSet": {"domainRules": [{"name": "r", "kind": "BOGUS", "type": "CEL"}]}},
    {"schema": AVRO_INT, "ruleSet": "x"},
    {"schema": AVRO_INT, "references": [{"name": "a", "subject": "b", "version": "1"}]},
    {"schema": AVRO_INT, "references": [None]}, {"schema": AVRO_INT, "references": "x"},
]):
    s.reg(s.s(f"r{i}"), None, **b) if False else s.post(f"/subjects/{q(s.s(f'r{i}'))}/versions", b)
s.post(f"/subjects/{q(s.s('dup'))}/versions", raw='{"schema":"\\"int\\"","schema":"\\"long\\""}')
s.ver(s.s("dup"), 1)
for i, b in enumerate([{"normalize": "true"}, {"normalize": 1}, {"normalize": None}, {"compatibility": True},
                       {"compatibility": None, "normalize": True}, {"alias": 5},
                       {"defaultMetadata": {"properties": {"a": 1}}}, {"validateFields": "false"}]):
    s.cfg(s.s(f"c{i}"), b)
    s.cfg(s.s(f"c{i}"))
s.put(f"/config/{q(s.s('c-arr'))}", [])
s.put(f"/config/{q(s.s('c-str'))}", "x")
for i, b in enumerate([{"mode": 5}, {"mode": True}, {"mode": "import"}, {"mode": "ReadOnly"}]):
    s.mode(s.s(f"m{i}"), b)
s.put(f"/mode/{q(s.s('m-arr'))}", [])
s.post("/", [])
s.post("/", {"a": 1})
s.post("/", {"a": {}})
s.post("/", raw="")

s = Sc("query-flags", "boolean and integer query parameter spellings")
S = s.s("a")
s.reg(S, AVRO_USER)
s.del_ver(S, 1)
for v in ["true", "TRUE", "True", "1", "yes", "false", ""]:
    s.vers(S, f"?deleted={v}")
s.reg(S, AVRO_USER_AGE)
for v in ["true", "TRUE", "1"]:
    s.compat(S, AVRO_USER_INT, None, "latest", f"?verbose={v}")
s.get(f"/subjects?subjectPrefix={q(s.name)}&limit=%2B1")
s.get(f"/subjects?subjectPrefix={q(s.name)}&limit=1.0")
s.get(f"/subjects?subjectPrefix={q(s.name)}&limit=%201")
s.get(f"/subjects?subjectPrefix={q(s.name)}&offset=0&offset=5")
s.get(f"/schemas?subjectPrefix={q(s.name)}&limit=2147483648")

s = Sc("mode-scopes", "global/context/subject mode interplay")
C = s.ctx()
D, X = s.s("d"), s.cs("x")
s.reg(D, AVRO_STRING)
s.reg(X, AVRO_STRING)
s.mode(None, {"mode": "READONLY"})
s.reg(D, AVRO_INT)                 # global READONLY blocks the default context
s.reg(X, AVRO_INT)                 # ... but not other contexts
s.mode(X, qs="?defaultToGlobal=true")
s.mode(D, qs="?defaultToGlobal=true")
s.mode(f":{C}:", {"mode": "READONLY"})
s.reg(X, AVRO_LONG)
s.mode(X, {"mode": "READWRITE"})
s.reg(X, AVRO_LONG)
s.mode(None, {"mode": "READONLY_OVERRIDE"})
s.mode(X)                          # override shows everywhere
s.mode(D)
s.reg(X, AVRO_USER)
s.delete(f"/mode/{q(X)}")
s.delete(f"/mode/{q(D)}")
s.mode(None, {"mode": "READWRITE"})
s.mode(X, qs="?defaultToGlobal=true")
s.mode(f":{C}:", qs="?defaultToGlobal=true")
s.restore_global()

s = Sc("config-scopes", "global/context/subject config interplay")
C = s.ctx()
D, X = s.s("d"), s.cs("x")
s.cfg(None, {"compatibility": "NONE", "normalize": True})
s.cfg(D, qs="?defaultToGlobal=true")
s.cfg(X, qs="?defaultToGlobal=true")     # global config does not apply inside contexts
s.cfg(f":{C}:", {"compatibility": "FORWARD"})
s.cfg(X, qs="?defaultToGlobal=true")
s.cfg(X, {"normalize": True})
s.cfg(X)                                   # subject record only, level filled from the default
s.cfg(X, qs="?defaultToGlobal=true")
s.reg(X, AVRO_USER)
s.reg(X, AVRO_USER_INT)
s.delete(f"/config/{q(':' + C + ':')}")
s.cfg(f":{C}:")
s.cfg(":.:")
s.delete("/config")
s.cfg(None)
s.restore_global()

s = Sc("config-readonly", "config and mode writes under read-only modes")
S = s.s("a")
s.reg(S, AVRO_STRING)
s.mode(S, {"mode": "READONLY"})
s.cfg(S, {"compatibility": "NONE"})
s.delete(f"/config/{q(S)}")
s.mode(S, {"mode": "IMPORT"}, "?force=true")
s.cfg(S, {"compatibility": "NONE"})
s.mode(S, {"mode": "READONLY"})
s.del_subj(S)
s.del_ver(S, 1)
s.delete(f"/mode/{q(S)}")
s.cfg(S, {"compatibility": "NONE"})

s = Sc("import-clears", "entering IMPORT without force hard-deletes soft-deleted versions")
S = s.s("a")
s.reg(S, AVRO_STRING)
s.reg(s.s("b"), AVRO_INT)
s.del_subj(S)
s.vers(S, "?deleted=true")
s.mode(S, {"mode": "IMPORT"})
s.vers(S, "?deleted=true")
s.by_id("{$id1}")
s.mode(S, {"mode": "READWRITE"})
s.reg(S, AVRO_STRING)
s.vers(S)
s.mode(s.s("b"), {"mode": "IMPORT"})
s.mode(s.s("b"), {"mode": "IMPORT"})

s = Sc("cross-context-reads", "unqualified reads fall back to other contexts")
X = s.cs("a", "x")
s.reg(X, AVRO_USER)
s.vers(s.s("a"))
s.ver(s.s("a"), 1)
s.ver(s.s("a"), "latest")
s.look(s.s("a"), AVRO_USER)
s.look(s.s("a"), AVRO_USER_INT)
s.get(f"/subjects/{q(s.s('a'))}/versions/1/referencedby", ids=True)
s.by_id("{$id1}")
s.by_id("{$id1}", "", f"?subject={q(s.s('a'))}")
s.by_id("{$id1}", "/versions", f"?subject={q(s.s('a'))}")
s.by_id("{$id1}", "", f"?subject={q(':' + s.ctx('x') + ':')}")
s.by_id("{$id1}", "", f"?subject={q(':' + s.ctx('nope') + ':')}")

s = Sc("cross-context-shadow", "a default-context subject shadows other contexts once it exists")
X = s.cs("a", "x")
s.reg(X, AVRO_USER)
s.reg(s.s("a"), AVRO_STRING)
s.ver(s.s("a"), 1)
s.look(s.s("a"), AVRO_USER)
s.del_subj(s.s("a"))
s.ver(s.s("a"), 1)
s.ver(s.s("a"), 1, "?deleted=true")
s.vers(s.s("a"))

s = Sc("context-urls", "/contexts/{ctx}/... URL forms")
C = s.ctx()
base = f"/contexts/{C}"
s.post(f"{base}/subjects/{q(s.name + '.a')}/versions", {"schema": AVRO_USER})
s.get(f"{base}/subjects/{q(s.name + '.a')}/versions/1")
s.get(f"{base}/subjects", keep=[f":{C}:"])
s.get(f"{base}/schemas", keep=[f":{C}:"])
s.get(f"{base}/schemas/ids/{{$id1}}")
s.get(f"{base}/schemas/ids/{{$id1}}/subjects")
s.put(f"{base}/config", {"compatibility": "NONE"})
s.get(f"{base}/config")
s.put(f"{base}/config/{q(s.name + '.a')}", {"compatibility": "FULL"})
s.get(f"/config/{q(s.cs('a'))}")
s.put(f"{base}/mode", {"mode": "READONLY"})
s.get(f"/mode/{q(':' + C + ':')}")
s.post(f"{base}/compatibility/subjects/{q(s.name + '.a')}/versions/latest", {"schema": AVRO_USER_INT})
s.put(f"{base}/mode", {"mode": "READWRITE"})
s.get("/contexts/.bad:name/subjects")
s.get(f"/contexts/{C.lstrip('.')}/subjects", keep=[f":{C}:"])
s.get(f"{base}/subjects/{q(':.other:' + s.name + '.a')}/versions")

s = Sc("alias-filter", "aliases rewrite every subject path and the schemas/ids subject")
T, A = s.s("target"), s.s("alias")
s.reg(T, AVRO_USER)
s.cfg(A, {"alias": T})
s.reg(A, AVRO_USER_AGE)
s.vers(T)
s.vers(A)
s.look(A, AVRO_USER)
s.compat(A, AVRO_USER_INT, None, "latest", "?verbose=true")
s.by_id("{$id1}", "", f"?subject={q(A)}")
s.get(f"/subjects/{q(A)}/versions/1/referencedby", ids=True)
s.cfg(A)
s.mode(A, {"mode": "READONLY"})
s.mode(A)
s.mode(T)
s.delete(f"/mode/{q(A)}")
s.del_ver(A, 2)
s.vers(T)
s.cfg(s.cs("ca"), {"alias": s.name + ".ctarget"})
s.reg(s.cs("ctarget"), AVRO_STRING)
s.vers(s.cs("ca"))
s.cfg(s.s("q"), {"alias": s.cs("ctarget")})
s.vers(s.s("q"))
s.cfg(s.s("chain"), {"alias": A})
s.vers(s.s("chain"))
s.del_subj(A)
s.vers(T, "?deleted=true")

s = Sc("refs-deletes", "reference protection across soft/hard deletes")
D, H = s.s("dep"), s.s("holder")
s.reg(D, AVRO_DEP)
ref = [{"name": "com.x.Dep", "subject": D, "version": 1}]
s.reg(H, AVRO_USES_DEP, refs=ref)
s.del_subj(D)
s.del_subj(H)
s.get(f"/subjects/{q(D)}/versions/1/referencedby", ids=True)
s.del_subj(D)
s.del_subj(D, "?permanent=true")
# (Re-registering the holder now depends on Confluent's parsed-schema cache: not tested.)
s.reg(D, AVRO_DEP)
s.reg(H, AVRO_USES_DEP, refs=[{"name": "com.x.Dep", "subject": D, "version": -1}])
s.ver(H, "latest")
s.get(f"/subjects/{q(D)}/versions/latest/referencedby", ids=True)
s.del_ver(D, "latest")
s.del_ver(H, "latest")
s.del_ver(D, "latest")
s.reg(s.s("h2"), AVRO_USES_DEP, refs=[{"name": "com.x.Dep", "subject": D, "version": 1}])
s.reg(s.s("h3"), AVRO_USES_DEP, refs=[{"name": "com.x.Dep", "subject": D, "version": -1}])

s = Sc("compat-group", "compatibilityGroup splits the history by a metadata property")
S = s.s("a")
s.cfg(S, {"compatibility": "BACKWARD", "compatibilityGroup": "major"})
s.reg(S, AVRO_USER, metadata={"properties": {"major": "1"}})
s.reg(S, AVRO_USER_INT, metadata={"properties": {"major": "2"}})
s.reg(S, AVRO_USER_INT, metadata={"properties": {"major": "1"}})
s.compat(S, AVRO_USER, None, None, "?verbose=true")
s.post(f"/compatibility/subjects/{q(S)}/versions?verbose=true",
       {"schema": AVRO_USER, "metadata": {"properties": {"major": "2"}}})
s.vers(S)

s = Sc("confluent-version", "confluent:version metadata bookkeeping")
S = s.s("a")
s.reg(S, AVRO_STRING, version=-1)
s.reg(S, AVRO_STRING, version=-1)
s.reg(S, AVRO_STRING)
s.ver(S, 1)
s.look(S, AVRO_STRING)
s.look(S, AVRO_STRING, metadata={"properties": {"confluent:version": "1"}})
s.look(S, AVRO_STRING, metadata={"properties": {"confluent:version": "2"}})
s.none_compat(S)
s.reg(S, AVRO_INT)
s.ver(S, 2)
s.reg(S, AVRO_INT, metadata={"properties": {"confluent:version": "2"}})
s.reg(S, AVRO_INT, metadata={"properties": {"confluent:version": "7"}})
s.vers(S)

s = Sc("metadata-lookup", "GET /subjects/{s}/metadata")
S = s.s("a")
s.none_compat(S)
s.reg(S, AVRO_STRING, metadata={"properties": {"a": "1", "b": "2"}})
s.reg(S, AVRO_INT, metadata={"properties": {"a": "1", "b": "3"}})
s.reg(S, AVRO_LONG)
for qs in ["?key=a&value=1", "?key=a&value=1&key=b&value=2", "?key=b&value=3", "?key=a", "?value=1",
           "?key=a&value=1&key=b", "?key=zz&value=1"]:
    s.get(f"/subjects/{q(S)}/metadata{qs}")
s.del_ver(S, 2)
s.get(f"/subjects/{q(S)}/metadata?key=b&value=3")
s.get(f"/subjects/{q(S)}/metadata?key=b&value=3&deleted=true")
s.get(f"/subjects/{q(S)}/metadata?key=a&value=1&format=serialized")

s = Sc("subject-names-reserved", "reserved and odd subject names")
for i, n in enumerate(["__GLOBAL", "__EMPTY", f":.{s.name}-c:__GLOBAL", f":.{s.name}-c:", f":.{s.name}-d",
                       "::", f":abc:{s.name}", ":.:" + s.name + ".x"]):
    s.reg(n, AVRO_STRING)
    s.cfg(n, {"compatibility": "NONE"})
    s.mode(n, {"mode": "READWRITE"})
    s.vers(n)
s.get("/subjects", keep=[f":abc:{s.name}", "__", s.name, f":.{s.name}-"])

s = Sc("avro-invalid-2", "more invalid Avro shapes")
for i, bad in enumerate([
    '{"type":"record","name":"R","fields":[{"name":"1a","type":"int"}]}',
    '{"type":"record","name":"R","fields":[{"type":"int"}]}',
    '{"type":"record","name":"R","fields":[{"name":"a"}]}',
    '{"type":"record","name":"R","fields":[{"name":"a","type":"int","order":"sideways"}]}',
    '{"type":"record","name":"int","fields":[]}',
    '{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"record","name":"R","fields":[]}}]}',
    '{"type":"enum","name":"E","symbols":["1A"]}',
    '{"type":"enum","name":"E","symbols":["A"],"default":"B"}',
    '{"type":"enum","name":"E"}',
    '{"type":"fixed","name":"F","size":"4"}',
    '{"type":"fixed","name":"F","size":-1}',
    '["int","int"]',
    '["int",{"type":"int"}]',
    '{"type":"record","name":"R","aliases":"x","fields":[]}',
    '{"type":"record","name":"R","aliases":[1],"fields":[]}',
    '{"type":"map"}',
    '{"type":{"type":"int"}}',
    '{"type":"record","fields":[]}',
    '{"type":"record","name":"a.b.1c","fields":[]}',
    '{"type":"record","name":"R","namespace":"1ns","fields":[]}',
    '"string" "x"',
    '{"type":"record","name":"R","fields":[{"name":"a","type":"int","default":null}]}',
]):
    s.reg(s.s(f"b{i}"), bad)
    s.reg(s.s(f"n{i}"), bad, qs="?normalize=true")

s = Sc("avro-defaults", "default values with and without normalize")
for i, (ty, d) in enumerate([("int", "1"), ("int", '"1"'), ("int", "1.5"), ("long", "9999999999"), ("float", '"NaN"'),
                             ("double", "1"), ("boolean", "0"), ("null", "null"), ("string", "1"), ("bytes", '"\\u00ff"'),
                             ('["null","int"]', "null"), ('["null","int"]', "1"), ('["int","null"]', "null"),
                             ('{"type":"array","items":"int"}', "[1,2]"), ('{"type":"array","items":"int"}', '["x"]'),
                             ('{"type":"map","values":"int"}', '{"a":1}'), ('{"type":"enum","name":"E","symbols":["A"]}', '"A"'),
                             ('{"type":"enum","name":"E","symbols":["A"]}', '"Z"'),
                             ('{"type":"fixed","name":"F","size":2}', '"ab"'),
                             ('{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}', '{"x":1}'),
                             ('{"type":"record","name":"In","fields":[{"name":"x","type":"int"}]}', '{}')]):
    t = ty if ty.startswith("{") or ty.startswith("[") else f'"{ty}"'
    sch = '{"type":"record","name":"R","fields":[{"name":"f","type":%s,"default":%s}]}' % (t, d)
    s.reg(s.s(f"d{i}"), sch)
    s.reg(s.s(f"n{i}"), sch, qs="?normalize=true")

s = Sc("json-shapes", "JSON Schema shapes and drafts")
for i, sch in enumerate([
    '{"type":"object","properties":{"a":{"type":["string","null"]}}}',
    '{"type":"object","patternProperties":{"^x":{"type":"string"}}}',
    '{"type":"object","required":["a"],"properties":{"a":{"type":"string"}}}',
    '{"type":"array","items":[{"type":"string"}]}',
    '{"oneOf":[{"type":"string"},{"type":"integer"}]}',
    '{"allOf":[{"type":"string"}]}',
    '{"not":{"type":"string"}}',
    '{"enum":["a","b"]}',
    '{"const":1}',
    '{"type":"string","format":"date-time"}',
    '{"type":"object","dependencies":{"a":["b"]}}',
    '{"type":"object","additionalProperties":{"type":"integer"}}',
    '{"$id":"http://x/y","type":"object"}',
    '{"$schema":"http://json-schema.org/draft-06/schema#","type":"object"}',
    '{"$schema":"https://json-schema.org/draft/2019-09/schema","type":"object"}',
    '{"type":"object","properties":{"a":{"$ref":"#"}}}',
    '{"type":"integer","minimum":1,"maximum":0}',
    '{"type":"string","pattern":"("}',
    '{"type":"object","properties":[]}',
    '{"items":{"type":"nope"}}',
]):
    s.reg(s.s(f"j{i}"), sch, "JSON")
s.subjects()

s = Sc("proto-descriptors", "format=serialized for richer Protobuf schemas")
for i, sch in enumerate([
    'syntax = "proto3";\npackage a.b;\nmessage Outer {\n  message Inner {\n    string x = 1;\n  }\n  Inner i = 1;\n  repeated Inner is = 2;\n  enum Kind {\n    K0 = 0;\n  }\n  Kind k = 3;\n}\n',
    'syntax = "proto3";\nmessage M {\n  optional int32 a = 1;\n  repeated string b = 2 [packed = false];\n  map<int32, M> c = 3;\n}\n',
    'syntax = "proto2";\nmessage M {\n  required int32 a = 1;\n  optional string b = 2 [default = "hi"];\n  extensions 100 to 199;\n}\nextend M {\n  optional int32 ext = 100;\n}\n',
    'syntax = "proto3";\nenum E {\n  option allow_alias = true;\n  A = 0;\n  B = 0;\n  reserved 5, 7 to 9;\n  reserved "X";\n}\n',
    'syntax = "proto3";\nimport "google/protobuf/any.proto";\nimport "google/protobuf/wrappers.proto";\nmessage M {\n  google.protobuf.Any a = 1;\n  google.protobuf.StringValue s = 2;\n}\n',
    'syntax = "proto3";\noption go_package = "x/y";\noption java_multiple_files = true;\noption optimize_for = SPEED;\nmessage M {\n  string a = 1 [deprecated = true];\n}\nservice S {\n  rpc A (stream M) returns (stream M);\n  rpc B (M) returns (M) {\n    option deprecated = true;\n  }\n}\n',
    'syntax = "proto3";\nmessage M {\n  .M self = 1;\n  oneof o {\n    string a = 2;\n    M b = 3;\n  }\n}\n',
]):
    S = s.s(f"p{i}")
    s.reg(S, sch, "PROTOBUF")
    s.ver(S, 1)
    s.ver(S, 1, "/schema?format=serialized")
    s.ver(S, 1, "/schema?format=ignore_extensions")
D = s.s("dep")
s.reg(D, PROTO_DEP, "PROTOBUF")
H = s.s("holder")
s.reg(H, PROTO_USES_DEP, "PROTOBUF", refs=[{"name": "dep.proto", "subject": D, "version": 1}])
s.ver(H, 1, "/schema?format=serialized")
s.by_id("{$id9}", "", "?format=serialized")

s = Sc("delete-matrix", "delete forms against every version state")
S = s.s("a")
s.none_compat(S)
for sch in [AVRO_STRING, AVRO_INT, AVRO_LONG, AVRO_USER]:
    s.reg(S, sch)
s.del_ver(S, 2)
s.del_ver(S, 3)
s.del_ver(S, 3, "?permanent=true")
for v, qs in [(1, "?permanent=true"), (2, "?permanent=true"), (3, ""), (3, "?permanent=true"), ("latest", "?permanent=true"),
              ("latest", ""), ("latest", ""), (4, "?permanent=true"), (1, ""), (1, "?permanent=true"), (9, ""), (9, "?permanent=true")]:
    s.del_ver(S, v, qs)
s.vers(S, "?deleted=true")
s.subjects("?deleted=true")
s.del_subj(S)
s.del_subj(S, "?permanent=true")
s.subjects("?deleted=true")

# ---------------------------------------------------------------------------
# Schema tags
# ---------------------------------------------------------------------------

def tag(path, tags, record=False):
    return {"schemaEntity": {"entityPath": path, "entityType": "sr_record" if record else "sr_field"}, "tags": tags}


s = Sc("tags-avro", "tags are written into the schema and registered as a new version")
S = s.s("a")
s.reg(S, AVRO_USER)
s.post(f"/subjects/{q(S)}/versions/1/tags", {"newVersion": 2, "tagsToAdd": [tag("User.name", ["PII"])]})
s.ver(S, 2)
s.post(f"/subjects/{q(S)}/versions/2/tags",
       {"newVersion": 3, "tagsToAdd": [tag("User", ["INTERNAL"], record=True)], "tagsToRemove": [tag("User.name", ["PII"])]})
s.ver(S, 3)
s.post(f"/subjects/{q(S)}/versions/3/tags", {"tagsToAdd": [tag("User.name", ["A", "B"])]})   # no newVersion
s.ver(S, "latest")
s.post(f"/subjects/{q(S)}/versions/latest/tags", {"tagsToAdd": [tag("User.name", ["A"])]})   # already there
s.vers(S)
s.look(S, AVRO_USER)
s.post(f"/subjects/{q(S)}/versions/1/tags", {"newVersion": 9, "tagsToAdd": [tag("User.name", ["X"])]})
s.post(f"/subjects/{q(S)}/versions/1/tags", {"tagsToAdd": [tag("User.nope", ["X"])]})
s.post(f"/subjects/{q(S)}/versions/1/tags", {"tagsToAdd": [tag("Nope.name", ["X"])]})
s.post(f"/subjects/{q(S)}/versions/1/tags", {"tagsToAdd": [tag("User", ["X"])]})
s.post(f"/subjects/{q(S)}/versions/1/tags", {"tagsToRemove": [tag("User.name", ["not-there"])]})
s.post(f"/subjects/{q(S)}/versions/9/tags", {"tagsToAdd": [tag("User.name", ["X"])]})
s.post(f"/subjects/{q(S)}/versions/abc/tags", {"tagsToAdd": [tag("User.name", ["X"])]})
s.post(f"/subjects/{q(s.s('missing'))}/versions/1/tags", {"tagsToAdd": [tag("User.name", ["X"])]})
s.post(f"/subjects/{q(S)}/versions/1/tags", raw="")
s.post(f"/subjects/{q(S)}/versions/1/tags", {})
s.post(f"/subjects/{q(S)}/versions/1/tags", {"tagsToAdd": []})
s.post(f"/subjects/{q(S)}/versions/1/tags", {"newVersion": 2, "tagsToAdd": [tag(".User.name", ["D"])]})
s.vers(S)

s = Sc("tags-namespaced", "namespaced Avro records and metadata")
D = s.s("d")
s.reg(D, AVRO_DEP)
s.post(f"/subjects/{q(D)}/versions/1/tags", {"tagsToAdd": [tag("com.x.Dep.v", ["N"])]})
s.ver(D, 2)
s.post(f"/subjects/{q(D)}/versions/2/tags", {"tagsToAdd": [tag("com.x.Dep", ["R"], record=True)],
                                             "metadata": {"properties": {"owner": "me"}}})
s.ver(D, 3)
s.post(f"/subjects/{q(D)}/versions/3/tags", {"tagsToAdd": [tag("Dep.v", ["X"])]})

s = Sc("tags-json-proto", "tags in JSON Schema and Protobuf")
J = s.s("j")
s.reg(J, JSON_A, "JSON")
s.post(f"/subjects/{q(J)}/versions/1/tags", {"tagsToAdd": [tag("object.a", ["PII"])]})
s.ver(J, 2)
s.post(f"/subjects/{q(J)}/versions/2/tags", {"tagsToAdd": [tag("object", ["R"], record=True)]})
s.ver(J, 3)
s.post(f"/subjects/{q(J)}/versions/1/tags", {"tagsToAdd": [tag("object.nope", ["X"])]})
s.post(f"/subjects/{q(J)}/versions/1/tags", {"tagsToAdd": [tag("a", ["X"])]})
P = s.s("p")
s.none_compat(P)
s.reg(P, PROTO_M_PKG, "PROTOBUF")
s.post(f"/subjects/{q(P)}/versions/1/tags", {"tagsToAdd": [tag("M.a", ["PII"])]})
s.ver(P, 2)
s.post(f"/subjects/{q(P)}/versions/2/tags", {"tagsToAdd": [tag("M", ["R"], record=True)]})
s.ver(P, 3)
s.post(f"/subjects/{q(P)}/versions/3/tags", {"tagsToRemove": [tag("M.a", ["PII"])]})
s.ver(P, 4)
s.post(f"/subjects/{q(P)}/versions/1/tags", {"tagsToAdd": [tag("M.nope", ["X"])]})
s.post(f"/subjects/{q(P)}/versions/1/tags", {"tagsToAdd": [tag("Nope.a", ["X"])]})
N = s.s("n")
s.none_compat(N)
s.reg(N, 'syntax = "proto3";\npackage p;\nmessage M {\n  message Inner {\n    string x = 1;\n  }\n  Inner i = 1;\n}\n', "PROTOBUF")
s.post(f"/subjects/{q(N)}/versions/1/tags", {"tagsToAdd": [tag("M.Inner.x", ["T"])]})
s.ver(N, 2)

s = Sc("tags-nopackage", "Confluent writes `package null;` when tagging a Protobuf schema without a package")
P = s.s("p")
s.none_compat(P)
s.reg(P, PROTO_M, "PROTOBUF")
s.post(f"/subjects/{q(P)}/versions/1/tags", {"tagsToAdd": [tag("M.a", ["PII"])]})
s.ver(P, 2)


def write():
    with open(os.path.join(HERE, "scenarios.json"), "w") as f:
        json.dump(SCENARIOS, f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(f"{len(SCENARIOS)} scenarios, {sum(len(s['steps']) for s in SCENARIOS)} steps")


if __name__ == "__main__":
    write()
