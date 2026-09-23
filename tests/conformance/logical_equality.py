#!/usr/bin/env python3
"""Which spellings of the "same" schema share an id? Run against two registries
and compare: python3 tests/conformance/logical_equality.py URL [URL...]

Each group registers variants under separate subjects; a variant's id is
reported relative to the group's first registration ("=" same id, "new"
a different id). normalize=true variants are marked with (n).
"""
import base64, json, sys, urllib.request

def post(url, path, body):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

rec = lambda d: json.dumps(d, separators=(",", ":"))
AV = {"type": "record", "name": "User", "namespace": "acme", "fields": [
    {"name": "id", "type": "long"}, {"name": "tag", "type": "string", "x-a": 1, "x-b": 2}]}
AVRO = [
    ("compact", rec(AV), False),
    ("pretty", json.dumps(AV, indent=3), False),
    ("key order", '{"fields":[{"type":"long","name":"id"},{"x-a":1,"x-b":2,"type":"string","name":"tag"}],"namespace":"acme","name":"User","type":"record"}', False),
    ("fullname", rec({"type": "record", "name": "acme.User", "fields": AV["fields"]}), False),
    ("prim as object", rec({"type": "record", "name": "User", "namespace": "acme", "fields": [
        {"name": "id", "type": {"type": "long"}}, {"name": "tag", "type": "string", "x-a": 1, "x-b": 2}]}), False),
    ("prop order", rec({"type": "record", "name": "User", "namespace": "acme", "fields": [
        {"name": "id", "type": "long"}, {"name": "tag", "type": "string", "x-b": 2, "x-a": 1}]}), False),
    ("prop order (n)", rec({"type": "record", "name": "User", "namespace": "acme", "fields": [
        {"name": "id", "type": "long"}, {"name": "tag", "type": "string", "x-b": 2, "x-a": 1}]}), True),
    ("doc added", rec({**AV, "doc": "x"}), False),
]
JS = {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}, "required": ["a"]}
JSON = [
    ("compact", rec(JS), False),
    ("pretty", json.dumps(JS, indent=2), False),
    ("key order", '{"required":["a"],"properties":{"b":{"type":"integer"},"a":{"type":"string"}},"type":"object"}', False),
    ("key order (n)", '{"required":["a"],"properties":{"b":{"type":"integer"},"a":{"type":"string"}},"type":"object"}', True),
    ("1.0 vs 1", rec({**JS, "maxProperties": 5.0}), False),
]
P = 'syntax = "proto3";\npackage acme;\n\nmessage User {\n  int64 id = 1;\n  Tag tag = 2;\n}\nmessage Tag {\n  string v = 1;\n}\n'
PROTO = [
    ("canonical", P, False),
    ("compact+comments", 'syntax="proto3";package acme; // c\nmessage User{int64 id=1;Tag tag=2;} /* x */ message Tag{string v=1;}', False),
    ("field order", 'syntax = "proto3";\npackage acme;\nmessage User { Tag tag = 2; int64 id = 1; }\nmessage Tag { string v = 1; }\n', False),
    ("field order (n)", 'syntax = "proto3";\npackage acme;\nmessage User { Tag tag = 2; int64 id = 1; }\nmessage Tag { string v = 1; }\n', True),
    ("qualified type", 'syntax = "proto3";\npackage acme;\nmessage User { int64 id = 1; .acme.Tag tag = 2; }\nmessage Tag { string v = 1; }\n', False),
    ("qualified type (n)", 'syntax = "proto3";\npackage acme;\nmessage User { int64 id = 1; .acme.Tag tag = 2; }\nmessage Tag { string v = 1; }\n', True),
    ("canonical (n)", P, True),
    ("message order", 'syntax = "proto3";\npackage acme;\nmessage Tag { string v = 1; }\nmessage User { int64 id = 1; Tag tag = 2; }\n', False),
]

def run(url):
    out = []
    for label, group, stype in (("AVRO", AVRO, None), ("JSON", JSON, "JSON"), ("PROTOBUF", PROTO, "PROTOBUF")):
        first = None
        for i, (name, schema, norm) in enumerate(group):
            body = {"schema": schema}
            if stype:
                body["schemaType"] = stype
            r = post(url, f"/subjects/eq-{label}-{i}/versions" + ("?normalize=true" if norm else ""), body)
            rid = r.get("id")
            if first is None:
                first = rid
            rel = "=" if rid == first else ("new" if rid else f"ERR {r}")
            # also: re-register the same variant into the first subject -> same version?
            out.append(f"{label:9} {name:20} {rel}")
    return out

results = [run(u) for u in sys.argv[1:]]
for row in zip(*results):
    mark = "" if len(set(x.split()[-1] for x in row)) == 1 else "   <-- differs"
    print(" | ".join(row) + mark)
