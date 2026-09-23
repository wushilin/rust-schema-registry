#!/usr/bin/env python3
"""With `normalize: true` configured, which spellings share an id?
python3 tests/conformance/logical_equality_normalized.py URL [URL...]"""
import json, sys, urllib.request

def call(url, method, path, body):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode(), method=method,
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

GROUPS = [
    ("JSON", [
        '{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"}},"required":["a"]}',
        '{"required":["a"],"properties":{"b":{"type":"integer"},"a":{"type":"string"}},"type":"object"}',
        '{ "type" : "object", "required" : ["a"], "properties" : { "b":{"type":"integer"}, "a":{"type":"string"} } }',
    ]),
    ("PROTOBUF", [
        'syntax = "proto3";\npackage acme;\nmessage User { int64 id = 1; Tag tag = 2; }\nmessage Tag { string v = 1; }\n',
        'syntax = "proto3";\npackage acme;\nmessage User { Tag tag = 2; int64 id = 1; }\nmessage Tag { string v = 1; }\n',
        'syntax = "proto3";\npackage acme;\nmessage User { .acme.Tag tag = 2; int64 id = 1; } // x\nmessage Tag { string v = 1; }\n',
        'syntax = "proto3";\npackage acme;\nmessage Tag { string v = 1; }\nmessage User { int64 id = 1; Tag tag = 2; }\n',
    ]),
    ("AVRO", [
        '{"type":"record","name":"U","namespace":"a","fields":[{"name":"t","type":"string","x-a":1,"x-b":2}]}',
        '{"fields":[{"x-b":2,"x-a":1,"type":"string","name":"t"}],"name":"a.U","type":"record"}',
    ]),
]

def run(url):
    out = []
    for t, variants in GROUPS:
        first = None
        for i, s in enumerate(variants):
            subj = f"eqn-{t}-{i}"
            call(url, "PUT", f"/config/{subj}", {"normalize": True})
            body = {"schema": s} if t == "AVRO" else {"schema": s, "schemaType": t}
            rid = call(url, "POST", f"/subjects/{subj}/versions", body).get("id")
            first = first or rid
            out.append(f"{t:9} variant {i}: {'=' if rid == first else 'new'}")
    return out

for row in zip(*[run(u) for u in sys.argv[1:]]):
    print(" | ".join(row) + ("" if len(set(r.split()[-1] for r in row)) == 1 else "   <-- differs"))
