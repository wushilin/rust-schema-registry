#!/usr/bin/env python3
"""Register response shape, metadata inheritance, metadata-only versions,
explicit versions and soft-deleted cleanup. Run against fresh registries:
python3 tests/conformance/register_semantics.py URL [URL...]"""
import json, sys, urllib.request

def call(url, method, path, body=None):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode() if body is not None else None, method=method,
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())

def run(url):
    M = {"schema": '"string"', "metadata": {"properties": {"k": "v"}}}
    N = {"schema": '"string"'}
    steps = [
        ("new subject, with metadata", "POST", "/subjects/sq8-1/versions", M),
        ("repeat with metadata", "POST", "/subjects/sq8-1/versions", M),
        ("repeat without metadata (inherits)", "POST", "/subjects/sq8-1/versions", N),
        ("same schema, different metadata", "POST", "/subjects/sq8-1/versions", {"schema": '"string"', "metadata": {"properties": {"k": "v2"}}}),
        ("no schema, metadata only", "POST", "/subjects/sq8-1/versions", {"metadata": {"properties": {"k": "v3"}}}),
        ("versions", "GET", "/subjects/sq8-1/versions", None),
        ("latest", "GET", "/subjects/sq8-1/versions/latest", None),
        ("no schema, new subject", "POST", "/subjects/sq8-2/versions", {"metadata": {"properties": {"k": "v"}}}),
        ("explicit version=1 on new subject", "POST", "/subjects/sq8-3/versions", {"schema": '"int"', "version": 1}),
        ("explicit version=5 (next is 2)", "POST", "/subjects/sq8-3/versions", {"schema": '"long"', "version": 5}),
        ("explicit version=2", "POST", "/subjects/sq8-3/versions", {"schema": '"long"', "version": 2}),
        ("next without version keeps confluent:version", "POST", "/subjects/sq8-3/versions", {"schema": '"float"'}),
        ("v3 metadata", "GET", "/subjects/sq8-3/versions/3", None),
        ("compatibility NONE for subject 4", "PUT", "/config/sq8-4", {"compatibility": "NONE"}),
        ("v1", "POST", "/subjects/sq8-4/versions", {"schema": '"double"'}),
        ("v2 (different)", "POST", "/subjects/sq8-4/versions", {"schema": '"boolean"'}),
        ("soft-delete v1 (v2 stays live, config stays)", "DELETE", "/subjects/sq8-4/versions/1", None),
        ("re-register v1's schema -> v3 with v1's id", "POST", "/subjects/sq8-4/versions", {"schema": '"double"'}),
        ("versions incl. deleted (soft-deleted v1 removed)", "GET", "/subjects/sq8-4/versions?deleted=true", None),
        ("v1 gone for good", "GET", "/subjects/sq8-4/versions/1?deleted=true", None),
        ("subject 5 at NONE", "PUT", "/config/sq8-5", {"compatibility": "NONE"}),
        ("subject 5 v1", "POST", "/subjects/sq8-5/versions", {"schema": '"int"'}),
        ("soft-delete subject 5", "DELETE", "/subjects/sq8-5", None),
        ("subject 5 config is gone", "GET", "/config/sq8-5", None),
    ]
    return [(label, call(url, m, p, b)) for label, m, p, b in steps]

results = [run(u) for u in sys.argv[1:]]
for items in zip(*results):
    same = len({json.dumps(v, sort_keys=True) for _, v in items}) == 1
    print(("  same  " if same else "  DIFF  ") + items[0][0] + ("" if not same else f"  -> {json.dumps(items[0][1])[:120]}"))
    if not same:
        for i, (_, v) in enumerate(items):
            print(f"        [{i}] {json.dumps(v)[:300]}")
