#!/usr/bin/env python3
"""GET /schemas aliases/ruleType/limits, POST /, raw format. Run against fresh registries:
python3 tests/conformance/listing.py URL [URL...]"""
import json, sys, urllib.request

def call(url, method, path, body=None):
    req = urllib.request.Request(url + path, data=json.dumps(body).encode() if body is not None else None, method=method,
                                 headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    try:
        with urllib.request.urlopen(req) as r:
            raw = r.read().decode()
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
    try:
        return json.loads(raw)
    except ValueError:
        return raw

def run(url):
    out = []
    rs = lambda t: {"domainRules": [{"name": "r1", "kind": "CONDITION", "mode": "WRITE", "type": t, "expr": "true"}]}
    call(url, "POST", "/subjects/lst-a/versions", {"schema": '"string"', "ruleSet": rs("CEL")})
    call(url, "POST", "/subjects/lst-b/versions", {"schema": '"long"', "ruleSet": {"migrationRules": [{"name": "m", "kind": "TRANSFORM", "mode": "UPGRADE", "type": "JSONATA", "expr": "$"}]}})
    call(url, "POST", "/subjects/lst-c/versions", {"schema": '"int"', "ruleSet": {"encodingRules": [{"name": "e", "kind": "TRANSFORM", "mode": "WRITEREAD", "type": "ENCRYPT_PAYLOAD"}]}})
    call(url, "POST", "/subjects/other-target/versions", {"schema": '"bytes"'})
    call(url, "PUT", "/config/lst-alias", {"alias": "other-target"})
    call(url, "PUT", "/config/lst-alias2", {"alias": "other-target"})
    subj = lambda l: [(x["subject"], x.get("aliases")) for x in l] if isinstance(l, list) else l
    out.append(("ruleType=CEL", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&ruleType=CEL"))))
    out.append(("ruleType=JSONATA (migration)", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&ruleType=JSONATA"))))
    out.append(("ruleType=ENCRYPT_PAYLOAD (encoding)", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&ruleType=ENCRYPT_PAYLOAD"))))
    out.append(("aliases=true", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&aliases=true"))))
    out.append(("aliases=false", subj(call(url, "GET", "/schemas?subjectPrefix=lst-"))))
    out.append(("limit=2", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&limit=2"))))
    out.append(("limit=0", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&limit=0"))))
    out.append(("offset=1&limit=1", subj(call(url, "GET", "/schemas?subjectPrefix=lst-&offset=1&limit=1"))))
    out.append(("subjects limit=2", call(url, "GET", "/subjects?subjectPrefix=lst-&limit=2")))
    out.append(("POST /", call(url, "POST", "/", {})))
    pid = call(url, "POST", "/subjects/lst-proto/versions", {"schemaType": "PROTOBUF", "schema": 'syntax = "proto3";\nmessage M { string a = 1; }\n'})["id"]
    out.append(("raw format=serialized is base64", str(call(url, "GET", f"/schemas/ids/{pid}/schema?format=serialized"))[:12]))
    out.append(("raw version format=serialized", str(call(url, "GET", "/subjects/lst-proto/versions/1/schema?format=serialized"))[:12]))
    return out

rows = [run(u) for u in sys.argv[1:]]
for items in zip(*rows):
    same = len({json.dumps(v, sort_keys=True) for _, v in items}) == 1
    print(("  same  " if same else "  DIFF  ") + items[0][0])
    if not same:
        for i, (_, v) in enumerate(items):
            print(f"        [{i}] {json.dumps(v)[:300]}")
