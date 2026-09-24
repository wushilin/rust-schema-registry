#!/usr/bin/env python3
"""End-to-end API test against a real server binary.

Usage: cargo build && python3 tests/e2e.py [path/to/schema-registry]

Starts two instances (a source with Basic auth and an exporter destination),
exercises the Confluent REST API and checks responses and error codes.
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import requests

def resolve_binary(argv=None):
    """The server under test: `argv[1]` when it is a path, else the debug build.

    Printed, and checked to exist, because the worst failure this harness can
    have is quietly testing a binary nobody just built - a suite that consumed
    argv[1] for its own flags once made fixed bugs look unfixed for an hour.
    """
    argv = sys.argv if argv is None else argv
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "target", "debug", "schema-registry")
    chosen = argv[1] if len(argv) > 1 and not argv[1].startswith("-") else default
    path = os.path.abspath(chosen)
    if not os.path.exists(path):
        sys.exit(f"no server binary at {path} - build it first (cargo build), or pass one as the first argument")
    age = time.time() - os.path.getmtime(path)
    print(f"using {path} (built {age / 60:.0f} min ago)")
    return path


BIN = resolve_binary()
CT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
FAILURES = []


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def start(tmp, name, auth, extra=""):
    port = free_port()
    data = os.path.join(tmp, name)
    cfg = os.path.join(tmp, f"{name}.toml")
    with open(cfg, "w") as f:
        f.write(f'listen = "127.0.0.1:{port}"\ndata_dir = "{data}"\nexporter_poll_seconds = 1\ncluster_id = "{name}"\n{extra}')
        if auth:
            f.write(
                '[auth]\nenabled = true\n'
                '[[auth.users]]\nusername = "admin"\npassword = "admin-secret"\nroles = ["admin"]\n'
                '[[auth.users]]\nusername = "reader"\npassword = "reader-secret"\nroles = ["readonly"]\n'
                '[[auth.users]]\nusername = "dev"\npassword = "dev-secret"\nroles = ["write"]\n'
            )
    log = open(os.path.join(tmp, f"{name}.log"), "w")
    proc = subprocess.Popen([BIN, "--config", cfg], stdout=log, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            requests.get(url, timeout=0.2)
            return proc, url, cfg
        except requests.ConnectionError:
            time.sleep(0.05)
    raise RuntimeError(f"{name} did not start; see {log.name}")


def check(label, cond, detail=""):
    if cond:
        print(f"  ok   {label}")
    else:
        print(f"  FAIL {label} {detail}")
        FAILURES.append(label)


class Client:
    def __init__(self, url, auth=None):
        self.url, self.s = url, requests.Session()
        if auth:
            self.s.auth = auth

    def req(self, method, path, body=None, **params):
        r = self.s.request(method, self.url + path, headers=CT, params=params or None,
                           data=json.dumps(body) if body is not None else None)
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, r.text

    def get(self, path, **p):
        return self.req("GET", path, **p)

    def post(self, path, body, **p):
        return self.req("POST", path, body, **p)

    def put(self, path, body, **p):
        return self.req("PUT", path, body, **p)

    def delete(self, path, **p):
        return self.req("DELETE", path, **p)


def err(resp):
    return resp[1].get("error_code") if isinstance(resp[1], dict) else None


AVRO_V1 = {"type": "record", "name": "User", "namespace": "com.acme", "fields": [{"name": "name", "type": "string"}]}
AVRO_V2_BAD = {**AVRO_V1, "fields": AVRO_V1["fields"] + [{"name": "age", "type": "int"}]}
AVRO_V2 = {**AVRO_V1, "fields": AVRO_V1["fields"] + [{"name": "age", "type": "int", "default": 0}]}


def main():
    tmp = tempfile.mkdtemp(prefix="sr-e2e-")
    procs = []
    try:
        src_proc, src_url, src_cfg = start(tmp, "src", auth=True)
        procs.append(src_proc)
        dst_proc, dst_url, _ = start(tmp, "dst", auth=False)
        procs.append(dst_proc)
        c = Client(src_url, ("admin", "admin-secret"))
        ro = Client(src_url, ("reader", "reader-secret"))
        dev = Client(src_url, ("dev", "dev-secret"))
        anon = Client(src_url)

        print("auth")
        r = anon.get("/subjects")
        check("no credentials -> 401", r[0] == 401 and err(r) == 401, r)
        r = Client(src_url, ("admin", "wrong")).get("/subjects")
        check("bad password -> 401", r[0] == 401, r)
        check("reader can list", ro.get("/subjects")[0] == 200)
        r = ro.post("/subjects/u-value/versions", {"schema": json.dumps(AVRO_V1)})
        check("reader cannot register -> 403", r[0] == 403, r)
        r = dev.put("/config", {"compatibility": "NONE"})
        check("writer cannot change global config -> 403", r[0] == 403, r)

        print("register / dedup")
        r = dev.post("/subjects/u-value/versions", {"schema": json.dumps(AVRO_V1, indent=2)})
        check("register v1", r == (200, {"id": 1}), r)
        r = c.post("/subjects/u-value/versions", {"schema": json.dumps(AVRO_V1)})
        check("re-register same schema is idempotent", r == (200, {"id": 1}), r)
        r = c.post("/subjects/other-value/versions", {"schema": json.dumps(AVRO_V1)})
        check("same schema in another subject shares id", r == (200, {"id": 1}), r)
        r = c.get("/schemas/ids/1")
        check("GET /schemas/ids/1", r[0] == 200 and r[1]["schema"] == json.dumps(AVRO_V1, separators=(",", ":")) and "schemaType" not in r[1], r)
        r = c.get("/schemas/ids/1/schema")
        check("GET /schemas/ids/1/schema returns raw schema", r[0] == 200 and r[1] == AVRO_V1, r)
        r = c.get("/schemas/ids/1/versions")
        check("id -> subject versions", r[1] == [{"subject": "other-value", "version": 1}, {"subject": "u-value", "version": 1}], r)
        r = c.get("/schemas/ids/999")
        check("unknown id -> 40403", err(r) == 40403, r)
        r = c.post("/subjects/u-value/versions", {"schema": "{not json"})
        check("invalid schema -> 42201", err(r) == 42201, r)

        print("compatibility")
        r = c.post("/compatibility/subjects/u-value/versions/latest", {"schema": json.dumps(AVRO_V2_BAD)}, verbose="true")
        check("verbose compat check reports incompatibility",
              r[0] == 200 and r[1]["is_compatible"] is False and any("READER_FIELD_MISSING_DEFAULT_VALUE" in m for m in r[1]["messages"]), r)
        r = c.post("/subjects/u-value/versions", {"schema": json.dumps(AVRO_V2_BAD)})
        check("incompatible register -> 409", r[0] == 409 and err(r) == 409, r)
        r = c.post("/compatibility/subjects/u-value/versions/latest", {"schema": json.dumps(AVRO_V2)})
        check("compatible compat check", r == (200, {"is_compatible": True}), r)
        r = c.post("/subjects/u-value/versions", {"schema": json.dumps(AVRO_V2)})
        check("register compatible v2", r == (200, {"id": 2}), r)
        r = c.get("/subjects/u-value/versions")
        check("versions [1,2]", r == (200, [1, 2]), r)
        r = c.get("/subjects/u-value/versions/latest")
        check("latest is v2", r[1]["version"] == 2 and r[1]["id"] == 2 and r[1]["subject"] == "u-value", r)
        r = c.post("/subjects/u-value", {"schema": json.dumps(AVRO_V1)})
        check("lookup finds v1", r[0] == 200 and r[1]["version"] == 1 and r[1]["id"] == 1, r)
        r = c.post("/subjects/u-value", {"schema": json.dumps({"type": "string"})})
        check("lookup unknown schema -> 40403", err(r) == 40403, r)
        r = c.post("/subjects/nope", {"schema": json.dumps(AVRO_V1)})
        check("lookup unknown subject -> 40401", err(r) == 40401, r)
        r = c.get("/subjects/u-value/versions/7")
        check("unknown version -> 40402", err(r) == 40402, r)
        r = c.get("/subjects/u-value/versions/abc")
        check("invalid version -> 42202", err(r) == 42202, r)

        print("config")
        r = c.get("/config")
        check("global defaults: BACKWARD, normalize on", r == (200, {"compatibilityLevel": "BACKWARD", "normalize": True}), r)
        r = c.get("/config/u-value")
        check("unset subject config -> 40408", err(r) == 40408, r)
        r = c.get("/config/u-value", defaultToGlobal="true")
        check("defaultToGlobal falls back", r[1].get("compatibilityLevel") == "BACKWARD", r)
        r = dev.put("/config/u-value", {"compatibility": "NONE"})
        check("PUT subject config", r == (200, {"compatibility": "NONE"}), r)
        r = c.post("/subjects/u-value/versions", {"schema": json.dumps({"type": "string"})})
        check("NONE allows anything", r == (200, {"id": 3}), r)
        r = c.put("/config/u-value", {"compatibility": "BOGUS"})
        check("invalid level -> 42203", err(r) == 42203, r)
        r = c.delete("/config/u-value")
        check("DELETE subject config returns previous", r == (200, {"compatibilityLevel": "NONE", "normalize": True}), r)

        print("delete semantics")
        r = c.delete("/subjects/u-value/versions/3", permanent="true")
        check("hard delete before soft -> 40407", err(r) == 40407, r)
        r = c.delete("/subjects/u-value/versions/3")
        check("soft delete v3", r == (200, 3), r)
        r = c.delete("/subjects/u-value/versions/3")
        check("soft delete again -> 40406", err(r) == 40406, r)
        check("deleted hidden", c.get("/subjects/u-value/versions")[1] == [1, 2])
        check("deleted=true shows it", c.get("/subjects/u-value/versions", deleted="true")[1] == [1, 2, 3])
        r = c.get("/subjects/u-value/versions/3", deleted="true")
        check("GET deleted version with deleted=true", r[0] == 200 and r[1]["id"] == 3, r)
        r = c.delete("/subjects/u-value/versions/3", permanent="true")
        check("hard delete v3", r == (200, 3), r)
        check("gone even with deleted=true", c.get("/subjects/u-value/versions", deleted="true")[1] == [1, 2])
        r = c.delete("/subjects/other-value", permanent="true")
        check("hard delete subject before soft -> 40405", err(r) == 40405, r)
        r = c.delete("/subjects/other-value")
        check("soft delete subject", r == (200, [1]), r)
        r = c.get("/subjects")
        check("soft-deleted subject hidden", r[1] == ["u-value"], r)
        r = c.get("/subjects", deleted="true")
        check("deleted=true lists it", r[1] == ["other-value", "u-value"], r)
        r = c.get("/subjects", deletedOnly="true")
        check("deletedOnly", r[1] == ["other-value"], r)
        r = c.delete("/subjects/other-value")
        check("soft delete subject twice -> 40404", err(r) == 40404, r)
        r = c.delete("/subjects/other-value", permanent="true")
        check("hard delete subject", r == (200, [1]), r)
        check("id 1 still resolvable (u-value v1 uses it)", c.get("/schemas/ids/1")[0] == 200)

        print("references")
        addr = {"type": "record", "name": "Address", "namespace": "com.acme", "fields": [{"name": "street", "type": "string"}]}
        person = {"type": "record", "name": "Person", "namespace": "com.acme",
                  "fields": [{"name": "home", "type": "com.acme.Address"}]}
        r = c.post("/subjects/address/versions", {"schema": json.dumps(addr)})
        addr_id = r[1]["id"]
        r = c.post("/subjects/person-value/versions", {"schema": json.dumps(person),
                   "references": [{"name": "com.acme.Address", "subject": "address", "version": 1}]})
        check("register with reference", r[0] == 200, r)
        person_id = r[1]["id"]
        r = c.post("/subjects/person2/versions", {"schema": json.dumps(person)})
        check("missing reference -> 42201", err(r) == 42201, r)
        r = c.get("/subjects/address/versions/1/referencedby")
        check("referencedby", r == (200, [person_id]), r)
        r = c.get(f"/schemas/ids/{person_id}")
        check("references returned by id", r[1].get("references") == [{"name": "com.acme.Address", "subject": "address", "version": 1}], r)
        r = c.delete("/subjects/address/versions/1")
        check("delete referenced -> 42206", err(r) == 42206, r)
        check("addr id", addr_id > 0)

        print("protobuf / json")
        proto_v1 = 'syntax = "proto3";\npackage acme;\nmessage Order { string id = 1; int32 qty = 2; }\n'
        r = c.post("/subjects/order-value/versions", {"schemaType": "PROTOBUF", "schema": proto_v1})
        check("register protobuf", r[0] == 200, r)
        r = c.get("/subjects/order-value/versions/1")
        check("protobuf schemaType returned", r[1].get("schemaType") == "PROTOBUF", r)
        r = c.post("/subjects/order-value/versions", {"schemaType": "PROTOBUF", "schema": proto_v1.replace("int32 qty", "string qty")})
        check("protobuf type change -> 409", r[0] == 409, r)
        r = c.post("/subjects/order-value/versions", {"schemaType": "PROTOBUF",
                   "schema": proto_v1.replace("int32 qty = 2;", "int32 qty = 2; string note = 3;")})
        check("protobuf add field ok", r[0] == 200, r)
        r = c.post("/subjects/bad-proto/versions", {"schemaType": "PROTOBUF", "schema": "message {"})
        check("invalid protobuf -> 42201", err(r) == 42201, r)
        js = {"type": "object", "properties": {"a": {"type": "string"}}, "additionalProperties": False}
        r = c.post("/subjects/js-value/versions", {"schemaType": "JSON", "schema": json.dumps(js)})
        check("register json schema", r[0] == 200, r)
        js2 = {**js, "properties": {}}
        r = c.post("/subjects/js-value/versions", {"schemaType": "JSON", "schema": json.dumps(js2)})
        check("json: property removed from closed model -> 409", r[0] == 409, r)
        js3 = {**js, "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}
        r = c.post("/subjects/js-value/versions", {"schemaType": "JSON", "schema": json.dumps(js3)})
        check("json: property added to closed model ok", r[0] == 200, r)
        r = c.get("/schemas/types")
        check("schema types", r == (200, ["JSON", "PROTOBUF", "AVRO"]), r)

        print("contexts")
        r = c.post("/subjects/:.dev:orders/versions", {"schema": '"string"'})
        check("register in .dev context gets id 1 (separate id space)", r == (200, {"id": 1}), r)
        r = c.post("/contexts/.dev/subjects/orders2/versions", {"schema": '"long"'})
        check("/contexts/{ctx}/subjects/... URL form", r == (200, {"id": 2}), r)
        r = c.get("/contexts")
        check("GET /contexts", r == (200, [".", ".dev"]), r)
        r = c.get("/schemas/ids/2", subject=":.dev:")
        check("qualified hint picks the .dev schema", r[1]["schema"] == '"long"', r)
        r = c.get("/subjects")
        check("all contexts listed by default", ":.dev:orders" in r[1] and "u-value" in r[1], r)
        r = c.get("/contexts/.dev/subjects")
        check("context-scoped listing", r == (200, [":.dev:orders", ":.dev:orders2"]), r)
        r = c.get("/schemas/ids/1", subject=":.dev:")
        check("id lookup scoped by subject context", r[1]["schema"] == '"string"', r)
        r = c.get("/contexts/.dev/schemas/ids/2")
        check("id lookup via /contexts URL", r[1]["schema"] == '"long"', r)
        r = c.get("/schemas/ids/1")
        check("default context id 1 unaffected", r[1]["schema"] != '"string"', r)
        r = c.post("/subjects/:.dev:only-dev/versions", {"schema": '"double"'})
        dev_only = r[1]["id"]
        check("id only in .dev", dev_only == 3, r)
        r = c.get(f"/schemas/ids/{dev_only}")
        check("no hint: default context only (no fallback) -> 40403", err(r) == 40403, r)
        r = c.get(f"/schemas/ids/{dev_only}", subject="only-dev")
        check("unqualified hint: falls back to the context where that subject uses the id", r[0] == 200 and r[1]["schema"] == '"double"', r)
        r = c.get(f"/schemas/ids/{dev_only}", subject="orders-value")
        check("unqualified hint whose subject doesn't use the id -> 40403", err(r) == 40403, r)
        r = c.get(f"/schemas/ids/{dev_only}", subject=":.:only-dev")
        check(":.: qualified default behaves like unqualified", r[0] == 200, r)
        r = c.get(f"/schemas/ids/{dev_only}", subject=":.dev:")
        check("context-only hint: any subject in that context", r[0] == 200, r)
        r = c.get(f"/schemas/ids/{dev_only}", subject=":.dev:orders")
        check("qualified subject that doesn't use the id -> 40403", err(r) == 40403, r)
        r = c.get(f"/schemas/ids/{dev_only}", subject=":.other:")
        check("qualified other-context hint: no fallback -> 40403", err(r) == 40403, r)
        r = c.get(f"/schemas/ids/{dev_only}/versions", subject=":.dev:")
        check("id versions in context", r == (200, [{"subject": ":.dev:only-dev", "version": 1}]), r)
        r = c.get(f"/schemas/ids/{dev_only}/subjects", subject="only-dev")
        check("id subjects via unqualified hint", r == (200, [":.dev:only-dev"]), r)
        r = c.get("/schemas/ids/1", subject="whatever")
        check("default context wins when id exists there", r[1]["schema"] != '"string"', r)
        r = c.get("/schemas", subjectPrefix=":.dev:")
        check("GET /schemas scoped to context", [x["subject"] for x in r[1]] == [":.dev:only-dev", ":.dev:orders", ":.dev:orders2"], r)
        r = c.get("/subjects", subjectPrefix=":.dev:orders")
        check("subjectPrefix within context", r == (200, [":.dev:orders", ":.dev:orders2"]), r)
        r = c.get("/contexts/.dev/subjects/orders/versions/1")
        check("/contexts URL: get version", r[1]["subject"] == ":.dev:orders" and r[1]["id"] == 1, r)
        r = c.post("/contexts/.dev/subjects/orders", {"schema": '"string"'})
        check("/contexts URL: lookup", r[0] == 200 and r[1]["version"] == 1, r)
        r = c.post("/contexts/.dev/compatibility/subjects/orders/versions/latest", {"schema": '"int"'})
        check("/contexts URL: compatibility", r == (200, {"is_compatible": False}), r)
        r = c.post("/subjects/:.dev:dref/versions", {"schema": json.dumps({"type": "record", "name": "D", "fields": [{"name": "a", "type": "com.acme.Address"}]}),
                   "references": [{"name": "com.acme.Address", "subject": "address", "version": 1}]})
        check("unqualified reference resolves in referrer's context (not found in .dev)", err(r) == 42201, r)
        r = c.post("/subjects/:.dev:dref/versions", {"schema": json.dumps({"type": "record", "name": "D", "fields": [{"name": "a", "type": "com.acme.Address"}]}),
                   "references": [{"name": "com.acme.Address", "subject": ":.:address", "version": 1}]})
        check("qualified cross-context reference", r[0] == 200, r)
        r = c.put("/contexts/.dev/mode", {"mode": "READONLY"})
        check("context-level mode via /contexts URL", r == (200, {"mode": "READONLY"}), r)
        r = c.post("/subjects/:.dev:blocked/versions", {"schema": '"int"'})
        check("context READONLY applies to its subjects", err(r) == 42205, r)
        check("other contexts unaffected", c.post("/subjects/free/versions", {"schema": '"int"'})[0] == 200)
        c.delete("/mode/:.dev:")
        c.post("/subjects/:.empty:tmp/versions", {"schema": '"int"'})
        c.delete("/subjects/:.empty:tmp")
        c.delete("/subjects/:.empty:tmp", permanent="true")
        r = c.delete("/contexts/.empty")
        check("delete empty context", r[0] == 204, r)
        check("context gone", ".empty" not in c.get("/contexts")[1])
        r = c.put("/config/:.dev:", {"compatibility": "FULL"})
        check("context-level config", r[0] == 200, r)
        r = c.get("/config/:.dev:orders", defaultToGlobal="true")
        check("subject inherits context config", r[1].get("compatibilityLevel") == "FULL", r)
        r = c.delete("/contexts/.dev")
        check("delete non-empty context -> 42211", err(r) == 42211, r)

        print("mode")
        r = c.put("/mode/u-value", {"mode": "READONLY"})
        check("set subject READONLY", r == (200, {"mode": "READONLY"}), r)
        r = c.post("/subjects/u-value/versions", {"schema": json.dumps({"type": "int"})})
        check("register in READONLY -> 42205", err(r) == 42205, r)
        r = c.delete("/mode/u-value")
        check("delete subject mode", r == (200, {"mode": "READONLY"}), r)
        r = c.put("/mode", {"mode": "IMPORT"})
        check("global IMPORT without force blocked", err(r) == 42205, r)
        r = c.put("/mode/imported", {"mode": "IMPORT"})
        check("IMPORT on empty subject", r == (200, {"mode": "IMPORT"}), r)
        r = c.post("/subjects/imported/versions", {"schema": json.dumps({"type": "bytes"}), "id": 5000, "version": 7})
        check("import with explicit id and version", r == (200, {"id": 5000}), r)
        r = c.get("/subjects/imported/versions/7")
        check("imported version kept", r[1]["id"] == 5000, r)
        r = c.post("/subjects/imported/versions", {"schema": json.dumps({"type": "bytes"}), "id": 5000, "version": 7})
        check("import replay returns id+version+schema (Confluent)", r[0] == 200 and r[1].get("version") == 7 and "schema" in r[1], r)
        r = c.post("/subjects/imported/versions", {"schema": '"int"'})
        check("IMPORT without id rejected (Confluent: not in read-write mode)", err(r) == 42205, r)
        r = c.post("/subjects/imported/versions", {"schema": '"long"', "id": 5001, "version": 7})
        check("IMPORT overwrites an existing version (last write wins)", r == (200, {"id": 5001}), r)
        r = c.get("/subjects/imported/versions/7")
        check("overwritten version points at new id", r[1]["id"] == 5001, r)
        r = c.post("/subjects/x/versions", {"schema": json.dumps({"type": "bytes"}), "id": 42})
        check("explicit id outside IMPORT mode rejected", err(r) == 42205, r)
        r = c.get("/schemas/ids/5001", fetchMaxId="true")
        check("fetchMaxId", r[1].get("maxId") == 5001, r)

        print("exporter")
        r = c.post("/exporters", {"name": "to-dst", "contextType": "CUSTOM", "context": ".linked",
                                  "subjects": ["u-value", "address", "person-value"],
                                  "config": {"schema.registry.url": dst_url}})
        check("create exporter", r == (200, {"name": "to-dst"}), r)
        r = c.post("/exporters", {"name": "to-dst", "config": {"schema.registry.url": dst_url}})
        check("duplicate exporter -> 40950", err(r) == 40950, r)
        check("list exporters", c.get("/exporters")[1] == ["to-dst"])
        d = Client(dst_url)
        deadline = time.time() + 15
        subjects = []
        while time.time() < deadline:
            subjects = d.get("/subjects")[1]
            if len(subjects) >= 3:
                break
            time.sleep(0.2)
        check("exported subjects in custom context", sorted(subjects) == [":.linked:address", ":.linked:person-value", ":.linked:u-value"], subjects)
        r = d.get("/subjects/:.linked:u-value/versions/2")
        check("ids and versions preserved", r[0] == 200 and r[1]["id"] == 2, r)
        r = d.get("/subjects/:.linked:person-value/versions/1")
        check("references renamed into destination context", r[1].get("references", [{}])[0].get("subject") == ":.linked:address", r)
        r = c.get("/exporters/to-dst/status")
        check("exporter status RUNNING", r[1].get("state") == "RUNNING", r)
        check("pause", c.put("/exporters/to-dst/pause", {})[1] == {"name": "to-dst"})
        check("paused state", c.get("/exporters/to-dst/status")[1]["state"] == "PAUSED")
        r = c.get("/exporters/to-dst")
        check("exporter info", r[1]["contextType"] == "CUSTOM" and r[1]["context"] == ".linked", r)
        check("delete exporter", c.delete("/exporters/to-dst")[0] == 204)
        check("deleted exporter -> 40450", err(c.get("/exporters/to-dst")) == 40450)

        print("persistence")
        src_proc.terminate()
        src_proc.wait(10)
        procs.remove(src_proc)
        # restart on the same config/data dir
        port = int(open(src_cfg).read().split('127.0.0.1:')[1].split('"')[0])
        p2 = subprocess.Popen([BIN, "--config", src_cfg], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(p2)
        for _ in range(100):
            try:
                requests.get(f"http://127.0.0.1:{port}", timeout=0.2)
                break
            except requests.ConnectionError:
                time.sleep(0.05)
        r = c.get("/subjects/u-value/versions")
        check("data survives restart", r == (200, [1, 2]), r)
        r = c.post("/subjects/fresh/versions", {"schema": json.dumps({"type": "float"})})
        check("id counter survives restart", r[0] == 200 and r[1]["id"] > 5001, r)

        print("logical equality (normalize on by default)")
        js_a = '{"type":"object","properties":{"x":{"type":"string"},"y":{"type":"integer"}}}'
        js_b = '{"properties":{"y":{"type":"integer"},"x":{"type":"string"}},"type":"object"}'
        r1 = c.post("/subjects/eq-json-1/versions", {"schemaType": "JSON", "schema": js_a})
        r2 = c.post("/subjects/eq-json-2/versions", {"schemaType": "JSON", "schema": js_b})
        check("JSON key order: same id", r1[0] == 200 and r1 == r2, (r1, r2))
        pa = 'syntax = "proto3";\npackage eq;\nmessage M { int32 a = 1; N n = 2; }\nmessage N { string s = 1; }\n'
        pb = 'syntax="proto3";package eq;message M{.eq.N n=2;int32 a=1;}message N{string s=1;} // c'
        r1 = c.post("/subjects/eq-pb-1/versions", {"schemaType": "PROTOBUF", "schema": pa})
        r2 = c.post("/subjects/eq-pb-2/versions", {"schemaType": "PROTOBUF", "schema": pb})
        check("Protobuf field order + qualified names + comments: same id", r1[0] == 200 and r1 == r2, (r1, r2))
        aa = '{"type":"record","name":"E","namespace":"eq","fields":[{"name":"f","type":"int","p1":1,"p2":2}]}'
        ab = '{"fields":[{"p2":2,"p1":1,"type":"int","name":"f"}],"name":"eq.E","type":"record"}'
        r1 = c.post("/subjects/eq-avro-1/versions", {"schema": aa})
        r2 = c.post("/subjects/eq-avro-2/versions", {"schema": ab})
        check("Avro attribute/property order + full name: same id", r1[0] == 200 and r1 == r2, (r1, r2))
        r = c.post("/subjects/eq-json-1", {"schemaType": "JSON", "schema": js_b})
        check("lookup with a different spelling finds it", r[0] == 200 and r[1]["version"] == 1, r)

    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()
        if not FAILURES:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print(f"logs kept in {tmp}")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {FAILURES}")
        sys.exit(1)
    print("all e2e checks passed")


if __name__ == "__main__":
    main()
