#!/usr/bin/env python3
"""Backup and restore: the logical dump, over the API and over the CLI.

    python3 tests/backup.py [path/to/schema-registry]

Fills a registry with everything a dump is supposed to carry - contexts,
references, every schema type, soft-deleted versions, metadata, configs, modes
and an exporter - then backs it up, wipes it, restores it, and compares.
Checks that ids and version numbers come back as they were, that a dry run
writes nothing, that restoring twice changes nothing, and that the CLI and the
admin endpoint produce the same dump.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

# e2e reads argv[1] as the binary; keep that and drop the rest.
ARGS = sys.argv[2:]
sys.argv = sys.argv[:2]

from e2e import BIN, FAILURES, Client, check, start  # noqa: E402

AVRO_DEP = json.dumps({"type": "record", "name": "Dep", "namespace": "com.x", "fields": [{"name": "v", "type": "int"}]})
AVRO_HOLDER = json.dumps({"type": "record", "name": "Holder", "namespace": "com.x",
                          "fields": [{"name": "d", "type": "com.x.Dep"}]})
AVRO_USER = json.dumps({"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]})
AVRO_USER_V2 = json.dumps({"type": "record", "name": "User",
                           "fields": [{"name": "name", "type": "string"},
                                      {"name": "age", "type": "int", "default": 0}]})
JSON_A = json.dumps({"type": "object", "properties": {"a": {"type": "string"}}})
PROTO_M = 'syntax = "proto3";\nmessage M {\n  string a = 1;\n}\n'


def populate(c, url):  # noqa: ARG001 - url kept for symmetry with the other suites
    c.put("/config", {"compatibility": "FULL"})
    c.post("/subjects/dep/versions", {"schema": AVRO_DEP})
    c.post("/subjects/holder/versions",
           {"schema": AVRO_HOLDER, "references": [{"name": "com.x.Dep", "subject": "dep", "version": 1}]})
    c.post("/subjects/user-value/versions", {"schema": AVRO_USER})
    c.post("/subjects/user-value/versions", {"schema": AVRO_USER_V2})
    c.post("/subjects/json-value/versions", {"schema": JSON_A, "schemaType": "JSON"})
    c.post("/subjects/proto-value/versions", {"schema": PROTO_M, "schemaType": "PROTOBUF"})
    c.post("/subjects/meta-value/versions", {"schema": AVRO_USER, "metadata": {"properties": {"owner": "team-a"}}})
    c.put("/config/:.eu:", {"compatibility": "NONE"})
    c.post("/subjects/:.eu:orders-value/versions", {"schema": AVRO_USER})
    # A soft-deleted version, and a subject that is entirely soft-deleted.
    c.post("/subjects/gone-value/versions", {"schema": AVRO_USER})
    c.post("/subjects/gone-value/versions", {"schema": JSON_A, "schemaType": "JSON"})
    c.put("/config/gone-value", {"compatibility": "NONE"})
    c.delete("/subjects/gone-value/versions/1")
    c.post("/subjects/dead-value/versions", {"schema": AVRO_USER})
    c.delete("/subjects/dead-value")
    c.put("/config/user-value", {"compatibility": "BACKWARD_TRANSITIVE"})
    c.put("/mode/proto-value", {"mode": "READONLY"})
    # A destination nothing answers on: the exporter record is what a dump has
    # to carry, and a replicating one would add subjects while the test runs.
    c.post("/exporters", {"name": "to-dr", "contextType": "CUSTOM", "context": ".dr", "subjects": ["user-*"],
                          "config": {"schema.registry.url": "http://127.0.0.1:1"}})


def state(c):
    """Everything a dump is supposed to carry."""
    out = {"config": c.get("/config")[1], "subjects": {}, "exporters": sorted(c.get("/exporters")[1])}
    for subject in sorted(c.get("/subjects", subjectPrefix=":*:", deleted="true")[1]):
        live = c.get(f"/subjects/{subject}/versions")
        entry = {"live": live[1] if live[0] == 200 else [],
                 "config": c.get(f"/config/{subject}")[1], "mode": c.get(f"/mode/{subject}")[1], "versions": {}}
        for v in c.get(f"/subjects/{subject}/versions", deleted="true")[1]:
            s = c.get(f"/subjects/{subject}/versions/{v}", deleted="true")[1]
            entry["versions"][str(v)] = {k: s.get(k) for k in ("id", "version", "schema", "schemaType", "references", "metadata")}
        out["subjects"][subject] = entry
    return out


def wipe(c):
    c.put("/mode?force=true", {"mode": "READWRITE"})
    for name in c.get("/exporters")[1]:
        c.delete(f"/exporters/{name}")
    for _ in range(10):
        subjects = c.get("/subjects", subjectPrefix=":*:", deleted="true")[1]
        if not subjects:
            return
        for s in subjects:
            c.delete(f"/mode/{s}")
            c.delete(f"/subjects/{s}")
            c.delete(f"/subjects/{s}", permanent="true")


def main():
    tmp = tempfile.mkdtemp(prefix="sr-backup-")
    procs = []
    try:
        proc, url, _ = start(tmp, "one", auth=False)
        procs.append(proc)
        c = Client(url)
        populate(c, url)
        before = state(c)

        print("the admin endpoint and the CLI agree")
        code, api_dump = c.req("GET", "/admin/api/backup")
        check("admin backup responds", code == 200, code)
        cli = subprocess.run([BIN, "backup", "--from", url], capture_output=True, text=True)
        check("cli backup succeeds", cli.returncode == 0, cli.stderr[-300:])
        kinds = lambda text: sorted(json.loads(l)["type"] for l in text.strip().splitlines())
        check("same records either way", kinds(api_dump) == kinds(cli.stdout),
              f"{kinds(api_dump)} != {kinds(cli.stdout)}")
        dump_path = os.path.join(tmp, "dump.ndjson")
        with open(dump_path, "w") as f:
            f.write(cli.stdout)

        print("a dry run writes nothing")
        wipe(c)
        r = subprocess.run([BIN, "restore", "--from", dump_path, "--to", url, "--dry-run"], capture_output=True, text=True)
        check("dry run succeeds", r.returncode == 0, r.stderr[-300:])
        check("dry run says what it would do", "would restore" in r.stdout, r.stdout)
        check("and wrote nothing", c.get("/subjects", subjectPrefix=":*:", deleted="true")[1] == [])

        print("restore brings it all back")
        r = subprocess.run([BIN, "restore", "--from", dump_path, "--to", url], capture_output=True, text=True)
        check("restore succeeds", r.returncode == 0, r.stdout + r.stderr[-400:])
        after = state(c)
        subset = lambda want, got: isinstance(got, dict) and all(got.get(k) == v for k, v in (want or {}).items())
        check("same subjects", sorted(after["subjects"]) == sorted(before["subjects"]),
              f"{sorted(after['subjects'])} != {sorted(before['subjects'])}")
        for subject, want in before["subjects"].items():
            got = after["subjects"].get(subject, {})
            check(f"{subject}: versions and ids", got.get("versions") == want["versions"],
                  f"{json.dumps(got.get('versions'))[:260]} != {json.dumps(want['versions'])[:260]}")
            check(f"{subject}: live versions", got.get("live") == want["live"], f"{got.get('live')} != {want['live']}")
            check(f"{subject}: config", subset(want["config"], got.get("config")), f"{got.get('config')} != {want['config']}")
            check(f"{subject}: mode", got.get("mode") == want["mode"], f"{got.get('mode')} != {want['mode']}")
        check("global config", subset(before["config"], after["config"]), f"{after['config']} != {before['config']}")
        check("exporters", after["exporters"] == before["exporters"], f"{after['exporters']} != {before['exporters']}")

        print("restoring again changes nothing")
        r = subprocess.run([BIN, "restore", "--from", dump_path, "--to", url], capture_output=True, text=True)
        check("second restore succeeds", r.returncode == 0, r.stdout + r.stderr[-400:])
        check("state unchanged", state(c) == after, "a second restore altered the registry")

        print("restoring into a different registry")
        other_proc, other_url, _ = start(tmp, "two", auth=False)
        procs.append(other_proc)
        other = Client(other_url)
        r = subprocess.run([BIN, "restore", "--from", dump_path, "--to", other_url], capture_output=True, text=True)
        check("restore into an empty registry succeeds", r.returncode == 0, r.stdout + r.stderr[-400:])
        moved = state(other)
        for subject, want in before["subjects"].items():
            got = moved["subjects"].get(subject, {})
            check(f"moved {subject}: versions and ids", got.get("versions") == want["versions"],
                  f"{json.dumps(got.get('versions'))[:200]}")

        print("the API restores what the API dumped")
        wipe(c)
        import requests
        resp = requests.post(url + "/admin/api/restore", data=cli.stdout,
                             headers={"Content-Type": "application/x-ndjson"})
        code, body = resp.status_code, resp.json()
        check("admin restore responds", code == 200, (code, body))
        check("and reports what it did", isinstance(body, dict) and body.get("versions") == sum(
            len(s["versions"]) for s in before["subjects"].values()), body)
        check("subjects are back", sorted(state(c)["subjects"]) == sorted(before["subjects"]))
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
    print("all backup and restore checks passed")


if __name__ == "__main__":
    main()
