#!/usr/bin/env python3
"""Migration: `schema-registry migrate` copying a whole registry.

    python3 tests/migrate.py                       # source and destination are both this server
    python3 tests/migrate.py --source URL [u:p]    # migrate from another registry (a Confluent, say)

Fills the source with subjects across contexts, references, every schema type,
soft-deleted versions, metadata, configuration and modes; migrates; then
compares the two registries subject by subject, version by version - ids,
versions, schema text, references, metadata, configs and modes - and checks
that a dry run writes nothing and that a second run changes nothing.

A source given with --source is wiped first, so point it at a test instance.
"""

import json
import shutil
import subprocess
import sys
import tempfile

# Our own flags must not reach e2e.py, which reads argv[1] as the binary path.
ARGS = sys.argv[1:]
sys.argv = sys.argv[:1]

from e2e import BIN, FAILURES, Client, check, start  # noqa: E402

AVRO_DEP = json.dumps({"type": "record", "name": "Dep", "namespace": "com.x",
                       "fields": [{"name": "v", "type": "int"}]})
AVRO_USES_DEP = json.dumps({"type": "record", "name": "Holder", "namespace": "com.x",
                            "fields": [{"name": "d", "type": "com.x.Dep"}]})
AVRO_USER = json.dumps({"type": "record", "name": "User", "fields": [{"name": "name", "type": "string"}]})
AVRO_USER_V2 = json.dumps({"type": "record", "name": "User",
                           "fields": [{"name": "name", "type": "string"},
                                      {"name": "age", "type": "int", "default": 0}]})
JSON_A = json.dumps({"type": "object", "properties": {"a": {"type": "string"}}})
PROTO_M = 'syntax = "proto3";\nmessage M {\n  string a = 1;\n}\n'


def wipe(c):
    """Empty a registry (subjects in every context, plus global settings)."""
    c.put("/mode?force=true", {"mode": "READWRITE"})
    c.delete("/config")
    for _ in range(10):
        subjects = c.get("/subjects", subjectPrefix=":*:", deleted="true")[1]
        if not subjects:
            return
        for s in subjects:
            c.delete(f"/mode/{s}")
            c.delete(f"/subjects/{s}")
            c.delete(f"/subjects/{s}", permanent="true")


def populate(c):
    """A source with something of everything worth preserving."""
    c.put("/config", {"compatibility": "FULL"})
    c.post("/subjects/dep/versions", {"schema": AVRO_DEP})
    c.post("/subjects/holder/versions",
           {"schema": AVRO_USES_DEP, "references": [{"name": "com.x.Dep", "subject": "dep", "version": 1}]})
    c.post("/subjects/user-value/versions", {"schema": AVRO_USER})
    c.post("/subjects/user-value/versions", {"schema": AVRO_USER_V2})
    c.post("/subjects/json-value/versions", {"schema": JSON_A, "schemaType": "JSON"})
    c.post("/subjects/proto-value/versions", {"schema": PROTO_M, "schemaType": "PROTOBUF"})
    c.post("/subjects/meta-value/versions", {"schema": AVRO_USER, "metadata": {"properties": {"owner": "team-a"}}})
    # A context of its own, with its own config.
    c.put("/config/:.eu:", {"compatibility": "NONE"})
    c.post("/subjects/:.eu:orders-value/versions", {"schema": AVRO_USER})
    c.post("/subjects/:.eu:orders-value/versions", {"schema": JSON_A, "schemaType": "JSON"})
    # A soft-deleted version, and a subject that is entirely soft-deleted.
    c.put("/config/gone-value", {"compatibility": "NONE"})
    c.post("/subjects/gone-value/versions", {"schema": AVRO_USER})
    c.post("/subjects/gone-value/versions", {"schema": JSON_A, "schemaType": "JSON"})
    c.delete("/subjects/gone-value/versions/1")
    c.post("/subjects/dead-value/versions", {"schema": AVRO_USER})
    c.delete("/subjects/dead-value")
    # Subject-level settings.
    c.put("/config/user-value", {"compatibility": "BACKWARD_TRANSITIVE"})
    c.put("/mode/proto-value", {"mode": "READONLY"})


def state(c):
    """Everything a migration is supposed to preserve."""
    out = {"config": c.get("/config")[1], "subjects": {}}
    for subject in sorted(c.get("/subjects", subjectPrefix=":*:", deleted="true")[1]):
        entry = {"versions": {}, "live": c.get(f"/subjects/{subject}/versions")[1] if
                 c.get(f"/subjects/{subject}/versions")[0] == 200 else [],
                 "config": c.get(f"/config/{subject}")[1], "mode": c.get(f"/mode/{subject}")[1]}
        for v in c.get(f"/subjects/{subject}/versions", deleted="true")[1]:
            s = c.get(f"/subjects/{subject}/versions/{v}", deleted="true")[1]
            entry["versions"][str(v)] = {k: s.get(k) for k in ("id", "version", "schema", "schemaType", "references", "metadata")}
        out["subjects"][subject] = entry
    return out


def migrate(src_url, dst_url, src_auth=None, *args):
    cmd = [BIN, "migrate", "--from", src_url, "--to", dst_url, *args]
    if src_auth:
        cmd += ["--from-auth", src_auth]
    return subprocess.run(cmd, capture_output=True, text=True)


def main():
    args = ARGS
    source_url = source_auth = None
    if "--source" in args:
        i = args.index("--source")
        source_url = args[i + 1]
        source_auth = args[i + 2] if len(args) > i + 2 and ":" in args[i + 2] else None

    tmp = tempfile.mkdtemp(prefix="sr-migrate-")
    procs = []
    try:
        if source_url:
            src = Client(source_url, auth=tuple(source_auth.split(":", 1)) if source_auth else None)
            print(f"source: {source_url} (wiping it first)")
            wipe(src)
        else:
            src_proc, source_url, _ = start(tmp, "src", auth=False)
            procs.append(src_proc)
            src = Client(source_url)
        dst_proc, dst_url, _ = start(tmp, "dst", auth=False)
        procs.append(dst_proc)
        dst = Client(dst_url)

        populate(src)
        before = state(src)

        print("dry run")
        r = migrate(source_url, dst_url, source_auth, "--dry-run")
        check("dry run succeeds", r.returncode == 0, r.stderr[-400:])
        check("dry run reports what it would copy", "would copy" in r.stdout, r.stdout)
        check("dry run wrote nothing", dst.get("/subjects", subjectPrefix=":*:", deleted="true")[1] == [], dst.get("/subjects")[1])

        print("migrate")
        r = migrate(source_url, dst_url, source_auth)
        check("migrate succeeds", r.returncode == 0, r.stdout + r.stderr[-600:])
        after = state(dst)

        check("same subjects", sorted(after["subjects"]) == sorted(before["subjects"]),
              f"{sorted(after['subjects'])} != {sorted(before['subjects'])}")
        # This server reports its own `normalize` default in configs, so the
        # source's settings must be present, not identical.
        subset = lambda want, got: isinstance(got, dict) and all(got.get(k) == v for k, v in (want or {}).items())
        for subject, want in before["subjects"].items():
            got = after["subjects"].get(subject, {})
            check(f"{subject}: versions and ids", got.get("versions") == want["versions"],
                  f"{json.dumps(got.get('versions'))[:300]} != {json.dumps(want['versions'])[:300]}")
            check(f"{subject}: live versions", got.get("live") == want["live"], f"{got.get('live')} != {want['live']}")
            check(f"{subject}: config", subset(want["config"], got.get("config")), f"{got.get('config')} != {want['config']}")
            check(f"{subject}: mode", got.get("mode") == want["mode"], f"{got.get('mode')} != {want['mode']}")
        check("global config", subset(before["config"], after["config"]), f"{after['config']} != {before['config']}")
        check("context config", subset(src.get("/config/:.eu:")[1], dst.get("/config/:.eu:")[1]),
              f"{dst.get('/config/:.eu:')[1]} != {src.get('/config/:.eu:')[1]}")

        print("running it again changes nothing")
        r = migrate(source_url, dst_url, source_auth)
        check("second run succeeds", r.returncode == 0, r.stdout + r.stderr[-600:])
        check("state unchanged", state(dst) == after, "second run altered the destination")
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
    print("all migration checks passed")


if __name__ == "__main__":
    main()
