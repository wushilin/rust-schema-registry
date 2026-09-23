#!/usr/bin/env python3
"""Exporter (schema linking) tests: one instance exporting into another.

Usage: cargo build && python3 tests/exporter.py [path/to/schema-registry]

Starts a source and a destination (the destination has Basic auth on, so the
exporter's `basic.auth.user.info` is exercised) and checks every context
mapping: NONE (keep the source context), AUTO (namespace under the source
cluster id), DEFAULT (collapse into the default context) and CUSTOM (one
chosen context), plus subject renaming, cross-context references, replayed
deletes, and recovery from a dead destination.
"""

import json
import shutil
import subprocess
import sys
import tempfile
import time

from e2e import CT, FAILURES, Client, check, start

AVRO_ADDRESS = json.dumps({"type": "record", "name": "Address", "namespace": "com.x",
                           "fields": [{"name": "street", "type": "string"}]})
AVRO_PERSON = json.dumps({"type": "record", "name": "Person", "namespace": "com.x",
                          "fields": [{"name": "home", "type": "com.x.Address"}]})
AVRO_ADDRESS_V2 = json.dumps({"type": "record", "name": "Address", "namespace": "com.x",
                              "fields": [{"name": "street", "type": "string"},
                                         {"name": "zip", "type": "string", "default": ""}]})
AVRO_STR = json.dumps({"type": "string"})
AVRO_INT = json.dumps({"type": "int"})


def until(fn, timeout=15):
    """Poll until `fn` returns something truthy (the exporter is asynchronous)."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(0.1)
    return last


def registered(client, subject, schema, **body):
    """Register and fail loudly, so a test bug can't look like an export bug."""
    r = client.post(f"/subjects/{subject}/versions", {"schema": schema, **body})
    check(f"source registers {subject}", r[0] == 200, r)
    return r[1].get("id")


def make_exporter(src, name, subjects, context_type, dst_url, context=None, rename=None):
    body = {"name": name, "subjects": subjects, "contextType": context_type,
            "config": {"schema.registry.url": dst_url, "basic.auth.user.info": "admin:admin-secret"}}
    if context:
        body["context"] = context
    if rename:
        body["subjectRenameFormat"] = rename
    r = src.post("/exporters", body)
    check(f"create exporter {name}", r == (200, {"name": name}), r)


def main():
    tmp = tempfile.mkdtemp(prefix="sr-exporter-")
    procs = []
    try:
        src_proc, src_url, _ = start(tmp, "srcA", auth=False)
        dst_proc, dst_url, _ = start(tmp, "dst", auth=True)
        procs = [src_proc, dst_proc]
        s = Client(src_url)
        d = Client(dst_url, auth=("admin", "admin-secret"))

        print("context to context (contextType NONE keeps the source context)")
        registered(s, ":.src:address", AVRO_ADDRESS)
        person_id = registered(s, ":.src:person-value", AVRO_PERSON,
                               references=[{"name": "com.x.Address", "subject": ":.src:address", "version": 1}])
        make_exporter(s, "none-exp", [":.src:*"], "NONE", dst_url)
        got = until(lambda: d.get("/subjects", subjectPrefix=":.src:")[1] or None)
        check("source context preserved on the destination",
              sorted(got or []) == [":.src:address", ":.src:person-value"], got)
        r = d.get("/subjects/:.src:person-value/versions/1")
        check("id and version preserved across contexts", r[1].get("id") == person_id and r[1]["version"] == 1, r)
        check("reference still points inside the same context",
              r[1].get("references", [{}])[0].get("subject") == ":.src:address", r)

        print("contextType AUTO namespaces under the source cluster id")
        registered(s, "auto-value", AVRO_STR)
        registered(s, ":.x:auto2-value", AVRO_STR)
        make_exporter(s, "auto-exp", [":*:auto*"], "AUTO", dst_url)
        got = until(lambda: d.get("/subjects", subjectPrefix=":.srcA:")[1] or None)
        check("default context lands in .{clusterId}", got == [":.srcA:auto-value"], got)
        got = until(lambda: d.get("/subjects", subjectPrefix=":.srcA.x:")[1] or None)
        check("other contexts keep their suffix", got == [":.srcA.x:auto2-value"], got)

        print("contextType DEFAULT collapses into the default context")
        registered(s, ":.flat:flat-value", AVRO_INT)
        make_exporter(s, "flat-exp", [":.flat:*"], "DEFAULT", dst_url)
        got = until(lambda: d.get("/subjects", subjectPrefix="flat-value")[1] or None)
        check("context dropped on the destination", got == ["flat-value"], got)

        print("contextType CUSTOM with subjectRenameFormat")
        registered(s, ":.a:ren-value", AVRO_STR)
        registered(s, ":.a:ren2-value", AVRO_INT)
        make_exporter(s, "custom-exp", [":.a:*"], "CUSTOM", dst_url,
                      context=".linked", rename="${subject}-copy")
        got = until(lambda: sorted(d.get("/subjects", subjectPrefix=":.linked:")[1]) == [
            ":.linked:ren-value-copy", ":.linked:ren2-value-copy"])
        check("subjects renamed into the custom context", got, d.get("/subjects", subjectPrefix=":.linked:"))

        print("a mapping that would merge contexts is refused")
        # Ids are per context (each starts at 1), so two source contexts cannot
        # share one destination context without losing them.
        cfg = {"schema.registry.url": dst_url}
        r = s.post("/exporters", {"name": "clash-exp", "subjects": [":.a:*", ":.b:*"],
                                  "contextType": "CUSTOM", "context": ".clash", "config": cfg})
        check("CUSTOM across two contexts -> 42250", r[1].get("error_code") == 42250, r)
        r = s.post("/exporters", {"name": "clash-exp", "subjects": [":*:*"],
                                  "contextType": "DEFAULT", "config": cfg})
        check("DEFAULT with the context wildcard -> 42250", r[1].get("error_code") == 42250, r)
        r = s.post("/exporters", {"name": "ok-exp", "subjects": [":*:*"], "contextType": "NONE", "config": cfg})
        check("NONE keeps contexts apart, so the wildcard is fine", r[0] == 200, r)
        s.delete("/exporters/ok-exp")

        print("deletes are replayed")
        registered(s, ":.src:address", AVRO_ADDRESS_V2)
        check("v2 exported", until(lambda: d.get("/subjects/:.src:address/versions")[1] == [1, 2]),
              d.get("/subjects/:.src:address/versions"))
        s.delete("/subjects/:.src:address/versions/2")
        got = until(lambda: d.get("/subjects/:.src:address/versions")[1] == [1])
        check("soft delete replayed", got, d.get("/subjects/:.src:address/versions"))
        check("soft-deleted version still listed with deleted=true",
              d.get("/subjects/:.src:address/versions", deleted="true")[1] == [1, 2],
              d.get("/subjects/:.src:address/versions", deleted="true"))
        s.delete("/subjects/:.src:address/versions/2", permanent="true")
        got = until(lambda: d.get("/subjects/:.src:address/versions", deleted="true")[1] == [1])
        check("permanent delete replayed", got, d.get("/subjects/:.src:address/versions", deleted="true"))

        print("the destination leaving IMPORT mode is re-applied")
        # Once a subject has been exported, the exporter remembers that the
        # destination is in IMPORT mode. If that stops being true (an operator
        # changes it, or the subject's last live version is deleted, which drops
        # its mode), the exporter has to set it again instead of failing forever.
        registered(s, ":.src:modecheck", AVRO_ADDRESS)
        check("first version exported", until(lambda: d.get("/subjects/:.src:modecheck/versions")[0] == 200),
              d.get("/subjects/:.src:modecheck/versions"))
        d.put("/mode/:.src:modecheck", {"mode": "READWRITE"})
        registered(s, ":.src:modecheck", AVRO_ADDRESS_V2)
        check("export recovers by setting IMPORT again",
              until(lambda: d.get("/subjects/:.src:modecheck/versions")[1] == [1, 2]),
              d.get("/subjects/:.src:modecheck/versions"))

        print("a dead destination goes to ERROR and recovers")
        make_exporter(s, "broken", [":.err:*"], "NONE", "http://127.0.0.1:1")
        registered(s, ":.err:e-value", AVRO_STR)
        got = until(lambda: s.get("/exporters/broken/status")[1].get("state") == "ERROR")
        check("exporter reports ERROR", got, s.get("/exporters/broken/status"))
        check("error trace is kept", s.get("/exporters/broken/status")[1].get("trace"), s.get("/exporters/broken/status"))
        r = s.put("/exporters/broken/config",
                  {"schema.registry.url": dst_url, "basic.auth.user.info": "admin:admin-secret"})
        check("update exporter config", r == (200, {"name": "broken"}), r)
        got = until(lambda: d.get("/subjects", subjectPrefix=":.err:")[1] or None)
        check("export resumes after the fix", got == [":.err:e-value"], got)
        check("state back to RUNNING", until(lambda: s.get("/exporters/broken/status")[1]["state"] == "RUNNING"),
              s.get("/exporters/broken/status"))

        print("pause, reset and replay are idempotent")
        check("pause", s.put("/exporters/none-exp/pause", {})[1] == {"name": "none-exp"})
        registered(s, ":.src:paused-value", AVRO_STR)
        time.sleep(2)
        check("nothing exported while paused", d.get("/subjects/:.src:paused-value/versions")[0] == 404)
        check("resume", s.put("/exporters/none-exp/resume", {})[1] == {"name": "none-exp"})
        check("exports again after resume", until(lambda: d.get("/subjects/:.src:paused-value/versions")[0] == 200))
        check("reset", s.put("/exporters/none-exp/reset", {})[1] == {"name": "none-exp"})
        before = d.get("/subjects/:.src:person-value/versions/1")[1]
        time.sleep(2)
        check("replay from the start changes nothing",
              d.get("/subjects/:.src:person-value/versions/1")[1] == before)
        check("destination subject is left in IMPORT mode",
              d.get("/mode/:.src:person-value")[1] == {"mode": "IMPORT"}, d.get("/mode/:.src:person-value"))
        print("a new exporter still gets everything after the log was pruned")
        # The change log only keeps what exporters still need, so an exporter
        # created later has to export the current state instead of replaying.
        late = until(lambda: s.get("/exporters/none-exp/status")[1].get("offset", 0) > 0)
        check("existing exporters have consumed the log", late, s.get("/exporters/none-exp/status"))
        make_exporter(s, "late-exp", [":.src:*"], "CUSTOM", dst_url, context=".late")
        got = until(lambda: sorted(d.get("/subjects", subjectPrefix=":.late:")[1]) or None)
        check("late exporter exported the current state",
              got == [":.late:address", ":.late:modecheck", ":.late:paused-value", ":.late:person-value"], got)
        r = d.get("/subjects/:.late:person-value/versions/1")
        check("and kept ids and references", r[1].get("references", [{}])[0].get("subject") == ":.late:address", r)

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
    print("all exporter checks passed")


if __name__ == "__main__":
    main()
