#!/usr/bin/env python3
"""Role bindings: roles scoped to subject patterns, over real HTTP.

    python3 tests/rbac.py [path/to/schema-registry]

Starts one server whose users mix registry-wide roles with per-subject
bindings, then checks what each of them can do, what they are refused, and -
just as important - what they are *shown*: listings and the admin UI are
filtered to the subjects a caller may see, never refused outright.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import requests

from e2e import BIN, CT, FAILURES, Client, check, free_port

AVRO = json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "int"}]})
AVRO2 = json.dumps({"type": "record", "name": "R",
                    "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "int", "default": 0}]})

CONFIG = """
listen = "127.0.0.1:{port}"
data_dir = "{data}"
cluster_id = "rbac"

[auth]
enabled = true

[[auth.users]]
username = "root"
password = "root-secret"
roles = ["admin"]

# Reads everything, owns abc* and def* (the shape asked for: several patterns
# per role, several roles per user).
[[auth.users]]
username = "owner"
password = "owner-secret"
roles = ["readonly"]
[[auth.users.bindings]]
role = "admin"
subjects = [".::abc*", ".::def*"]
[[auth.users.bindings]]
role = "write"
subjects = [".::xyz-*"]

# A plain registry-wide role, no bindings: the behaviour that existed before.
[[auth.users]]
username = "plain"
password = "plain-secret"
roles = ["readonly"]

# A registry-wide writer: owns subjects everywhere, owns no context's settings.
[[auth.users]]
username = "writer"
password = "writer-secret"
roles = ["write"]

# No registry-wide role at all: sees only what its bindings name.
[[auth.users]]
username = "eu"
password = "eu-secret"
roles = []
[[auth.users.bindings]]
role = "admin"
subjects = [".eu::*"]
"""


def start(tmp):
    port = free_port()
    cfg = os.path.join(tmp, "rbac.toml")
    with open(cfg, "w") as f:
        f.write(CONFIG.format(port=port, data=os.path.join(tmp, "data")))
    log = open(os.path.join(tmp, "rbac.log"), "w")
    proc = subprocess.Popen([BIN, "--config", cfg], stdout=log, stderr=subprocess.STDOUT)
    url = f"http://127.0.0.1:{port}"
    import time

    import requests
    for _ in range(100):
        try:
            requests.get(url, timeout=0.2)
            return proc, url
        except requests.ConnectionError:
            time.sleep(0.05)
    raise RuntimeError(f"server did not start; see {log.name}")


def main():
    tmp = tempfile.mkdtemp(prefix="sr-rbac-")
    proc = None
    try:
        proc, url = start(tmp)
        root = Client(url, auth=("root", "root-secret"))
        owner = Client(url, auth=("owner", "owner-secret"))
        eu = Client(url, auth=("eu", "eu-secret"))
        anon = Client(url)

        print("setup (as the global admin)")
        for s in ("abc-1", "def-1", "xyz-1", "other-1", ":.eu:orders", ":.us:orders"):
            code, body = root.post(f"/subjects/{s}/versions", {"schema": AVRO})
            check(f"register {s}", code == 200, body)

        print("bad credentials")
        check("no credentials -> 401", anon.get("/subjects")[0] == 401)
        check("wrong password -> 401", Client(url, auth=("owner", "nope")).get("/subjects")[0] == 401)

        print("a binding grants its role on matching subjects only")
        check("owner registers into abc-1", owner.post("/subjects/abc-1/versions", {"schema": AVRO2})[0] == 200)
        check("owner deletes abc-1 version", owner.delete("/subjects/abc-1/versions/2")[0] == 200)
        check("owner sets config on def-1", owner.put("/config/def-1", {"compatibility": "NONE"})[0] == 200)
        check("owner writes xyz-1 (write binding)", owner.post("/subjects/xyz-1/versions", {"schema": AVRO2})[0] == 200)
        # `write` includes deleting its own subjects, as it does registry-wide.
        check("owner deletes xyz-1 (write covers deletes)", owner.delete("/subjects/xyz-1")[0] == 200)
        check("and registers it back", owner.post("/subjects/xyz-1/versions", {"schema": AVRO})[0] == 200)
        check("owner may not write other-1", owner.post("/subjects/other-1/versions", {"schema": AVRO2})[0] == 403)
        check("owner may not delete other-1", owner.delete("/subjects/other-1")[0] == 403)
        check("owner still reads other-1 (global readonly)", owner.get("/subjects/other-1/versions")[0] == 200)

        print("global settings and exporters stay with the global admin")
        check("owner may not set the global config", owner.put("/config", {"compatibility": "NONE"})[0] == 403)
        check("owner may not set the global mode", owner.put("/mode", {"mode": "READONLY"})[0] == 403)
        check("owner may not create an exporter",
              owner.post("/exporters", {"name": "x", "config": {"schema.registry.url": url}})[0] == 403)
        check("root may", root.put("/config", {"compatibility": "BACKWARD"})[0] == 200)

        print("a context-wide binding owns that context")
        check("eu writes in .eu", eu.post("/subjects/:.eu:orders/versions", {"schema": AVRO2})[0] == 200)
        check("eu sets the context config", eu.put("/config/:.eu:", {"compatibility": "FULL"})[0] == 200)
        check("eu sets the context mode", eu.put("/mode/:.eu:", {"mode": "READONLY"})[0] == 200)
        check("eu restores the context mode", eu.put("/mode/:.eu:", {"mode": "READWRITE"})[0] == 200)
        check("eu may not touch .us", eu.put("/config/:.us:", {"compatibility": "FULL"})[0] == 403)
        check("eu may not read .us subjects", eu.get("/subjects/:.us:orders/versions")[0] == 403)
        check("eu may not touch the default context", eu.post("/subjects/abc-1/versions", {"schema": AVRO})[0] == 403)
        check("owner (abc* only) may not own the default context",
              owner.put("/config/:.:", {"compatibility": "FULL"})[0] == 403)

        print("listings are filtered, not refused")
        check("root sees every subject", len(root.get("/subjects", subjectPrefix=":*:")[1]) == 6)
        check("owner (global readonly) sees every subject", len(owner.get("/subjects", subjectPrefix=":*:")[1]) == 6)
        eu_subjects = eu.get("/subjects", subjectPrefix=":*:")[1]
        check("eu sees only its own context", eu_subjects == [":.eu:orders"], eu_subjects)
        check("eu's /schemas is filtered",
              {s["subject"] for s in eu.get("/schemas", subjectPrefix=":*:")[1]} == {":.eu:orders"})
        check("eu's /contexts is filtered", eu.get("/contexts")[1] == [".eu"], eu.get("/contexts")[1])
        check("root's /contexts is not", sorted(root.get("/contexts")[1]) == [".", ".eu", ".us"])
        ids = eu.get("/schemas/ids/1/subjects", **{"subject": ":.eu:orders"})
        check("id -> subjects is filtered", all(s.startswith(":.eu:") for s in ids[1]), ids)

        print("the admin UI shows what the caller administers")
        check("eu opens the admin UI", eu.get("/admin")[0] == 200)
        check("owner opens the admin UI", owner.get("/admin")[0] == 200)
        plain = Client(url, auth=("plain", "plain-secret"))
        check("a user who is admin of nothing does not", plain.get("/admin")[0] == 403)
        check("...and is refused the overview", plain.get("/admin/api/overview")[0] == 403)
        check("...but still reads subjects", len(plain.get("/subjects", subjectPrefix=":*:")[1]) == 6)
        ov = eu.get("/admin/api/overview", deleted="true")[1]
        check("eu's overview holds only .eu", [r["subject"] for r in ov["subjects"]] == [":.eu:orders"],
              [r["subject"] for r in ov["subjects"]])
        check("eu's overview counts match what it shows", ov["counts"]["subjects"] == 1, ov["counts"])
        check("eu's overview lists only its context", [c["name"] for c in ov["contexts"]] == [".eu"],
              [c["name"] for c in ov["contexts"]])
        ov = owner.get("/admin/api/overview", deleted="true")[1]
        shown = sorted(r["subject"] for r in ov["subjects"])
        check("owner's overview holds the subjects it administers", shown == ["abc-1", "def-1"], shown)
        check("owner's subject detail works for abc-1", owner.get("/admin/api/subjects/abc-1")[0] == 200)
        check("owner's subject detail is refused for other-1", owner.get("/admin/api/subjects/other-1")[0] == 403)
        check("root's overview holds everything", len(root.get("/admin/api/overview", deleted="true")[1]["subjects"]) == 6)
        check("the old /_admin path redirects", root.s.get(url + "/_admin", allow_redirects=False).status_code == 308)
        check("and still needs the admin role", plain.s.get(url + "/_admin", allow_redirects=False).status_code == 403)

        print("registering through the UI's own calls")
        # The admin page registers with the public API, so a scoped admin can
        # create subjects it owns and nothing else.
        check("owner registers a new abc subject", owner.post("/subjects/abc-2/versions", {"schema": AVRO})[0] == 200)
        check("owner may not create a foreign subject", owner.post("/subjects/nope-1/versions", {"schema": AVRO})[0] == 403)
        check("owner checks compatibility on its own subject",
              owner.post("/compatibility/subjects/abc-2/versions?verbose=true", {"schema": AVRO2})[0] == 200)

        writer = Client(url, auth=("writer", "writer-secret"))

        print("whole-container and whole-process endpoints need more than a role somewhere")
        # `owner` is readonly everywhere plus admin of abc*/def*. That passes
        # the path gate for /admin/api/*, so the handler is the only thing
        # between it and rewriting every subject in the container.
        check("a scoped admin may not back up the container", owner.get("/admin/api/backup")[0] == 403)
        ndjson = lambda c, path, body: c.s.post(url + path, data=body, headers=CT)
        check("nor restore it", ndjson(owner, "/admin/api/restore", "").status_code == 403)
        check("root may back up", root.get("/admin/api/backup")[0] == 200)
        # A dry run over HTTP, which only the CLI had ever exercised.
        dump = root.s.get(url + "/admin/api/backup", headers=CT).text
        r = ndjson(root, "/admin/api/restore?dryRun=true", dump)
        check("a dry run reports without writing", r.status_code == 200 and r.json()["subjects"] > 0,
              (r.status_code, r.text[:200]))
        # Metrics counts and names every host container in the process.
        check("metrics is not for a reader", plain.get("/metrics")[0] == 403)
        check("nor for a scoped admin", owner.get("/metrics")[0] == 403)
        check("metrics needs an operator", root.get("/metrics")[0] == 200)
        check("and no credentials at all is 401", Client(url).get("/metrics")[0] == 401)

        print("an exporter's destination credentials are not readable by a reader")
        secret = {"name": "with-secret", "contextType": "CUSTOM", "context": ".dr", "subjects": ["abc*"],
                  "config": {"schema.registry.url": "http://127.0.0.1:1",
                             "basic.auth.user.info": "dr-admin:DO-NOT-LEAK"}}
        check("root creates it", root.post("/exporters", secret)[0] == 200)
        check("a reader may not read its config", plain.get("/exporters/with-secret/config")[0] == 403)
        check("nor the record that carries it", plain.get("/exporters/with-secret")[0] == 403)
        check("but may see that it exists", plain.get("/exporters")[0] == 200)
        check("and how it is getting on", plain.get("/exporters/with-secret/status")[0] == 200)
        check("root reads the config", "DO-NOT-LEAK" in str(root.get("/exporters/with-secret/config")[1]))
        root.delete("/exporters/with-secret")

        print("a writer does not own a context however the colon is spelled")
        check("plain form is refused", writer.put("/config/:.eu:", {"compatibility": "NONE"})[0] == 403)
        check("percent-encoded form too", writer.put("/config/%3A.eu%3A", {"compatibility": "NONE"})[0] == 403)
        check("and the mode with it", writer.put("/mode/%3A.eu%3A", {"mode": "READONLY"})[0] == 403)
        check("the context's settings are untouched",
              root.get("/config/:.eu:")[1].get("compatibilityLevel") != "NONE", root.get("/config/:.eu:"))

        print("a reader cannot register through a path that looks read-only")
        check("tags is a write", plain.post("/subjects/abc-1/versions/1/tags", {"tagsToAdd": {"R.a": ["PII"]}})[0] == 403)
        check("abc-1 still has one version", root.get("/subjects/abc-1/versions")[1] == [1],
              root.get("/subjects/abc-1/versions"))

        print("a hashed password from `hash-password` is what the server accepts")
        # This is how auth is set up on the deployed instance: the CLI prints a
        # bcrypt hash, the hash goes in the file. Nothing connected the two
        # ends, so a change to the emitted cost or prefix would lock it out
        # while every test stayed green.
        hashed = subprocess.run([BIN, "hash-password", "s3cret"], capture_output=True, text=True, timeout=60)
        check("hash-password succeeds", hashed.returncode == 0, hashed.stderr[-200:])
        digest = hashed.stdout.strip().splitlines()[-1].strip()
        check("and prints a bcrypt hash", digest.startswith("$2"), digest[:20])
        hcfg = os.path.join(tmp, "hashed.toml")
        hport = free_port()
        with open(hcfg, "w") as f:
            f.write(f'listen = "127.0.0.1:{hport}"\ndata_dir = "{os.path.join(tmp, "data3")}"\n\n'
                    f'[auth]\nenabled = true\n\n[[auth.users]]\nusername = "hashed"\n'
                    f'password = "{digest}"\nroles = ["admin"]\n')
        hproc = subprocess.Popen([BIN, "--config", hcfg], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            hurl = f"http://127.0.0.1:{hport}"
            for _ in range(200):
                try:
                    requests.get(hurl, timeout=0.2)
                    break
                except requests.ConnectionError:
                    time.sleep(0.05)
            check("the hashed password logs in", Client(hurl, auth=("hashed", "s3cret")).get("/subjects")[0] == 200)
            check("and a wrong one does not", Client(hurl, auth=("hashed", "wrong")).get("/subjects")[0] == 401)
        finally:
            hproc.terminate()
            try:
                hproc.wait(5)
            except subprocess.TimeoutExpired:
                hproc.kill()

        print("a bad pattern is a startup error, not a silent grant")
        bad = os.path.join(tmp, "bad.toml")
        with open(bad, "w") as f:
            f.write(CONFIG.format(port=free_port(), data=os.path.join(tmp, "data2")).replace('".eu::*"', '".eu::"'))
        r = subprocess.run([BIN, "--config", bad], capture_output=True, text=True, timeout=30)
        check("invalid pattern refuses to start", r.returncode != 0 and "pattern" in (r.stderr + r.stdout),
              r.stderr[-200:])
    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()
        if not FAILURES:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print(f"logs kept in {tmp}")

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FAILED: {FAILURES}")
        sys.exit(1)
    print("all role-binding checks passed")


if __name__ == "__main__":
    main()
