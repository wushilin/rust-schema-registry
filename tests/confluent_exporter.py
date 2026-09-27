#!/usr/bin/env python3
"""Schema linking with a real Confluent on the other end, both directions.

    CONFLUENT_URL=http://confluent:8081 python3 tests/confluent_exporter.py [path/to/schema-registry]

Optional:
    CONFLUENT_AUTH=user:password    Basic auth for that Confluent
    SELF_URL=http://10.0.0.5:8081   what Confluent should call us (for the
                                    Confluent -> us direction; defaults to this
                                    machine's LAN address, which Confluent has
                                    to be able to reach)

Skips without CONFLUENT_URL: the gate cannot assume a Confluent is around.

Two directions, because they fail differently:

* **us -> Confluent** exercises our exporter against a destination we did not
  write: it has to accept our IMPORT-mode writes, our id and version numbers,
  and our rewritten references.
* **Confluent -> us** exercises us as a *destination* for Confluent's own
  schema exporter, which is what a migration onto this registry looks like.
  Exporters are a Confluent Platform feature, so this half skips against a
  community build (no `/exporters`).

Everything is created under contexts named after this process and removed
afterwards, so it can be pointed at a shared Confluent without eating anything.
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

from e2e import BIN, CT, FAILURES, Client, check

CONFLUENT = os.environ.get("CONFLUENT_URL", "").rstrip("/")
if not CONFLUENT:
    print("skipped: no CONFLUENT_URL (point it at a Confluent 7.9 to run this)")
    sys.exit(0)

AUTH = tuple(os.environ["CONFLUENT_AUTH"].split(":", 1)) if os.environ.get("CONFLUENT_AUTH") else None
MARK = f"sr-it-{os.getpid()}"
OURS_AT_CONFLUENT = f".{MARK}-from-us"
THEIRS_AT_OURS = f".{MARK}-from-them"

AVRO_ADDRESS = json.dumps({"type": "record", "name": "Address", "namespace": "com.x",
                           "fields": [{"name": "street", "type": "string"}]})
AVRO_PERSON = json.dumps({"type": "record", "name": "Person", "namespace": "com.x",
                          "fields": [{"name": "home", "type": "com.x.Address"}]})


def until(fn, timeout=30):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = fn()
        if last:
            return last
        time.sleep(0.25)
    return last


def lan_address():
    """An address Confluent can dial back on. Guessed, and overridable."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


def free_port():
    s = socket.socket()
    s.bind(("0.0.0.0", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Confluent:
    """Just enough of a client, with the subjects we created remembered."""

    def __init__(self, url):
        self.url = url
        self.s = requests.Session()
        if AUTH:
            self.s.auth = AUTH

    def req(self, method, path, body=None, **params):
        r = self.s.request(method, self.url + path, headers=CT, params=params or None,
                           data=json.dumps(body) if body is not None else None, timeout=20)
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


def wipe_context(client, ctx):
    """Remove every subject in one context, and the context itself if we can."""
    for subject in client.get("/subjects", subjectPrefix=f":{ctx}:", deleted="true")[1] or []:
        if not isinstance(subject, str):
            continue
        client.put(f"/mode/{subject}", {"mode": "READWRITE"}, force="true")
        client.delete(f"/subjects/{subject}")
        client.delete(f"/subjects/{subject}", permanent="true")
    client.put(f"/mode/:{ctx}:", {"mode": "READWRITE"}, force="true")
    client.delete(f"/contexts/{ctx}")


def start_ours(tmp):
    """Our registry, reachable from outside so Confluent can export into it."""
    port = free_port()
    cfg = os.path.join(tmp, "ours.toml")
    with open(cfg, "w") as f:
        f.write(f'listen = "0.0.0.0:{port}"\ndata_dir = "{tmp}/data"\nlog_reads = true\n')
    log = open(os.path.join(tmp, "ours.log"), "w")
    proc = subprocess.Popen([BIN, "--config", cfg], stdout=log, stderr=subprocess.STDOUT)
    local = f"http://127.0.0.1:{port}"
    for _ in range(200):
        try:
            requests.get(local, timeout=0.3)
            break
        except requests.ConnectionError:
            time.sleep(0.05)
    advertised = os.environ.get("SELF_URL") or f"http://{lan_address()}:{port}"
    return proc, local, advertised


def main():
    tmp = tempfile.mkdtemp(prefix="sr-confluent-")
    proc = None
    cf = Confluent(CONFLUENT)
    try:
        code, body = cf.get("/subjects")
        if code != 200:
            print(f"skipped: {CONFLUENT} did not answer GET /subjects ({code}: {body})")
            return
        print(f"confluent at {CONFLUENT}")

        proc, ours_local, ours_advertised = start_ours(tmp)
        us = Client(ours_local)
        print(f"ours on {ours_local} (advertised to confluent as {ours_advertised})")

        print("us -> confluent")
        check("we register the referenced schema",
              us.post("/subjects/address-value/versions", {"schema": AVRO_ADDRESS})[0] == 200)
        src_address_id = us.get("/subjects/address-value/versions/1")[1]["id"]
        check("and one that references it",
              us.post("/subjects/person-value/versions",
                      {"schema": AVRO_PERSON,
                       "references": [{"name": "com.x.Address", "subject": "address-value", "version": 1}]})[0] == 200)
        src_person_id = us.get("/subjects/person-value/versions/1")[1]["id"]

        body = {"name": "to-confluent", "subjects": ["*"], "contextType": "CUSTOM",
                "context": OURS_AT_CONFLUENT, "config": {"schema.registry.url": CONFLUENT}}
        if AUTH:
            body["config"]["basic.auth.user.info"] = ":".join(AUTH)
        r = us.post("/exporters", body)
        check("create the exporter", r == (200, {"name": "to-confluent"}), r)

        dst_address = f":{OURS_AT_CONFLUENT}:address-value"
        dst_person = f":{OURS_AT_CONFLUENT}:person-value"
        check("the schema arrives at confluent",
              until(lambda: cf.get(f"/subjects/{dst_address}/versions")[0] == 200),
              us.get("/exporters/to-confluent/status"))
        check("confluent kept our id",
              cf.get(f"/subjects/{dst_address}/versions/1")[1].get("id") == src_address_id,
              cf.get(f"/subjects/{dst_address}/versions/1"))
        check("the referencing schema arrives too",
              until(lambda: cf.get(f"/subjects/{dst_person}/versions")[0] == 200),
              us.get("/exporters/to-confluent/status"))
        landed = cf.get(f"/subjects/{dst_person}/versions/1")[1]
        check("with our id", landed.get("id") == src_person_id, landed)
        check("and its reference rewritten into that context",
              landed.get("references", [{}])[0].get("subject") == dst_address, landed)
        check("confluent left the context in IMPORT mode",
              cf.get(f"/mode/:{OURS_AT_CONFLUENT}:")[1].get("mode") == "IMPORT",
              cf.get(f"/mode/:{OURS_AT_CONFLUENT}:"))
        check("our exporter is running, not paused",
              us.get("/exporters/to-confluent/status")[1]["state"] == "RUNNING",
              us.get("/exporters/to-confluent/status"))

        print("confluent -> us")
        exporters = cf.get("/exporters")
        if exporters[0] != 200:
            print(f"    skipped: this confluent has no /exporters ({exporters[0]}) - "
                  "schema linking is a Confluent Platform feature")
        else:
            check("confluent has a subject of its own to send",
                  cf.post(f"/subjects/{MARK}-theirs-value/versions", {"schema": AVRO_ADDRESS})[0] == 200)
            their_id = cf.get(f"/subjects/{MARK}-theirs-value/versions/1")[1]["id"]
            r = cf.post("/exporters", {"name": MARK, "subjects": [f"{MARK}-theirs-*"],
                                       "contextType": "CUSTOM", "context": THEIRS_AT_OURS,
                                       "config": {"schema.registry.url": ours_advertised}})
            check("confluent accepts the exporter", r[0] == 200, r)
            arrived = f":{THEIRS_AT_OURS}:{MARK}-theirs-value"
            check("it reaches us",
                  until(lambda: us.get(f"/subjects/{arrived}/versions")[0] == 200),
                  (cf.get(f"/exporters/{MARK}/status"), us.get("/subjects", subjectPrefix=":*:")))
            check("we kept confluent's id",
                  us.get(f"/subjects/{arrived}/versions/1")[1].get("id") == their_id,
                  us.get(f"/subjects/{arrived}/versions/1"))
            check("we put the destination context in IMPORT mode for it",
                  us.get(f"/mode/:{THEIRS_AT_OURS}:")[1].get("mode") == "IMPORT",
                  us.get(f"/mode/:{THEIRS_AT_OURS}:"))
            check("confluent reports it running",
                  cf.get(f"/exporters/{MARK}/status")[1].get("state") in ("RUNNING", "STARTING"),
                  cf.get(f"/exporters/{MARK}/status"))
    finally:
        # Confluent is someone else's: take back exactly what we put there.
        try:
            if cf.get("/exporters")[0] == 200:
                for name in cf.get("/exporters")[1] or []:
                    if isinstance(name, str) and name.startswith(MARK):
                        cf.put(f"/exporters/{name}/pause", {})
                        cf.delete(f"/exporters/{name}")
            wipe_context(cf, OURS_AT_CONFLUENT)
            for subject in cf.get("/subjects", deleted="true")[1] or []:
                if isinstance(subject, str) and subject.startswith(MARK):
                    cf.delete(f"/subjects/{subject}")
                    cf.delete(f"/subjects/{subject}", permanent="true")
        except requests.RequestException as e:
            print(f"    cleanup at confluent failed, look for {MARK}: {e}")
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
    print("all confluent exporter checks passed")


if __name__ == "__main__":
    main()
