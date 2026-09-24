#!/usr/bin/env python3
"""Host containers: several registries in one process, sharing one store.

    python3 tests/containers.py [path/to/schema-registry]

Two containers reached by `Host`, on one port and one data directory. Checks
that they share nothing a client can observe - subjects, ids, versions, config,
modes, contexts, the admin view - that a user bound to one cannot use the other
whatever Host it claims, and that an unknown host is refused rather than served
by whichever container happens to be first.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import requests

BIN = sys.argv[1] if len(sys.argv) > 1 else "./target/release/schema-registry"
CT = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
AVRO = json.dumps({"type": "record", "name": "R", "fields": [{"name": "a", "type": "int"}]})
AVRO2 = json.dumps({"type": "record", "name": "R", "fields": [{"name": "b", "type": "string"}]})

CONFIG = """
listen = "127.0.0.1:{port}"
data_dir = "{data}"

[[containers]]
name = "prod"
hosts = ["sr.example.com", "sr.example.com:{port}"]

[[containers]]
name = "dev"
hosts = ["*.dev.example.com"]

[auth]
enabled = true

[[auth.users]]
username = "root"
password = "root-secret"
roles = ["admin"]

# Bound to one container: the Host header cannot get it into the other.
[[auth.users]]
username = "prod-only"
password = "prod-secret"
roles = ["admin"]
containers = ["prod"]
"""

FAILURES = []


def same_schema(got, want):
    """Schemas come back canonicalized, so compare what they mean."""
    try:
        return json.loads(got) == json.loads(want)
    except (TypeError, ValueError):
        return False


def check(label, cond, detail=""):
    print(("  ok   " if cond else "  FAIL ") + label + ("" if cond else f"  {detail}"))
    if not cond:
        FAILURES.append(label)


def free_port():
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Client:
    """One host's view of the server."""

    def __init__(self, url, host, auth):
        self.url, self.host, self.auth = url, host, auth

    def req(self, method, path, body=None, **params):
        r = requests.request(method, self.url + path, headers={**CT, "Host": self.host}, auth=self.auth,
                             params=params or None, data=json.dumps(body) if body is not None else None)
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


def main():
    tmp = tempfile.mkdtemp(prefix="sr-containers-")
    proc = None
    try:
        port = free_port()
        cfg = os.path.join(tmp, "c.toml")
        with open(cfg, "w") as f:
            f.write(CONFIG.format(port=port, data=os.path.join(tmp, "data")))
        log = open(os.path.join(tmp, "log"), "w")
        proc = subprocess.Popen([BIN, "--config", cfg], stdout=log, stderr=subprocess.STDOUT)
        url = f"http://127.0.0.1:{port}"
        for _ in range(100):
            try:
                requests.get(url, headers={"Host": "sr.example.com"}, timeout=0.2)
                break
            except requests.ConnectionError:
                time.sleep(0.05)

        root = ("root", "root-secret")
        prod = Client(url, "sr.example.com", root)
        dev = Client(url, "a.dev.example.com", root)

        print("the same names, in both, with their own ids")
        check("prod registers", prod.post("/subjects/orders-value/versions", {"schema": AVRO}) == (200, {"id": 1}))
        check("dev registers the same subject name",
              dev.post("/subjects/orders-value/versions", {"schema": AVRO2}) == (200, {"id": 1}),
              "each container has its own id space, so this is id 1 as well")
        check("prod still has its own schema",
              same_schema(prod.get("/schemas/ids/1")[1].get("schema"), AVRO), prod.get("/schemas/ids/1"))
        check("dev has its own", same_schema(dev.get("/schemas/ids/1")[1].get("schema"), AVRO2),
              dev.get("/schemas/ids/1"))

        print("nothing one container holds is visible in the other")
        check("prod only sees its subject", prod.get("/subjects", subjectPrefix=":*:")[1] == ["orders-value"])
        dev.post("/subjects/:.eu:only-in-dev/versions", {"schema": AVRO})
        check("a context exists only where it was made",
              sorted(dev.get("/contexts")[1]) == [".", ".eu"] and prod.get("/contexts")[1] == ["."],
              f"{dev.get('/contexts')[1]} vs {prod.get('/contexts')[1]}")
        check("prod cannot read dev's subject", prod.get("/subjects/:.eu:only-in-dev/versions")[0] == 404)

        print("settings are per container too")
        check("prod global config", prod.put("/config", {"compatibility": "NONE"})[0] == 200)
        check("dev's global config is untouched",
              dev.get("/config")[1].get("compatibilityLevel") == "BACKWARD", dev.get("/config"))
        check("prod mode", prod.put("/mode", {"mode": "READONLY"})[0] == 200)
        check("dev still writes while prod is read-only",
              dev.post("/subjects/dev-only/versions", {"schema": AVRO})[0] == 200)
        check("prod does not", prod.post("/subjects/blocked/versions", {"schema": AVRO})[0] == 422)
        prod.put("/mode", {"mode": "READWRITE"})

        print("the admin view shows one container")
        ov = prod.get("/admin/api/overview", deleted="true")[1]
        check("admin lists only this container's subjects",
              sorted(r["subject"] for r in ov["subjects"]) == ["orders-value"],
              [r["subject"] for r in ov["subjects"]])
        check("and reports which container it is", ov.get("container") == "prod", ov.get("container"))

        print("a Host header is routing, not permission")
        bound = Client(url, "sr.example.com", ("prod-only", "prod-secret"))
        check("the user works in its own container", bound.get("/subjects")[0] == 200)
        elsewhere = Client(url, "a.dev.example.com", ("prod-only", "prod-secret"))
        check("and is refused in the other, Host or no Host", elsewhere.get("/subjects")[0] == 403,
              elsewhere.get("/subjects"))

        print("an unknown host is refused, not served by whichever is first")
        other = Client(url, "not.configured.example.com", root)
        code, body = other.get("/subjects")
        check("unknown host is 421", code == 421, (code, body))
        check("and says so", "container" in str(body), body)

        print("restarting keeps each container's data apart")
        proc.terminate()
        proc.wait(5)
        proc = subprocess.Popen([BIN, "--config", cfg], stdout=log, stderr=subprocess.STDOUT)
        for _ in range(100):
            try:
                requests.get(url, headers={"Host": "sr.example.com"}, timeout=0.2)
                break
            except requests.ConnectionError:
                time.sleep(0.05)
        check("prod after restart", same_schema(prod.get("/schemas/ids/1")[1].get("schema"), AVRO))
        check("dev after restart", same_schema(dev.get("/schemas/ids/1")[1].get("schema"), AVRO2))
        check("dev's context survived", sorted(dev.get("/contexts")[1]) == [".", ".eu"])
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
    print("all host container checks passed")


if __name__ == "__main__":
    main()
