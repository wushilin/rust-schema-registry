#!/usr/bin/env python3
"""Replay scenarios.json against a live registry.

    run.py record URL [user:pw]     # write golden.json (point this at Confluent 7.9)
    run.py check  URL [user:pw]     # diff a live server against golden.json
    run.py check  URL --only NAME   # one scenario

The normalization here must match src/http_conformance_tests.rs.
"""

import base64
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SYM = re.compile(r"\{\$id(\d+)\}")
# Ids inside error messages.
MSG_ID = re.compile(r"(Schema |with id |input id )(-?\d+)")
CT = "application/vnd.schemaregistry.v1+json"


def context_of(subject):
    """Confluent's QualifiedSubject context of a (decoded) subject."""
    if subject.startswith(":."):
        rest = subject[2:]
        i = rest.find(":")
        return subject[1:2 + i] if i >= 0 else subject[1:]
    return "."


def step_context(path):
    """The context a step's ids live in: from /contexts/{ctx}/..., the subject
    after /subjects/, or the `subject` query parameter."""
    p, _, q = path.partition("?")
    segs = p.strip("/").split("/")
    if len(segs) > 1 and segs[0] == "contexts":
        c = urllib.parse.unquote(segs[1])
        return c if c.startswith(".") else "." + c
    for i, seg in enumerate(segs[:-1]):
        if seg == "subjects":
            return context_of(urllib.parse.unquote(segs[i + 1]))
    for kv in q.split("&"):
        k, _, v = kv.partition("=")
        if k == "subject":
            return context_of(urllib.parse.unquote(v))
    return "."


class Ids:
    """(context, server id) <-> "$idN" in first-seen order, per scenario."""

    def __init__(self):
        self.to_sym, self.from_sym = {}, {}
        self.ctx = "."

    def sym(self, real, ctx=None):
        key = (ctx or self.ctx, real)
        if key not in self.to_sym:
            n = len(self.to_sym) + 1
            self.to_sym[key] = f"$id{n}"
            self.from_sym[str(n)] = real
        return self.to_sym[key]

    def subst_path(self, p):
        return SYM.sub(lambda m: str(self.from_sym.get(m.group(1), "{$id" + m.group(1) + "}")), p)

    def subst_body(self, v):
        if isinstance(v, str):
            m = SYM.fullmatch(v)
            if m and m.group(1) in self.from_sym:
                return self.from_sym[m.group(1)]
            return v
        if isinstance(v, list):
            return [self.subst_body(x) for x in v]
        if isinstance(v, dict):
            return {k: self.subst_body(x) for k, x in v.items()}
        return v


def keep_filter(body, keep):
    def ok(s):
        return any(s == k[1:] if k.startswith("=") else s.startswith(k) for k in keep)
    if isinstance(body, list):
        return [x for x in body if ok(x if isinstance(x, str) else x.get("subject", "") if isinstance(x, dict) else "")]
    return body


def symbolize(body, ids, id_list):
    if id_list and isinstance(body, list):
        return [ids.sym(x) if isinstance(x, int) and not isinstance(x, bool) else x for x in body]

    def walk(v, ctx):
        if isinstance(v, dict):
            if isinstance(v.get("subject"), str):
                ctx = context_of(v["subject"])
            return {k: (ids.sym(x, ctx) if k in ("id", "maxId") and isinstance(x, int) and not isinstance(x, bool)
                        else MSG_ID.sub(lambda m: m.group(1) + ids.sym(int(m.group(2)), ctx), x)
                        if k == "message" and isinstance(x, str) else walk(x, ctx)) for k, x in v.items()}
        if isinstance(v, list):
            return [walk(x, ctx) for x in v]
        return v
    return walk(body, ids.ctx)


def send(base, auth, st, ids):
    ids.ctx = step_context(st["p"])
    path = ids.subst_path(st["p"])
    data = None
    if "raw" in st:
        data = st["raw"].encode()
    elif "b" in st:
        data = json.dumps(ids.subst_body(st["b"])).encode()
    req = urllib.request.Request(base + path, data=data, method=st["m"])
    ct = st.get("ct", CT)
    if data is not None and ct:
        req.add_header("Content-Type", ct)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    text = raw.decode("utf-8", "replace")
    try:
        body = json.loads(text) if text.strip() else {"text": text}
    except ValueError:
        body = {"text": text}
    if "keep" in st:
        body = keep_filter(body, st["keep"])
    return {"status": status, "body": symbolize(body, ids, st.get("ids", False))}


def call(base, auth, m, p, body=None):
    st = {"m": m, "p": p}
    if body is not None:
        st["b"] = body
    return send(base, auth, st, Ids())


def reset(base, auth):
    """Bring a registry back to an empty state (ids keep counting, which the
    symbolic id comparison doesn't see). Contexts are never removed by
    Confluent; steps listing them filter by scenario prefix."""
    q = urllib.parse.quote
    call(base, auth, "PUT", "/mode?force=true", {"mode": "READWRITE"})
    call(base, auth, "DELETE", "/config")
    for c in call(base, auth, "GET", "/contexts")["body"]:
        if c != ".":
            call(base, auth, "DELETE", f"/mode/{q(':' + c + ':', safe='')}")
            call(base, auth, "DELETE", f"/config/{q(':' + c + ':', safe='')}")
    for _ in range(10):
        subjects = call(base, auth, "GET", "/subjects?subjectPrefix=%3A*%3A&deleted=true")["body"]
        if not subjects:
            return
        for s in subjects:
            call(base, auth, "DELETE", f"/mode/{q(s, safe='')}")
            call(base, auth, "DELETE", f"/subjects/{q(s, safe='')}")
            call(base, auth, "DELETE", f"/subjects/{q(s, safe='')}?permanent=true")
    raise SystemExit(f"could not reset registry, left: {subjects}")


def touched_scopes(sc):
    """Subject/context config and mode paths a scenario writes: they outlive
    subject deletion, so a previous recording could leave them behind."""
    out = set()
    for st in sc["steps"]:
        m = re.match(r"^/(config|mode)/([^/?]+)", st["p"])
        if m:
            out.add(f"/{m.group(1)}/{m.group(2)}")
    return sorted(out)


def run_scenario(base, auth, sc):
    reset(base, auth)
    for p in touched_scopes(sc):
        call(base, auth, "DELETE", p)
    ids = Ids()
    out = [send(base, auth, st, ids) for st in sc["steps"]]
    for st in sc["teardown"]:
        send(base, auth, st, ids)
    return out


def rule(want):
    """Global relaxations; must match `rule` in src/http_conformance_tests.rs."""
    body = want["body"] if isinstance(want["body"], dict) else {}
    msg = body.get("message", "") if isinstance(body.get("message"), str) else ""
    detail = lambda m: "details: com.fasterxml.jackson" in m or "details: Could not parse Protobuf" in m
    if want["status"] == 400 and body.get("error_code") == 400:
        return "code"
    if body.get("error_code") == 500:
        return "skip"
    if detail(msg):
        return "code"
    if any(isinstance(m, str) and detail(m) for m in body.get("messages", []) or []):
        return "verdict"
    return None


def same(st, want, got, relax):
    cmp = st.get("cmp", "exact")
    if relax == "skip":
        return True
    if want["status"] != got["status"]:
        return False
    code = lambda r: r["body"].get("error_code") if isinstance(r["body"], dict) else None
    if relax == "code":
        return code(want) == code(got)
    if relax == "verdict":
        return want["body"].get("is_compatible") == got["body"].get("is_compatible")
    if cmp == "status":
        return True
    if cmp == "keys":
        return sorted(want["body"]) == sorted(got["body"]) if isinstance(want["body"], dict) else True
    return want["body"] == got["body"]


def main():
    mode, base = sys.argv[1], sys.argv[2].rstrip("/")
    args = sys.argv[3:]
    only = None
    if "--only" in args:
        i = args.index("--only")
        only = args[i + 1]
        del args[i:i + 2]
    auth = args[0] if args else None
    scenarios = json.load(open(os.path.join(HERE, "scenarios.json")))
    if only:
        scenarios = [s for s in scenarios if s["name"] == only]
    golden_path = os.path.join(HERE, "golden.json")
    if mode == "record":
        golden = {sc["name"]: run_scenario(base, auth, sc) for sc in scenarios}
        with open(golden_path, "w") as f:
            json.dump(golden, f, indent=1, ensure_ascii=False, sort_keys=True)
            f.write("\n")
        print(f"recorded {sum(len(v) for v in golden.values())} steps from {len(golden)} scenarios")
        return
    golden = json.load(open(golden_path))
    allowed = {e["step"]: e["cmp"] for e in json.load(open(os.path.join(HERE, "allowed.json")))}
    bad = total = relaxed = 0
    for sc in scenarios:
        got = run_scenario(base, auth, sc)
        for i, (st, w, g) in enumerate(zip(sc["steps"], golden[sc["name"]], got)):
            total += 1
            relax = allowed.get(f"{sc['name']}#{i}") or rule(w)
            relaxed += relax is not None and w != g
            if not same(st, w, g, relax):
                bad += 1
                print(f"--- {sc['name']}#{i} {st['m']} {st['p']}")
                if "b" in st or "raw" in st:
                    print(f"    body: {json.dumps(st.get('b', st.get('raw')))[:300]}")
                print(f"    confluent: {w['status']} {json.dumps(w['body'], ensure_ascii=False)[:600]}")
                print(f"    ours:      {g['status']} {json.dumps(g['body'], ensure_ascii=False)[:600]}")
    print(f"{total - bad}/{total} steps match ({relaxed} relaxed by documented rules, see allowed.json)")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
