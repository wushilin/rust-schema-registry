#!/usr/bin/env python3
"""The PROXY protocol, and what the log and the metrics make of it.

    python3 tests/proxy.py [path/to/schema-registry]

Drives real sockets: a direct connection, a v1 header, a v2 header, a v2
health check, and a client on an untrusted address claiming to be a proxy.
Checks the address each one ends up logged under, and that `/metrics` counts
what happened without naming any subject.
"""

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from e2e import FAILURES, check, free_port, resolve_binary

BIN = resolve_binary()
V2_SIG = b"\r\n\r\n\x00\r\nQUIT\n"


def start(tmp, name, trust):
    port = free_port()
    cfg = os.path.join(tmp, f"{name}.toml")
    with open(cfg, "w") as f:
        f.write(f'listen = "127.0.0.1:{port}"\ndata_dir = "{tmp}/{name}"\n'
                f'proxy_trust = "{trust}"\nlog_reads = true\nlog_format = "json"\n')
    log = open(os.path.join(tmp, f"{name}.log"), "w")
    proc = subprocess.Popen([BIN, "--config", cfg], stdout=log, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            request(port)
            return proc, port, os.path.join(tmp, f"{name}.log")
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    raise RuntimeError(f"{name} did not start")


def request(port, prefix=b"", path="/subjects"):
    """One request, optionally behind some first bytes of our choosing."""
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.sendall(prefix + f"GET {path} HTTP/1.1\r\nHost: sr.example.com\r\nConnection: close\r\n\r\n".encode())
    data = b""
    try:
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
    except ConnectionResetError:
        # A rejected request may be answered and closed before we finish
        # sending, which arrives as a reset. Status 0 means "refused".
        pass
    s.close()
    head, _, body = data.partition(b"\r\n\r\n")
    status = int(head.split(b" ")[1]) if head.startswith(b"HTTP/") else 0
    return status, body.decode(errors="replace")


def v1(src="198.51.100.7", sport=56324):
    return f"PROXY TCP4 {src} 10.0.0.2 {sport} 8081\r\n".encode()


def v2(src=(203, 0, 113, 9), sport=4711, command=0x21, family=0x11):
    body = bytes(src) + bytes([10, 0, 0, 2]) + sport.to_bytes(2, "big") + (8081).to_bytes(2, "big")
    return V2_SIG + bytes([command, family]) + len(body).to_bytes(2, "big") + body


def clients(log_path):
    """Every client address the server logged, in order."""
    out = []
    with open(log_path) as f:
        for line in f:
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if rec.get("message") == "request" and rec.get("client"):
                out.append(rec["client"])
    return out


def main():
    tmp = tempfile.mkdtemp(prefix="sr-proxy-")
    procs = []
    try:
        # Loopback is trusted here, so our headers are believed.
        proc, port, log = start(tmp, "trusting", "127.0.0.1/32;192.168.44.0/24")
        procs.append(proc)

        print("a header from a trusted peer names the client")
        check("direct connection works", request(port)[0] == 200)
        check("v1 works", request(port, v1())[0] == 200)
        check("v2 works", request(port, v2())[0] == 200)
        # v2 LOCAL: the proxy's own health check, speaking for nobody.
        check("v2 LOCAL works", request(port, v2(command=0x20, family=0x00))[0] == 200)
        time.sleep(0.4)
        seen = clients(log)
        check("direct is logged as the socket", any(c.startswith("127.0.0.1:") for c in seen), seen)
        check("v1 is logged as the header said", any(c == "198.51.100.7:56324" for c in seen), seen)
        check("v2 is logged as the header said", any(c == "203.0.113.9:4711" for c in seen), seen)
        check("a LOCAL header falls back to the socket",
              sum(1 for c in seen if c.startswith("127.0.0.1:")) >= 2, seen)

        print("metrics count what happened, and name no subject")
        status, body = request(port, path="/metrics")
        check("metrics responds", status == 200, status)
        check("requests are counted", 'sr_requests_total{method="GET",route="/subjects"' in body.replace(",container", "\x00,container").replace("\x00", ""),
              [l for l in body.splitlines() if "sr_requests_total" in l][:3])
        check("durations are recorded", "sr_request_duration_seconds_count{" in body)
        check("gauges are there", "sr_subjects{container=\"default\"}" in body and "sr_uptime_seconds" in body,
              [l for l in body.splitlines() if l.startswith("sr_subjects") or l.startswith("sr_uptime")][:3])
        # Register something, then check the mutation was counted by verb.
        s = socket.create_connection(("127.0.0.1", port), timeout=5)
        payload = json.dumps({"schema": '"string"'}).encode()
        s.sendall(b"POST /subjects/proxy-value/versions HTTP/1.1\r\nHost: sr.example.com\r\n"
                  b"Content-Type: application/vnd.schemaregistry.v1+json\r\n"
                  + f"Content-Length: {len(payload)}\r\n".encode() + b"Connection: close\r\n\r\n" + payload)
        s.recv(4096)
        s.close()
        _, body = request(port, path="/metrics")
        check("mutations are counted by verb",
              'sr_mutations_total{verb="RegisterSchema",container="default",outcome="ok"} 1' in body,
              [l for l in body.splitlines() if "sr_mutations_total" in l][:3])
        check("no subject name leaks into a label", "proxy-value" not in body,
              [l for l in body.splitlines() if "proxy-value" in l][:2])
        check("the route label is a shape",
              'route="/subjects/*/versions"' in body, [l for l in body.splitlines() if "versions" in l][:2])

        print("a claim from an untrusted address is not believed")
        proc2, port2, log2 = start(tmp, "strict", "192.168.44.0/24")
        procs.append(proc2)
        check("direct still works", request(port2)[0] == 200)
        status, _ = request(port2, v1())
        # The bytes stay in the stream and are parsed as the HTTP they claimed
        # not to be, so the request is refused - as a 400, or as a reset if the
        # server closes before we have finished sending.
        check("a spoofed v1 header is refused", status in (400, 0),
              f"expected the claim to be treated as payload, got {status}")
        time.sleep(0.4)
        seen2 = clients(log2)
        check("and nothing was logged under the claimed address",
              all(c != "198.51.100.7:56324" for c in seen2), seen2)
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
    print("all proxy protocol and metrics checks passed")


if __name__ == "__main__":
    main()
