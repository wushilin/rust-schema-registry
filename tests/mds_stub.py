#!/usr/bin/env python3
"""Minimal stand-in for Confluent's Metadata Service, so the Confluent CLI can
"log in" to a local Confluent Platform and then talk to our registry."""
import base64, hashlib, hmac, json, sys, time
from http.server import BaseHTTPRequestHandler, HTTPServer

def b64(obj):
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()


# The CLI decodes the token as a JWT to read its claims.
def jwt():
    head = b64({"alg": "HS256", "typ": "JWT"})
    body = b64({"sub": "admin", "exp": int(time.time()) + 36000, "iat": int(time.time()), "iss": "mds-stub"})
    sig = hmac.new(b"stub", f"{head}.{body}".encode(), hashlib.sha256).digest()
    return f"{head}.{body}." + base64.urlsafe_b64encode(sig).rstrip(b"=").decode()


TOKEN = jwt()

class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        print("   MDS", self.command, self.path, file=sys.stderr)

    def reply(self, code, body):
        raw = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.startswith("/security/1.0/authenticate"):
            return self.reply(200, {"auth_token": TOKEN, "token_type": "Bearer", "expires_in": 3600})
        if "/security/1.0/" in self.path:
            return self.reply(200, [])
        self.reply(404, {"error": "not found"})

    def do_POST(self):
        self.do_GET()

if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), H).serve_forever()
