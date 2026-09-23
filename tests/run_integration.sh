#!/usr/bin/env bash
# Run every integration suite against a freshly built server with Basic auth on.
#
#   tests/run_integration.sh                          # build + start our server, run all suites
#   tests/run_integration.sh --against URL [user:pw]  # run the client suites against any registry
#                                                     # (e.g. a real Confluent, to validate the tests)
#
# Suites:
#   e2e        tests/e2e.py              HTTP API: auth, deletes, refs, contexts, import, exporter, restart
#   exporter   tests/exporter.py        schema linking between two instances: every context mapping,
#                                        renaming, replayed deletes, error/resume
#   migrate    tests/migrate.py         `schema-registry migrate` copying a whole registry
#   rbac       tests/rbac.py            roles and role bindings: what each user may do, and is shown
#   cli        tests/exporter_cli.sh    the official `confluent` CLI driving the exporter API
#   tools      tests/confluent_tools.sh Confluent's own console producers/consumers and load tool,
#                                        over a real Kafka (needs CONFLUENT_HOME)
#   python     tests/confluent_client.py official confluent-kafka client: Avro/JSON/Protobuf serde,
#                                        references, evolution, deletes
#   java       tests/java                official Java client + serializers: Avro/JSON/Protobuf serde,
#                                        references, evolution, production serializer mode, contexts
#
# Prerequisites: cargo, python3 (a venv with confluent-kafka is created in tests/.venv),
# Maven + JDK 17 for the Java suite, the `confluent` CLI (CONFLUENT_CLI or on PATH)
# and a Confluent Platform directory (CONFLUENT_HOME). Missing tools skip that suite.
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
R_e2e="not run"; R_exporter="not run"; R_python="not run"; R_java="not run"
R_cli="not run"; R_tools="not run"; R_migrate="not run"; R_rbac="not run"
EXTERNAL_URL=""
EXTERNAL_AUTH=""
if [[ "${1:-}" == "--against" ]]; then
  EXTERNAL_URL="${2:?URL required}"
  EXTERNAL_AUTH="${3:-}"
fi

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

if [[ -z "$EXTERNAL_URL" ]]; then
  say "build"
  cargo build --quiet || { echo "build failed"; exit 1; }

  say "e2e (tests/e2e.py starts its own servers)"
  if python3 -c "import requests" 2>/dev/null; then
    python3 tests/e2e.py && R_e2e=pass || R_e2e=FAIL
  else
    echo "python3 'requests' not installed"; R_e2e=skipped
  fi

  say "exporter (starts a source and a destination)"
  if python3 -c "import requests" 2>/dev/null; then
    (cd tests && python3 exporter.py) && R_exporter=pass || R_exporter=FAIL
  else
    R_exporter=skipped
  fi

  say "migrate (a full copy between two instances)"
  if python3 -c "import requests" 2>/dev/null; then
    (cd tests && python3 migrate.py) && R_migrate=pass || R_migrate=FAIL
  else
    R_migrate=skipped
  fi

  say "rbac (roles, bindings and filtered listings)"
  if python3 -c "import requests" 2>/dev/null; then
    (cd tests && python3 rbac.py) && R_rbac=pass || R_rbac=FAIL
  else
    R_rbac=skipped
  fi

  say "confluent CLI exporter (skipped without the CLI)"
  out="$(tests/exporter_cli.sh 2>&1)"; rc=$?
  echo "$out" | tail -25
  case "$out" in *"skipped:"*) R_cli=skipped;; *) [[ $rc -eq 0 ]] && R_cli=pass || R_cli=FAIL;; esac

  say "confluent console tools (skipped without CONFLUENT_HOME)"
  out="$(tests/confluent_tools.sh 2>&1)"; rc=$?
  echo "$out" | tail -15
  case "$out" in *"skipped:"*) R_tools=skipped;; *) [[ $rc -eq 0 ]] && R_tools=pass || R_tools=FAIL;; esac

  WORK="$(mktemp -d)"
  PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')
  cat > "$WORK/config.toml" <<EOF
listen = "127.0.0.1:$PORT"
data_dir = "$WORK/data"
[auth]
enabled = true
[[auth.users]]
username = "it"
password = "it-secret"
roles = ["admin"]
EOF
  ./target/debug/schema-registry --config "$WORK/config.toml" > "$WORK/server.log" 2>&1 &
  SERVER_PID=$!
  trap 'kill $SERVER_PID 2>/dev/null; rm -rf "$WORK"' EXIT
  for _ in $(seq 100); do curl -s -o /dev/null "http://127.0.0.1:$PORT" && break; sleep 0.1; done
  URL="http://127.0.0.1:$PORT"
  AUTH="it:it-secret"
  echo "server on $URL (basic auth on), log: $WORK/server.log"
else
  URL="$EXTERNAL_URL"
  AUTH="$EXTERNAL_AUTH"
fi

say "python client (confluent-kafka)"
VENV="$ROOT/tests/.venv"
if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV" && "$VENV/bin/pip" install -q "confluent-kafka[avro,schemaregistry,json,protobuf]" requests \
    || { echo "could not create venv"; rm -rf "$VENV"; }
fi
if [[ -x "$VENV/bin/python" ]]; then
  "$VENV/bin/python" tests/confluent_client.py "$URL" ${AUTH:+"$AUTH"} && R_python=pass || R_python=FAIL
else
  R_python=skipped
fi

say "java client (kafka-*-serializer 7.9)"
if command -v mvn >/dev/null && command -v java >/dev/null; then
  if (cd tests/java && mvn -q compile dependency:build-classpath -Dmdep.outputFile=cp.txt); then
    OUT="$(mktemp)"
    if java -cp "tests/java/target/classes:$(cat tests/java/cp.txt)" t.Main "$URL" ${AUTH:+"$AUTH"} > "$OUT" 2>&1; then R_java=pass; else R_java=FAIL; fi
    grep -v '^SLF4J' "$OUT"; rm -f "$OUT"
  else
    R_java=FAIL
  fi
else
  echo "mvn/java not found"; R_java=skipped
fi

say "summary"
status=0
for pair in "e2e:$R_e2e" "exporter:$R_exporter" "migrate:$R_migrate" "rbac:$R_rbac" "cli:$R_cli" "tools:$R_tools" "python:$R_python" "java:$R_java"; do
  printf '  %-8s %s\n' "${pair%%:*}" "${pair#*:}"
  [[ "${pair#*:}" == "FAIL" ]] && status=1
done
exit $status
