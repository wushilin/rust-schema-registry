#!/usr/bin/env bash
# Schema linking driven by the official Confluent CLI.
#
#   tests/exporter_cli.sh [/path/to/confluent]      # or set CONFLUENT_CLI
#   curl -sSL https://cnfl.io/cli | sh -s -- -b /tmp/cli latest   # to get one
#
# Starts a source and a destination instance of this registry and drives the
# exporter API with `confluent schema-registry exporter ...`: create, list,
# describe, status, pause, resume, reset, update, configuration and delete,
# checking that the schemas really arrive at the destination.
#
# The CLI insists on a login: for on-prem it authenticates against Confluent's
# Metadata Service, which is a commercial component, so `mds_stub.py` answers
# that handshake. The registry itself runs without auth here, so it ignores the
# token the CLI then sends. The CLI's own config is kept in $WORK (HOME is
# overridden), so a developer's real `confluent` session is untouched.
set -uo pipefail

CLI="${1:-${CONFLUENT_CLI:-$(command -v confluent || true)}}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/schema-registry"

[[ -x "$CLI" ]] || { echo "skipped: no confluent CLI (pass its path or set CONFLUENT_CLI)"; exit 0; }
command -v python3 >/dev/null || { echo "skipped: no python3"; exit 0; }
[[ -x "$BIN" ]] || { echo "build first: cargo build"; exit 1; }

WORK="$(mktemp -d)"
SRC_PORT=39181
DST_PORT=39182
MDS_PORT=39183
SRC="http://127.0.0.1:$SRC_PORT"
DST="http://127.0.0.1:$DST_PORT"
CT='Content-Type: application/vnd.schemaregistry.v1+json'
FAILED=0
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  [[ $FAILED -eq 0 ]] && rm -rf "$WORK" || echo "logs kept in $WORK"
}
trap cleanup EXIT

check() { if [[ "$2" == *"$3"* ]]; then echo "  ok   $1"; else echo "  FAIL $1: expected [$3] in [$2]"; FAILED=1; fi; }

for n in src dst; do
  port=$([[ $n == src ]] && echo $SRC_PORT || echo $DST_PORT)
  printf 'listen = "127.0.0.1:%s"\ndata_dir = "%s/%s-data"\nexporter_poll_seconds = 1\ncluster_id = "%s"\n' \
    "$port" "$WORK" "$n" "$n" > "$WORK/$n.toml"
  "$BIN" --config "$WORK/$n.toml" > "$WORK/$n.log" 2>&1 &
  PIDS+=($!)
done
python3 "$ROOT/tests/mds_stub.py" $MDS_PORT > "$WORK/mds.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 50); do curl -s -o /dev/null "$SRC" && curl -s -o /dev/null "$DST" && break; sleep 0.2; done

export HOME="$WORK/home"
mkdir -p "$HOME"
CONFLUENT_PLATFORM_USERNAME=admin CONFLUENT_PLATFORM_PASSWORD=admin-secret \
  CONFLUENT_PLATFORM_MDS_URL="http://127.0.0.1:$MDS_PORT" "$CLI" login > "$WORK/cli.log" 2>&1
sr() { "$CLI" schema-registry "$@" --schema-registry-endpoint "$SRC" 2>&1; }

curl -s -X POST "$SRC/subjects/orders-value/versions" -H "$CT" \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}' > /dev/null
curl -s -X POST "$SRC/subjects/:.eu:orders-value/versions" -H "$CT" -d '{"schema":"\"string\""}' > /dev/null

echo "== create, list, describe"
check "create" "$(sr exporter create cli-exp --subjects orders-value --context-type custom \
  --context-name .linked --config "schema.registry.url=$DST")" 'Created schema exporter "cli-exp"'
check "list" "$(sr exporter list)" "cli-exp"
described="$(sr exporter describe cli-exp)"
check "describe shows the context type" "$described" "CUSTOM"
check "describe shows the context" "$described" ".linked"
check "describe shows the config" "$described" "schema.registry.url=$DST"

echo "== the schemas arrive at the destination"
for _ in $(seq 50); do
  subjects="$(curl -s "$DST/subjects")"
  [[ "$subjects" == *":.linked:orders-value"* ]] && break
  sleep 0.2
done
check "exported into the custom context" "$subjects" ":.linked:orders-value"
check "id preserved" "$(curl -s "$DST/subjects/:.linked:orders-value/versions/1")" '"id":1'

echo "== status, pause, resume, reset"
check "status is RUNNING" "$(sr exporter status describe cli-exp)" "RUNNING"
check "pause" "$(sr exporter pause cli-exp)" 'Paused schema exporter "cli-exp"'
check "status is PAUSED" "$(sr exporter status describe cli-exp)" "PAUSED"
check "resume" "$(sr exporter resume cli-exp)" 'Resumed schema exporter "cli-exp"'
check "reset" "$(sr exporter reset cli-exp)" 'Reset schema exporter "cli-exp"'
check "configuration" "$(sr exporter configuration describe cli-exp)" "schema.registry.url"

echo "== update and a second exporter (contextType none, renamed subjects)"
check "update subjects" "$(sr exporter update cli-exp --subjects orders-value,other-value)" \
  'Updated schema exporter "cli-exp"'
check "update took effect" "$(sr exporter describe cli-exp)" "other-value"
check "create with none + subject format" "$(sr exporter create eu-exp --subjects ':.eu:*' \
  --context-type none --subject-format 'copy-${subject}' --config "schema.registry.url=$DST")" \
  'Created schema exporter "eu-exp"'
for _ in $(seq 50); do
  eu="$(curl -s "$DST/subjects?subjectPrefix=:.eu:")"
  [[ "$eu" == *"copy-orders-value"* ]] && break
  sleep 0.2
done
check "source context kept, subject renamed" "$eu" ":.eu:copy-orders-value"

echo "== delete"
check "delete" "$(sr exporter delete cli-exp --force)" 'Deleted schema exporter "cli-exp"'
sr exporter delete eu-exp --force > /dev/null
check "none left" "$(sr exporter list)" "None found"

echo
[[ $FAILED -eq 0 ]] && echo "all Confluent CLI exporter checks passed" || echo "FAILURES (see $WORK)"
exit $FAILED
