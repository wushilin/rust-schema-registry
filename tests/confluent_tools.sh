#!/usr/bin/env bash
# Confluent Platform's own command line tools against this registry.
#
#   tests/confluent_tools.sh [/path/to/confluent-7.9.0]
#
# The Schema Registry distribution ships console producers and consumers for
# Avro, JSON Schema and Protobuf (they auto-register schemas and read them back
# by id) and a load tool. This script starts a KRaft Kafka from the same
# distribution plus our registry, round-trips a record of each type, and runs
# the load tool. It skips (exit 0) when the distribution or a JDK is missing.
set -uo pipefail

CP="${1:-${CONFLUENT_HOME:-}}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/schema-registry"

if [[ -z "$CP" || ! -x "$CP/bin/kafka-avro-console-producer" ]]; then
  echo "skipped: no Confluent Platform (pass its path or set CONFLUENT_HOME)"
  exit 0
fi
if ! command -v java >/dev/null; then echo "skipped: no java"; exit 0; fi
[[ -x "$BIN" ]] || { echo "build first: cargo build"; exit 1; }

WORK="$(mktemp -d)"
KPORT=39092
SRPORT=39081
SR="http://127.0.0.1:$SRPORT"
FAILED=0

cleanup() {
  [[ -n "${SR_PID:-}" ]] && kill "$SR_PID" 2>/dev/null
  "$CP/bin/kafka-server-stop" "$WORK/kafka.properties" >/dev/null 2>&1
  pkill -f "$WORK/kafka.properties" 2>/dev/null
  sleep 1
  [[ $FAILED -eq 0 ]] && rm -rf "$WORK" || echo "logs kept in $WORK"
}
trap cleanup EXIT

check() { if [[ "$2" == "$3" ]]; then echo "  ok   $1"; else echo "  FAIL $1: expected [$3], got [$2]"; FAILED=1; fi; }

cat > "$WORK/kafka.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:$((KPORT + 1))
listeners=PLAINTEXT://localhost:$KPORT,CONTROLLER://localhost:$((KPORT + 1))
advertised.listeners=PLAINTEXT://localhost:$KPORT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$WORK/kafka-data
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
num.partitions=1
EOF
printf 'listen = "127.0.0.1:%s"\ndata_dir = "%s/sr-data"\n' "$SRPORT" "$WORK" > "$WORK/sr.toml"

echo "== starting Kafka and the registry"
"$CP/bin/kafka-storage" format -t "$($CP/bin/kafka-storage random-uuid)" -c "$WORK/kafka.properties" >/dev/null 2>&1
nohup "$CP/bin/kafka-server-start" "$WORK/kafka.properties" > "$WORK/kafka.log" 2>&1 &
"$BIN" --config "$WORK/sr.toml" > "$WORK/sr.log" 2>&1 &
SR_PID=$!
for _ in $(seq 60); do nc -z localhost $KPORT 2>/dev/null && break; sleep 1; done
for _ in $(seq 50); do curl -s -o /dev/null "$SR" && break; sleep 0.2; done

produce() { # type topic schema record
  echo "$4" | "$CP/bin/kafka-$1-console-producer" --broker-list "localhost:$KPORT" --topic "$2" \
    --property schema.registry.url="$SR" --property value.schema="$3" >> "$WORK/tools.log" 2>&1
}
consume() { # type topic
  "$CP/bin/kafka-$1-console-consumer" --bootstrap-server "localhost:$KPORT" --topic "$2" \
    --from-beginning --max-messages 1 --property schema.registry.url="$SR" 2>> "$WORK/tools.log" \
    | grep -E '^\{' | head -1 | tr -d ' \n'   # the tools also log their config to stdout
}

echo "== avro console producer/consumer"
produce avro t-avro '{"type":"record","name":"U","fields":[{"name":"n","type":"string"}]}' '{"n":"hello"}'
check "avro round trip" "$(consume avro t-avro)" '{"n":"hello"}'
check "avro schema registered under the topic subject" \
  "$(curl -s "$SR/subjects/t-avro-value/versions")" '[1]'

echo "== json schema console producer/consumer"
produce json-schema t-json '{"type":"object","properties":{"a":{"type":"string"}}}' '{"a":"x"}'
check "json schema round trip" "$(consume json-schema t-json)" '{"a":"x"}'

echo "== protobuf console producer/consumer"
produce protobuf t-proto 'syntax = "proto3"; message M { string a = 1; }' '{"a":"y"}'
check "protobuf round trip" "$(consume protobuf t-proto)" '{"a":"y"}'
check "three subjects registered by the tools" \
  "$(curl -s "$SR/subjects" | tr -d ' ')" '["t-avro-value","t-json-value","t-proto-value"]'

echo "== schema-registry-run-class load tool"
"$CP/bin/schema-registry-run-class" io.confluent.kafka.schemaregistry.tools.SchemaRegistryPerformance \
  "$SR" perf-subject 200 100 AVRO >> "$WORK/tools.log" 2>&1
registered=$(curl -s "$SR/subjects" | tr ',' '\n' | grep -c perf-subject)
check "SchemaRegistryPerformance registered its subject" "$registered" "1"
check "its schemas are all there" "$(curl -s "$SR/subjects/perf-subject/versions" | tr ',' '\n' | grep -c .)" "200"

echo
[[ $FAILED -eq 0 ]] && echo "all Confluent tool checks passed" || echo "FAILURES (see $WORK)"
exit $FAILED
