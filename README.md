# rust-schema-registry

A Confluent-compatible Schema Registry in Rust: single node, RocksDB storage,
contexts, schema exporters (schema linking), IMPORT mode, and HTTP Basic auth.
Speaks the Confluent REST API, so the official Java, Python (and other)
serializers work against it unchanged.

```
cargo build --release
./target/release/schema-registry --config config.example.toml
./target/release/schema-registry hash-password 's3cret'   # bcrypt for the config file
```

## What's supported

| Area | Endpoints |
|---|---|
| Schemas | `GET /schemas`, `/schemas/types`, `/schemas/ids/{id}[/schema\|/subjects\|/versions]` (`subject`, `format=serialized`, `fetchMaxId`) |
| Subjects | `GET/POST/DELETE /subjects/...`, versions, `latest`/`-1`, `referencedby`, `/subjects/{s}/metadata`, soft/hard delete, `deleted`/`deletedOnly`, pagination |
| Compatibility | `POST /compatibility/subjects/{s}/versions[/{v}]` (`verbose`); NONE, BACKWARD, FORWARD, FULL and `_TRANSITIVE` variants; `compatibilityGroup` |
| Formats | Avro, JSON Schema, Protobuf; references for all three; base64 `FileDescriptorProto` input (Python/Go/.NET clients) and `format=serialized` output |
| Config / mode | global, context (`:.ctx:`) and subject scope with fallback; `defaultToGlobal`; READWRITE/READONLY/READONLY_OVERRIDE/IMPORT; `force` |
| Contexts | `:.ctx:subject` naming, `/contexts`, `DELETE /contexts/{ctx}`, and the `/contexts/{ctx}/...` URL form |
| Data contracts | `metadata`/`ruleSet` stored and returned; default/override metadata and rules merged from config; inherited from the previous version; rule sets validated (42210) |
| Schema tags | `POST /subjects/{s}/versions/{v}/tags`: `tagsToAdd`/`tagsToRemove` for Avro, JSON Schema and Protobuf, `newVersion`, `metadata`, `rulesToMerge`/`rulesToRemove` |
| Exporters | `/exporters` CRUD, `/status`, `/config`, `pause`/`resume`/`reset`; context types AUTO/CUSTOM/NONE/DEFAULT, subject globs, `subjectRenameFormat` |
| Auth | HTTP Basic, roles `admin` / `write` / `readonly`, bcrypt or plaintext passwords |
| Migration | `schema-registry migrate --from URL --to URL`: copies every subject, version, id, reference, soft delete, config and mode from another registry |

## Design decisions

**RocksDB is the source of truth; memory is the read model.** Confluent keeps
its state in a Kafka topic and rebuilds an in-memory cache on startup. With a
single node we don't need a replicated log, so RocksDB (fsync'd WAL,
atomic `WriteBatch`) replaces Kafka as the durable store. One column family per
table, with big-endian integer keys so prefix scans come back sorted
(`src/store.rs` has the key layout).

**Every write is one atomic transaction, and readers see snapshots.**
All registry *metadata* (every id, subject, version, fingerprint, reference
edge, config, mode and counter) lives in an immutable `Snapshot`
(`src/snapshot.rs`), built from persistent maps (`imbl`), so cloning it is
O(1). A write:

1. takes the single write lock,
2. builds a RocksDB batch and, in lock-step, a list of snapshot ops,
3. commits the batch (durable),
4. applies the ops to a clone of the snapshot and publishes it with one
   atomic pointer swap (`ArcSwap`).

Each request pins one snapshot (`Registry::reader()`) and answers entirely
from it: no read locks, and no torn reads across a concurrent write. Small
per-subject and per-id collections are copy-on-write `Arc<Vec>`s, not trees,
which cut metadata memory by about 3× (about 1.2 KB per subject).

**Schema bodies and parsed schemas are cached with a bound.** Three `moka`
caches, each capped by `cache_max_entries`: bodies by (context, id), parsed
stored schemas by (context, id) (used by compatibility checks), and parsed
request schemas by hash(type, text, references) (repeated register/lookup
calls skip parsing entirely). The content under a (context, id) never changes,
so these caches never need invalidating. A miss costs one RocksDB point read
and stays consistent with any snapshot. Writes insert new bodies as part of
the commit.

**Reads skip the thread hop.** GET handlers run inline on the async workers,
because they're memory-speed. Writes and anything that may parse run on the
blocking pool. Re-registering an existing schema (every `auto.register`
serializer does this) is answered lock-free from the snapshot; only real
writes take the lock.

**Compatibility.** Avro uses our own implementation of the Avro resolution
rules (Java `SchemaCompatibility` semantics, logical types lowered). The
`apache-avro` checker compares `Ref` nodes by name only, so it can't follow
references. Protobuf is compiled in-process with `protox` against an
in-memory file set (references, well-known types, and Confluent's
`confluent/meta.proto` and `confluent/type/decimal.proto`), and diffed with
Confluent's rule set. JSON Schema checking covers open, closed and
partially-open content models, bounds, enums, `required`, combiners, and
`$ref` (local and cross-schema).

**Canonical storage, like Confluent.** Avro and JSON are stored compacted.
Protobuf is stored as a canonical rendering of its descriptor, so base64
descriptor input and Java's runtime-descriptor output (explicit `map_entry`
messages) both become ordinary `.proto` text. Schemas with custom options are
stored verbatim, because extension values can't be re-rendered.

**A panic is one request's problem.** Schema parsers take untrusted input, so
the HTTP stack catches panics and turns them into a 500 for that request; the
server and other connections are unaffected.

**Exporters tail a change log.** Every mutation appends to a `log` column
family, and each exporter keeps an offset into it, the same model as
Confluent's exporter tailing `_schemas`. The worker replays events against the
destination through the normal REST API in IMPORT mode, so ids and versions
are preserved and referenced subjects go first. Replays are idempotent
(at-least-once is fine). Failures park the exporter in ERROR with a trace,
and it is retried automatically. The log is trimmed to what the exporters
still need (everything, when there are none), so it does not grow with the
lifetime of the server; an exporter whose next event has already been trimmed
- a new one, or one that fell far behind - copies the current state instead
and then follows the log again. Because ids are per context and each context
starts at 1, `contextType` CUSTOM and DEFAULT (which map every source context
onto one destination context) are only accepted when the exporter's subject
patterns name a single source context; merging several would lose ids, so it
is refused with 42250. AUTO and NONE keep contexts apart and take any pattern.

## Conformance: behavior verified against Confluent 7.9.0

These were probed on a live Confluent Schema Registry and are pinned as unit
tests in `src/conformance_tests.rs`. The HTTP probes in `tests/conformance/`
reproduce them against any two servers (`compare.sh` diffs the responses).

**Schema ids are per context, and collisions across contexts are normal.**
Each context has its own counter starting at 1. There is no attempt to avoid
ids used elsewhere, and no global uniqueness, even for imports: id 777 can
be three different schemas in `.xa`, `.xb` and the default context.
Importing id 900 into one context doesn't move any other context's counter.
Within a context, the same schema under different subjects shares one id.

**Resolving an id (`GET /schemas/ids/{id}?subject=...`):**

| `subject` hint | where Confluent looks |
|---|---|
| none | default context only (no fallback) |
| `foo` or `:.:foo` | first context (default, then others in sorted order) where `foo` itself uses the id; else any subject in the default context |
| `:.ctx:` | that context, any subject |
| `:.ctx:foo` | that context, only if `foo` uses the id |

So a plain deserializer (which passes the topic's unqualified subject) can
read data whose schema lives in another context under the *same subject
name*, for example one brought in by an exporter. There is no blind search:
an unrelated subject gets 40403. On `/schemas/ids/{id}/subjects` and
`/versions`, `subject` only locates the context and does not filter.

**Registration** is a line-by-line port of Confluent's `registerOrForward` /
`register` / `lookUpSchemaUnderSubject`:

- An existing schema answers `{id}`; when metadata or rules were merged in
  (or the schema was taken from the previous version) the full entity comes
  back instead.
- An `id` in the body outside IMPORT mode is accepted only if it's the id
  the schema already has; otherwise 42205 "is not in import mode". `version`
  0 and -1 mean "not given"; any other explicit version must be the next one.
- IMPORT mode: new schemas need an `id` ("not in read-write mode"); `id` 0 is
  a valid id; the same content may be imported under a second id; version
  gaps are fine and an existing version is overwritten; different content
  under a used id is 42205 "Overwrite new schema with id N is not permitted.";
  compatibility isn't checked. Entering IMPORT without `force` on a scope
  that only has soft-deleted versions hard-deletes them.

**Config and mode resolve first-match, not merged**: the subject's record,
else its context's (non-default contexts) or the global one (default
context), else the server default; a missing level is always filled from
the server default. Global config and mode do **not** reach into other
contexts, except a global `READONLY_OVERRIDE`, which wins everywhere.
Config writes and deletes are refused in read-only modes; deleting a config
or mode that isn't set is 40401.

**URL filters**: `/contexts/{ctx}/...` and subject aliases are applied to the
request URI before routing, exactly like Confluent's `ContextFilter` and
`AliasFilter`, so an alias redirects *every* `/subjects/{alias}/...` request
(reads, writes, lookups, compatibility, deletes) and the `subject` parameter
of `/schemas/ids/...`, but not `/config` or `/mode`.

**Request bodies** are read the way Jersey + Jackson read them: unsupported
`Content-Type` is 415 (a missing header is fine); an empty body is 422
`{method}.argN must not be null`; malformed JSON or a wrong shape is 400;
scalars coerce (`{"schema": 42}` is the Avro schema `42`, `"1"` and `1.7` are
the version 1); unknown fields are ignored; a repeated key keeps its last
value; a repeated query parameter keeps its first.

## Configuration

See `config.example.toml`. Key settings: `listen`, `data_dir`,
`default_compatibility`, `sync_writes` (fsync per write, default on),
`cache_max_entries` (per cache, default 20,000), `normalize` (default on), `exporter_poll_seconds`, and
`[auth]` users with roles. `--listen` and `--data-dir` (or `SR_LISTEN`,
`SR_DATA_DIR`) override the file.

Roles: `admin` can do everything. `write` can read, register and delete
schemas, and set subject-level config and mode. `readonly` can do GETs plus
lookup and compatibility tests. Verified bcrypt credentials are cached in
memory, so the ~100 ms hash only runs once per credential.

## Logically equal schemas share an id (on by default)

`normalize = true` (the default) normalizes every registration and lookup,
so spellings of the same schema share one id:

| format | treated as equal |
|---|---|
| Avro | whitespace, attribute order, `ns.Name` vs `name`+`namespace`, `{"type":"long"}` vs `"long"`, custom-property order |
| JSON Schema | whitespace, key order at any depth |
| Protobuf | whitespace, comments, field declaration order, `.pkg.Type` vs `Type`, option order, `reserved` grouping |

Message *declaration* order in Protobuf and a changed `doc` are real
differences. This departs from Confluent, whose default is `normalize=false`
(only identical canonical forms share an id). Set `normalize = false` in the
server config, or per subject or context via `PUT /config`, for Confluent's
exact default. The stored and returned text is the normalized form, as
Confluent does when normalization is on.

## Building

```
cargo build --release          # the server, `srbench` and the migration tool
```

RocksDB is built from source, so a C++ toolchain and libclang are needed
(`brew install llvm` on macOS, `apt install clang libclang-dev` on Debian).
If bindgen cannot find libclang, point `LIBCLANG_PATH` at it.

## Testing

```
cargo test                                    # 107 tests, including the HTTP conformance replay
tests/run_integration.sh                      # e2e + exporter + official Python and Java clients (with auth)
python3 tests/exporter.py                     # schema linking: a source exporting into a destination
python3 tests/migrate.py [--source URL]       # a full copy between two registries
tests/exporter_cli.sh [/path/to/confluent]    # the official `confluent` CLI driving the exporter API
tests/confluent_tools.sh [/path/to/confluent-7.9.0]  # Confluent's console producers/consumers over real Kafka
tests/run_integration.sh --against URL [u:p]  # the client suites against any registry (e.g. Confluent)
python3 tests/http_conformance/run.py check URL          # the conformance corpus against a live server
python3 tests/http_conformance/run.py record CONFLUENT_URL  # re-record golden.json
```

**Exporter tests** (`tests/exporter.py`): start a source and a destination
(the destination with Basic auth) and check every context mapping - NONE
keeps the source context, AUTO namespaces under the source cluster id,
DEFAULT flattens, CUSTOM plus `subjectRenameFormat` - with ids, versions and
cross-context references preserved, soft and permanent deletes replayed,
pause/resume/reset, and recovery from a dead destination.

**Migration tests** (`tests/migrate.py`): fill a source with contexts,
references, every schema type, soft-deleted versions, metadata, configs and
modes; migrate; then compare the two registries version by version (ids,
schema text, references, metadata, configs, modes), and check that a dry run
writes nothing and a second run changes nothing. With `--source URL` the
source can be a real Confluent.

**Confluent's own tools** run against this server: `tests/confluent_tools.sh`
starts a KRaft Kafka from a Confluent Platform directory and round-trips a
record through `kafka-avro-console-producer`/`-consumer` and the JSON Schema
and Protobuf pairs (schemas auto-registered here, read back by id), then
registers 200 schemas with `SchemaRegistryPerformance`.
`tests/exporter_cli.sh` drives the exporter API with the official `confluent`
CLI (create, list, describe, status, pause, resume, reset, update,
configuration, delete) and checks the schemas arrive at the destination. The
CLI requires a login against Confluent's Metadata Service, a commercial
component, so `tests/mds_stub.py` answers that handshake; the CLI's own
config is kept out of `$HOME`.

**HTTP conformance** (`tests/http_conformance/`, replayed by
`src/http_conformance_tests.rs`): 71 scenarios, 1,499 requests over every
Confluent 7.9 REST endpoint. They cover valid and invalid input (malformed JSON,
wrong types, bad versions and ids, bad schemas of every type, bad
references), repeated mutations, every delete state transition, mode and
config scoping across global/context/subject, IMPORT, aliases,
`/contexts/{ctx}/...` URLs, cross-context reads, odd and reserved subject
names, metadata, `confluent:version`, compatibility groups, paging and
`format=serialized`. `scenarios.py` generates them; `run.py record` sends them
to a real Confluent (resetting it between scenarios) and stores every status
and body in `golden.json` (recording is deterministic). The Rust test replays
each scenario against a fresh in-process server and requires identical
status and JSON. Server-assigned ids are compared as symbols per context
(`$id1`, `$id2`, ...), so "the same id again" is checked but absolute
numbers aren't. Every tolerated difference is listed with its reason in
`allowed.json` or is one of three global rules in the test (Jackson's
parse-error wording, Wire/Jackson parser detail text, and Confluent 500s).

The unit tests are layered:

- **Golden tests** (`src/golden_tests.rs`): `tests/golden/corpus.json`
  (127 schemas, 193 evolution pairs and 36 version chains, generated by
  `make_corpus.py`) was run through **Confluent 7.9.0's own schema libraries**
  by `tests/oracle` (a small Java program) to produce `golden.json`. We must
  match it on: accept or reject, the canonical stored string, the normalized
  form, BACKWARD and FORWARD verdicts and exact messages, and all 7 levels over
  every chain. Regenerate with the commands at the top of `make_corpus.py`.
- **Level tests** (`src/compat_level_tests.rs`): readable examples through
  the registry, each with schemas inline and an expected row for
  NONE/BACKWARD/BACKWARD_TRANSITIVE/FORWARD/FORWARD_TRANSITIVE/FULL/FULL_TRANSITIVE.
  They include transitive-gap histories where the transitive and
  non-transitive levels disagree.
- **Conformance tests** (`src/conformance_tests.rs`): server behavior
  probed on a live Confluent (context id resolution, id allocation,
  IMPORT mode, logical equality, error formats), at the registry API level.
- **HTTP conformance** (`src/http_conformance_tests.rs`): the recorded
  Confluent corpus above, at the HTTP level.

Both client suites pass against this server **and** against a real Confluent
7.9.0. Where the Confluent behavior came from its source (the JSON Schema and
Protobuf diff packages, everit's loader, Avro's `SchemaCompatibility`), the
logic was re-implemented and then verified against the oracle output.

### Deliberate differences from Confluent

Each of these is pinned in `tests/http_conformance/allowed.json` or the test's rules.

- `normalize` is on by default (see above); with it, configs report `normalize: true`.
- The same schema text under two schema types (say the Avro and JSON Schema
  `[]`) is two schemas here. Confluent's content hash ignores the type, so it
  answers the second registration with 42205.
- Protobuf schemas that protoc rejects but Confluent's Wire parser accepts
  (field number 0, duplicate field numbers, a proto3 enum whose first value
  isn't 0) are rejected, because no generated client could use them.
- Where Confluent crashes with a 500 (a negative `offset`, a missing `mode`
  in `PUT /mode`, `GET /schemas/ids/{missing}/schema`, a `null` metadata
  property), we answer with a proper 4xx or result.
- Error texts that come from Jackson (request-body parse errors) or from
  the Wire/Jackson parsers inside a schema error are not reproduced word for
  word; the status, error code and message envelope are.
- Two IMPORT-mode states Confluent itself reports inconsistently (after the
  same content got a second id, or an id's only version was overwritten) are
  answered from our actual data.
- A JSON Schema compatibility check where `exclusiveMaximum` decreases
  without a `maximum` crashes inside Confluent 7.9 (a NullPointerException).
  We report `EXCLUSIVE_MAXIMUM_DECREASED`.
- `ruleSet` is stored, returned and merged (Confluent Enterprise behavior).
  Community Confluent silently drops rule sets, so `GET /schemas?ruleType=`
  finds nothing there. Here it matches `domainRules` and `migrationRules`.
- Extra endpoints that community 7.9 does not have: `/exporters` and
  `DELETE /contexts/{ctx}`.
- Tagging a Protobuf schema that has no package: Confluent writes
  `package null;` into the schema, which then fails its own compatibility
  check. We leave the package out.
- Rule sets are validated (42210, Confluent's own rules). Community Confluent
  discards rule sets before validating, so it accepts anything.

## Benchmark

`srbench` (in this repo) with 1,000 Avro subjects, 64 connections, 5 s
warmup and 10 s measured per workload. Apple M3 Max, everything on one host,
each server benchmarked alone. Confluent 7.9.0 runs on single-node KRaft Kafka
with default settings. This server runs with `sync_writes = true` (fsync
per write; Kafka does not fsync per write).

```
./target/release/srbench --url http://127.0.0.1:8081
```

| workload | Confluent req/s | this req/s | speedup | Confluent p99 | this p99 |
|---|---:|---:|---:|---:|---:|
| get-by-id | 29,237 | 131,724 | **4.5×** | 6.85 ms | 0.99 ms |
| latest | 28,677 | 128,043 | **4.5×** | 6.08 ms | 1.04 ms |
| lookup | 27,565 | 94,073 | **3.4×** | 7.45 ms | 2.45 ms |
| register-exist | 12,191 | 101,593 | **8.3×** | 12.30 ms | 1.72 ms |
| compat | 12,107 | 101,746 | **8.4×** | 13.15 ms | 1.77 ms |
| register-new | 3,017 | 7,127 | **2.4×** | 27.19 ms | 13.69 ms |

Memory after the same run (20,000 subjects registered): this server 283 MB,
Confluent 1.1 GB across its two JVMs (Schema Registry 475 MB, Kafka 663 MB).

Reads were about 20% faster before the REST behavior was ported endpoint by
endpoint against a live Confluent; matching it exactly costs a little work per
request, which seemed the right trade.

## Not implemented

Rule *execution* (rules are stored, returned and validated, but running them
is a client-side concern), Avro `format=resolved`, `findTags` on schema
reads, `validateFields` enforcement, metrics, and TLS (put a proxy in front).
Schema GUIDs (`/schemas/guids`) are a Confluent 8 feature and are not part of
the 7.9 API this server targets. The exporter API is modeled on Confluent
Platform's schema linking, which Confluent's community edition does not ship
(it returns 404), so it could not be conformance-tested the same way.

## License

Apache-2.0. See [LICENSE](LICENSE).
