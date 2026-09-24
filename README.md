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

## Why this exists

Confluent's Schema Registry keeps its schemas in a Kafka topic, so running a
registry means running and operating Kafka. For what is, in the end, a small
validated key-value store, that is a lot of machinery: two JVMs and about
1.1 GB of memory in the measurement below, plus a broker to keep alive,
before a single schema is registered. That weight is felt most in the places
a registry is least interesting: CI pipelines, development laptops, edge and
single-tenant deployments, and small clusters that need a registry but do not
otherwise need Kafka's durability story for it.

It also sits in a hot path. Every producer and consumer resolves schemas at
startup and on cache misses, and a serializer that cannot reach the registry
does not serialize. Latency and restart time are worth something here.

This server is one binary with RocksDB underneath: no JVM, no Kafka, no
cluster. On the same host, against Confluent 7.9.0 with single-node Kafka:

| | Confluent | this server |
|---|---:|---:|
| reads (by id) | 29,237 req/s, 6.85 ms p99 | **131,724 req/s, 0.99 ms p99** |
| repeat registrations | 12,191 req/s | **101,593 req/s** |
| new registrations (fsync per write) | 3,017 req/s | **7,127 req/s** |
| memory, 20k subjects | 1.1 GB (two JVMs) | **283 MB** |
| restart with 20k subjects | Kafka log replay | **317 ms, 171 MB** |

Full method and the rest of the workloads are in [Benchmark](#benchmark).

The trade is deliberate: a single node, so no replication and no failover.
Schemas live in RocksDB on local disk, fsynced per write by default, and the
usual answer to durability is a file backup rather than a quorum.

None of that matters if clients have to change, so compatibility is treated
as the hard requirement: the REST behaviour is recorded from a real Confluent
7.9.0 and replayed as a test, and Confluent's own Java and Python clients,
the `confluent` CLI and its console producers and consumers all run against
this server unchanged. Where behaviour deliberately differs, it is
[listed](#deliberate-differences-from-confluent). Moving an existing registry
over is `schema-registry migrate`, which copies subjects, ids, versions, soft
deletes, configs and modes out of a Confluent.

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
| Exporters | `/exporters` CRUD, `/status`, `/config`, `pause`/`resume`/`reset`; context types AUTO/CUSTOM/NONE/DEFAULT, subject globs, `subjectRenameFormat`; states RUNNING/PAUSED/ERROR/FAILED |
| Auth | HTTP Basic, roles `admin` / `write` / `readonly` registry-wide or bound to subject patterns (`.eu::orders-*`), bcrypt or plaintext passwords |
| Host containers | several logically separate registries in one process and one store, chosen by the `Host` header and separated by a prefix on every row - contexts, subjects, ids, config, modes, exporters and the change log. One implicit `default` container unless configured ([how](#host-containers-several-registries-one-process)) |
| Backup / restore | `schema-registry backup --from URL` and `restore --from dump --to URL`, plus `GET /admin/api/backup` and `POST /admin/api/restore`: a newline-delimited JSON dump of subjects, versions, ids, references, metadata, rule sets, config, modes and exporters |
| Logging | one record per mutation and per request, as text or JSON; records on stdout, warnings and errors on stderr |
| Admin UI | `/admin`: one page over the same REST API - register schemas and new versions (with a compatibility check), subjects, versions and schemas, compatibility and mode per subject/context/global, contexts, exporters (pause/resume/reset/create/edit), soft and permanent deletes. Admin role only |
| Migration | `schema-registry migrate --from URL --to URL`: copies every subject, version, id, reference, soft delete, config and mode from another registry |

## How it is built

Three principles, in the order they matter.

**1. A rule is enforced by the shape of the code, not by remembering to call
it.** Every bug this server has had in its rules was the same bug: a check that
lived at a call site. So the rules live in one place each, and the code is
arranged so that skipping one does not compile.

| rule | where it lives | why it cannot be skipped |
|---|---|---|
| which operations a mode allows | `modegate.rs`, one table over (intent, mode) | every store write that defines registry state demands an `Allowed`, and only consulting that table produces one. Exemptions are named - `is_a_mode_change()`, `not_schema_state()` - so `grep` lists everything that skips it |
| who may do what, and see what | `authz.rs`, pure functions over (principal, target, intent) | the target is read off the request; listings are filtered by the same predicate that authorizes, so "shown" and "allowed" cannot drift |
| one container's rows | `store.rs`, `Store` prefixes every key | nothing above the store builds a key, so one container cannot name another's rows |
| when a change is legal, locked, logged, published | `engine.rs`, one pipeline | a verb declares; the engine performs. A verb that forgot a step never had the step to forget |

What that buys, stated as promises the code keeps rather than habits it has:

* **A write cannot skip the mode table.** The store will not write registry
  state without an `Allowed`, and only consulting the table makes one. The two
  exemptions are named (`Intent::SetMode`, `Intent::NotSchemaState`), so
  "what is not gated" is a grep, not an audit.
* **A write cannot skip the lock, the log or the snapshot swap.** `engine::run`
  is the only place that takes a write lock - one line, one grep - and it
  applies a verb's rows and its log events in a single batch, so "committed but
  not logged" and "exported but not committed" are unrepresentable.
* **A container cannot read another's rows.** Every key is built inside
  `store.rs` behind a container prefix; nothing above it constructs one.
* **What a caller is shown and what they may do cannot drift**, because both
  come from the same predicate in `authz`.
* **A reader sees one consistent instant** for a whole request, without taking
  a lock, and the shared caches are safe across snapshots because what they
  hold cannot change under a key (see *Design decisions*).

**2. A change is a verb, and verbs are data.** Each mutation is its own file in
`mutations/`: it declares what it touches, what kind of change it is, when the
mode should be checked, and turns a snapshot into a plan (writes + log events +
answer). `plan()` takes no lock, writes nothing and reads no clock, so a verb is
tested by comparing plans - no server, no port, no waiting. The engine is the
only code that sequences a write, so ordering exists once rather than per
handler. Every write in the server goes through it - `engine::run` is the only
place that takes a write lock, and the grep is one line.

**3. Confluent's answers are the specification, including their order.**
Compatibility is checked by replaying 1,553 recorded request/response pairs
from a live Confluent 7.9. That corpus has twice rejected designs that were
cleaner than what shipped - most usefully a mode gate in front of the routes,
which is wrong because Confluent answers some requests from state before it
considers the mode at all (`POST /subjects/x/versions` with an id that already
matches is 200 in READWRITE mode; `DELETE /config/x` on a missing subject is
404, not the read-only error). Anything that reorders those answers is a
regression, however tidy it looks.

## Design decisions

**Where things live.**

```
src/api/          transport: axum, media types, Jersey/Jackson quirks, URI rewrite,
                  host-container routing, the admin console
src/auth.rs       authentication: Basic, bcrypt cache
src/authz.rs      authorization: roles, bindings, targets, visibility
src/containers.rs which host container a request belongs to
src/tenant.rs     a container's name, and the key prefix it becomes
src/store.rs      PhysicalStore (RocksDB) + Store (one container's view of it)
src/snapshot.rs   immutable metadata, swapped after each commit
src/registry/     the read model, by concern:
                    mod.rs        the type, the reader, the verb entry points
                    resolution.rs which config and mode apply
                    lookup.rs     finding a schema, and compatibility
                    schemas.rs    canonical forms, references, the parse cache
                    listings.rs   subjects, schemas, versions, ids
                    views.rs      request and response shapes
                    locks.rs      what a mutation excludes
                    admin.rs      projections for the console
src/mutations/    one file per verb: what it touches, and its plan
src/engine.rs     the one path a change takes
src/modegate.rs   the mode table, and the token the store demands
src/exporter.rs   a change-log subscriber that ships schemas elsewhere
src/backup.rs     the logical dump, both directions
src/migrate.rs    copying a whole registry over the wire
src/schema/       Avro, JSON Schema, Protobuf: parsing, normalization, compatibility
```

**RocksDB is the source of truth; memory is the read model.** Confluent keeps
its state in a Kafka topic and rebuilds an in-memory cache on startup. With a
single node we don't need a replicated log, so RocksDB (fsync'd WAL,
atomic `WriteBatch`) replaces Kafka as the durable store. One column family per
table, with big-endian integer keys so prefix scans come back sorted
(`src/store.rs` has the key layout).

**Reads are wait-free; the caches are safe because their values cannot
change.** A read takes one `Arc<Snapshot>` (an atomic load) and walks immutable
persistent maps, so it never blocks, is never blocked, and sees one consistent
point in time for the whole request. The schema-body and parsed-schema caches
sit *outside* the snapshot, shared by all readers, which is only sound because
their keys map to values that cannot change: content under an id is fixed
(registering over an id is refused, and a hard delete removes versions, not
bodies), and the parsed cache is additionally keyed by a fingerprint of what
the schema's references resolved to - so a parse built against a schema that
has since been replaced cannot be looked up.

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

### Role bindings

A role can be held registry-wide (`roles = [...]`) or bound to subjects. A
user may hold several bindings, one per role, and they add to `roles` rather
than limiting them - so "reads everything, owns `abc*` and `def*`, writes
`xyz-*`" is one user:

```toml
[[auth.users]]
username = "team-a"
password = "$2b$12$..."
roles = ["readonly"]
[[auth.users.bindings]]
role = "admin"
subjects = [".::abc*", ".::def*"]
[[auth.users.bindings]]
role = "write"
subjects = [".::xyz-*"]
```

A pattern is `context::subject`, both sides accepting `*`: `.::orders-value`,
`.::test*`, `.eu::*` (a whole context), `*::*` (everywhere). Without `::` it
is a subject in the default context. An unparseable pattern stops the server
at startup rather than granting something unintended.

Which roles apply is decided by what the request names:

| the request is about | roles that count |
|---|---|
| a subject (`/subjects/x/...`, `/config/x`, `/compatibility/subjects/x/...`) | registry-wide roles + bindings matching `x` |
| a context (`/config/:.eu:`, `/mode/:.eu:`, `DELETE /contexts/.eu`) | registry-wide roles + bindings covering the whole context (`.eu::*`) |
| a listing (`/subjects`, `/schemas`, `/contexts`, `/admin`) | anyone holding a role; the **response is filtered** to what the caller may see |
| anything else: global config and mode, exporters, schema-by-id | registry-wide roles only |

Filtering rather than refusing is what makes a scoped role usable: an admin of
`abc*` sees `abc*` in `/subjects` and in the admin UI, and the UI lists exactly
the subjects that caller administers, so nothing it offers can come back 403.

Two deliberate limits. `GET /schemas/ids/{id}` is not subject-scoped - ids are
a registry-wide namespace and serializers fetch them constantly - so any
authenticated caller may read a schema by id. And global settings, contexts and
exporters always need a registry-wide `admin`.

## Host containers: several registries, one process

A container is a whole registry - its own contexts, subjects, ids, global
config and mode, exporters and change log. The request's `Host` header picks
one:

```toml
listen = "0.0.0.0:8081"

[[containers]]
name  = "prod"
hosts = ["sr.example.com", "sr.example.com:8081"]

[[containers]]
name  = "dev"
hosts = ["*.dev.example.com"]
```

Patterns are globs, matched without regard to case, and the first match wins.
A pattern without a port also matches that host *with* one, because a client
may or may not send it; a pattern that names a port matches only that port. A
host that matches nothing is answered **421 Misdirected Request**, not served
by whichever container happens to be first.

**Configure none and there is exactly one**, named `default`, answering on
every host - which is why the recorded corpus still replays unchanged.

The hierarchy is **container > context > subject**, and the isolation is a
prefix on every key: schemas, fingerprints, versions, reference edges, config,
modes, exporters, and the change log, which gets its own sequence, its own
floor and its own pruning. Nothing above `store.rs` builds a key, so one
container cannot name another's rows. Container names may not start with `.`
(that is how contexts start) and the prefix ends in NUL, so `prod` never scans
`prod-eu`'s rows. Each container also gets its own cluster id, because an
exporter names its AUTO contexts after it and a shared one would collide at a
destination.

**`Host` routes; it does not authorize.** It is the client's to choose, so the
boundary is credentials:

```toml
[[auth.users]]
username   = "prod-team"
password   = "$2b$12$..."
roles      = ["write"]
containers = ["prod"]        # empty (the default) is every container
```

That check runs after the container is resolved, so a forged `Host` gets 403
rather than another container's data. For a harder boundary, give each
container its own listener and let the network decide who reaches which port -
the server binds one address today, so that part is not built yet.

A data directory written before containers existed is migrated in place on
first open: every row moves into `default`. It is restartable, and it is
one-way - keep the directory, or a dump, if you may want to go back.

`tests/containers.py` runs two containers on one port and one directory: the
same subject name in both with independent ids, contexts and settings that do
not leak, a read-only mode in one while the other writes, the admin view
scoped to one, a bound user refused in the other whatever `Host` it claims, an
unknown host 421, and all of it surviving a restart.

## Backup and restore

The dump is newline-delimited JSON describing the *registry*, never the store,
so it restores into a different build - and, since it is written with the
public API's shapes, into Confluent:

```
schema-registry backup  --from http://localhost:8081 --out dump.ndjson
schema-registry restore --from dump.ndjson --to http://localhost:8081 [--dry-run]
```

The same thing over HTTP, for the container the request reaches:
`GET /admin/api/backup` streams it, `POST /admin/api/restore` replays it
(`?dryRun=true` reports what it would do). Both need registry-wide admin.

Restoring replays through IMPORT mode: ids and version numbers come back as
they were, referenced schemas are written first, and soft deletes are
re-applied after everything else, because deleting the last live version of a
subject drops that subject's settings. Re-running is safe - registering the
same id and version again is a no-op - and unknown record types are skipped,
so a newer build's dump still restores what an older one understands.

## Logging

One record per mutation (`verb`, `container`, `elapsed_us`, and on refusal
`error_code` and the message) and one per request (`method`, `path`, `status`,
`container`, `elapsed_us`). **Records go to stdout; warnings and errors go to
stderr**, so a shell can separate them without parsing. `log_format = "json"`
writes one JSON object per line with the same fields.

Reads are the bulk of the traffic, so they are recorded at `debug` unless
`log_reads = true`; writes and failures are always recorded. `RUST_LOG` still
works for anything finer.

## Admin UI

`/admin` serves a single self-contained page - no build step, no assets, no
outside requests - laid out as a console with a left rail:

* **Subjects**: every subject with its version count, latest version and id,
  schema type, effective compatibility and mode (and where each is inherited
  from). Open one to read every version's schema, references, metadata and
  rule set, see what references it, and soft- or permanently delete versions.
* **Registering**: *Register schema* takes a subject, a type and the schema
  (with references), checks compatibility on request, and registers it;
  *Register new version* inside a subject starts from its latest schema. A
  rejected registration keeps the form and shows the registry's reason.
* **Contexts**: what each context holds, its own compatibility and mode, and
  deleting an empty one.
* **Exporters**: state, offset, destination and the last error; pause, resume,
  reset, edit the config, create and delete.
* **Backup & Restore**: download a dump, or restore one (with a dry run
  first).
* **Settings**: container, cluster id, counts, global compatibility and mode.

Changes go through the public REST API, so the UI can do nothing an admin
could not do with `curl`, and every refusal is the registry's own error. It is
restricted to the `admin` role - registry-wide, or over some subjects, in
which case the page shows exactly those. Two JSON endpoints back it,
`GET /admin/api/overview` and `GET /admin/api/subjects/{subject}`; no Confluent
resource starts with `admin`, so nothing is shadowed. The UI first shipped
under `/_admin`, which now redirects.

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
cargo test                                    # 116 tests, including the HTTP conformance replay
tests/run_integration.sh                      # e2e + exporter + rbac + official Python and Java clients (with auth)
python3 tests/exporter.py                     # schema linking: a source exporting into a destination
python3 tests/migrate.py [--source URL]       # a full copy between two registries
python3 tests/rbac.py                         # roles and role bindings over HTTP
tests/exporter_cli.sh [/path/to/confluent]    # the official `confluent` CLI driving the exporter API
tests/confluent_tools.sh [/path/to/confluent-7.9.0]  # Confluent's console producers/consumers over real Kafka
tests/run_integration.sh --against URL [u:p]  # the client suites against any registry (e.g. Confluent)
python3 tests/http_conformance/run.py check URL          # the conformance corpus against a live server
python3 tests/http_conformance/run.py record CONFLUENT_URL  # re-record golden.json
```

### Exporter states

An export carries the source's ids and versions, which is only legal where the
destination is in IMPORT mode. The exporter checks that mode; it sets it only
for a destination context that is still empty, so a context an operator has
taken out of IMPORT mode is never quietly forced back in. What a failure means:

| what happened | state | how it continues |
|---|---|---|
| the destination is unreachable, 5xx, a timeout | `ERROR` | retried from the failed event |
| the destination is no longer in IMPORT mode | `PAUSED` | put it right, then `resume` - it carries on from where it stopped |
| the replay conflicts with what the destination holds (an id there is a different schema) | `FAILED` | `resume` is refused; `reset` starts over |

**Exporter tests** (`tests/exporter.py`): start a source and a destination
(the destination with Basic auth) and check every context mapping - NONE
keeps the source context, AUTO namespaces under the source cluster id,
DEFAULT flattens, CUSTOM plus `subjectRenameFormat` - with ids, versions and
cross-context references preserved, soft and permanent deletes replayed,
pause/resume/reset, and recovery from a dead destination.

**Host container tests** (`tests/containers.py`): two containers on one port
and one data directory - independent ids for the same subject name, contexts
and settings that do not leak, one read-only while the other writes, the admin
view scoped to one, a user bound to one refused in the other whatever `Host`
it claims, an unknown host 421, and all of it surviving a restart.

**Role-binding tests** (`tests/rbac.py`): one server whose users mix
registry-wide roles with bindings; checks what each may do and is refused
(subjects, contexts, global settings, exporters) and what each is *shown* -
`/subjects`, `/schemas`, `/contexts`, `/schemas/ids/{id}/subjects` and the
admin UI all filtered to that caller - plus that an unparseable pattern stops
the server from starting.

**Mode rules** (`src/modegate.rs`): which operations a mode allows lives in
one table, and every write in the store that defines registry state - schemas,
versions, hard deletes, config - demands the token that only consulting that
table produces. A write path cannot forget the check; it has nothing to pass.
Two exemptions exist and are named rather than implied: `is_a_mode_change()`
(a mode change is the way out of READONLY and IMPORT, including the `force`
emptying when entering IMPORT) and `not_schema_state()` (exporter records, and
deleting an already-empty context). `grep` for either to see every write that
skips the table. (A gate in front of the routes would be wrong: what
Confluent answers depends on registry state, e.g. a re-registration of a schema
that already has that id answers 200 in READWRITE mode, and `DELETE /config/x`
on a missing subject answers 404 before any mode is considered.)

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
Restarting on that data reads the metadata snapshot in 317 ms and settles at
171 MB.

Reads were about 20% faster before the REST behavior was ported endpoint by
endpoint against a live Confluent; matching it exactly costs a little work per
request, which seemed the right trade.

## What a hard delete frees

A soft delete hides a version and keeps everything. A **permanent** delete
removes the version row, and - once no version in that context still holds its
id - the schema's content as well, which is the only thing that ever frees the
space a deleted schema took.

That also matches Confluent, where a tombstone drops the id from the index when
its subject-version map empties (`InMemoryCache#schemaTombstoned`), so
`GET /schemas/ids/{id}` answers 40403 afterwards. The fingerprint index keeps
pointing at the old id in both, but `schemaIdAndSubjects` returns nothing while
no version holds it, so registering that content again allocates a **new** id
rather than resurrecting the old one.

This was settled from the decompiled 7.9 sources rather than a recording: the
corpus exercises hard deletes but never reads an id back afterwards.

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
