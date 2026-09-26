# Changelog

## Unreleased

**Additional review fixes.** Basic auth now bounds concurrent bcrypt work to
four requests and verifies unknown usernames against a dummy bcrypt digest;
at capacity it returns the same 401 challenge. The auth cache key encodes
component lengths, and the `Basic` scheme is parsed case-insensitively.
Bracketed IPv6 host patterns now route with or without a port, zero-length
proxy reads preserve the pending prefix, alias lookup errors propagate, and
`backup --out` writes mode-0600 files. The backup section documents that dumps
contain destination credentials. Exporters now commit their log offset after
each successful event, and dumps preserve exporter state and offset. Restoring
an existing exporter leaves its current cursor alone. Bootstrap after log
pruning now removes stale destination subjects that match the exporter's
context and rename patterns.

### Fixed after a review of the whole implementation

**Authorization.** Four ways a caller reached past their role, all found by
asking the same question twice in two ways:

* `POST /subjects/{s}/versions/{v}/tags` registers a version, and was
  classified as a read-only POST, so any `readonly` caller could write to any
  subject it could read. Read-only POSTs are now matched by shape.
* `/config/%3A.eu%3A` is `/config/:.eu:`, a context's own settings, and the
  percent-encoded spelling passed the "subject-scoped" test - letting a
  `write` user put a whole context into `READONLY` or `IMPORT`. The decision
  now uses the target that was already decoded, not the raw path.
* Backup and restore asked "does this caller see every subject", which is true
  of a registry-wide `readonly` role. With any narrow `admin` binding that was
  whole-container backup and restore, including the global config. They now
  require admin everywhere.
* **`GET /metrics` and an exporter's config now need the admin role.** Metrics
  names and counts every host container in the process; an exporter's config
  map carries the destination's credentials. Which exporters exist and how
  each is getting on stay readable to any role.

**The change log could hold less than what was committed**, which an exporter
notices and nothing else does:

* **The migration into host containers orphaned the whole change log.** A log
  key is a big-endian u64, so every sequence below 2^56 starts with the byte
  that marks the store's own rows, and those were skipped in every column
  family instead of only in `meta`. An upgraded registry kept answering
  correctly while its exporters silently skipped their backlog.
* Two contexts could commit holding the same log sequence, because the
  sequence was read under a per-context lock, and one log row would overwrite
  the other. The counter lock now lives with the counter and a transaction
  cannot be opened without it.
* A single exporter record that would not deserialize read as "no exporters",
  which read as "nothing needs the log", and pruned every other exporter's
  backlog. Nothing is pruned now when anything cannot be read.

**Other data kept, or handed back, correctly:**

* **Aliases survive a backup.** An alias is a subject with a setting and no
  versions, and both dump paths walked subjects by their versions.
  `backup --from` now asks one of our own registries for its own dump and
  falls back to the Confluent API walk, which `--confluent-api` selects.
* **A container's cluster id no longer moves when another container is
  configured.** It used to depend on how many there were, so adding a second
  renamed the first - and an AUTO exporter names its destination context after
  it.
* `sync_writes` applies to every write. It had been honoured by the batch
  commit alone, leaving the cluster id, the log floor, exporter cursors and
  the on-disk migration with their own durability.
* A permanent delete drops the freed schema body from the cache, which is what
  reclaiming it was for.

**Transport and process:**

* **HTTP/2 clients work with host containers.** The authority arrives in
  `:authority`, which hyper leaves in the URI, so every h2 request answered
  421 as soon as containers were configured. An absolute-form request-target's
  authority is honoured too, as RFC 7230 section 5.4 requires.
* The PROXY header read has a five-second timeout. It necessarily happens
  before the peer can be checked against `trust`, so any client could hold a
  task and a socket indefinitely, below hyper where its timeouts never apply.
* Route and method labels are bounded. Both were built from client-controlled
  input before authentication, and an unrecognised value became a counter and
  a histogram that are never evicted - unbounded memory from an anonymous
  scanner. Anything unrecognised is now one label.
* `/metrics` renders on the blocking pool; it walks every version of every
  container.
* The panic guard sits outside the URI rewriting rather than inside it.
* `backup --out` writes beside the target and renames, so an interrupted
  backup leaves the previous dump intact.

### Tested that was not

Concurrency (two writers in one context, sixteen across contexts, a reader
running against a writer), SIGKILL durability, `ServerConfig::validate`'s
fifteen refusals plus `config.example.toml` itself, the `hash-password` ->
config -> login round trip, aliases and rule sets through a backup,
`?dryRun=true` over HTTP, and every authorization rule above over real HTTP.

* `[proxy]` is now an explicit section, and off by default:

  ```toml
  [proxy]
  proxy_on      = true
  proxy_version = 2
  trust         = "192.168.44.0/24;127.0.0.1/32"
  ```

  It replaces `proxy_protocol` and `proxy_trust` from 0.2.0. The version is
  declared rather than auto-detected: a header of the other version is refused
  with a warning, so a mismatch between this and the proxy is visible instead
  of working by accident.

## 0.2.0

Everything a single-node registry did in 0.1.0, plus the things you need to run
one: several registries in one process, a console, backups, metrics, and a way
to sit behind a proxy. Nothing about the Confluent API changed - the 1,553-step
conformance corpus replays unchanged, which is what makes the rest believable.

### Host containers

Several logically separate registries in one process and one store, chosen by
the request's `Host` header. Each has its own contexts, subjects, ids, global
config and mode, exporters, change log and cluster id; isolation is a prefix on
every key, so nothing above `store.rs` can name another container's rows.

```toml
[[containers]]
name  = "prod"
hosts = ["sr.example.com", "sr.example.com:8081"]
```

`Host` routes, it does not authorize: a user can be bound to containers
(`containers = ["prod"]`), and that is what a forged header runs into. An
unmatched host is 421. Configure none and there is exactly one, `default`, on
every host - which is why nothing changed for existing deployments.

**A data directory from 0.1.0 is migrated in place on first open** (every row
moves into `default`). It is restartable and one-way: keep the directory or a
dump if you may want to go back.

### Backup and restore

A logical dump - newline-delimited JSON of subjects, versions, ids, references,
metadata, rule sets, config, modes and exporters - that restores into another
build, and into Confluent.

```
schema-registry backup  --from http://localhost:8081 --out dump.ndjson
schema-registry restore --from dump.ndjson --to http://localhost:8081 [--dry-run]
```

Also `GET /admin/api/backup` and `POST /admin/api/restore`, and buttons for
both in the console.

### Admin console

`/admin` (it was `/_admin`, which now redirects): a single self-contained page
with a left rail - Overview, Subjects, Contexts, Exporters, Backup & Restore,
Settings. Register schemas and new versions with a compatibility check, read
every version's schema and references, change compatibility and mode at any
scope, drive exporters, soft- and hard-delete. Everything it does goes through
the public API, so it can do nothing an admin could not do with `curl`.

Admin-only, and an admin scoped to some subjects sees exactly those.

### Metrics

`GET /metrics` in Prometheus' text format: requests by method, route shape and
status; mutations by verb and outcome; durations for both; per-container gauges
for subjects, versions, contexts and exporter positions. Route labels are
shapes (`/subjects/*/versions`), never subjects.

### Behind a proxy

PROXY protocol v1 and v2, auto-detected from the first bytes of the connection
- before TLS, long before HTTP. Believed only from `proxy_trust` (loopback and
the private ranges by default); a claim from anywhere else is refused and
logged as such, naming the peer and the setting.

### Logging

One record per request and per mutation, with the client address, as text or
JSON. **Records go to stdout, warnings and errors to stderr.** Reads are `debug`
unless `log_reads = true`.

### Role bindings

A role can be held registry-wide or bound to subject patterns, and a user may
hold several:

```toml
roles = ["readonly"]
[[auth.users.bindings]]
role = "admin"
subjects = [".::abc*", ".eu::*"]
```

Listings and the console are filtered to what a caller may see rather than
refused, so a scoped admin gets a working console over their own subjects.

### Behaviour fixed

* **An exporter no longer forces its destination back into IMPORT mode.** It
  checks, and sets the mode only for a destination context that is still empty.
  A destination that has left IMPORT mode pauses the exporter (`resume` picks
  up where it stopped); a replay that conflicts with what the destination holds
  fails it (`reset` starts over). Both arrive as 42205, so the mode is re-read
  rather than the message parsed.
* **A permanent delete now frees the schema's content** once no version in that
  context holds its id, and the id answers 40403 afterwards - matching
  Confluent, whose tombstone drops the id from its index. Before this, a
  registry that churned schemas grew for ever.
* **The parsed-schema cache is keyed by what its references resolved to**, so a
  parse built against a schema that has since been replaced cannot be returned.
* **Rule sets are validated** (42210) and schema tags are supported on all three
  formats.

### Inside

The read path and the write path are now separate and each in one place. Every
change is a verb in `mutations/` that says what it touches and turns a snapshot
into a plan; `engine::run` is the only code that takes a write lock, checks the
mode table, applies the rows and the log events as one batch, publishes the
snapshot and wakes the exporters. The mode rules live in one table whose token
the store demands before it will write, so a write path cannot skip them. Locks
follow the verb's target, so writes to different contexts and different
containers no longer wait for each other.

`registry.rs` (2,390 lines) became a directory split by concern; the largest
source file is now the store.

## 0.1.0

A Confluent-compatible Schema Registry: single node, RocksDB, contexts,
exporters (schema linking), IMPORT mode, HTTP Basic auth, and a migration tool.
Verified against a live Confluent 7.9.0 by replaying 1,553 recorded
request/response pairs.
