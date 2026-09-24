# Changelog

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
