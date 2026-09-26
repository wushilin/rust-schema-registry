# Pending

What the review of 2026-09-26 found and did not fix. Everything here was
verified by reading the code or reproducing it; nothing is speculative. The
fixed half is in the CHANGELOG under "Unreleased" and in the six commits from
`309035f` to `88f6900`.

Ordered by what it costs if left alone.

## 1. Authentication has no rate limit, and leaks which usernames exist

`src/auth.rs:59-82`. `self.users.get(&user)?` returns early for an unknown
user, so bcrypt runs *only* for a name that exists - roughly 100-300 ms against
microseconds, which enumerates valid usernames from outside. Failures are
correctly never cached, so every wrong password costs a fresh bcrypt on a
`spawn_blocking` thread, and that is the same pool `api::blocking` uses for
RocksDB writes (`src/api/mod.rs`). A few hundred concurrent
`Authorization: Basic <admin:wrong>` requests starve registry writes, with no
credentials and no limit anywhere.

Needs a decision before code: where the limiter lives (per source address, per
username, or a global semaphore in front of bcrypt), and what a refused
attempt answers. A constant-time path that hashes against a dummy digest for
unknown users closes the oracle half and is independent of the rest.

Two nits in the same file, both theoretical, both free to fix while there:

* `cache_key` is `format!("{user}\0{password}\0{stored}")`, which is not
  injective - `decode_basic` splits on the first `:` only, so a username may
  contain a NUL, and `("a", "b\0secret")` collides with `("a\0b", "secret")`
  when the two users' stored hashes are identical. Hash the lengths, or the
  components separately.
* `decode_basic` accepts `Basic ` and `basic ` only. RFC 7235 makes the scheme
  case-insensitive, so `BASIC ...` gets a 401.

## 2. An exporter's state and offset do not survive a restore

`restore_local` / `restore` replay `ExporterInfo` and nothing else, so an
exporter that was `PAUSED` or `FAILED` comes back `RUNNING` at offset 0 and
re-exports everything. The dump already carries the record; it is the envelope
that is dropped on the way in.

Decide first whether a restored exporter *should* resume where it stopped: into
a different registry that is wrong, into the same one it is the only right
answer. Probably a flag on the restore, defaulting to "start over", with the
state carried in the dump either way.

## 3. A dump carries the destination's credentials in plaintext

`basic.auth.user.info` lives in an exporter's config map, and the map goes into
the dump verbatim. That is inherent to a dump being restorable - redacting it
would produce a backup that does not restore - but it is not written down
anywhere, and `backup --out` creates a file with whatever umask gives it.

Cheapest: say so in the README next to the backup section, and have
`backup --out` create the file `0600`. Redaction would need a matching "fill
these in again" story on restore, which is a bigger feature.

## 4. The exporter's bootstrap path never deletes

`src/exporter.rs:180`. An exporter whose next event has been pruned exports
current state and jumps to the log end - but it only *adds*. A subject the
destination still holds and the source no longer does stays there for ever.
This is the path every new exporter takes on any registry older than
`PRUNE_EVERY` (30 s), so it is the normal path, not the rare one.

## 5. `alias_of` swallows a store error as "no alias"

A RocksDB read failure is reported as "this subject is not an alias", so a
transient error silently routes a request to the wrong subject instead of
failing. It should propagate.

## 6. IPv6 hosts cannot be configured

`src/containers.rs:34` compiles each host pattern with `glob::Pattern::new`,
where `[` and `]` are character-class syntax - so `[::1]` becomes a class that
matches nothing. `without_port` at `:61` uses `rsplit_once(':')`, which on a
bracketed literal with no port yields the garbage `"[:"`. Harmless today (it
only ever adds match candidates) but a bracketed host is unconfigurable.

## 7. `Prefixed::poll_read` reports EOF when asked for zero bytes

`src/proxy_protocol.rs:158-165` returns `Ready(Ok(()))` having written nothing
when `buf.remaining() == 0` while a prefix is still pending, which by tokio's
convention means end of stream. Not reachable from hyper today; it makes a
correct caller wrong.

# Tests that are missing

None of these are known failures. They are guarantees the brief makes, or
branches that exist, with nothing asserting them.

## Cannot be covered by the conformance corpus at all

* **`rulesToMerge` / `rulesToRemove`** (`src/registry/schemas.rs:262`,
  porting `maybeModifyPreviousRuleSet`). The corpus's `ruleSet` steps only send
  `ruleSet`, the `/tags` steps only send `metadata`/`tagsToAdd`/`tagsToRemove`,
  and `allowed.json` already relaxes `reg-metadata#14/#15` because community
  Confluent drops rule sets. So a unit test is the only option. The
  `newVersion`-relative version selection is three untested branches, and
  `ruleSet` together with `rulesToMerge` should be 42210.

## Exporter state machine

Covered today: `RUNNING->PAUSED`, `PAUSED->PAUSED`, `PAUSED->RUNNING`,
`RUNNING->FAILED`, `FAILED` resume refused, `FAILED->reset->RUNNING`,
`RUNNING->ERROR`, `ERROR->RUNNING`, explicit pause/resume, reset from RUNNING.

Not covered:

* `reset` from `PAUSED` and from `ERROR`. `run()` skips `Paused|Failed`
  records, so whether a reset un-sticks a paused exporter is unasserted.
* `PAUSED->FAILED`: an explicitly paused exporter whose destination then
  acquires a conflict must fail on resume, not loop in ERROR.
* **State and offset across a restart** - the durable part of the feature.
* `PUT /exporters/{name}` and `GET /exporters/{name}/config` are called by no
  test at all.
* `DELETE` of an actively RUNNING exporter mid-replay.
* The bootstrap-after-pruning path (item 4 above). Making `PRUNE_EVERY`
  configurable is the cheaper change and unlocks an end-to-end assertion.

## Store and restore

* **An interrupted migration.** `migrate_to_current_format` writes in 10,000
  row batches and stamps the format version only at the end; the comment says
  re-running is safe and nothing checks it. Seed a directory where some keys
  carry the prefix and some do not, with no version key, and assert every row
  lands exactly once.
* **Restore into a conflicting destination.** Every existing test restores into
  an empty or wiped registry. Nobody restores a dump whose id 5 is schema A
  into a registry whose id 5 is schema B. Restore is a sequence of writes with
  no rollback, so a half-restore is representable and unexamined.
* **A single corrupt row stops the process.** `load_snapshot` propagates any
  `serde_json` failure, so one bad byte means the registry does not start, with
  no way to skip the row. Whether that is the intended answer should be pinned
  down by a test either way. `ApiError::store` / 50001 is asserted nowhere.

## Transport

* A body over `max_body_bytes` should be 413. Untested; `fresh_app()` is
  already there to build on.
* The panic guard turning a panic into a 500 while the server and other
  connections survive. Needs a deliberately-panicking test route, so it is
  worth less than the rest - record it rather than build it.
* A request with no `Host` header at all, once containers are configured.

## Parameters with no test on the route that reads them

* `format` on `GET /subjects/{s}/versions/{v}` (the corpus uses it only on
  `.../schema` and `/schemas/ids/{id}`).
* `normalize` on `POST /compatibility/subjects/{s}/versions/{v}`.
* `subjectPrefix` on `GET /admin/api/backup`; `subjectPrefix` and `limit` on
  `/admin/api/overview`.
* `migrate --skip-deleted`, `--subject-prefix`, `--to-auth`.

## Form gaps rather than holes

* `mutations/set_mode.rs`, `register_schema.rs`, `delete_subject.rs`,
  `delete_subject_version.rs`, `delete_context.rs` have no `#[cfg(test)]`
  module, although `plan()` is pure precisely so a verb is testable without a
  server. Only three of the eight verbs exploit that. They are covered
  indirectly by the HTTP replay, so this buys clarity, not safety.
* `schema/tags.rs`: no unit test; four corpus scenarios, two of them relaxed.
* `containers.py` never calls `/v1/metadata/id`, so no suite asserts that two
  containers report different cluster ids over HTTP (the derivation itself now
  has a unit test).
* `/metrics` per-container gauges are only ever asserted for `default`.
* `tests/conformance/*.sh` and `*.py` are documented manual two-server probes
  and are not in `run_integration.sh`. Deliberate, but worth running before
  they rot.
