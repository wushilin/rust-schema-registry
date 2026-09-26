# Pending

Remaining work from the review of 2026-09-26. Everything here was verified by
reading the code or reproducing it; nothing is speculative. Resolved review
items are recorded in the CHANGELOG under "Unreleased".

Ordered by what it costs if left alone.

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
* `PUT /exporters/{name}` and `GET /exporters/{name}/config` are called by no
  test at all.
* `DELETE` of an actively RUNNING exporter mid-replay.
* The bootstrap-after-pruning path. Making `PRUNE_EVERY` configurable is the
  cheaper change and unlocks an end-to-end assertion.

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
