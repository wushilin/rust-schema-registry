# Working rules for this repository

A Confluent-compatible Schema Registry in Rust: single node, RocksDB, contexts,
exporters, IMPORT mode, Basic auth with role bindings, an admin UI at `/admin`.

Read this before changing anything. It is the standing brief, so it does not
have to be repeated in each session.

## 1. Compatibility is the product

The promise is that a client written against Confluent Schema Registry 7.9
cannot tell the difference. That includes status codes, `error_code` values,
message text, field order where it is observable, and **the order in which
errors are decided**.

* Never "improve" an error, a status code or a message because it seems
  wrong. If Confluent answers 404 where 422 looks righter, we answer 404.
* When behaviour is unclear, find evidence before coding: the recorded corpus
  (`tests/http_conformance/`), the decompiled 7.9 sources, or a live Confluent.
  Say which one was used. Never guess and never state a Confluent behaviour
  without having checked it.
* Deliberate differences are allowed, but they go in README under "Deliberate
  differences from Confluent" with the reason.

### Error precedence is a real trap

Confluent decides in this order: body/bean validation, then path and query
parameters, then existence, then mode, then the operation. Two recorded cases
that have already broken a "clean" design:

* `POST /subjects/x/versions` with an explicit `id` in **READWRITE** mode
  answers **200** when that schema already has that id - the lookup runs first
  and nothing is written, so no mode applies.
* `DELETE /config/x` on a subject that does not exist answers **404**, not the
  read-only error - existence is checked before the mode.

This is why there is **no mode gate in front of the routes**. Do not add one.

## 2. Tests are the gate, not a formality

* `cargo test` - unit tests plus the 1,553-step HTTP conformance replay.
* `tests/run_integration.sh` - e2e, exporter, migrate, rbac, official Python
  and Java clients. `cli` and `tools` skip without the Confluent CLI.
* Run both before claiming anything works. Report failures with their output.

Rules:

* **A refactor commit contains no test edits.** If a test has to change, the
  behaviour changed: separate commit, reason in the message.
* The conformance corpus is regenerated only against a live Confluent, never
  hand-edited to match us. A new relaxation goes in `allowed.json` with a
  comment saying why it is acceptable.
* New behaviour gets a test at the lowest level that can hold it: a pure unit
  test if possible, an HTTP test if it is about the wire.

## 3. Rules are enforced structurally, never by remembering

The recurring bug in this codebase has been a rule that lives at a call site.
Three examples, all fixed: the exporter forcing IMPORT mode back on; fifteen
store writes with no mode check; authorization filtering threaded by hand.

So:

* **No `if` at a call site for a rule that belongs to the system.** If the same
  question is asked in two places, it belongs in one table or one type.
* Prefer a type that cannot be constructed without the check over a check that
  callers must remember. `modegate::Allowed` is the pattern: the store will not
  write state without one, and only consulting the mode table produces one.
* When a rule genuinely does not apply, say so by name (`is_a_mode_change()`,
  `not_schema_state()`), so `grep` finds every exemption. Never by omission.
* Decisions belong in data - a table, a trait impl, a declaration on a type -
  not in branching that each new caller has to reproduce.

## 4. Target architecture

Work towards these layers. Do not add code to `registry.rs` that belongs in a
layer below; move the layer instead.

```
http/        transport only: axum, media types, Jackson/Jersey quirks, URI
             rewrite (context + alias), body limits, panic guard, TLS later.
auth/        authentication: Basic, bcrypt cache, principal construction.
rbac/        authorization: roles, bindings, targets, visibility predicates.
store/       PhysicalStore: RocksDB CFs, batches, key encoding. No rules.
readmodel/   Cache: imbl snapshot + ArcSwap + moka. Read-only projections.
mutations/   one file per verb, self-contained.
engine/      MutationExecutionEngine: the only place that sequences a write.
events/      the durable change log and its subscribers.
exporters/   a subscriber: reads the log in order, ships to a destination.
schema/      format parsing, normalization, compatibility. Already self-contained.
admin/       read-only projections for the UI, plus the embedded page.
```

Each mutation is a verb (`RegisterSchema`, `UpdateCompatibility`,
`DeleteSubjectVersion`, ...) that declares its target and intent and produces a
plan; the engine does authorization, locking, the mode check, the batch, the
log append and the notification - once, for every verb:

```rust
trait Mutation {
    type Output;
    fn target(&self) -> Target;              // what it touches
    fn intent(&self) -> Intent;              // Write | Import | Modify | ...
    fn gate(&self) -> Gate { Gate::BeforePlan }   // AfterPlan where Confluent
                                                  // checks existence first
    fn plan(&self, view: &ReadView) -> ApiResult<Plan<Self::Output>>;
}
```

`plan()` is **pure**: snapshot in, ops + events + output out. No I/O, no clock,
no randomness. That purity is what makes a verb testable without a server, and
it is worth preserving even when it costs a little plumbing.

Ops and events are applied in **one RocksDB batch**. "Committed but not
logged" and "exported but not committed" must stay unrepresentable.

## 5. Multi-tenancy: host containers

Hierarchy is **host container > context > subject**. Containers share nothing
logically and everything physically: one RocksDB, one process, tenant as the
first component of every key - including the change log, which gets
`(tenant, seq)` with its own counter, its own floor and its own pruning.

* Tenant is resolved in `http/` from the request's host against a configured
  whitelist, and passed down as a `TenantId`. Nothing above `store/` builds a
  key by hand.
* **The `Host` header is client-controlled**: it routes, it does not authorize.
  Anything that must not leak across containers is enforced by credentials
  (users are scoped to containers) and, where it matters, by which listener the
  connection arrived on.
* Unconfigured means one implicit `default` container and byte-identical
  behaviour - the conformance corpus must keep passing untouched.
* Per container: its own global config and mode, its own contexts, its own id
  space, its own exporters, its own cluster id (AUTO exporter contexts are
  named after it, so a shared cluster id would collide across containers).
* Existing data has no tenant prefix. Any change to key layout needs a
  versioned on-disk migration, tested against a copy of a real data directory
  (192.168.44.99 has one), not only against a fresh one.

## 6. Exporters

* An export carries the source's ids, which is only legal where the
  destination is in IMPORT mode. **Check that mode; never force it.** Setting
  it is allowed only for a destination context that is still empty.
* Failure states are meaningful and must stay distinct: `ERROR` (transient,
  retried), `PAUSED` (the destination needs an operator; `resume` continues
  from where it stopped), `FAILED` (the replay conflicts with what the
  destination holds; `resume` is refused, only `reset` starts over).
* Both refusals arrive as `42205`. Classify by re-reading the destination's
  effective mode (`?defaultToGlobal=true`), never by parsing message text.

## 7. Style

* Comments explain **why**, not what. No comment that restates the code.
* Doc comments on modules and non-obvious functions; name the Confluent method
  being ported when there is one (`// checkRegisterMode`).
* Match the surrounding code: terse names, no emoji, no decorative banners
  beyond the `// ---------------- section ----------------` already in use.
* Hyphens in prose comments, not em dashes; ASCII in source.
* `cargo fmt` before committing. Keep functions small enough to read whole.
* Tests read as documentation: the name says the behaviour, the body shows it.

## 8. Operational facts

* Deployed at `wushilin@192.168.44.99`, under processmaster:
  `/opt/services/systemd/processmaster/auto_services/schema-registry/`
  (`bin/`, `config.toml`, `run.sh`, `service.yml`, `data/`, `logs/`).
* **Always build from source on that host** (`~/workspace/rust-schema-registry`,
  `cargo build --release`), never copy a binary from a laptop.
* Control it with `sudo pmctl --sock /tmp/processmaster.sock <cmd>
  schema-registry`; it runs as `service:service` with restart policy `always`.
* Auth is on there: user `admin`, bcrypt-hashed in `config.toml`. Never write a
  plaintext password into a config file or a commit.
* An exporter `self-to-new` copies the default context into `.new` on that
  instance. `.new` must stay in IMPORT mode for it to run.
* Ask before touching data on that host. It holds subjects that are not ours.

## 9. Working agreements

* Share a design before implementing anything structural; implement after
  agreement, not before.
* Deliver in slices that each end green. Commit per slice with a message that
  says what changed and why, in prose.
* Say plainly when something is unverified, skipped or failing. "Tests pass"
  means they were run in this session.
* Prefer deleting code to adding a flag. Prefer one rule to two paths.
