# Pending

Remaining work from the review of 2026-09-26 and the follow-up review of
2026-09-27. Everything here was verified by reading the code or reproducing it;
nothing is speculative. Resolved items are recorded in the CHANGELOG under
"Unreleased".

# Decisions waiting on you

These are not bugs. Each is a choice that has already been made in code, and is
worth confirming rather than discovering later.

## 1. A bootstrap replay hard-deletes at the destination

`reconcile_bootstrap` (`src/exporter.rs`) removes destination subjects that the
source no longer has, with `DELETE ...?permanent=true`. Three things about it:

* **It is irreversible.** A soft delete would replicate the source's own
  soft-delete state; a permanent one cannot be undone at the destination.
* **Whether Confluent's schema linking reconciles this way is unverified.** The
  decompiled sources were not available in the session that added it, and the
  recorded corpus does not cover exporters. Section 1 of the brief says a
  Confluent behaviour is not to be stated without checking, so this needs
  either evidence or an entry in README under "Deliberate differences from
  Confluent". It is in the CHANGELOG but not there.
* **The blast radius is one context, bounded by IMPORT mode.** The delete is
  reachable only for a destination context already in IMPORT mode, and
  `ensure_import_mode` sets that only for an *empty* context and otherwise
  pauses - so an operator has explicitly handed the context over. Within it,
  `subject_matches_renamed` decides what is deletable, and an exporter whose
  `subjects` is `*` claims everything there. That predicate now has a unit
  test; the end-to-end path does not (see below).

Recommended: soft delete instead, or keep the permanent delete and write it
down as a deliberate difference.

## 2. `named_queue` is a new dependency for exporter wakeups

`registry/mod.rs` replaced `tokio::sync::Notify` with `named_queue` 0.2.0
(which brings `flume` and `arc-swap`). The motivation is sound -
`notify_waiters` only wakes tasks already waiting, so a commit landing between
scans could be missed - but `tokio::sync::broadcast` has the same semantics,
including the lag behaviour the comment describes, and tokio is already a
dependency. Section 11 of the brief asks for a design to be shared before
anything structural.

Recommended: either move to `tokio::sync::broadcast`, or say why the crate
earns its place.

# Tests that are missing

None of these are known failures. They are guarantees the brief makes, or
branches that exist, with nothing asserting them.

## The exporter

Covered: `RUNNING->PAUSED`, `PAUSED->PAUSED`, `PAUSED->RUNNING`,
`PAUSED->FAILED`, `RUNNING->FAILED`, `FAILED` resume refused,
`FAILED->reset->RUNNING`, `RUNNING->ERROR`, `ERROR->RUNNING`, explicit
pause/resume, reset from RUNNING, and the crash window between destination
acknowledgement and cursor persistence. Exact IMPORT replay is verified against
Confluent 7.9.0.

Not covered:

* **The bootstrap-after-pruning path end to end**, including the deletes in
  item 1 above - the destructive half of the exporter has no test that runs it.
  Making `PRUNE_EVERY` configurable is the cheaper change and unlocks the
  assertion.
* `DELETE` of an actively RUNNING exporter mid-replay.

## Form gaps rather than holes

* `tests/conformance/*.sh` and `*.py` are documented manual two-server probes
  and are not in `run_integration.sh`. Deliberate, but worth running before
  they rot.
