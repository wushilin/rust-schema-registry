# Pending

Remaining work from the review of 2026-09-26. Everything here was verified by
reading the code or reproducing it; nothing is speculative. Resolved review
items are recorded in the CHANGELOG under "Unreleased".

Ordered by what it costs if left alone.

# Tests that are missing

None of these are known failures. They are guarantees the brief makes, or
branches that exist, with nothing asserting them.

## Exporter state machine

Covered today: `RUNNING->PAUSED`, `PAUSED->PAUSED`, `PAUSED->RUNNING`,
`RUNNING->FAILED`, `FAILED` resume refused, `FAILED->reset->RUNNING`,
`RUNNING->ERROR`, `ERROR->RUNNING`, explicit pause/resume, reset from RUNNING.

Not covered:

* `PAUSED->FAILED`: an explicitly paused exporter whose destination then
  acquires a conflict must fail on resume, not loop in ERROR.
* Per-event cursor persistence after destination acknowledgement, and replay
  when the process stops between acknowledgement and the cursor write.
* `DELETE` of an actively RUNNING exporter mid-replay.
* The bootstrap-after-pruning path. Making `PRUNE_EVERY` configurable is the
  cheaper change and unlocks an end-to-end assertion.

## Form gaps rather than holes

* `tests/conformance/*.sh` and `*.py` are documented manual two-server probes
  and are not in `run_integration.sh`. Deliberate, but worth running before
  they rot.
