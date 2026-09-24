//! The mutation execution engine: the one path a change to the registry takes.
//!
//! For every verb, in this order:
//!
//! 1. take the write lock for what it touches,
//! 2. check the mode table for (intent, mode in scope) - before the plan, or
//!    after it where Confluent answers from state first,
//! 3. run the verb's pure `plan`,
//! 4. apply its writes and its log events as one batch, one fsync,
//! 5. publish the new snapshot and wake whoever is waiting (exporters).
//!
//! Because this is the only place that does any of it, a new verb cannot
//! forget a step, and the `Allowed` token the store demands is produced here -
//! a verb never sees it and so cannot fabricate one.
//!
//! Locking is still the registry-wide write lock. Per-target locks (parallel
//! writes to unrelated subjects) belong here too, but only once every verb
//! runs through this engine: while legacy paths take the global lock directly,
//! a finer lock here would not be serialising against them.

use crate::error::ApiResult;
use crate::modegate;
use crate::mutations::{Gate, Mutation, Plan, ReadView, Write};
use crate::registry::Registry;

pub fn run<M: Mutation>(reg: &Registry, m: M) -> ApiResult<M::Output> {
    // Lock-free first: most "writes" turn out to be a schema that is already
    // registered, and answering those without the lock is what keeps a busy
    // producer fleet from serialising behind one another.
    if let Some(out) = m.fast_path(&ReadView::new(reg))? {
        return Ok(out);
    }
    let _guard = reg.write_lock();
    let view = ReadView::new(reg);
    let target = m.target();
    let mode = view.mode_for(&target)?;
    let gate = |()| modegate::check(m.intent(), mode, &target.scope_name());

    // Before the plan unless the verb needs its own answer first (a missing
    // subject is 404, not the read-only error).
    let allowed = match m.gate() {
        Gate::BeforePlan => Some(gate(())?),
        Gate::AfterPlan => None,
    };
    let plan = m.plan(&view)?;
    let allowed = match allowed {
        Some(a) => a,
        None => gate(())?,
    };
    apply(reg, plan, &allowed)
}

fn apply<T>(reg: &Registry, plan: Plan<T>, allowed: &modegate::Allowed) -> ApiResult<T> {
    let mut tx = reg.store.tx()?;
    for w in &plan.writes {
        match w {
            Write::PutSchema { ctx, id, rec, index } => tx.put_schema(ctx, *id, rec, *index, allowed)?,
            Write::PutVersion { ctx, subject, version, rec } => tx.put_version(ctx, subject, *version, rec, allowed)?,
            Write::DeleteVersion { ctx, subject, version, id } => tx.delete_version(ctx, subject, *version, *id, allowed),
            Write::PutRefby { ctx, subject, version, id } => tx.put_refby(ctx, subject, *version, *id),
            Write::DeleteRefby { ctx, subject, version, id } => tx.delete_refby(ctx, subject, *version, *id),
            Write::SetNextId { ctx, next } => tx.set_next_id(ctx, *next),
            Write::PutConfig { scope, rec } => tx.put_config(scope, rec, allowed)?,
            Write::DeleteConfig { scope } => tx.delete_config(scope, allowed),
            Write::PutMode { scope, mode } => tx.put_mode(scope, *mode, allowed)?,
            Write::DeleteMode { scope } => tx.delete_mode(scope, allowed),
            Write::DeleteContext { ctx } => tx.delete_context(ctx, allowed),
        }
    }
    for e in &plan.events {
        tx.append_log(e)?;
    }
    reg.commit(tx)?;
    Ok(plan.output)
}
