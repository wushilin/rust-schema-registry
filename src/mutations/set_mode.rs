//! `PUT /mode`, `PUT /mode/{subject}` and the delete of either - Confluent's
//! `setMode` / `deleteSubjectMode`.
//!
//! A mode change is never gated on the current mode: it is the way out of
//! READONLY and out of IMPORT.
//!
//! Entering IMPORT is the one that does work. Without `force` it refuses while
//! live subjects exist in scope, and purges the soft-deleted leftovers so that
//! imported ids cannot collide with rows nobody can see. With `force` it
//! changes the mode and touches nothing - the caller has said they know what
//! is there.

use std::collections::HashSet;

use super::{Gate, Mutation, Plan, ReadView, Target, Write};
use crate::context::QualifiedSubject;
use crate::error::{ApiError, ApiResult};
use crate::model::{Mode, VersionRecord};
use crate::modegate::Intent;
use crate::store::Scope;

pub struct SetMode {
    /// `None` is the registry as a whole.
    subject: Option<QualifiedSubject>,
    mode: Mode,
    force: bool,
}

impl SetMode {
    pub fn new(subject: Option<&str>, mode: Mode, force: bool) -> ApiResult<Self> {
        Ok(Self { subject: subject.map(QualifiedSubject::parse).transpose()?, mode, force })
    }

    fn scope(&self) -> Scope {
        match &self.subject {
            None => Scope::Global,
            Some(q) if q.is_context_only() => Scope::Context(q.context.clone()),
            Some(q) => Scope::Subject(q.context.clone(), q.subject.clone()),
        }
    }

    /// Every subject the new mode will cover.
    fn subjects_in_scope(&self, view: &ReadView<'_>) -> ApiResult<Vec<QualifiedSubject>> {
        let r = view.reader();
        Ok(match &self.subject {
            None => {
                let mut all = Vec::new();
                for ctx in r.list_contexts()? {
                    all.extend(r.list_subject_names(&ctx)?.into_iter().map(|n| QualifiedSubject::new(&ctx, &n)));
                }
                all
            }
            Some(q) if q.is_context_only() => {
                r.list_subject_names(&q.context)?.into_iter().map(|n| QualifiedSubject::new(&q.context, &n)).collect()
            }
            Some(q) => vec![q.clone()],
        })
    }
}

impl Mutation for SetMode {
    type Output = ();

    fn target(&self) -> Target {
        match &self.subject {
            None => Target::Global,
            Some(q) if q.is_context_only() => Target::Context(q.context.clone()),
            Some(q) => Target::Subject(q.clone()),
        }
    }

    fn intent(&self) -> Intent {
        Intent::SetMode
    }

    fn gate(&self) -> Gate {
        Gate::BeforePlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<()>> {
        let (reg, r) = (view.reg(), view.reader());
        let current = view.mode_for(&self.target())?;
        let mut writes = Vec::new();
        let mut events = Vec::new();

        if self.mode == Mode::Import && current != Mode::Import && !self.force {
            let has_live = match &self.subject {
                Some(q) => reg.has_subjects(r, q, false)?,
                None => reg.has_any_subject(r, false)?,
            };
            if has_live {
                return Err(ApiError::operation_not_permitted("Cannot import since found existing subjects"));
            }
            // Only soft-deleted rows are left; they go, so an import cannot
            // land on an id that something invisible still holds.
            let mut doomed: Vec<(QualifiedSubject, u32, VersionRecord)> = Vec::new();
            for sq in self.subjects_in_scope(view)? {
                for (v, vr) in r.list_versions(&sq.context, &sq.subject)? {
                    reg.check_not_referenced(r, &sq, v)?;
                    doomed.push((sq.clone(), v, vr));
                }
            }
            let keys: HashSet<(String, u32)> = doomed.iter().map(|(q, v, _)| (q.subject.clone(), *v)).collect();
            for (sq, v, vr) in &doomed {
                let (ws, ev) = reg.hard_delete_writes(r, sq, *v, vr, &keys)?;
                writes.extend(ws);
                events.push(ev);
            }
        }

        writes.push(Write::PutMode { scope: self.scope(), mode: self.mode });
        let mut plan = Plan::new(()).writes(writes);
        plan.events = events;
        Ok(plan)
    }
}

/// `DELETE /mode/{subject}`: answers with the mode that was set.
pub struct DeleteMode {
    subject: QualifiedSubject,
}

impl DeleteMode {
    pub fn new(subject: &str) -> ApiResult<Self> {
        Ok(Self { subject: QualifiedSubject::parse(subject)? })
    }
}

impl Mutation for DeleteMode {
    type Output = Mode;

    fn target(&self) -> Target {
        if self.subject.is_context_only() {
            Target::Context(self.subject.context.clone())
        } else {
            Target::Subject(self.subject.clone())
        }
    }

    fn intent(&self) -> Intent {
        Intent::SetMode
    }

    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<Mode>> {
        let q = &self.subject;
        let previous = view
            .reg()
            .mode_of_scope(view.reader(), Some(q))?
            .ok_or_else(|| ApiError::subject_not_found(&q.qualified()))?;
        let scope = if q.is_context_only() {
            Scope::Context(q.context.clone())
        } else {
            Scope::Subject(q.context.clone(), q.subject.clone())
        };
        Ok(Plan::new(previous).write(Write::DeleteMode { scope }))
    }
}
