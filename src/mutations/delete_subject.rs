//! `DELETE /subjects/{subject}` - Confluent's `deleteSubject`.
//!
//! Twice, in order: a soft delete hides every live version and drops the
//! subject's own config and mode; a permanent delete then removes the rows.
//! Skipping the soft delete is refused, and so is deleting a version something
//! else still references.

use std::collections::HashSet;

use super::{Gate, Mutation, Plan, ReadView, Target, Write};
use crate::context::QualifiedSubject;
use crate::error::{ApiError, ApiResult};
use crate::model::{LogEvent, LogEventKind, VersionRecord};
use crate::modegate::Intent;
use crate::store::Scope;

pub struct DeleteSubject {
    subject: QualifiedSubject,
    permanent: bool,
}

impl DeleteSubject {
    pub fn new(subject: &str, permanent: bool) -> ApiResult<Self> {
        Ok(Self { subject: QualifiedSubject::parse(subject)?, permanent })
    }
}

impl Mutation for DeleteSubject {
    type Output = Vec<u32>;

    fn target(&self) -> Target {
        Target::Subject(self.subject.clone())
    }

    fn intent(&self) -> Intent {
        Intent::Modify
    }

    /// A subject that is not there is 404 whatever mode it is in.
    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<Vec<u32>>> {
        let (reg, r, q) = (view.reg(), view.reader(), &self.subject);
        if !reg.has_subjects(r, q, true)? {
            return Err(ApiError::subject_not_found(&q.qualified()));
        }
        if !self.permanent && !reg.has_subjects(r, q, false)? {
            return Err(ApiError::subject_soft_deleted(&q.qualified()));
        }
        let versions: Vec<(u32, VersionRecord)> =
            r.list_versions(&q.context, &q.subject)?.into_iter().filter(|(_, v)| self.permanent || !v.deleted).collect();
        for (v, vr) in &versions {
            reg.check_not_referenced(r, q, *v)?;
            if self.permanent && !vr.deleted {
                return Err(ApiError::subject_not_soft_deleted(&q.qualified()));
            }
        }

        let mut writes = Vec::new();
        let mut events = Vec::new();
        if self.permanent {
            // Every version goes at once, so none of them counts as a user of
            // an id that another of them still holds.
            let all: HashSet<(String, u32)> = versions.iter().map(|(v, _)| (q.subject.clone(), *v)).collect();
            for (v, vr) in &versions {
                let (ws, ev) = reg.hard_delete_writes(r, q, *v, vr, &all)?;
                writes.extend(ws);
                events.push(ev);
            }
        } else {
            let scope = Scope::Subject(q.context.clone(), q.subject.clone());
            for (v, vr) in &versions {
                writes.push(Write::PutVersion {
                    ctx: q.context.clone(),
                    subject: q.subject.clone(),
                    version: *v,
                    rec: VersionRecord { deleted: true, ..vr.clone() },
                });
                events.push(LogEvent {
                    ctx: q.context.clone(),
                    subject: q.subject.clone(),
                    version: *v,
                    id: vr.id,
                    kind: LogEventKind::SoftDelete,
                });
            }
            // A subject with no live version has no settings either.
            writes.push(Write::DeleteMode { scope: scope.clone() });
            writes.push(Write::DeleteConfig { scope });
        }

        let mut plan = Plan::new(versions.iter().map(|(v, _)| *v).collect()).writes(writes);
        plan.events = events;
        Ok(plan)
    }
}
