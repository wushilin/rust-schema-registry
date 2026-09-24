//! `DELETE /subjects/{subject}/versions/{version}` - Confluent's
//! `deleteSchemaVersion`.
//!
//! The same two steps as deleting a whole subject, one version at a time. The
//! last live version taking the subject's config and mode with it is
//! Confluent's behaviour, and it is the reason a migration writes every
//! version before it deletes any.

use std::collections::HashSet;

use super::{Gate, Mutation, Plan, ReadView, Target, Write};
use crate::context::QualifiedSubject;
use crate::error::{ApiError, ApiResult};
use crate::model::{LogEvent, LogEventKind, VersionRecord};
use crate::modegate::Intent;
use crate::registry::VersionSpec;
use crate::store::Scope;

pub struct DeleteSubjectVersion {
    subject: QualifiedSubject,
    spec: VersionSpec,
    permanent: bool,
}

impl DeleteSubjectVersion {
    pub fn new(subject: &str, spec: VersionSpec, permanent: bool) -> ApiResult<Self> {
        Ok(Self { subject: QualifiedSubject::parse(subject)?, spec, permanent })
    }
}

impl Mutation for DeleteSubjectVersion {
    type Output = u32;

    fn target(&self) -> Target {
        Target::Subject(self.subject.clone())
    }

    fn intent(&self) -> Intent {
        Intent::Modify
    }

    /// A version that is not there is 404 whatever mode it is in.
    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<u32>> {
        let (reg, r, q) = (view.reg(), view.reader(), &self.subject);
        let any = reg.get_exact(r, q, self.spec, true)?;
        // Asking to soft-delete something already soft-deleted is its own error.
        if any.is_some() && !self.permanent && reg.get_exact(r, q, self.spec, false)?.is_none() {
            let (v, _) = any.expect("checked");
            return Err(ApiError::version_soft_deleted(&q.qualified(), v));
        }
        let Some((version, vr)) = any else {
            return Err(if reg.has_subjects(r, q, true)? {
                ApiError::version_not_found(self.spec)
            } else {
                ApiError::subject_not_found(&q.qualified())
            });
        };
        reg.check_not_referenced(r, q, version)?;
        if self.permanent && !vr.deleted {
            return Err(ApiError::version_not_soft_deleted(&q.qualified(), version));
        }

        let mut writes = Vec::new();
        let mut events = Vec::new();
        if self.permanent {
            let (ws, ev) = reg.hard_delete_writes(r, q, version, &vr, &HashSet::new())?;
            writes.extend(ws);
            events.push(ev);
        } else {
            writes.push(Write::PutVersion {
                ctx: q.context.clone(),
                subject: q.subject.clone(),
                version,
                rec: VersionRecord { deleted: true, ..vr.clone() },
            });
            let live_left = r.list_versions(&q.context, &q.subject)?.iter().any(|(v, x)| *v != version && !x.deleted);
            if !live_left {
                // That was the last live version: the subject's mode and config go too.
                let scope = Scope::Subject(q.context.clone(), q.subject.clone());
                writes.push(Write::DeleteMode { scope: scope.clone() });
                writes.push(Write::DeleteConfig { scope });
            }
            events.push(LogEvent {
                ctx: q.context.clone(),
                subject: q.subject.clone(),
                version,
                id: vr.id,
                kind: LogEventKind::SoftDelete,
            });
        }

        let mut plan = Plan::new(version).writes(writes);
        plan.events = events;
        Ok(plan)
    }
}
