//! `DELETE /config`, `DELETE /config/{subject}` - Confluent's `deleteConfig`.
//!
//! Answers with the configuration that was in effect before the delete.
//!
//! This verb is why [`Gate::AfterPlan`] exists: on a subject that has no
//! configuration Confluent answers 404, *even when that subject is in
//! read-only mode*. Existence is decided by the plan, so the mode is checked
//! after it.

use super::{Gate, Mutation, Plan, ReadView, Target, Write};
use crate::context::QualifiedSubject;
use crate::error::{ApiError, ApiResult};
use crate::model::ConfigRecord;
use crate::modegate::Intent;
use crate::store::Scope;

pub struct DeleteSubjectConfig {
    /// `None` is the registry as a whole.
    subject: Option<QualifiedSubject>,
}

impl DeleteSubjectConfig {
    pub fn new(subject: Option<&str>) -> ApiResult<Self> {
        Ok(Self { subject: subject.map(QualifiedSubject::parse).transpose()? })
    }

    fn scope(&self) -> Scope {
        match &self.subject {
            None => Scope::Global,
            Some(q) if q.is_context_only() => Scope::Context(q.context.clone()),
            Some(q) => Scope::Subject(q.context.clone(), q.subject.clone()),
        }
    }
}

impl Mutation for DeleteSubjectConfig {
    type Output = ConfigRecord;

    fn target(&self) -> Target {
        match &self.subject {
            None => Target::Global,
            Some(q) if q.is_context_only() => Target::Context(q.context.clone()),
            Some(q) => Target::Subject(q.clone()),
        }
    }

    fn intent(&self) -> Intent {
        Intent::Modify
    }

    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<ConfigRecord>> {
        let previous = match &self.subject {
            None => view.stored_config(None)?.expect("the registry always has a global config"),
            Some(q) => view.stored_config(Some(q))?.ok_or_else(|| ApiError::subject_not_found(&q.qualified()))?,
        };
        Ok(Plan::new(previous).write(Write::DeleteConfig { scope: self.scope() }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CompatibilityLevel, Mode};
    use crate::mutations::test_support::registry;
    use crate::mutations::{Gate, Mutation, ReadView};

    #[test]
    fn a_subject_with_no_config_is_not_found_and_the_mode_never_gets_a_say() {
        let (reg, _d) = registry();
        let view = ReadView::new(&reg);
        let m = DeleteSubjectConfig::new(Some("never-configured")).unwrap();
        assert_eq!(m.gate(), Gate::AfterPlan, "existence is decided before the mode");
        let e = m.plan(&view).map(|_| ()).unwrap_err();
        assert_eq!(e.code, 40401, "{}", e.message);

        // ...and that stays true when the subject is read-only: the engine
        // only reaches the mode table if the plan succeeded.
        reg.set_mode(None, Mode::Readonly, false).unwrap();
        assert_eq!(reg.delete_config(Some("never-configured")).unwrap_err().code, 40401);
        reg.set_mode(None, Mode::Readwrite, false).unwrap();
    }

    #[test]
    fn it_answers_with_what_was_in_effect_and_plans_the_delete() {
        let (reg, _d) = registry();
        reg.set_config(
            Some("s"),
            ConfigRecord { compatibility_level: Some(CompatibilityLevel::Full), ..Default::default() },
        )
        .unwrap();
        let m = DeleteSubjectConfig::new(Some("s")).unwrap();
        let plan = m.plan(&ReadView::new(&reg)).unwrap();
        assert_eq!(plan.output.compatibility_level, Some(CompatibilityLevel::Full));
        match plan.writes.as_slice() {
            [Write::DeleteConfig { scope }] => assert_eq!(*scope, Scope::Subject(".".into(), "s".into())),
            other => panic!("unexpected plan: {other:?}"),
        }

        // A configured subject in read-only mode is refused, not 404: the plan
        // succeeded, so the mode decides.
        reg.set_mode(Some("s"), Mode::Readonly, false).unwrap();
        let e = reg.delete_config(Some("s")).unwrap_err();
        assert_eq!(e.code, 42205, "{}", e.message);
    }
}
