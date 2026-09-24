//! `PUT /config`, `PUT /config/{subject}` - Confluent's `updateConfig`.
//!
//! Fields present in the update replace the stored ones; fields absent keep
//! what was there. The scope is whatever the subject string named: a subject,
//! a context (`:.eu:`), or the registry as a whole.

use super::{Gate, Mutation, Plan, ReadView, Target, Write};
use crate::context::QualifiedSubject;
use crate::error::ApiResult;
use crate::model::ConfigRecord;
use crate::modegate::Intent;
use crate::store::Scope;

pub struct UpdateCompatibility {
    /// `None` is the registry as a whole.
    subject: Option<QualifiedSubject>,
    update: ConfigRecord,
}

impl UpdateCompatibility {
    /// Parsing here, not in `plan`, so an unusable subject name is reported
    /// before anything looks at modes - the order Confluent answers in.
    pub fn new(subject: Option<&str>, update: ConfigRecord) -> ApiResult<Self> {
        Ok(Self { subject: subject.map(QualifiedSubject::parse).transpose()?, update })
    }

    fn scope(&self) -> Scope {
        match &self.subject {
            None => Scope::Global,
            Some(q) if q.is_context_only() => Scope::Context(q.context.clone()),
            Some(q) => Scope::Subject(q.context.clone(), q.subject.clone()),
        }
    }
}

impl Mutation for UpdateCompatibility {
    type Output = ();

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
        Gate::BeforePlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<()>> {
        let scope = self.scope();
        let merged = view.config(&scope)?.unwrap_or_default().merged_with(&self.update);
        Ok(Plan::new(()).write(Write::PutConfig { scope, rec: merged }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CompatibilityLevel;
    use crate::mutations::test_support::{registry, writes_of};
    use crate::mutations::{Mutation, Target};

    fn level(l: CompatibilityLevel) -> ConfigRecord {
        ConfigRecord { compatibility_level: Some(l), ..Default::default() }
    }

    #[test]
    fn an_update_replaces_only_the_fields_it_carries() {
        let (reg, _d) = registry();
        reg.set_config(Some("s"), ConfigRecord { normalize: Some(false), ..level(CompatibilityLevel::Full) }).unwrap();

        // Planning a compatibility change leaves `normalize` as it was.
        let m = UpdateCompatibility::new(Some("s"), level(CompatibilityLevel::None)).unwrap();
        let writes = writes_of(&reg, &m).unwrap();
        match writes.as_slice() {
            [Write::PutConfig { scope, rec }] => {
                assert_eq!(*scope, Scope::Subject(".".into(), "s".into()));
                assert_eq!(rec.compatibility_level, Some(CompatibilityLevel::None));
                assert_eq!(rec.normalize, Some(false), "a field the update did not mention is kept");
            }
            other => panic!("unexpected plan: {other:?}"),
        }
    }

    #[test]
    fn the_scope_is_whatever_the_subject_string_named() {
        let (reg, _d) = registry();
        let target = |s: Option<&str>| UpdateCompatibility::new(s, ConfigRecord::default()).unwrap().target();
        assert_eq!(target(None), Target::Global);
        assert_eq!(target(Some(":.eu:")), Target::Context(".eu".into()));
        assert_eq!(target(Some("orders-value")), Target::Subject(QualifiedSubject::new(".", "orders-value")));
        assert_eq!(target(Some(":.eu:orders-value")), Target::Subject(QualifiedSubject::new(".eu", "orders-value")));

        let m = UpdateCompatibility::new(Some(":.eu:"), level(CompatibilityLevel::None)).unwrap();
        match writes_of(&reg, &m).unwrap().as_slice() {
            [Write::PutConfig { scope, .. }] => assert_eq!(*scope, Scope::Context(".eu".into())),
            other => panic!("unexpected plan: {other:?}"),
        }
    }

}
