//! `DELETE /contexts/{context}`.
//!
//! Only an empty context can go, and the default one never can. What it
//! removes is the context's own existence and its settings - no schema state,
//! which is why it is not gated on modes (Confluent does not gate it either).

use super::{Mutation, Plan, ReadView, Target, Write};
use crate::context::{DEFAULT_CONTEXT, WILDCARD_CONTEXT, normalize_context};
use crate::error::{ApiError, ApiResult};
use crate::modegate::Intent;
use crate::store::Scope;

pub struct DeleteContext {
    ctx: String,
}

impl DeleteContext {
    pub fn new(ctx: &str) -> ApiResult<Self> {
        let ctx = normalize_context(ctx).ok_or_else(|| ApiError::invalid_subject(ctx))?;
        if ctx == DEFAULT_CONTEXT || ctx == WILDCARD_CONTEXT {
            return Err(ApiError::operation_not_permitted("The default context cannot be deleted"));
        }
        Ok(Self { ctx })
    }
}

impl Mutation for DeleteContext {
    type Output = ();

    fn target(&self) -> Target {
        Target::Context(self.ctx.clone())
    }

    fn intent(&self) -> Intent {
        Intent::NotSchemaState
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<()>> {
        if !view.reader().list_subject_names(&self.ctx)?.is_empty() {
            return Err(ApiError::context_not_empty(&self.ctx));
        }
        Ok(Plan::new(())
            .write(Write::DeleteContext { ctx: self.ctx.clone() })
            .write(Write::DeleteConfig { scope: Scope::Context(self.ctx.clone()) })
            .write(Write::DeleteMode { scope: Scope::Context(self.ctx.clone()) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RegisterSchemaRequest;
    use crate::mutations::test_support::registry;

    #[test]
    fn empty_context_deletes_its_existence_and_scoped_settings() {
        let (reg, _dir) = registry();
        let plan = DeleteContext::new("empty").unwrap().plan(&ReadView::new(&reg)).unwrap();
        assert!(matches!(plan.writes.as_slice(), [
            Write::DeleteContext { ctx },
            Write::DeleteConfig { scope: Scope::Context(config_ctx) },
            Write::DeleteMode { scope: Scope::Context(mode_ctx) }
        ] if ctx == ".empty" && config_ctx == ".empty" && mode_ctx == ".empty"));

        reg.register(":.busy:item", RegisterSchemaRequest { schema: Some("\"string\"".into()), ..Default::default() }, false).unwrap();
        assert_eq!(DeleteContext::new("busy").unwrap().plan(&ReadView::new(&reg)).err().unwrap().code, 42211);
    }
}
