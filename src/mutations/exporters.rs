//! `/exporters`: create, update, delete, and pause / resume / reset.
//!
//! Exporter records are registry metadata rather than schema state, so they
//! are not gated on modes (Confluent does not gate them either) - but they are
//! still mutations, so they are planned, locked and applied like everything
//! else. Their lock is their own: the worker writes its cursor after every
//! batch, and an export must not queue behind a registration.
//!
//! Validation lives here because it is about the request, not about the
//! registry's state: a context type that maps several source contexts onto one
//! destination cannot preserve ids, and an exporter that cannot preserve ids
//! is not an exporter.

use serde_json::{Map, Value};

use super::{Gate, Mutation, Plan, ReadView, Target, Write};
use crate::context::{DEFAULT_CONTEXT, QualifiedSubject};
use crate::error::{ApiError, ApiResult};
use crate::model::{ExporterInfo, ExporterRecord, ExporterState, ExporterUpdateRequest};
use crate::modegate::Intent;

/// The shape rules an exporter has to satisfy, whatever put it there.
pub fn validate(info: &ExporterInfo) -> ApiResult<()> {
    let valid_name = !info.name.is_empty()
        && info.name.len() <= 256
        && info.name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    if !valid_name {
        return Err(ApiError::invalid_exporter(format!("Invalid exporter name '{}'", info.name)));
    }
    match info.context_type.as_str() {
        "AUTO" | "NONE" | "DEFAULT" => {}
        "CUSTOM" => {
            if info.context.as_deref().is_none_or(|c| crate::context::normalize_context(c).is_none()) {
                return Err(ApiError::invalid_exporter("Context type CUSTOM requires a valid 'context'"));
            }
        }
        other => return Err(ApiError::invalid_exporter(format!("Invalid context type '{other}'"))),
    }
    if !info.config.get("schema.registry.url").and_then(Value::as_str).is_some_and(|s| !s.is_empty()) {
        return Err(ApiError::invalid_exporter("Missing required config 'schema.registry.url'"));
    }
    if info.subjects.is_empty() {
        return Err(ApiError::invalid_exporter("Exporter 'subjects' must not be empty"));
    }
    // CUSTOM and DEFAULT map every source context onto one destination
    // context. Since ids are per context (each starts at 1), two source
    // contexts would collide there and the import could not keep ids.
    if matches!(info.context_type.as_str(), "CUSTOM" | "DEFAULT") {
        let mut contexts: Vec<String> = Vec::new();
        for p in &info.subjects {
            let q = QualifiedSubject::parse(p)?;
            if q.is_wildcard() {
                return Err(ApiError::invalid_exporter(format!(
                    "Exporter '{}' with contextType {} cannot use the context wildcard ':*:': ids are per context and could not be preserved in '{}'",
                    info.name,
                    info.context_type,
                    info.context.as_deref().unwrap_or(DEFAULT_CONTEXT)
                )));
            }
            if !contexts.contains(&q.context) {
                contexts.push(q.context);
            }
        }
        if contexts.len() > 1 {
            return Err(ApiError::invalid_exporter(format!(
                "Exporter '{}' with contextType {} would merge contexts {} into one destination context; ids are per context and could not be preserved",
                info.name,
                info.context_type,
                contexts.join(", ")
            )));
        }
    }
    Ok(())
}

fn exporter_plan(rec: ExporterRecord) -> Plan<String> {
    let name = rec.info.name.clone();
    Plan::new(name).write(Write::PutExporter { rec })
}

/// `POST /exporters`
pub struct CreateExporter {
    req: ExporterUpdateRequest,
}

impl CreateExporter {
    pub fn new(req: ExporterUpdateRequest) -> Self {
        Self { req }
    }
}

impl Mutation for CreateExporter {
    type Output = String;

    fn target(&self) -> Target {
        Target::Exporters
    }

    fn intent(&self) -> Intent {
        Intent::NotSchemaState
    }

    /// The name has to be free, and that is state.
    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<String>> {
        let name = self.req.name.clone().unwrap_or_default();
        if view.exporter(&name)?.is_some() {
            return Err(ApiError::exporter_exists(&name));
        }
        let info = ExporterInfo {
            name,
            subjects: self.req.subjects.clone().unwrap_or_else(|| vec!["*".into()]),
            context_type: self.req.context_type.clone().map(|c| c.to_ascii_uppercase()).unwrap_or_else(|| "AUTO".into()),
            context: self.req.context.clone(),
            subject_rename_format: self.req.subject_rename_format.clone(),
            config: self.req.config.clone().unwrap_or_default(),
        };
        validate(&info)?;
        Ok(exporter_plan(ExporterRecord {
            info,
            state: ExporterState::Running,
            offset: 0,
            ts: view.now(),
            trace: String::new(),
        }))
    }
}

/// `PUT /exporters/{name}` and `PUT /exporters/{name}/config`: present fields
/// replace what is stored, and config entries are merged into it.
pub struct UpdateExporter {
    name: String,
    req: ExporterUpdateRequest,
}

impl UpdateExporter {
    pub fn new(name: &str, req: ExporterUpdateRequest) -> Self {
        Self { name: name.to_string(), req }
    }

    pub fn config_only(name: &str, config: Map<String, Value>) -> Self {
        Self::new(name, ExporterUpdateRequest { config: Some(config), ..Default::default() })
    }
}

impl Mutation for UpdateExporter {
    type Output = String;

    fn target(&self) -> Target {
        Target::Exporters
    }

    fn intent(&self) -> Intent {
        Intent::NotSchemaState
    }

    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<String>> {
        let mut rec = view.exporter(&self.name)?.ok_or_else(|| ApiError::exporter_not_found(&self.name))?;
        if let Some(s) = self.req.subjects.clone() {
            rec.info.subjects = s;
        }
        if let Some(c) = self.req.context_type.clone() {
            rec.info.context_type = c.to_ascii_uppercase();
        }
        if self.req.context.is_some() {
            rec.info.context = self.req.context.clone();
        }
        if self.req.subject_rename_format.is_some() {
            rec.info.subject_rename_format = self.req.subject_rename_format.clone();
        }
        if let Some(cfg) = self.req.config.clone() {
            rec.info.config.extend(cfg);
        }
        validate(&rec.info)?;
        rec.ts = view.now();
        Ok(exporter_plan(rec))
    }
}

/// `DELETE /exporters/{name}`
pub struct DeleteExporter {
    name: String,
}

impl DeleteExporter {
    pub fn new(name: &str) -> Self {
        Self { name: name.to_string() }
    }
}

impl Mutation for DeleteExporter {
    type Output = ();

    fn target(&self) -> Target {
        Target::Exporters
    }

    fn intent(&self) -> Intent {
        Intent::NotSchemaState
    }

    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<()>> {
        view.exporter(&self.name)?.ok_or_else(|| ApiError::exporter_not_found(&self.name))?;
        Ok(Plan::new(()).write(Write::DeleteExporter { name: self.name.clone() }))
    }
}

/// `PUT /exporters/{name}/pause|resume|reset`.
pub struct TransitionExporter {
    name: String,
    action: String,
}

impl TransitionExporter {
    pub fn new(name: &str, action: &str) -> Self {
        Self { name: name.to_string(), action: action.to_string() }
    }
}

impl Mutation for TransitionExporter {
    type Output = String;

    fn target(&self) -> Target {
        Target::Exporters
    }

    fn intent(&self) -> Intent {
        Intent::NotSchemaState
    }

    fn gate(&self) -> Gate {
        Gate::AfterPlan
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<String>> {
        let mut rec = view.exporter(&self.name)?.ok_or_else(|| ApiError::exporter_not_found(&self.name))?;
        match self.action.as_str() {
            "pause" => rec.state = ExporterState::Paused,
            "resume" => {
                // A failed exporter conflicts with what the destination holds;
                // picking up where it stopped would hit the same wall.
                if rec.state == ExporterState::Failed {
                    return Err(ApiError::operation_not_permitted(format!(
                        "Exporter {} has failed and cannot be resumed; reset it to start over. Last error: {}",
                        self.name, rec.trace
                    )));
                }
                rec.state = ExporterState::Running;
                rec.trace.clear();
            }
            "reset" => {
                // Start over from the beginning of the log, whatever state it
                // was in: this is the way out of Failed.
                rec.offset = 0;
                rec.trace.clear();
                if rec.state != ExporterState::Paused {
                    rec.state = ExporterState::Running;
                }
            }
            other => return Err(ApiError::unprocessable(format!("unknown action {other}"))),
        }
        rec.ts = view.now();
        Ok(exporter_plan(rec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(context_type: &str, context: Option<&str>, subjects: &[&str]) -> ExporterInfo {
        ExporterInfo {
            name: "x".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            context_type: context_type.into(),
            context: context.map(String::from),
            subject_rename_format: None,
            config: serde_json::from_value(serde_json::json!({"schema.registry.url": "http://d"})).expect("map"),
        }
    }

    #[test]
    fn an_exporter_that_could_not_keep_ids_is_refused() {
        // One source context onto one destination context: fine.
        assert!(validate(&info("CUSTOM", Some(".dr"), &[":.eu:*"])).is_ok());
        // Several, or every context: their ids would collide over there.
        let e = validate(&info("CUSTOM", Some(".dr"), &[":.eu:*", ":.us:*"])).unwrap_err();
        assert!(e.message.contains("would merge contexts"), "{}", e.message);
        let e = validate(&info("DEFAULT", None, &[":*:*"])).unwrap_err();
        assert!(e.message.contains("wildcard"), "{}", e.message);
        // NONE keeps each source context, so it may span them.
        assert!(validate(&info("NONE", None, &[":*:*"])).is_ok());
    }

    #[test]
    fn the_shape_rules_are_checked_before_anything_is_stored() {
        let mut bad = info("CUSTOM", None, &["*"]);
        assert!(validate(&bad).unwrap_err().message.contains("requires a valid 'context'"));
        bad = info("AUTO", None, &[]);
        assert!(validate(&bad).unwrap_err().message.contains("must not be empty"));
        bad = info("SIDEWAYS", None, &["*"]);
        assert!(validate(&bad).unwrap_err().message.contains("Invalid context type"));
        bad = info("AUTO", None, &["*"]);
        bad.name = "not a name".into();
        assert!(validate(&bad).unwrap_err().message.contains("Invalid exporter name"));
        bad = info("AUTO", None, &["*"]);
        bad.config.clear();
        assert!(validate(&bad).unwrap_err().message.contains("schema.registry.url"));
    }
}
