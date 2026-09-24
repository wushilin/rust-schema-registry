//! Read-only projections for the admin console: one row per subject, one per
//! context, and everything about a single subject.
//!
//! They exist because the console would otherwise make a request per subject
//! to say the same things. Nothing here decides anything - what a caller may
//! see is `authz`, and the caller's predicate is passed in.

use super::*;

impl Registry {
    /// One row per subject for the admin UI: how many versions it has, which
    /// compatibility level and mode apply, and where those come from.
    /// `visible` decides which subjects the caller is shown: the admin UI
    /// lists exactly the subjects that caller administers.
    pub fn admin_overview(
        &self,
        prefix: Option<&str>,
        deleted: bool,
        limit: usize,
        visible: &dyn Fn(&str, &str) -> bool,
    ) -> ApiResult<Value> {
        let r = &self.reader();
        let p = Self::parse_prefix(prefix)?;
        let contexts = self.contexts_matching(r, &p.context)?;
        let mut rows = Vec::new();
        let (mut total, mut total_versions) = (0usize, 0usize);
        for ctx in &contexts {
            for name in r.list_subject_names(ctx)? {
                if !name.starts_with(&p.subject) {
                    continue;
                }
                if !visible(ctx, &name) {
                    continue;
                }
                let q = QualifiedSubject::new(ctx, &name);
                let versions = r.list_versions(ctx, &name)?;
                let live: Vec<&(u32, VersionRecord)> = versions.iter().filter(|(_, v)| !v.deleted).collect();
                if live.is_empty() && !deleted {
                    continue;
                }
                total += 1;
                total_versions += versions.len();
                if rows.len() >= limit {
                    continue;
                }
                let own = r.get_config(&Self::scope_for(&q))?;
                let cfg = self.config_in_scope(r, &q)?;
                let scope = if own.is_some() {
                    "subject"
                } else if ctx != DEFAULT_CONTEXT && r.get_config(&Scope::Context(ctx.clone()))?.is_some() {
                    "context"
                } else if r.get_config(&Scope::Global)?.is_some() {
                    "global"
                } else {
                    "default"
                };
                let own_mode = r.get_mode(&Self::scope_for(&q))?;
                let latest = live.last().copied().or_else(|| versions.last());
                let schema_type = match latest {
                    Some((_, vr)) => r.get_schema(ctx, vr.id)?.map(|rec| rec.schema_type.as_str()),
                    None => None,
                };
                rows.push(json!({
                    "subject": q.qualified(),
                    "context": ctx,
                    "versions": versions.len(),
                    "deletedVersions": versions.len() - live.len(),
                    "deleted": live.is_empty(),
                    "latestVersion": latest.map(|(v, _)| *v),
                    "latestId": latest.map(|(_, vr)| vr.id),
                    "schemaType": schema_type,
                    "compatibility": cfg.compatibility_level.unwrap_or(self.default_compatibility).as_str(),
                    "compatibilityFrom": scope,
                    "normalize": cfg.normalize == Some(true),
                    "mode": self.mode_in_scope(r, &q)?.as_str(),
                    "modeFrom": if own_mode.is_some() { "subject" } else { "inherited" },
                    "alias": own.and_then(|c| c.alias),
                }));
            }
        }
        rows.sort_by(|a, b| a["subject"].as_str().cmp(&b["subject"].as_str()));
        let exporters: Vec<Value> = self
            .store
            .list_exporters()?
            .into_iter()
            .map(|e| {
                json!({
                    "name": e.info.name,
                    "state": e.state,
                    "offset": e.offset,
                    "ts": e.ts,
                    "trace": e.trace,
                    "subjects": e.info.subjects,
                    "contextType": e.info.context_type,
                    "context": e.info.context,
                    "subjectRenameFormat": e.info.subject_rename_format,
                    "config": e.info.config,
                })
            })
            .collect();
        Ok(json!({
            "container": self.container().as_str(),
            "clusterId": self.cluster_id,
            "version": env!("CARGO_PKG_VERSION"),
            "contexts": self.admin_contexts(r, visible)?,
            "global": {
                "compatibility": self.config_of(r, None)?.and_then(|c| c.compatibility_level).unwrap_or(self.default_compatibility).as_str(),
                "normalize": self.normalize_default,
                "mode": self.global_mode_in(r)?.as_str(),
            },
            "counts": { "subjects": total, "versions": total_versions, "shown": rows.len() },
            "exporters": exporters,
            "subjects": rows,
        }))
    }

    /// One row per context: how much it holds and what is configured on it.
    fn admin_contexts(&self, r: &Reader<'_>, visible: &dyn Fn(&str, &str) -> bool) -> ApiResult<Vec<Value>> {
        let global_mode = self.global_mode_in(r)?;
        let mut out = Vec::new();
        for ctx in r.list_contexts()? {
            let (mut subjects, mut deleted_subjects, mut versions) = (0usize, 0usize, 0usize);
            for name in r.list_subject_names(&ctx)? {
                if !visible(&ctx, &name) {
                    continue;
                }
                let vs = r.list_versions(&ctx, &name)?;
                versions += vs.len();
                if vs.iter().any(|(_, v)| !v.deleted) {
                    subjects += 1;
                } else {
                    deleted_subjects += 1;
                }
            }
            // The scope a context's own settings live in: the default context
            // has none of its own - it reads the global ones.
            let own_scope = if ctx == DEFAULT_CONTEXT { Scope::Global } else { Scope::Context(ctx.clone()) };
            let cfg = r.get_config(&own_scope)?;
            let mode = r.get_mode(&own_scope)?;
            // A context without configuration of its own falls back to the
            // server default, not to the global config: settings are resolved
            // by first match, never merged.
            let effective = cfg.as_ref().and_then(|c| c.compatibility_level).unwrap_or(self.default_compatibility);
            // A context the caller can see nothing in is not listed - unless
            // they administer the whole context, which includes the empty ones.
            if subjects + deleted_subjects == 0 && !visible(&ctx, "") {
                continue;
            }
            out.push(json!({
                "name": ctx,
                "subjects": subjects,
                "deletedSubjects": deleted_subjects,
                "versions": versions,
                "compatibility": effective.as_str(),
                "compatibilityFrom": if cfg.is_some() { "own" } else { "default" },
                "normalize": cfg.as_ref().map(|c| c.normalize.unwrap_or(self.normalize_default)).unwrap_or(self.normalize_default),
                "mode": if global_mode == Mode::ReadonlyOverride {
                    global_mode.as_str()
                } else {
                    mode.unwrap_or(Mode::Readwrite).as_str()
                },
                "modeFrom": if mode.is_some() { "own" } else { "default" },
            }));
        }
        Ok(out)
    }

    /// Every version of one subject, with the schema itself, for the admin UI.
    pub fn admin_subject(&self, subject: &str) -> ApiResult<Value> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let versions = r.list_versions(&q.context, &q.subject)?;
        if versions.is_empty() {
            return Err(ApiError::subject_not_found(&q.qualified()));
        }
        let mut out = Vec::new();
        for (v, vr) in &versions {
            let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            out.push(json!({
                "version": v,
                "id": vr.id,
                "deleted": vr.deleted,
                "registeredAt": vr.ts,
                "schemaType": rec.schema_type.as_str(),
                "schema": rec.schema,
                "references": rec.references,
                "metadata": rec.metadata,
                "ruleSet": rec.rule_set,
                "referencedBy": self.live_referrers(r, &q.context, &q.subject, *v)?,
            }));
        }
        let cfg = self.config_in_scope(r, &q)?;
        Ok(json!({
            "subject": q.qualified(),
            "config": r.get_config(&Self::scope_for(&q))?,
            "effectiveCompatibility": cfg.compatibility_level.unwrap_or(self.default_compatibility).as_str(),
            "mode": self.mode_in_scope(r, &q)?.as_str(),
            "versions": out,
        }))
    }
}
