//! Everything that answers "what is in here": subject and schema listings,
//! versions, the id indexes, and the small reads the exporter and the backup
//! need. Paging and the search limits are Confluent's, and live with the
//! handlers; these return whole answers.

use super::*;

impl Registry {
    /// Contexts matched by a (possibly wildcard) context name.
    pub(super) fn contexts_matching(&self, r: &Reader<'_>, ctx: &str) -> ApiResult<Vec<String>> {
        if ctx == crate::context::WILDCARD_CONTEXT {
            r.list_contexts()
        } else {
            Ok(vec![ctx.to_string()])
        }
    }

    /// Parse a `subjectPrefix` parameter. Default (absent) is every context.
    pub(super) fn parse_prefix(prefix: Option<&str>) -> ApiResult<QualifiedSubject> {
        match prefix {
            None => Ok(QualifiedSubject::new(crate::context::WILDCARD_CONTEXT, "")),
            Some(p) => QualifiedSubject::parse(p),
        }
    }

    pub fn list_subjects(&self, prefix: Option<&str>, deleted: bool, deleted_only: bool) -> ApiResult<Vec<String>> {
        self.list_subjects_in(&self.reader(), prefix, deleted, deleted_only)
    }

    fn list_subjects_in(&self, r: &Reader<'_>, prefix: Option<&str>, deleted: bool, deleted_only: bool) -> ApiResult<Vec<String>> {
        let p = Self::parse_prefix(prefix)?;
        let mut out = Vec::new();
        for ctx in self.contexts_matching(r, &p.context)? {
            for name in r.list_subject_names(&ctx)? {
                if !name.starts_with(&p.subject) {
                    continue;
                }
                let versions = r.list_versions(&ctx, &name)?;
                let live = versions.iter().any(|(_, v)| !v.deleted);
                let include = if deleted_only { !live && !versions.is_empty() } else { live || (deleted && !versions.is_empty()) };
                if include {
                    out.push(qualify(&ctx, &name));
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// `GET /schemas`. With `include_aliases`, each schema lists the subjects
    /// aliasing it, and alias targets outside the prefix are included too
    /// (Confluent's `allVersionsIncludingAliasesWithSubjectPrefix`).
    pub fn list_schemas(
        &self,
        prefix: Option<&str>,
        deleted: bool,
        latest_only: bool,
        include_aliases: bool,
        rule_type: Option<&str>,
    ) -> ApiResult<Vec<SchemaView>> {
        let r = &self.reader();
        let p = Self::parse_prefix(prefix)?;
        let prefix_ctxs = self.contexts_matching(r, &p.context)?;
        let in_prefix = |ctx: &str, name: &str| prefix_ctxs.iter().any(|c| c == ctx) && name.starts_with(&p.subject);

        // alias target (qualified) -> aliasing subjects (qualified), from configs within the prefix
        let mut aliases: std::collections::BTreeMap<String, Vec<String>> = std::collections::BTreeMap::new();
        if include_aliases {
            for (ctx, subject, cfg) in r.subject_configs() {
                let Some(alias) = cfg.alias.filter(|a| !a.is_empty()) else { continue };
                if !in_prefix(&ctx, &subject) {
                    continue;
                }
                let target = QualifiedSubject::parse_subject(&alias)?;
                let target = if alias.starts_with(':') { target } else { QualifiedSubject::new(&ctx, &target.subject) };
                aliases.entry(target.qualified()).or_default().push(qualify(&ctx, &subject));
            }
        }

        let mut subjects: Vec<QualifiedSubject> = Vec::new();
        for ctx in &prefix_ctxs {
            for name in r.list_subject_names(ctx)? {
                if name.starts_with(&p.subject) {
                    subjects.push(QualifiedSubject::new(ctx, &name));
                }
            }
        }
        for target in aliases.keys() {
            let q = QualifiedSubject::parse_subject(target)?;
            if !subjects.contains(&q) {
                subjects.push(q);
            }
        }

        let mut out = Vec::new();
        for q in subjects {
            let versions: Vec<(u32, VersionRecord)> =
                r.list_versions(&q.context, &q.subject)?.into_iter().filter(|(_, v)| deleted || !v.deleted).collect();
            let chosen: Vec<&(u32, VersionRecord)> =
                if latest_only { versions.last().into_iter().collect() } else { versions.iter().collect() };
            for (v, vr) in chosen {
                let mut view = self.entity(r, &q, *v, vr)?.view();
                if let Some(t) = rule_type.filter(|t| !t.is_empty())
                    && !has_rules_with_type(&view.rule_set, t)
                {
                    continue;
                }
                if include_aliases {
                    // Confluent's config store iterates subjects in sorted order.
                    view.aliases = aliases.get(&q.qualified()).map(|a| {
                        let mut a = a.clone();
                        a.sort();
                        a
                    });
                }
                out.push(view);
            }
        }
        out.sort_by(|a, b| a.subject.cmp(&b.subject).then(a.version.cmp(&b.version)));
        Ok(out)
    }

    /// Find the context holding schema `id`. Mirrors Confluent 7.9 exactly
    /// (verified against a live Confluent server):
    ///
    /// | hint                      | where we look                                        |
    /// |---------------------------|------------------------------------------------------|
    /// | none / `:.:`              | default context (any subject using the id)           |
    /// | `foo` / `:.:foo`          | first context (default, then others in order) in     |
    /// |                           | which `foo` itself uses the id; else default context |
    /// |                           | (any subject)                                        |
    /// | `:.ctx:`                  | that context (any subject)                           |
    /// | `:.ctx:foo`               | that context, only if `foo` uses the id              |
    ///
    /// The fallback is what lets a plain deserializer (which passes an
    /// unqualified topic subject) read data whose schema lives in another
    /// context under the same subject name, e.g. one brought in by an exporter.
    fn locate_id(&self, r: &Reader<'_>, id: u32, subject: Option<&str>) -> ApiResult<(String, Arc<SchemaRecord>)> {
        let found = |ctx: &str, name: Option<&str>| -> ApiResult<Option<Arc<SchemaRecord>>> {
            let usages = r.id_usages(ctx, id)?;
            let used = match name {
                None => !usages.is_empty(),
                Some(n) => usages.iter().any(|(s, _)| s == n),
            };
            if used { r.get_schema(ctx, id) } else { Ok(None) }
        };
        let hint = match subject.filter(|s| !s.is_empty()) {
            Some(s) => Some((QualifiedSubject::parse(s)?, s.starts_with(":."))),
            None => None,
        };
        match hint {
            Some((q, true)) if q.context != DEFAULT_CONTEXT => {
                let name = (!q.subject.is_empty()).then_some(q.subject.as_str());
                if let Some(rec) = found(&q.context, name)? {
                    return Ok((q.context, rec));
                }
            }
            hint => {
                // A context where this very subject uses the id wins (default
                // context first, then the rest in order) ...
                if let Some((q, _)) = hint.filter(|(q, _)| !q.subject.is_empty()) {
                    let mut ctxs = vec![DEFAULT_CONTEXT.to_string()];
                    ctxs.extend(r.list_contexts()?.into_iter().filter(|c| c != DEFAULT_CONTEXT));
                    for ctx in ctxs {
                        if let Some(rec) = found(&ctx, Some(&q.subject))? {
                            return Ok((ctx, rec));
                        }
                    }
                }
                // ... otherwise any subject in the default context.
                if let Some(rec) = found(DEFAULT_CONTEXT, None)? {
                    return Ok((DEFAULT_CONTEXT.to_string(), rec));
                }
            }
        }
        Err(ApiError::schema_id_not_found(id))
    }

    pub fn get_schema_by_id(
        &self,
        id: i64,
        subject: Option<&str>,
        fetch_max_id: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaByIdView> {
        let r = &self.reader();
        let uid = u32::try_from(id).ok().filter(|i| *i > 0).ok_or_else(|| ApiError::schema_id_not_found(id))?;
        let (ctx, rec) = self.locate_id(r, uid, subject)?;
        let max_id = if fetch_max_id { Some(r.next_id(&ctx)?.saturating_sub(1)) } else { None };
        Ok(SchemaByIdView {
            schema_type: schema_type_view(rec.schema_type),
            schema: self.formatted(r, &ctx, uid, &rec, format)?,
            references: rec.references.clone(),
            metadata: rec.metadata.clone(),
            rule_set: rec.rule_set.clone(),
            max_id,
        })
    }

    /// `listVersionsForId`: one entry per subject (its latest version with the
    /// id), in the iteration order of Confluent's per-id `ConcurrentHashMap`
    /// (subjects in the order they first used the id).
    pub fn id_versions(&self, id: i64, subject: Option<&str>, deleted: bool) -> ApiResult<Vec<SubjectVersion>> {
        self.id_versions_in(&self.reader(), id, subject, deleted)
    }

    fn id_versions_in(&self, r: &Reader<'_>, id: i64, subject: Option<&str>, deleted: bool) -> ApiResult<Vec<SubjectVersion>> {
        let uid = u32::try_from(id).ok().filter(|i| *i > 0).ok_or_else(ApiError::schema_not_found)?;
        let (ctx, _) = self.locate_id(r, uid, subject).map_err(|_| ApiError::schema_not_found())?;
        // subject -> (latest version with this id, first registration time)
        let mut per_subject: Vec<(String, u32, i64)> = Vec::new();
        for (s, v) in r.id_usages(&ctx, uid)? {
            let Some(vr) = r.get_version(&ctx, &s, v)? else { continue };
            match per_subject.iter_mut().find(|(x, ..)| *x == s) {
                Some(e) => {
                    e.1 = e.1.max(v);
                    e.2 = e.2.min(vr.ts);
                }
                None => per_subject.push((s, v, vr.ts)),
            }
        }
        per_subject.sort_by(|a, b| a.2.cmp(&b.2).then(a.1.cmp(&b.1)).then(a.0.cmp(&b.0)));
        let names: Vec<String> = per_subject.iter().map(|(s, ..)| qualify(&ctx, s)).collect();
        let mut out = Vec::new();
        for name in schema::java_order::chm_order(&names) {
            let q = QualifiedSubject::parse(&name)?;
            let (_, v, _) = per_subject.iter().find(|(s, ..)| *s == q.subject).expect("present");
            if deleted || r.get_version(&ctx, &q.subject, *v)?.is_some_and(|vr| !vr.deleted) {
                out.push(SubjectVersion { subject: name, version: *v });
            }
        }
        Ok(out)
    }

    pub fn id_subjects(&self, id: i64, subject: Option<&str>, deleted: bool) -> ApiResult<Vec<String>> {
        let r = &self.reader();
        Ok(self.id_versions_in(r, id, subject, deleted)?.into_iter().map(|sv| sv.subject).collect())
    }

    /// Ids of live schemas referencing (subject, version). Confluent drops a
    /// referrer from this index when it is soft-deleted.
    pub(super) fn live_referrers(&self, r: &Reader<'_>, ctx: &str, subject: &str, version: u32) -> ApiResult<Vec<u32>> {
        let mut out = Vec::new();
        for id in r.referenced_by(ctx, subject, version)? {
            let mut used = false;
            for (s, v) in r.id_usages(ctx, id)? {
                if r.get_version(ctx, &s, v)?.is_some_and(|vr| !vr.deleted) {
                    used = true;
                    break;
                }
            }
            if used {
                out.push(id);
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }


    pub(crate) fn check_not_referenced(&self, r: &Reader<'_>, q: &QualifiedSubject, version: u32) -> ApiResult<()> {
        if !self.live_referrers(r, &q.context, &q.subject, version)?.is_empty() {
            return Err(ApiError::reference_exists(format!(
                "One or more references exist to the schema {{magic=1,keytype=SCHEMA,subject={},version={version}}}.",
                q.qualified()
            )));
        }
        Ok(())
    }

    /// `hasSubjects(subject, lookupDeleted)`: the subject has a (live) version;
    /// a context-only name (`:.ctx:`) matches any subject in the context.
    pub(crate) fn has_subjects(&self, r: &Reader<'_>, q: &QualifiedSubject, deleted: bool) -> ApiResult<bool> {
        let any = |vs: Vec<(u32, VersionRecord)>| vs.iter().any(|(_, v)| deleted || !v.deleted);
        if any(r.list_versions(&q.context, &q.subject)?) {
            return Ok(true);
        }
        if q.is_context_only() {
            for name in r.list_subject_names(&q.context)? {
                if any(r.list_versions(&q.context, &name)?) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// `hasSubjects(null, ..)`: any subject anywhere.
    pub(crate) fn has_any_subject(&self, r: &Reader<'_>, deleted: bool) -> ApiResult<bool> {
        for ctx in r.list_contexts()? {
            for name in r.list_subject_names(&ctx)? {
                if r.list_versions(&ctx, &name)?.iter().any(|(_, v)| deleted || !v.deleted) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }


    /// Every (context, subject, version) currently stored, oldest version
    /// first: what an exporter replays when the change log no longer reaches
    /// back far enough.
    pub fn all_subject_versions(&self) -> ApiResult<Vec<(String, String, u32)>> {
        let r = &self.reader();
        let mut out = Vec::new();
        for ctx in r.list_contexts()? {
            for subject in r.list_subject_names(&ctx)? {
                for (v, _) in r.list_versions(&ctx, &subject)? {
                    out.push((ctx.clone(), subject.clone(), v));
                }
            }
        }
        Ok(out)
    }

    /// Everything the exporter needs to replay one subject-version.
    pub fn export_payload(&self, ctx: &str, subject: &str, version: u32) -> ApiResult<Option<(VersionRecord, Arc<SchemaRecord>)>> {
        let r = &self.reader();
        let Some(vr) = r.get_version(ctx, subject, version)? else { return Ok(None) };
        let rec = r.get_schema(ctx, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        Ok(Some((vr, rec)))
    }

    /// The `alias` configured for exactly this subject (`AliasFilter`).
    pub fn alias_of(&self, subject: &str) -> Option<String> {
        let r = &self.reader();
        if !r.has_aliases() {
            return None;
        }
        let q = QualifiedSubject::parse(subject).ok()?;
        r.get_config(&Self::scope_for(&q)).ok()??.alias
    }

}
