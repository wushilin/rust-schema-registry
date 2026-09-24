//! Finding a schema, and deciding whether a new one may follow it.
//!
//! `lookup` answers "is this already registered, and as what" - the check a
//! serializer makes constantly, and the one registration makes before it takes
//! a lock. Compatibility picks the versions a level cares about and asks the
//! format to compare them.

use super::*;

impl Registry {
    /// `POST /subjects/{subject}` (`SubjectsResource#lookUpSchemaUnderSubject`).
    pub fn lookup(
        &self,
        subject: &str,
        req: RegisterSchemaRequest,
        normalize: bool,
        deleted: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaView> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let normalize = normalize || self.normalize_in_scope(r, &q)?;
        let d = Draft::from(req);
        let Some(e) = self.lookup_using_contexts(r, &q, &d, normalize, deleted)? else {
            return Err(if self.has_subjects(r, &q, deleted)? { ApiError::schema_not_found() } else { ApiError::subject_not_found(&q.qualified()) });
        };
        let mut view = e.view();
        if format.is_some_and(|f| !f.trim().is_empty()) {
            let eq = QualifiedSubject::parse(&view.subject)?;
            if let Some(rec) = r.get_schema(&eq.context, view.id)? {
                view.schema = self.formatted(r, &eq.context, view.id, &rec, format)?;
            }
        }
        Ok(view)
    }

    /// `lookUpSchemaUnderSubject`.
    pub(super) fn lookup_under_subject(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        d: &Draft,
        normalize: bool,
        lookup_deleted: bool,
        latest_only: bool,
    ) -> ApiResult<Option<Entity>> {
        let canon = self.canonicalize(r, q, d, false, normalize)?;
        if let Some(c) = &canon
            && !latest_only
            && let Some((id, Some((v, vr)))) = self.hash_lookup(r, q, c)?
            && (lookup_deleted || !vr.deleted)
        {
            return Ok(Some(Entity::from_canon(q, v, id, c)));
        }
        let Some(c) = &canon else { return Ok(None) };
        let versions = r.list_versions(&q.context, &q.subject)?;
        if latest_only {
            if let Some((v, vr)) = versions.iter().rev().find(|(_, x)| !x.deleted) {
                let prev = self.entity(r, q, *v, vr)?;
                if can_lookup(c, &prev) {
                    return Ok(Some(prev));
                }
            }
        } else {
            for (v, vr) in versions.iter().rev() {
                if vr.deleted && !lookup_deleted {
                    continue;
                }
                let prev = self.entity(r, q, *v, vr)?;
                if can_lookup(c, &prev) {
                    return Ok(Some(prev));
                }
            }
        }
        Ok(None)
    }

    /// `lookUpSchemaUnderSubjectUsingContexts`: an unqualified subject is
    /// also looked up under the same name in every other context.
    fn lookup_using_contexts(&self, r: &Reader<'_>, q: &QualifiedSubject, d: &Draft, normalize: bool, deleted: bool) -> ApiResult<Option<Entity>> {
        if let Some(e) = self.lookup_under_subject(r, q, d, normalize, deleted, false)? {
            return Ok(Some(e));
        }
        if q.context != DEFAULT_CONTEXT {
            return Ok(None);
        }
        for ctx in r.list_contexts()?.into_iter().filter(|c| c != DEFAULT_CONTEXT) {
            let cq = QualifiedSubject::new(&ctx, &q.subject);
            match self.lookup_under_subject(r, &cq, d, normalize, deleted, false) {
                Ok(Some(e)) => return Ok(Some(e)),
                Ok(None) => {}
                Err(e) if e.code == 42201 => {}
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }

    /// `get(subject, version, returnDeleted)`: `latest` is the newest live version.
    pub(crate) fn get_exact(&self, r: &Reader<'_>, q: &QualifiedSubject, spec: VersionSpec, deleted: bool) -> ApiResult<Option<(u32, VersionRecord)>> {
        let vs = r.list_versions(&q.context, &q.subject)?;
        Ok(match spec {
            VersionSpec::Latest => vs.into_iter().rev().find(|(_, v)| !v.deleted),
            VersionSpec::Exact(n) => vs.into_iter().find(|(v, x)| *v == n && (deleted || !x.deleted)),
        })
    }

    /// `getUsingContexts`: an unqualified subject not found in the default
    /// context is looked up under the same name in every other context.
    pub(super) fn get_using_contexts(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        spec: VersionSpec,
        deleted: bool,
    ) -> ApiResult<Option<(QualifiedSubject, u32, VersionRecord)>> {
        if let Some((v, vr)) = self.get_exact(r, q, spec, deleted)? {
            return Ok(Some((q.clone(), v, vr)));
        }
        if q.context != DEFAULT_CONTEXT {
            return Ok(None);
        }
        for ctx in r.list_contexts()?.into_iter().filter(|c| c != DEFAULT_CONTEXT) {
            let cq = QualifiedSubject::new(&ctx, &q.subject);
            if let Some((v, vr)) = self.get_exact(r, &cq, spec, deleted)? {
                return Ok(Some((cq, v, vr)));
            }
        }
        Ok(None)
    }

    /// `POST /compatibility/subjects/{subject}/versions[/{version}]`
    /// (`CompatibilityResource` then `isCompatible`). With `verbose`, an
    /// invalid schema is reported as an incompatibility message.
    pub fn test_compatibility(
        &self,
        subject: &str,
        version: Option<VersionSpec>,
        req: RegisterSchemaRequest,
        normalize: bool,
        verbose: bool,
    ) -> ApiResult<Vec<String>> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let previous: Vec<(u32, VersionRecord)> = match version {
            Some(spec) => match self.get_exact(r, &q, spec, false)? {
                Some(found) => vec![found],
                None if spec == VersionSpec::Latest => Vec::new(),
                None => return Err(ApiError::version_not_found(spec)),
            },
            None => r.list_versions(&q.context, &q.subject)?.into_iter().rev().filter(|(_, v)| !v.deleted).collect(),
        };
        let normalize = normalize || self.normalize_in_scope(r, &q)?;
        let d = Draft::from(req);
        let result = (|| {
            let c = self.canonicalize(r, &q, &d, true, normalize)?.ok_or_else(|| ApiError::new(42201, "Empty schema"))?;
            let cfg = self.config_in_scope(r, &q)?;
            let mut olds = Vec::with_capacity(previous.len());
            for (v, vr) in &previous {
                let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
                if let Some(group) = cfg.compatibility_group.as_deref()
                    && metadata_property(&rec.metadata, group) != metadata_property(&c.metadata, group)
                {
                    continue;
                }
                olds.push(OldVersion { version: *v, id: vr.id, rec });
            }
            let level = cfg.compatibility_level.unwrap_or(self.default_compatibility);
            self.check_compatibility(r, &q.context, &c.parsed, &olds, level, &cfg)
        })();
        match result {
            Err(e) if e.code == 42201 && verbose => Ok(vec![e.message]),
            other => other,
        }
    }

    /// Live versions of a subject that participate in compatibility checks, newest first.
    pub(crate) fn compat_candidates(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        cfg: &ConfigRecord,
        new_metadata: &Option<Value>,
    ) -> ApiResult<Vec<OldVersion>> {
        let mut out = Vec::new();
        for (v, vr) in r.list_versions(&q.context, &q.subject)?.into_iter().rev() {
            if vr.deleted {
                continue;
            }
            let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            if let Some(group) = cfg.compatibility_group.as_deref()
                && metadata_property(&rec.metadata, group) != metadata_property(new_metadata, group)
            {
                continue;
            }
            out.push(OldVersion { version: v, id: vr.id, rec });
        }
        Ok(out)
    }

    /// Check `new` against `olds` (newest first) at `level`, with the server's
    /// trailer (`{validateFields: ..., compatibility: ...}`) on failure.
    pub(crate) fn check_compatibility(
        &self,
        r: &Reader<'_>,
        ctx: &str,
        new: &ParsedSchema,
        olds: &[OldVersion],
        level: CompatibilityLevel,
        cfg: &ConfigRecord,
    ) -> ApiResult<Vec<String>> {
        if level == CompatibilityLevel::None || olds.is_empty() {
            return Ok(Vec::new());
        }
        let needed = if level.transitive() { olds.len() } else { 1 };
        let mut parsed = Vec::with_capacity(needed);
        for OldVersion { version, id, rec } in &olds[..needed] {
            parsed.push((*version, self.parse_record(r, ctx, *id, rec)?));
        }
        let previous: Vec<schema::Previous<'_>> =
            parsed.iter().map(|(v, p)| schema::Previous { version: *v, schema: p.as_ref() }).collect();
        let mut msgs = schema::check_level(new, &previous, level);
        if !msgs.is_empty() {
            msgs.push(format!(
                "{{validateFields: '{}', compatibility: '{}'}}",
                cfg.validate_fields.unwrap_or(false),
                level.as_str()
            ));
        }
        Ok(msgs)
    }

    /// `GET /subjects/{subject}/versions/{version}` (`getSchemaByVersion`).
    pub fn get_version_formatted(
        &self,
        subject: &str,
        spec: VersionSpec,
        deleted: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaView> {
        let r = &self.reader();
        let (cq, v, vr) = self.version_or_error(r, subject, spec, deleted)?;
        let rec = r.get_schema(&cq.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        let mut view = Entity::stored(&cq, v, vr.id, &rec).view();
        if format.is_some_and(|f| !f.trim().is_empty()) {
            view.schema = self.formatted(r, &cq.context, vr.id, &rec, format)?;
        }
        Ok(view)
    }

    pub(super) fn version_or_error(&self, r: &Reader<'_>, subject: &str, spec: VersionSpec, deleted: bool) -> ApiResult<(QualifiedSubject, u32, VersionRecord)> {
        let q = QualifiedSubject::parse(subject)?;
        match self.get_using_contexts(r, &q, spec, deleted)? {
            Some(found) => Ok(found),
            None if !self.has_subjects(r, &q, deleted)? => Err(ApiError::subject_not_found(&q.qualified())),
            None => Err(ApiError::version_not_found(spec)),
        }
    }

    /// `GET /subjects/{subject}/metadata?key=k&value=v...`: the newest version
    /// whose metadata properties contain every given pair.
    pub fn latest_with_metadata(
        &self,
        subject: &str,
        pairs: &[(String, String)],
        deleted: bool,
        format: Option<&str>,
    ) -> ApiResult<SchemaView> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        let wanted: std::collections::HashMap<&str, &str> = pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        for (v, vr) in r.list_versions(&q.context, &q.subject)?.iter().rev() {
            if vr.deleted && !deleted {
                continue;
            }
            let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
            let Some(props) = rec.metadata.as_ref().and_then(|m| m.get("properties")).and_then(Value::as_object) else { continue };
            if wanted.iter().all(|(k, val)| props.get(*k).and_then(Value::as_str) == Some(*val)) {
                let mut view = Entity::stored(&q, *v, vr.id, &rec).view();
                if format.is_some_and(|f| !f.trim().is_empty()) {
                    view.schema = self.formatted(r, &q.context, vr.id, &rec, format)?;
                }
                return Ok(view);
            }
        }
        Err(if self.has_subjects(r, &q, deleted)? { ApiError::schema_not_found() } else { ApiError::subject_not_found(&q.qualified()) })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn get_version(&self, subject: &str, spec: VersionSpec, deleted: bool) -> ApiResult<SchemaView> {
        self.get_version_formatted(subject, spec, deleted, None)
    }

    pub fn list_versions(&self, subject: &str, deleted: bool, deleted_only: bool) -> ApiResult<Vec<u32>> {
        let r = &self.reader();
        let q = QualifiedSubject::parse(subject)?;
        if !self.has_subjects(r, &q, deleted || deleted_only)? {
            return Err(ApiError::subject_not_found(&q.qualified()));
        }
        Ok(r.list_versions(&q.context, &q.subject)?
            .iter()
            .filter(|(_, v)| if deleted_only { v.deleted } else { deleted || !v.deleted })
            .map(|(v, _)| *v)
            .collect())
    }
}
