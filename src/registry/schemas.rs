//! Turning a request's schema into something storable, and a stored schema
//! into something usable: canonical and normalized forms, reference
//! resolution, the content hash Confluent indexes by, and the parse cache.
//!
//! The cache keys are the subtle part - see `parse_record`.

use super::*;

impl Registry {
    /// Parse a request schema, memoized by (type, text, resolved references).
    pub(super) fn parse_cached(
        &self,
        schema_type: SchemaType,
        text: &str,
        refs: &[ResolvedRef],
        validate_defaults: bool,
    ) -> Result<Arc<ParsedSchema>, schema::SchemaError> {
        let mut h = Sha256::new();
        h.update(schema_type.as_str());
        h.update([validate_defaults as u8]);
        h.update(text);
        for r in refs {
            h.update([0]);
            h.update(&r.name);
            h.update([1]);
            h.update(&r.schema);
        }
        let key: [u8; 32] = h.finalize().into();
        if let Some(p) = self.parsed_by_text.get(&key) {
            return Ok(p);
        }
        let parsed = Arc::new(schema::parse_with(schema_type, text, refs, validate_defaults)?);
        self.parsed_by_text.insert(key, parsed.clone());
        Ok(parsed)
    }

    /// Parse, validate and (optionally) normalize a request schema:
    /// `canonicalizeSchema`. `None` for an empty schema.
    pub(crate) fn canonicalize(&self, r: &Reader<'_>, q: &QualifiedSubject, d: &Draft, is_new: bool, normalize: bool) -> ApiResult<Option<Canon>> {
        let Some(text) = d.schema.as_deref().filter(|s| !s.trim().is_empty()) else { return Ok(None) };
        let schema_type = SchemaType::parse(d.schema_type.as_deref())?;
        let (refs, resolved, targets) = self
            .resolve_references(r, &q.context, &d.refs)
            .map_err(|detail| d.invalid(q, schema::ErrorKind::Parse, &detail))?;
        let parsed = self
            .parse_cached(schema_type, text, &resolved, normalize && is_new)
            .map_err(|e| d.invalid(q, e.kind, &e.message))?;
        if normalize && let Some(e) = &parsed.normalize_error {
            return Err(d.invalid(q, schema::ErrorKind::Validate, e));
        }
        let text = if normalize { parsed.normalized.clone() } else { parsed.canonical.clone() };
        Ok(Some(Canon { schema_type, text, refs, targets, metadata: d.metadata.clone(), rule_set: d.rule_set.clone(), parsed }))
    }

    /// Confluent's `lookupCache.schemaIdAndSubjects(schema)`: the id holding
    /// this exact content (text, references, metadata, rules) in the context,
    /// if any subject-version still uses it, and this subject's version of it.
    pub(crate) fn hash_lookup(&self, r: &Reader<'_>, q: &QualifiedSubject, c: &Canon) -> ApiResult<Option<(u32, Option<(u32, VersionRecord)>)>> {
        let Some(id) = r.id_for_fingerprint(&q.context, &c.fingerprint())? else { return Ok(None) };
        let usages = r.id_usages(&q.context, id)?;
        if usages.is_empty() {
            return Ok(None);
        }
        let v = usages.iter().filter(|(s, _)| *s == q.subject).map(|(_, v)| *v).max();
        let sv = match v {
            Some(v) => r.get_version(&q.context, &q.subject, v)?.map(|vr| (v, vr)),
            None => None,
        };
        Ok(Some((id, sv)))
    }

    pub(crate) fn entity(&self, r: &Reader<'_>, q: &QualifiedSubject, version: u32, vr: &VersionRecord) -> ApiResult<Entity> {
        let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        Ok(Entity::stored(q, version, vr.id, &rec))
    }

    /// Confluent's `AbstractSchemaProvider#resolveReferences`: every reference
    /// must name a subject and version (`-1` is the latest); soft-deleted
    /// versions resolve. Returns the concrete references, the transitive
    /// closure (dependencies first) for the parsers, and the direct targets.
    pub(super) fn resolve_references(
        &self,
        r: &Reader<'_>,
        ctx: &str,
        refs: &[RefIn],
    ) -> Result<(Vec<SchemaReference>, Vec<ResolvedRef>, Vec<(QualifiedSubject, u32)>), String> {
        let mut concrete = Vec::with_capacity(refs.len());
        let mut targets = Vec::with_capacity(refs.len());
        let mut out = Vec::new();
        let mut visited = HashSet::new();
        for rin in refs {
            if rin.null {
                return Err("Cannot invoke \"io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference.getName()\" because \"reference\" is null".into());
            }
            let (Some(name), Some(subject), Some(version)) = (&rin.name, &rin.subject, rin.version) else {
                return Err(format!("Invalid reference: {rin}"));
            };
            let sr = SchemaReference { name: name.clone(), subject: subject.clone(), version };
            let q = Self::ref_target(ctx, &sr).map_err(|e| e.message)?;
            let spec = match version {
                -1 => VersionSpec::Latest,
                v if v > 0 => VersionSpec::Exact(v as u32),
                v => return Err(format!("io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException: {v}")),
            };
            let Some((v, _)) = self.get_exact(r, &q, spec, true).map_err(|e| e.message)? else {
                return Err(format!("No schema reference found for subject \"{}\" and version {version}", q.qualified()));
            };
            let sr = SchemaReference { version: v as i32, ..sr };
            self.collect_ref(r, &sr.name, &q, v, &mut visited, &mut out, 0).map_err(|e| e.message)?;
            concrete.push(sr);
            targets.push((q, v));
        }
        Ok((concrete, out, targets))
    }

    /// Resolve the references of a stored schema.
    pub(super) fn resolve_stored(&self, r: &Reader<'_>, ctx: &str, refs: &[SchemaReference]) -> ApiResult<Vec<ResolvedRef>> {
        let ins: Vec<RefIn> = refs.iter().cloned().map(RefIn::from).collect();
        self.resolve_references(r, ctx, &ins).map(|(_, resolved, _)| resolved).map_err(ApiError::internal)
    }

    fn collect_ref(
        &self,
        r: &Reader<'_>,
        name: &str,
        q: &QualifiedSubject,
        version: u32,
        visited: &mut HashSet<(String, String, u32)>,
        out: &mut Vec<ResolvedRef>,
        depth: usize,
    ) -> ApiResult<()> {
        if depth > 64 {
            return Err(ApiError::invalid_schema("reference chain too deep"));
        }
        if !visited.insert((q.context.clone(), q.subject.clone(), version)) {
            return Ok(());
        }
        let vr = r.get_version(&q.context, &q.subject, version)?.ok_or_else(|| {
            ApiError::new(42201, format!("No schema reference found for subject \"{}\" and version {version}", q.qualified()))
        })?;
        let rec = r.get_schema(&q.context, vr.id)?.ok_or_else(ApiError::schema_not_found)?;
        for sr in &rec.references {
            let t = Self::ref_target(&q.context, sr)?;
            self.collect_ref(r, &sr.name, &t, sr.version.max(1) as u32, visited, out, depth + 1)?;
        }
        out.push(ResolvedRef { name: name.to_string(), schema: rec.schema.clone() });
        Ok(())
    }

    pub(super) fn ref_target(ctx: &str, r: &SchemaReference) -> ApiResult<QualifiedSubject> {
        let q = QualifiedSubject::parse_subject(&r.subject)?;
        Ok(if r.subject.starts_with(":.") { q } else { QualifiedSubject::new(ctx, &q.subject) })
    }

    /// Apply a `format` query parameter. Only Protobuf `serialized` (base64
    /// FileDescriptorProto, used by non-Java clients) changes the output.
    pub(super) fn formatted(&self, r: &Reader<'_>, ctx: &str, id: u32, rec: &SchemaRecord, format: Option<&str>) -> ApiResult<String> {
        if rec.schema_type == SchemaType::Protobuf
            && format == Some("ignore_extensions")
            && let Some(text) = schema::proto_wire::without_extensions(&rec.schema)
        {
            return Ok(text);
        }
        if rec.schema_type == SchemaType::Protobuf
            && format == Some("serialized")
            && let schema::Parsed::Protobuf(p) = &self.parse_record(r, ctx, id, rec)?.inner
            && let Some(s) = p.serialized()
        {
            return Ok(s);
        }
        Ok(rec.schema.clone())
    }

    /// Parse a stored schema, through the cache.
    ///
    /// Content under an id never changes (registering over an id is refused,
    /// and a hard delete removes versions, not bodies), so `(context, id)`
    /// would be enough for the schema itself. It is not enough for the parse:
    /// that also depends on what the schema's references resolved to, and a
    /// reference is a (subject, version) pair whose content *can* change -
    /// hard-delete the referrer, then re-import its target differently, and
    /// the same id would parse against something new. Keying on the resolved
    /// closure as well makes a stale entry impossible to look up rather than
    /// merely unlikely.
    pub(super) fn parse_record(&self, r: &Reader<'_>, ctx: &str, id: u32, rec: &SchemaRecord) -> ApiResult<Arc<ParsedSchema>> {
        let resolved = self.resolve_stored(r, ctx, &rec.references)?;
        let key = (ctx.to_string(), id, Self::deps_fingerprint(&resolved));
        if let Some(p) = self.parsed_by_id.get(&key) {
            return Ok(p);
        }
        let parsed = schema::parse_with(rec.schema_type, &rec.schema, &resolved, false)
            .map_err(|e| ApiError::internal(format!("stored schema no longer parses: {e}")))?;
        let parsed = Arc::new(parsed);
        self.parsed_by_id.insert(key, parsed.clone());
        Ok(parsed)
    }

    /// What a schema's references resolved to, as a cache key component.
    /// A schema without references - the common case - costs nothing.
    fn deps_fingerprint(resolved: &[ResolvedRef]) -> [u8; 32] {
        if resolved.is_empty() {
            return [0; 32];
        }
        let mut h = Sha256::new();
        for r in resolved {
            h.update([0]);
            h.update(&r.name);
            h.update([1]);
            h.update(&r.schema);
        }
        h.finalize().into()
    }

    /// Parse a stored schema by id, for tests that need to see whether the
    /// cache handed back the same parse.
    #[cfg(test)]
    pub(crate) fn parse_for_test(&self, ctx: &str, id: u32) -> ApiResult<Arc<ParsedSchema>> {
        let r = &self.reader();
        let rec = r.get_schema(ctx, id)?.ok_or_else(ApiError::schema_not_found)?;
        self.parse_record(r, ctx, id, &rec)
    }

    /// `maybePopulateFromPrevious`: an empty schema re-uses the latest
    /// version's; metadata and rules are inherited from it and wrapped with the
    /// configured defaults/overrides; `confluent:version` tracks the version.
    pub(crate) fn populate_from_previous(
        &self,
        r: &Reader<'_>,
        q: &QualifiedSubject,
        cfg: &ConfigRecord,
        d: &mut Draft,
        latest_live: Option<&(u32, VersionRecord)>,
        new_version: u32,
    ) -> ApiResult<bool> {
        let previous = match latest_live {
            Some((v, vr)) => Some(self.entity(r, q, *v, vr)?),
            None => None,
        };
        let mut populated = false;
        if d.schema.as_deref().is_none_or(|s| s.trim().is_empty()) {
            let Some(p) = &previous else { return Err(ApiError::new(42201, "Empty schema")) };
            d.schema = Some(p.schema.clone());
            d.schema_type = Some(p.schema_type.as_str().to_string());
            d.refs = p.references.iter().cloned().map(RefIn::from).collect();
            populated = true;
        }
        let specific_meta = d.metadata.clone().or_else(|| previous.as_ref().and_then(|p| p.metadata.clone()));
        let mut metadata = merge_metadata(&[cfg.default_metadata.as_ref(), specific_meta.as_ref(), cfg.override_metadata.as_ref()]);
        let specific_rules = d.rule_set.clone().or_else(|| previous.as_ref().and_then(|p| p.rule_set.clone()));
        let rule_set = merge_rule_sets(&[cfg.default_rule_set.as_ref(), specific_rules.as_ref(), cfg.override_rule_set.as_ref()]);
        if d.version != 0 || metadata_property(&metadata, "confluent:version").is_some() {
            metadata = with_confluent_version(metadata, new_version);
        }
        if metadata.is_some() || rule_set.is_some() {
            d.metadata = metadata;
            d.rule_set = rule_set;
            return Ok(true);
        }
        Ok(populated)
    }

    /// `maybeModifyPreviousRuleSet`: `rulesToMerge` / `rulesToRemove` apply to
    /// the rules of the version before the new one.
    pub(super) fn rule_set_for_tags(&self, r: &Reader<'_>, q: &QualifiedSubject, req: &TagSchemaRequest) -> ApiResult<Option<Value>> {
        if req.rules_to_merge.is_none() && req.rules_to_remove.is_empty() {
            return Ok(req.rule_set.clone());
        }
        let spec = match req.new_version {
            Some(v) if v > 1 => VersionSpec::Exact(v as u32 - 1),
            Some(_) => VersionSpec::Exact(1),
            None => VersionSpec::Latest,
        };
        let previous = match self.get_exact(r, q, spec, false)? {
            Some((v, vr)) => self.entity(r, q, v, &vr)?.rule_set,
            None => None,
        };
        let mut rules = match &req.rules_to_merge {
            Some(merge) => merge_rule_sets(&[previous.as_ref(), Some(merge)]),
            None => previous,
        };
        if !req.rules_to_remove.is_empty()
            && let Some(rs) = rules.as_mut().and_then(Value::as_object_mut)
        {
            for key in ["migrationRules", "domainRules"] {
                if let Some(list) = rs.get_mut(key).and_then(Value::as_array_mut) {
                    list.retain(|x| !x.get("name").and_then(Value::as_str).is_some_and(|n| req.rules_to_remove.iter().any(|r| r == n)));
                }
            }
        }
        Ok(rules)
    }
}
