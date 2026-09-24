//! `POST /subjects/{subject}/versions` - Confluent's `register`.
//!
//! The busiest endpoint a registry has, and the one with the most rules:
//!
//! * a schema that is already registered answers with its id and writes
//!   nothing - the lock-free [`Mutation::fast_path`];
//! * an empty schema inherits the latest version's, and metadata and rule sets
//!   are merged with whatever the config defaults and overrides say;
//! * a normal registration must be compatible with what came before, and gets
//!   the next version number and the next id;
//! * an *import* carries its own id and version, replaces the version that
//!   occupied that number, and must not point an existing id at new content.
//!
//! Which of the last two is allowed where is [`crate::modegate`]'s business,
//! not this file's: the engine checks the mode before the plan runs.

use std::collections::HashSet;

use super::{Mutation, Plan, ReadView, Target, Write};
use crate::context::QualifiedSubject;
use crate::error::{ApiError, ApiResult};
use crate::model::*;
use crate::modegate::Intent;
use crate::registry::{Draft, Entity, RegisterResponse, can_lookup};
use crate::schema;

pub struct RegisterSchema {
    subject: QualifiedSubject,
    draft: Draft,
    /// `normalize=true` on the request; the subject's own setting is read from
    /// the snapshot, so it cannot be decided until the plan runs.
    normalize_requested: bool,
}

impl RegisterSchema {
    /// Validating here, not in `plan`, keeps Confluent's order: a subject name
    /// that cannot hold a schema is refused before modes, compatibility or
    /// anything the snapshot has to say.
    pub fn new(subject: &str, req: RegisterSchemaRequest, normalize: bool) -> ApiResult<Self> {
        if !crate::context::is_valid_subject(subject) {
            return Err(ApiError::invalid_subject(subject));
        }
        Ok(Self { subject: QualifiedSubject::parse(subject)?, draft: Draft::from(req), normalize_requested: normalize })
    }

    fn normalize(&self, view: &ReadView<'_>) -> ApiResult<bool> {
        Ok(self.normalize_requested || view.reg().normalize_in_scope(view.reader(), &self.subject)?)
    }
}

impl Mutation for RegisterSchema {
    type Output = RegisterResponse;

    fn target(&self) -> Target {
        Target::Subject(self.subject.clone())
    }

    fn intent(&self) -> Intent {
        // An id in the request is what makes this an import.
        if self.draft.id >= 0 { Intent::Import } else { Intent::Write }
    }

    /// `registerOrForward`'s check: the schema may already be registered, in
    /// which case the answer needs no lock and no write at all.
    fn fast_path(&self, view: &ReadView<'_>) -> ApiResult<Option<RegisterResponse>> {
        let reg = view.reg();
        let normalize = self.normalize(view)?;
        reg.register_fast_path(view.reader(), &self.subject, &self.draft, normalize)
    }

    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<RegisterResponse>> {
        let (reg, r, q) = (view.reg(), view.reader(), &self.subject);
        let normalize = self.normalize(view)?;
        let mut d = self.draft.clone();
        let import = view.mode_for(&self.target())? == Mode::Import;

        let versions = r.list_versions(&q.context, &q.subject)?;
        let new_version = versions.iter().map(|(v, _)| *v).max().map_or(1, |m| m + 1);
        let undeleted: Vec<(u32, VersionRecord)> = versions.iter().rev().filter(|(_, v)| !v.deleted).cloned().collect();
        let cfg = reg.config_in_scope(r, q)?;
        // An import takes what it is given; a normal registration inherits.
        let modified = !import && reg.populate_from_previous(r, q, &cfg, &mut d, undeleted.first(), new_version)?;

        let mut schema_id = d.id;
        let canon = reg.canonicalize(r, q, &d, schema_id < 0, normalize)?;
        let answer = |e: &Entity| if modified { RegisterResponse::full(e) } else { RegisterResponse::id(e.id) };

        // Already stored under this content hash?
        if let Some(c) = &canon
            && let Some((id, sv)) = reg.hash_lookup(r, q, c)?
            && (schema_id < 0 || schema_id as u32 == id)
        {
            if d.version == 0
                && let Some((v, vr)) = sv
                && !vr.deleted
            {
                return Ok(Plan::new(answer(&Entity::from_canon(q, v, id, c))));
            }
            schema_id = id as i32;
        }
        // Or equal to a version this subject already has?
        if d.version == 0
            && let Some(c) = &canon
        {
            for (v, vr) in &undeleted {
                if schema_id >= 0 && schema_id as u32 != vr.id {
                    continue;
                }
                if can_lookup(c, &reg.entity(r, q, *v, vr)?) {
                    return Ok(Plan::new(answer(&Entity::from_canon(q, *v, vr.id, c))));
                }
            }
        }

        // IMPORT with an empty schema: Confluent would store it unparsed.
        let c = canon.ok_or_else(|| ApiError::new(42201, "Empty schema"))?;
        if !import {
            let level = cfg.compatibility_level.unwrap_or(reg.default_compatibility);
            let olds = reg.compat_candidates(r, q, &cfg, &c.metadata.clone())?;
            let msgs = reg.check_compatibility(r, &q.context, &c.parsed, &olds, level, &cfg)?;
            if !msgs.is_empty() {
                return Err(ApiError::incompatible(&q.qualified(), &msgs));
            }
        }
        let version = if d.version <= 0 {
            new_version
        } else if new_version != d.version as u32 && !import {
            return Err(ApiError::new(42201, "Version is not one more than previous version"));
        } else {
            d.version as u32
        };

        let fp = c.fingerprint();
        let next_id = r.next_id(&q.context)?;
        let id = if schema_id >= 0 {
            let id = schema_id as u32;
            // checkIfSchemaWithIdExist: an id is a promise about content.
            if !r.id_usages(&q.context, id)?.is_empty()
                && let Some(existing) = r.get_schema(&q.context, id)?
                && existing.fingerprint != fp
            {
                return Err(ApiError::operation_not_permitted(format!("Overwrite new schema with id {id} is not permitted.")));
            }
            id
        } else {
            next_id
        };

        let mut writes = Vec::new();
        // Hard deletes are logged too, before the registration that replaces them.
        let mut events = Vec::new();
        // An import may land on a version number that is taken; that row goes.
        if let Some(old) = versions.iter().find(|(v, _)| *v == version).map(|(_, vr)| vr.clone()) {
            let (ws, ev) = reg.hard_delete_writes(r, q, version, &old, &HashSet::new())?;
            writes.extend(ws);
            events.push(ev);
        }
        // Older soft-deleted versions carrying the same id are removed for good.
        let stale: Vec<(u32, VersionRecord)> =
            versions.iter().filter(|(v, vr)| vr.deleted && vr.id == id && *v < version).cloned().collect();
        let stale_keys: HashSet<(String, u32)> = stale.iter().map(|(v, _)| (q.subject.clone(), *v)).collect();
        for (v, vr) in &stale {
            let (ws, ev) = reg.hard_delete_writes(r, q, *v, vr, &stale_keys)?;
            writes.extend(ws);
            events.push(ev);
        }

        let rec = match r.get_schema(&q.context, id)? {
            Some(existing) if existing.fingerprint == fp => SchemaRecord::clone(&existing),
            _ => SchemaRecord {
                schema_type: c.schema_type,
                schema: c.text.clone(),
                references: c.refs.clone(),
                metadata: c.metadata.clone(),
                rule_set: c.rule_set.clone(),
                fingerprint: fp,
                schema_fingerprint: schema::fingerprint(c.schema_type, &&c.parsed.normalized, &c.refs, None, None),
                guid: view.new_guid().to_string(),
            },
        };
        // Like Confluent's hash index, the latest registration of a content wins.
        writes.push(Write::PutSchema { ctx: q.context.clone(), id, rec, index: true });
        if id >= next_id {
            writes.push(Write::SetNextId { ctx: q.context.clone(), next: id + 1 });
        }
        writes.push(Write::PutVersion {
            ctx: q.context.clone(),
            subject: q.subject.clone(),
            version,
            rec: VersionRecord { id, deleted: false, ts: view.now() },
        });
        for (t, v) in &c.targets {
            writes.push(Write::PutRefby { ctx: t.context.clone(), subject: t.subject.clone(), version: *v, id });
        }

        events.push(LogEvent {
            ctx: q.context.clone(),
            subject: q.subject.clone(),
            version,
            id,
            kind: LogEventKind::Register,
        });
        let out = answer(&Entity::from_canon(q, version, id, &c));
        let mut plan = Plan::new(out).writes(writes);
        plan.events = events;
        Ok(plan)
    }
}
