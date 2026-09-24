//! Which configuration and which mode apply to a request.
//!
//! Confluent resolves by first match, never by merging: a subject's own record
//! if it has one, else its context's (or the global one, in the default
//! context), else the server default. A global READONLY_OVERRIDE is the single
//! exception that reaches into every context.
//!
//! Getting this wrong is invisible until someone's compatibility level quietly
//! stops applying, so the fallbacks are spelled out rather than clever.

use super::*;

impl Registry {
    /// The config/mode key of a subject: `:.ctx:` (no subject) is the context itself.
    pub(super) fn scope_for(q: &QualifiedSubject) -> Scope {
        if q.is_context_only() {
            Scope::Context(q.context.clone())
        } else {
            Scope::Subject(q.context.clone(), q.subject.clone())
        }
    }

    /// Confluent fills a missing level with the server default on every read.
    /// Our `normalize` server default (on unless configured off) is filled the same way.
    pub(super) fn filled(&self, mut c: ConfigRecord) -> ConfigRecord {
        c.compatibility_level.get_or_insert(self.default_compatibility);
        if self.normalize_default {
            c.normalize.get_or_insert(true);
        }
        c
    }

    /// `getConfig(subject)`: the record stored for exactly this scope (the
    /// global scope always has one: stored or the default).
    pub(crate) fn config_of(&self, r: &Reader<'_>, q: Option<&QualifiedSubject>) -> ApiResult<Option<ConfigRecord>> {
        Ok(match q {
            None => Some(self.filled(r.get_config(&Scope::Global)?.unwrap_or_default())),
            Some(q) => r.get_config(&Self::scope_for(q))?.map(|c| self.filled(c)),
        })
    }

    /// `getConfigInScope(subject)`: the subject's record, else its context's
    /// (non-default contexts) or the global one (default context), else the
    /// server default. Records are not merged.
    pub fn config_in_scope(&self, r: &Reader<'_>, q: &QualifiedSubject) -> ApiResult<ConfigRecord> {
        if let Some(c) = r.get_config(&Self::scope_for(q))? {
            return Ok(self.filled(c));
        }
        let parent = if q.context != DEFAULT_CONTEXT {
            r.get_config(&Scope::Context(q.context.clone()))?
        } else {
            r.get_config(&Scope::Global)?
        };
        Ok(self.filled(parent.unwrap_or_default()))
    }

    /// The effective `normalize` for a request (our server default applies
    /// when no config in scope says otherwise).
    pub(crate) fn normalize_in_scope(&self, r: &Reader<'_>, q: &QualifiedSubject) -> ApiResult<bool> {
        Ok(self.config_in_scope(r, q)?.normalize == Some(true))
    }

    pub(crate) fn global_mode_in(&self, r: &Reader<'_>) -> ApiResult<Mode> {
        Ok(r.get_mode(&Scope::Global)?.unwrap_or(Mode::Readwrite))
    }

    /// `getMode(subject)`: a global READONLY_OVERRIDE wins, else exactly this scope's mode.
    pub(crate) fn mode_of_scope(&self, r: &Reader<'_>, q: Option<&QualifiedSubject>) -> ApiResult<Option<Mode>> {
        let global = self.global_mode_in(r)?;
        if global == Mode::ReadonlyOverride {
            return Ok(Some(global));
        }
        match q {
            None => Ok(Some(global)),
            Some(q) => r.get_mode(&Self::scope_for(q)),
        }
    }

    /// `getModeInScope(subject)`: a global READONLY_OVERRIDE wins; else the
    /// subject's mode, else its context's (non-default contexts) or the global
    /// one (default context), else READWRITE.
    pub fn mode_in_scope(&self, r: &Reader<'_>, q: &QualifiedSubject) -> ApiResult<Mode> {
        let global = self.global_mode_in(r)?;
        if global == Mode::ReadonlyOverride {
            return Ok(global);
        }
        if let Some(m) = r.get_mode(&Self::scope_for(q))? {
            return Ok(m);
        }
        let parent = if q.context != DEFAULT_CONTEXT { r.get_mode(&Scope::Context(q.context.clone()))? } else { Some(global) };
        Ok(parent.unwrap_or(Mode::Readwrite))
    }

    pub fn get_config(&self, subject: Option<&str>, default_to_global: bool) -> ApiResult<ConfigRecord> {
        let r = &self.reader();
        let Some(s) = subject else { return Ok(self.config_of(r, None)?.expect("global config")) };
        let q = QualifiedSubject::parse(s)?;
        if default_to_global {
            return self.config_in_scope(r, &q);
        }
        self.config_of(r, Some(&q))?.ok_or_else(|| ApiError::subject_compat_not_configured(&q.qualified()))
    }

    pub fn get_mode(&self, subject: Option<&str>, default_to_global: bool) -> ApiResult<Mode> {
        let r = &self.reader();
        let Some(s) = subject else { return Ok(self.mode_of_scope(r, None)?.expect("global mode")) };
        let q = QualifiedSubject::parse(s)?;
        if default_to_global {
            return self.mode_in_scope(r, &q);
        }
        self.mode_of_scope(r, Some(&q))?.ok_or_else(|| ApiError::subject_mode_not_configured(&q.qualified()))
    }
}
