//! The in-memory read model.
//!
//! [`Snapshot`] holds all registry *metadata* - every id, subject/version,
//! fingerprint, reference edge, config, mode and counter - in persistent
//! (structurally shared) maps from `imbl`. Cloning a snapshot is O(1); applying
//! a write copies only the touched paths, O(log n).
//!
//! Life of a write (always under the registry write lock):
//!   1. build a RocksDB `WriteBatch` and, in lock-step, a list of [`Op`]s
//!   2. commit the batch (fsync'd WAL) - the write is now durable
//!   3. clone the current snapshot, apply the ops, publish it with one atomic
//!      pointer swap (`ArcSwap`)
//!
//! Readers `load()` the pointer once per request and answer the whole request
//! from that snapshot, so a request never observes half of a write and never
//! takes a lock. Schema *bodies* are not in the snapshot; they live in a bounded
//! cache (see `registry.rs`). That's safe because the content stored under a
//! (context, id) never changes once written.

use std::sync::Arc;

use imbl::{HashMap, OrdMap, OrdSet};

use crate::model::{ConfigRecord, Mode, SchemaRecord, VersionRecord};
use crate::store::Scope;

/// Small sorted collection with copy-on-write semantics. Per-subject version
/// lists and per-id usage lists hold a handful of entries; a persistent tree
/// per entry would cost kilobytes each (imbl allocates fixed-size chunks),
/// while cloning a tiny Vec on write is cheap.
type Small<T> = Arc<Vec<T>>;

fn small_insert<K: Ord + Clone, V: Clone>(v: &mut Small<(K, V)>, key: K, value: V) {
    let v = Arc::make_mut(v);
    match v.binary_search_by(|(k, _)| k.cmp(&key)) {
        Ok(i) => v[i].1 = value,
        Err(i) => v.insert(i, (key, value)),
    }
}

fn small_remove<K: Ord + Clone, V: Clone>(v: &mut Small<(K, V)>, key: &K) {
    if let Ok(i) = v.binary_search_by(|(k, _)| k.cmp(key)) {
        Arc::make_mut(v).remove(i);
    }
}

fn set_insert<T: Ord + Clone>(v: &mut Small<T>, x: T) {
    if let Err(i) = v.binary_search(&x) {
        Arc::make_mut(v).insert(i, x);
    }
}

fn set_remove<T: Ord + Clone>(v: &mut Small<T>, x: &T) {
    if let Ok(i) = v.binary_search(x) {
        Arc::make_mut(v).remove(i);
    }
}


/// One mutation, mirrored 1:1 from the RocksDB batch.
#[derive(Debug, Clone)]
pub enum Op {
    PutSchema { ctx: String, id: u32, rec: Arc<SchemaRecord>, index: bool },
    /// The last version holding this id is gone, so its content goes too.
    /// Nothing in the snapshot holds a body; this is here for the caches.
    DeleteSchema { ctx: String, id: u32 },
    PutVersion { ctx: String, subject: String, version: u32, rec: VersionRecord },
    DeleteVersion { ctx: String, subject: String, version: u32, id: u32 },
    PutRefby { ctx: String, subject: String, version: u32, id: u32 },
    DeleteRefby { ctx: String, subject: String, version: u32, id: u32 },
    SetNextId { ctx: String, next: u32 },
    DeleteContext { ctx: String },
    PutConfig { scope: Scope, rec: ConfigRecord },
    DeleteConfig { scope: Scope },
    PutMode { scope: Scope, mode: Mode },
    DeleteMode { scope: Scope },
    SetLogSeq { seq: u64 },
}

#[derive(Clone, Default)]
pub struct CtxState {
    /// subject -> version -> record (including soft-deleted versions)
    pub subjects: OrdMap<String, Small<(u32, VersionRecord)>>,
    /// id -> (subject, version) pairs using it
    pub usages: HashMap<u32, Small<(String, u32)>>,
    pub fingerprints: HashMap<String, u32>,
    /// (subject, version) -> ids of schemas referencing it
    pub refby: HashMap<(String, u32), Small<u32>>,
    pub next_id: Option<u32>,
}

#[derive(Clone, Default)]
pub struct Snapshot {
    pub ctxs: OrdMap<String, CtxState>,
    pub known_contexts: OrdSet<String>,
    pub config: HashMap<Scope, ConfigRecord>,
    /// Whether any subject is an alias: when nothing is, the alias rewrite in
    /// front of the router can be skipped entirely.
    pub has_aliases: bool,
    pub mode: HashMap<Scope, Mode>,
    pub log_seq: u64,
}

impl Snapshot {
    fn ctx_mut(&mut self, ctx: &str) -> &mut CtxState {
        if !self.ctxs.contains_key(ctx) {
            self.ctxs.insert(ctx.to_string(), CtxState::default());
        }
        self.ctxs.get_mut(ctx).expect("just inserted")
    }

    pub fn apply(&mut self, op: &Op) {
        match op {
            Op::DeleteSchema { .. } => {}
            Op::PutSchema { ctx, id, rec, index } => {
                if *index {
                    self.ctx_mut(ctx).fingerprints.insert(rec.fingerprint.clone(), *id);
                }
            }
            Op::PutVersion { ctx, subject, version, rec } => {
                self.known_contexts.insert(ctx.clone());
                let c = self.ctx_mut(ctx);
                // A re-put can change the id only in theory; keep usages exact anyway.
                let old_id = c
                    .subjects
                    .get(subject)
                    .and_then(|vs| vs.iter().find(|(v, _)| v == version))
                    .map(|(_, r)| r.id)
                    .filter(|id| *id != rec.id);
                if let Some(old_id) = old_id
                    && let Some(u) = c.usages.get_mut(&old_id)
                {
                    set_remove(u, &(subject.clone(), *version));
                }
                small_insert(c.subjects.entry(subject.clone()).or_default(), *version, rec.clone());
                set_insert(c.usages.entry(rec.id).or_default(), (subject.clone(), *version));
            }
            Op::DeleteVersion { ctx, subject, version, id } => {
                let c = self.ctx_mut(ctx);
                let now_empty = match c.subjects.get_mut(subject) {
                    Some(vs) => {
                        small_remove(vs, version);
                        vs.is_empty()
                    }
                    None => false,
                };
                if now_empty {
                    c.subjects.remove(subject);
                }
                let usage_empty = match c.usages.get_mut(id) {
                    Some(u) => {
                        set_remove(u, &(subject.clone(), *version));
                        u.is_empty()
                    }
                    None => false,
                };
                if usage_empty {
                    c.usages.remove(id);
                }
            }
            Op::PutRefby { ctx, subject, version, id } => {
                set_insert(self.ctx_mut(ctx).refby.entry((subject.clone(), *version)).or_default(), *id);
            }
            Op::DeleteRefby { ctx, subject, version, id } => {
                let c = self.ctx_mut(ctx);
                let key = (subject.clone(), *version);
                let empty = match c.refby.get_mut(&key) {
                    Some(s) => {
                        set_remove(s, id);
                        s.is_empty()
                    }
                    None => false,
                };
                if empty {
                    c.refby.remove(&key);
                }
            }
            Op::SetNextId { ctx, next } => self.ctx_mut(ctx).next_id = Some(*next),
            Op::DeleteContext { ctx } => {
                self.known_contexts.remove(ctx);
            }
            Op::PutConfig { scope, rec } => {
                self.has_aliases |= rec.alias.as_ref().is_some_and(|a| !a.is_empty());
                self.config.insert(scope.clone(), rec.clone());
            }
            Op::DeleteConfig { scope } => {
                self.config.remove(scope);
                if self.has_aliases {
                    self.has_aliases = self.config.values().any(|c| c.alias.as_ref().is_some_and(|a| !a.is_empty()));
                }
            }
            Op::PutMode { scope, mode } => {
                self.mode.insert(scope.clone(), *mode);
            }
            Op::DeleteMode { scope } => {
                self.mode.remove(scope);
            }
            Op::SetLogSeq { seq } => self.log_seq = *seq,
        }
    }

    // ---------------- queries ----------------

    fn ctx(&self, ctx: &str) -> Option<&CtxState> {
        self.ctxs.get(ctx)
    }

    pub fn get_version(&self, ctx: &str, subject: &str, version: u32) -> Option<VersionRecord> {
        self.ctx(ctx)?.subjects.get(subject)?.iter().find(|(v, _)| *v == version).map(|(_, r)| r.clone())
    }

    pub fn list_versions(&self, ctx: &str, subject: &str) -> Vec<(u32, VersionRecord)> {
        self.ctx(ctx)
            .and_then(|c| c.subjects.get(subject))
            .map(|vs| vs.to_vec())
            .unwrap_or_default()
    }

    pub fn list_subject_names(&self, ctx: &str) -> Vec<String> {
        self.ctx(ctx).map(|c| c.subjects.keys().cloned().collect()).unwrap_or_default()
    }

    pub fn id_usages(&self, ctx: &str, id: u32) -> Vec<(String, u32)> {
        self.ctx(ctx).and_then(|c| c.usages.get(&id)).map(|u| u.to_vec()).unwrap_or_default()
    }

    pub fn referenced_by(&self, ctx: &str, subject: &str, version: u32) -> Vec<u32> {
        self.ctx(ctx)
            .and_then(|c| c.refby.get(&(subject.to_string(), version)))
            .map(|s| s.to_vec())
            .unwrap_or_default()
    }

    pub fn id_for_fingerprint(&self, ctx: &str, fp: &str) -> Option<u32> {
        self.ctx(ctx)?.fingerprints.get(fp).copied()
    }

    pub fn next_id(&self, ctx: &str) -> u32 {
        self.ctx(ctx).and_then(|c| c.next_id).unwrap_or(1)
    }

    pub fn list_contexts(&self) -> Vec<String> {
        let mut out: Vec<String> = self.known_contexts.iter().cloned().collect();
        if !out.iter().any(|c| c == crate::context::DEFAULT_CONTEXT) {
            out.insert(0, crate::context::DEFAULT_CONTEXT.to_string());
        }
        out.sort();
        out
    }

    pub fn get_config(&self, scope: &Scope) -> Option<ConfigRecord> {
        self.config.get(scope).cloned()
    }

    pub fn get_mode(&self, scope: &Scope) -> Option<Mode> {
        self.mode.get(scope).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_and_share() {
        let mut s = Snapshot::default();
        s.apply(&Op::PutVersion {
            ctx: ".".into(),
            subject: "a".into(),
            version: 1,
            rec: VersionRecord { id: 7, deleted: false, ts: 0 },
        });
        let old = s.clone();
        s.apply(&Op::DeleteVersion { ctx: ".".into(), subject: "a".into(), version: 1, id: 7 });
        // The old snapshot is untouched: readers holding it keep a consistent view.
        assert_eq!(old.list_versions(".", "a").len(), 1);
        assert_eq!(old.id_usages(".", 7), vec![("a".to_string(), 1)]);
        assert!(s.list_versions(".", "a").is_empty());
        assert!(s.list_subject_names(".").is_empty());
        assert!(s.id_usages(".", 7).is_empty());
    }
}
