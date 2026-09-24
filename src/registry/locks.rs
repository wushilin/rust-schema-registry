//! Who may write what, at the same time.

use super::*;

/// Who may write what, at the same time.
///
/// A mutation declares its [`crate::mutations::Target`], and that decides what
/// it excludes:
///
/// * **Global** - the registry as a whole (global config or mode, a mode
///   change that empties everything): the container lock, exclusively. Nothing
///   else writes while it runs.
/// * **Subject or Context** - a shared container lock plus the lock for that
///   *context*. Writes to different contexts, and to different host
///   containers, proceed at the same time.
///
/// Why a context and not a subject: registering allocates from the context's
/// `next_id`, so two registrations in one context cannot be independent.
/// Per-subject parallelism needs that counter to be atomic first, which is a
/// separate change with its own benchmark.
///
/// Context locks are striped rather than kept per name: a fixed array needs no
/// bookkeeping, and two contexts sharing a stripe only wait for each other,
/// which is correct, just occasionally slower.
///
/// Acquisition order is always container then context, so there is no cycle to
/// deadlock on.
const CONTEXT_STRIPES: usize = 64;

pub(crate) struct Locks {
    container: std::sync::RwLock<()>,
    contexts: [Mutex<()>; CONTEXT_STRIPES],
    /// Exporter records: their own lock, shared with the worker's cursor
    /// writes, so a busy export never waits behind a registration.
    pub(super) exporters: Mutex<()>,
    /// Held only around the snapshot swap in `commit`, never around an fsync.
    pub(super) publish: Mutex<()>,
}

impl Default for Locks {
    fn default() -> Self {
        Self {
            container: std::sync::RwLock::new(()),
            contexts: [const { Mutex::new(()) }; CONTEXT_STRIPES],
            exporters: Mutex::new(()),
            publish: Mutex::new(()),
        }
    }
}

/// Held for as long as the mutation runs; the guards are never read, they are
/// dropped.
#[allow(dead_code)]
pub(crate) enum WriteGuard<'a> {
    Container(std::sync::RwLockWriteGuard<'a, ()>),
    Context(std::sync::RwLockReadGuard<'a, ()>, std::sync::MutexGuard<'a, ()>),
    Exporters(std::sync::MutexGuard<'a, ()>),
}

impl Locks {
    /// Which stripe a context's lock lives in.
    fn stripe(ctx: &str) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        std::hash::Hash::hash(ctx, &mut h);
        (std::hash::Hasher::finish(&h) as usize) % CONTEXT_STRIPES
    }

    /// Acquire without waiting, for tests that assert what excludes what.
    #[cfg(test)]
    fn try_acquire(&self, target: &crate::mutations::Target) -> Option<WriteGuard<'_>> {
        use crate::mutations::Target;
        let ctx = match target {
            Target::Global => return self.container.try_write().ok().map(WriteGuard::Container),
            Target::Exporters => return self.exporters.try_lock().ok().map(WriteGuard::Exporters),
            Target::Context(ctx) => ctx.as_str(),
            Target::Subject(q) => q.context.as_str(),
        };
        let shared = self.container.try_read().ok()?;
        let held = self.contexts[Self::stripe(ctx)].try_lock().ok()?;
        Some(WriteGuard::Context(shared, held))
    }

    pub(super) fn acquire(&self, target: &crate::mutations::Target) -> WriteGuard<'_> {
        use crate::mutations::Target;
        let ctx = match target {
            Target::Global => {
                return WriteGuard::Container(self.container.write().unwrap_or_else(|e| e.into_inner()));
            }
            Target::Exporters => {
                return WriteGuard::Exporters(self.exporters.lock().unwrap_or_else(|e| e.into_inner()));
            }
            Target::Context(ctx) => ctx.as_str(),
            Target::Subject(q) => q.context.as_str(),
        };
        let shared = self.container.read().unwrap_or_else(|e| e.into_inner());
        let held = self.contexts[Self::stripe(ctx)].lock().unwrap_or_else(|e| e.into_inner());
        WriteGuard::Context(shared, held)
    }
}


#[cfg(test)]
mod lock_tests {
    use super::*;
    use crate::context::QualifiedSubject;
    use crate::mutations::Target;

    /// Two context names that do not share a stripe, so the test is about the
    /// rule and not about the hash.
    fn two_contexts() -> (String, String) {
        let a = ".a".to_string();
        let b = (0..CONTEXT_STRIPES * 4)
            .map(|i| format!(".b{i}"))
            .find(|b| Locks::stripe(b) != Locks::stripe(&a))
            .expect("some context lands in another stripe");
        (a, b)
    }

    #[test]
    fn writers_to_different_contexts_do_not_wait_for_each_other() {
        let locks = Locks::default();
        let (a, b) = two_contexts();
        let held = locks.try_acquire(&Target::Context(a.clone())).expect("free");
        assert!(locks.try_acquire(&Target::Context(b)).is_some(), "another context must proceed");
        // A subject's writer takes its context's lock, so it waits on the same one.
        assert!(
            locks.try_acquire(&Target::Subject(QualifiedSubject::new(&a, "s"))).is_none(),
            "two writers in one context would race on its id counter"
        );
        drop(held);
        assert!(locks.try_acquire(&Target::Subject(QualifiedSubject::new(&a, "s"))).is_some());
    }

    #[test]
    fn a_registry_wide_change_excludes_everything() {
        let locks = Locks::default();
        let (a, _) = two_contexts();
        let held = locks.try_acquire(&Target::Global).expect("free");
        assert!(locks.try_acquire(&Target::Context(a.clone())).is_none(), "nothing writes during a global change");
        drop(held);
        // ...and it waits for the writers already running.
        let held = locks.try_acquire(&Target::Context(a)).expect("free");
        assert!(locks.try_acquire(&Target::Global).is_none());
        drop(held);
        assert!(locks.try_acquire(&Target::Global).is_some());
    }
}
