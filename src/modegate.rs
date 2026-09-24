//! One place that says which operations a mode allows.
//!
//! Reads are never gated - a mode stops writes, not queries - and changing a
//! mode is always allowed, since that is the way out of every other mode.
//! What is left is the table below.
//!
//! Modes are the registry's only "you may not do that right now" rule, and
//! they are easy to get wrong by checking them in each handler: a path that
//! forgets the check (or a fast path that skips it) silently accepts writes
//! that should have been refused. So the rule lives here as a table over
//! (what the request is trying to do, what mode applies), and it is enforced
//! twice from this one definition:
//!
//! The interesting case is an *import* write - one that carries its own `id`
//! (and usually `version`). It is only legal where IMPORT mode applies, and
//! "where" means the mode in scope for that subject: its own, else its
//! context's, else the global one. That is what stops an exporter, a
//! migration or a hand-rolled `curl` from planting ids in a registry that is
//! serving normal traffic.
//!
//! The guarantee is structural rather than a check each caller remembers:
//! [`check`] is the only way to obtain an [`Allowed`], and the store will not
//! write a schema or a version without one. A new write path physically
//! cannot skip the table - it has nothing to pass.
//!
//! Why this sits here and not in a middleware in front of the routes: what
//! Confluent answers depends on registry state, not only on the request. Two
//! cases from the recorded corpus show it. `POST /subjects/x/versions` with an
//! explicit `id` while the subject is in READWRITE mode answers 200 when that
//! schema and id are already registered - the lookup comes first, and nothing
//! is written, so no mode applies. And `DELETE /config/x` on a read-only
//! subject answers 404 when there is no such subject: existence is checked
//! before the mode. A gate in front of the handler answers 42205 to both and
//! is wrong twice. So the intent is classified here, the table lives here, and
//! the enforcement happens where state actually changes.

use crate::error::{ApiError, ApiResult};
use crate::model::Mode;

/// What a caller is in the middle of doing, as far as modes are concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Intent {
    /// Register a schema and let the registry assign the id.
    Write,
    /// Register a schema with the id (and version) the caller chose.
    Import,
    /// Change stored state that is not a registration: delete a subject or a
    /// version, edit config, edit tags, delete a context.
    Modify,
}

/// Proof that the mode in scope allows the write about to happen. Only
/// [`check`] and the two named exemptions below produce one, and every write
/// in `Store` that defines registry state demands one, so the table cannot be
/// bypassed by forgetting to call it.
#[derive(Debug)]
pub struct Allowed(());

impl Allowed {
    /// A mode change itself. Never gated: it is the way out of READONLY and
    /// out of IMPORT, so gating it on the mode would be a trap with no key.
    pub fn is_a_mode_change() -> Self {
        Self(())
    }

    /// Registry metadata that is not schema state and that Confluent does not
    /// gate on modes either: exporter records, and removing an (already empty)
    /// context. Grep this constructor to find everything that skips the table.
    pub fn not_schema_state() -> Self {
        Self(())
    }
}

/// The rule. `scope` names what the mode belongs to, for the error message
/// ("null" is what Confluent prints for the global scope).
pub fn check(intent: Intent, mode: Mode, scope: &str) -> ApiResult<Allowed> {
    let refuse = |what: &str| Err(ApiError::operation_not_permitted(format!("Subject {scope} is {what}")));
    match (intent, mode) {
        (_, Mode::Readonly | Mode::ReadonlyOverride) => refuse("in read-only mode"),

        (Intent::Write, Mode::Readwrite) => Ok(Allowed(())),
        (Intent::Write, Mode::Import) => refuse("not in read-write mode"),

        (Intent::Import, Mode::Import) => Ok(Allowed(())),
        (Intent::Import, Mode::Readwrite) => refuse("not in import mode"),

        // Deletes and settings are allowed while importing, as in Confluent.
        (Intent::Modify, Mode::Readwrite | Mode::Import) => Ok(Allowed(())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_whole_matrix_is_decided_here() {
        let allowed = |i: Intent, m: Mode| check(i, m, "s").is_ok();
        // Nothing is written in a read-only mode...
        for m in [Mode::Readonly, Mode::ReadonlyOverride] {
            for i in [Intent::Write, Intent::Import, Intent::Modify] {
                assert!(!allowed(i, m), "{i:?} in {m:?}");
            }
        }
        // ...a normal write needs READWRITE, an import needs IMPORT, exactly...
        assert!(allowed(Intent::Write, Mode::Readwrite));
        assert!(!allowed(Intent::Write, Mode::Import));
        assert!(allowed(Intent::Import, Mode::Import));
        assert!(!allowed(Intent::Import, Mode::Readwrite));
        // ...and deletes and settings are allowed while importing, as in Confluent.
        assert!(allowed(Intent::Modify, Mode::Readwrite));
        assert!(allowed(Intent::Modify, Mode::Import));
    }

    #[test]
    fn the_messages_are_confluents() {
        let msg = |i, m| check(i, m, "abc").unwrap_err().message;
        assert_eq!(msg(Intent::Import, Mode::Readwrite), "Subject abc is not in import mode");
        assert_eq!(msg(Intent::Write, Mode::Import), "Subject abc is not in read-write mode");
        assert_eq!(msg(Intent::Write, Mode::Readonly), "Subject abc is in read-only mode");
        assert_eq!(check(Intent::Write, Mode::Readonly, "abc").unwrap_err().code, 42205);
    }

}
