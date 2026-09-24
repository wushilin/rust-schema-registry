//! Every intended change to the registry, one verb per file.
//!
//! A verb says what it touches and what kind of change it is, and turns a
//! snapshot into a plan. It does not lock, does not check modes, does not
//! write, does not log and does not notify - [`crate::engine`] does all of
//! that, the same way, for every verb. That is what keeps the rules in one
//! place instead of at each call site.
//!
//! [`Mutation::plan`] is pure in the way that matters: it reads one consistent
//! snapshot and returns what should happen. It writes nothing, takes no lock,
//! reads no clock and asks nothing of the network, so a verb can be tested by
//! setting up a snapshot and comparing plans - no HTTP, no ports, no waiting.

pub mod delete_context;
pub mod delete_subject;
pub mod delete_subject_config;
pub mod delete_subject_version;
pub mod register_schema;
pub mod set_mode;
pub mod update_compatibility;

pub use delete_context::DeleteContext;
pub use delete_subject::DeleteSubject;
pub use delete_subject_config::DeleteSubjectConfig;
pub use delete_subject_version::DeleteSubjectVersion;
pub use register_schema::RegisterSchema;
pub use set_mode::{DeleteMode, SetMode};
pub use update_compatibility::UpdateCompatibility;

use crate::context::QualifiedSubject;
use crate::error::ApiResult;
use crate::model::{ConfigRecord, LogEvent, Mode, SchemaRecord, VersionRecord};
use crate::modegate::Intent;
use crate::registry::{Reader, Registry};
use crate::store::Scope;

/// What a mutation touches. It decides which lock is taken and which mode
/// applies, so a verb that lies here is the one way to escape the rules -
/// which is why it is a declaration and not a computation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Subject(QualifiedSubject),
    /// A context's own settings, or the context itself.
    Context(String),
    /// The registry as a whole: global config and mode, exporters.
    Global,
}

/// When the mode is checked. Confluent sometimes answers from state before it
/// considers the mode at all - `DELETE /config/x` on a subject that does not
/// exist is 404, not the read-only error - so a verb can ask for the check to
/// happen after its plan has run and had its say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gate {
    BeforePlan,
    AfterPlan,
}

/// One row the store should write. The engine is the only thing that turns
/// these into a batch, which is why the mode token lives there and not here.
/// Variants are added as verbs move onto the engine.
#[derive(Debug, Clone)]
pub enum Write {
    PutSchema { ctx: String, id: u32, rec: SchemaRecord, index: bool },
    PutVersion { ctx: String, subject: String, version: u32, rec: VersionRecord },
    DeleteVersion { ctx: String, subject: String, version: u32, id: u32 },
    PutRefby { ctx: String, subject: String, version: u32, id: u32 },
    DeleteRefby { ctx: String, subject: String, version: u32, id: u32 },
    SetNextId { ctx: String, next: u32 },
    PutConfig { scope: Scope, rec: ConfigRecord },
    DeleteConfig { scope: Scope },
    PutMode { scope: Scope, mode: Mode },
    DeleteMode { scope: Scope },
    DeleteContext { ctx: String },
}

/// What a verb decided to do.
pub struct Plan<T> {
    pub writes: Vec<Write>,
    /// Appended to this container's change log, in the same batch as `writes`,
    /// so "committed but not logged" cannot happen.
    pub events: Vec<LogEvent>,
    pub output: T,
}

impl<T> Plan<T> {
    pub fn new(output: T) -> Self {
        Self { writes: Vec::new(), events: Vec::new(), output }
    }

    pub fn write(mut self, w: Write) -> Self {
        self.writes.push(w);
        self
    }

    pub fn writes(mut self, ws: impl IntoIterator<Item = Write>) -> Self {
        self.writes.extend(ws);
        self
    }
}

pub trait Mutation {
    type Output;

    /// An answer that needs no lock and changes nothing. Confluent's
    /// `registerOrForward` looks first, and by far the commonest "write" a
    /// registry sees is a producer registering a schema that is already there,
    /// so the engine runs this on a lock-free snapshot before it takes
    /// anything. Returning `None` means "do it properly".
    fn fast_path(&self, _view: &ReadView<'_>) -> ApiResult<Option<Self::Output>> {
        Ok(None)
    }

    /// What this change is about. Never fails: a verb that cannot name its
    /// target refuses to be constructed instead (which also keeps Confluent's
    /// "bad name before anything else" error ordering).
    fn target(&self) -> Target;

    /// What kind of change it is, for the mode table.
    fn intent(&self) -> Intent;

    fn gate(&self) -> Gate {
        Gate::BeforePlan
    }

    /// Pure: what to write, what to log, what to answer.
    fn plan(&self, view: &ReadView<'_>) -> ApiResult<Plan<Self::Output>>;
}

/// The registry as a verb sees it: one consistent snapshot, plus the two
/// things a plan cannot make up for itself - the time and a fresh id. Both are
/// fixed for the whole mutation, so a plan is a function of what it is given
/// and nothing else.
///
/// Read-only by construction: a verb can reach the reader, but writing needs
/// an `Allowed` token, and only the engine can produce one.
pub struct ReadView<'a> {
    reg: &'a Registry,
    reader: Reader<'a>,
    now: i64,
    /// Made once, on the first verb that stores a new schema - the lock-free
    /// fast path is the commonest call and needs no id at all.
    guid: std::cell::OnceCell<String>,
}

impl<'a> ReadView<'a> {
    pub fn new(reg: &'a Registry) -> Self {
        Self { reader: reg.reader(), reg, now: crate::model::now_millis(), guid: std::cell::OnceCell::new() }
    }

    /// The instant this mutation happened, the same for every row it writes.
    pub fn now(&self) -> i64 {
        self.now
    }

    /// The GUID a newly stored schema gets. Fixed for this mutation, so a plan
    /// built twice from the same view is the same plan.
    pub fn new_guid(&self) -> &str {
        self.guid.get_or_init(|| uuid::Uuid::new_v4().to_string())
    }

    /// The read side, for verbs that have not had their queries moved yet.
    pub fn reg(&self) -> &'a Registry {
        self.reg
    }

    pub fn reader(&self) -> &Reader<'a> {
        &self.reader
    }

    /// The stored config for exactly this scope, with no fallback.
    pub fn config(&self, scope: &Scope) -> ApiResult<Option<ConfigRecord>> {
        self.reader.get_config(scope)
    }

    /// What `GET /config` would answer for this scope: the stored record with
    /// the server defaults filled in, or `None` when nothing is stored (the
    /// global scope always has one).
    pub fn stored_config(&self, subject: Option<&QualifiedSubject>) -> ApiResult<Option<ConfigRecord>> {
        self.reg.config_of(&self.reader, subject)
    }

    /// The mode that applies to a target: its own, else its context's, else
    /// the global one, with a global READONLY_OVERRIDE winning everywhere.
    pub fn mode_for(&self, target: &Target) -> ApiResult<Mode> {
        match target {
            Target::Subject(q) => self.reg.mode_in_scope(&self.reader, q),
            Target::Context(ctx) => self.reg.mode_in_scope(&self.reader, &QualifiedSubject::new(ctx, "")),
            Target::Global => self.reg.global_mode_in(&self.reader),
        }
    }
}

impl Target {
    /// How this target is named in an error message. Confluent prints `null`
    /// for the registry as a whole.
    pub fn scope_name(&self) -> String {
        match self {
            Target::Subject(q) => q.qualified(),
            Target::Context(ctx) => QualifiedSubject::new(ctx, "").qualified(),
            Target::Global => "null".to_string(),
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use crate::model::CompatibilityLevel;
    use crate::store::Store;

    /// A registry on a throwaway directory. Verbs are tested through their
    /// plan, so this exists only to hold a snapshot.
    pub fn registry() -> (Registry, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path(), false).expect("open store");
        let snap = store.load_snapshot().expect("snapshot");
        (Registry::new(store, snap, CompatibilityLevel::Backward, "test".into(), 100, false), dir)
    }

    /// The writes a verb planned, as a plain list for comparison.
    pub fn writes_of<M: Mutation>(reg: &Registry, m: &M) -> ApiResult<Vec<Write>> {
        Ok(m.plan(&ReadView::new(reg))?.writes)
    }
}
