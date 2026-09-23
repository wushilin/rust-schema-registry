//! What a caller may do, and to which subjects.
//!
//! A user holds registry-wide roles (`roles = [...]`) and any number of
//! bindings, each giving one role over a set of subject patterns. The two add
//! up: a user can be `readonly` everywhere and `admin` over `abc*`.
//!
//! Every request names one of four things, and that decides which roles apply:
//!
//! | target                          | roles that count                          |
//! |---------------------------------|-------------------------------------------|
//! | a subject (`/subjects/x/...`)   | global roles + bindings matching `x`      |
//! | a context (`/config/:.eu:`)     | global roles + bindings covering `.eu`    |
//! | a listing (`GET /subjects`)     | any role the caller holds; the *response* is filtered |
//! | anything else (global settings, exporters, schema-by-id) | global roles only |
//!
//! Filtering, rather than refusing, is what makes a scoped role usable: an
//! admin for `abc*` sees `abc*` in `/subjects`, in `/schemas` and in the admin
//! UI, and nothing else.
//!
//! Deliberate limit: `GET /schemas/ids/{id}` is not subject-scoped. Ids are a
//! registry-wide namespace and serializers fetch them constantly, so any
//! authenticated caller may read a schema by id, as long as they hold a role
//! somewhere.

use crate::config::{Role, UserConfig};
use crate::context::{DEFAULT_CONTEXT, QualifiedSubject, WILDCARD_CONTEXT};

/// One `context::subject` pattern, both sides globbed with `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pattern {
    context: Glob,
    subject: Glob,
}

impl Pattern {
    /// `".eu::orders-*"`, or `"orders-*"` for the default context.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let (ctx, subject) = match raw.split_once("::") {
            Some((c, s)) => (c, s),
            None => (DEFAULT_CONTEXT, raw),
        };
        if subject.is_empty() {
            return Err("no subject part (did you mean '::*'?)".into());
        }
        // `.eu`, `eu` and `*` all name a context; `.` is the default one.
        let context = if ctx.is_empty() || ctx == DEFAULT_CONTEXT {
            Glob::literal(DEFAULT_CONTEXT)
        } else if ctx == WILDCARD_CONTEXT {
            Glob::any()
        } else if ctx.starts_with('.') {
            Glob::parse(ctx)
        } else {
            Glob::parse(&format!(".{ctx}"))
        };
        Ok(Self { context, subject: Glob::parse(subject) })
    }

    pub fn matches(&self, ctx: &str, subject: &str) -> bool {
        self.context.matches(ctx) && self.subject.matches(subject)
    }

    /// True when the pattern covers a whole context, which is what lets a
    /// binding reach that context's own configuration and mode.
    pub fn covers_context(&self, ctx: &str) -> bool {
        self.context.matches(ctx) && self.subject.is_any()
    }

    /// True when the pattern can match something in this context.
    pub fn touches_context(&self, ctx: &str) -> bool {
        self.context.matches(ctx)
    }
}

/// A `*`-glob. Literals are the common case and compare directly.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Glob {
    Any,
    Literal(String),
    /// Parts between `*`s, with flags for leading/trailing wildcards.
    Parts { parts: Vec<String>, open_start: bool, open_end: bool },
}

impl Glob {
    fn any() -> Self {
        Self::Any
    }

    fn literal(s: &str) -> Self {
        Self::Literal(s.to_string())
    }

    fn parse(raw: &str) -> Self {
        if raw == "*" {
            return Self::Any;
        }
        if !raw.contains('*') {
            return Self::Literal(raw.to_string());
        }
        let parts: Vec<String> = raw.split('*').filter(|p| !p.is_empty()).map(String::from).collect();
        Self::Parts { parts, open_start: raw.starts_with('*'), open_end: raw.ends_with('*') }
    }

    fn is_any(&self) -> bool {
        matches!(self, Self::Any)
    }

    fn matches(&self, s: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Literal(l) => l == s,
            Self::Parts { parts, open_start, open_end } => {
                let mut rest = s;
                for (i, part) in parts.iter().enumerate() {
                    let at = if i == 0 && !open_start {
                        if !rest.starts_with(part.as_str()) {
                            return false;
                        }
                        0
                    } else {
                        match rest.find(part.as_str()) {
                            Some(ix) => ix,
                            None => return false,
                        }
                    };
                    rest = &rest[at + part.len()..];
                }
                // A trailing `*` swallows whatever is left; otherwise the last
                // part has to end the string.
                *open_end || rest.is_empty()
            }
        }
    }
}

/// What a request is about, which decides whose roles apply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Tied to one subject.
    Subject(QualifiedSubject),
    /// A context's own settings, or the context itself.
    Context(String),
    /// A list whose response is filtered to what the caller may see.
    Listing,
    /// Everything else: global settings, exporters, schema-by-id, metadata.
    Global,
}

/// The roles and bindings of an authenticated caller.
#[derive(Debug, Clone, Default)]
pub struct Principal {
    pub roles: Vec<Role>,
    bindings: Vec<(Role, Vec<Pattern>)>,
}

impl Principal {
    /// Authentication is off: everything is allowed and nothing is filtered.
    pub fn unrestricted() -> Self {
        Self { roles: vec![Role::Admin], bindings: Vec::new() }
    }

    pub fn from_user(u: &UserConfig) -> Self {
        let bindings = u
            .bindings
            .iter()
            .map(|b| (b.role, b.subjects.iter().filter_map(|p| Pattern::parse(p).ok()).collect()))
            .collect();
        Self { roles: u.roles.clone(), bindings }
    }

    /// Roles that apply to one subject: the global ones plus every binding
    /// whose patterns match it.
    pub fn roles_for_subject(&self, q: &QualifiedSubject) -> Vec<Role> {
        let mut out = self.roles.clone();
        for (role, patterns) in &self.bindings {
            if !out.contains(role) && patterns.iter().any(|p| p.matches(&q.context, &q.subject)) {
                out.push(*role);
            }
        }
        out
    }

    /// Roles over a context's own settings: only a binding that covers the
    /// whole context reaches them.
    pub fn roles_for_context(&self, ctx: &str) -> Vec<Role> {
        let mut out = self.roles.clone();
        for (role, patterns) in &self.bindings {
            if !out.contains(role) && patterns.iter().any(|p| p.covers_context(ctx)) {
                out.push(*role);
            }
        }
        out
    }

    /// Does this caller see every subject? (No bindings, or a global role.)
    pub fn sees_everything(&self) -> bool {
        !self.roles.is_empty() || self.bindings.is_empty()
    }

    /// May this caller know that this subject exists?
    pub fn can_see_subject(&self, ctx: &str, subject: &str) -> bool {
        self.sees_everything() || self.bindings.iter().any(|(_, ps)| ps.iter().any(|p| p.matches(ctx, subject)))
    }

    /// May this caller know that this context exists?
    pub fn can_see_context(&self, ctx: &str) -> bool {
        self.sees_everything() || self.bindings.iter().any(|(_, ps)| ps.iter().any(|p| p.touches_context(ctx)))
    }

    /// Does the caller hold any role at all? (Used for listings and by-id reads.)
    fn has_any_role(&self) -> bool {
        !self.roles.is_empty() || !self.bindings.is_empty()
    }

    /// Does the caller hold `role` anywhere - globally or over some subjects?
    pub fn has_role_anywhere(&self, role: Role) -> bool {
        self.roles.contains(&role) || self.bindings.iter().any(|(r, _)| *r == role)
    }

    /// Is this caller an admin of this subject? The admin UI lists exactly the
    /// subjects this is true for, so everything it shows can also be operated.
    pub fn is_admin_of(&self, ctx: &str, subject: &str) -> bool {
        self.roles.contains(&Role::Admin)
            || self
                .bindings
                .iter()
                .any(|(r, ps)| *r == Role::Admin && ps.iter().any(|p| p.matches(ctx, subject)))
    }
}

/// The subject, context or listing a path is about. `path` is the rewritten
/// path (see `api::rewrite`), so contexts appear as `:.ctx:` in the subject.
pub fn target_of(path: &str) -> Target {
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
    let subject = |raw: &str| {
        let decoded = crate::api::percent_decode(raw);
        match QualifiedSubject::parse(&decoded) {
            // `:.eu:` names a context, `:.eu:x` a subject in it.
            Ok(q) if q.is_context_only() => Target::Context(q.context),
            Ok(q) => Target::Subject(q),
            Err(_) => Target::Global,
        }
    };
    match segs.as_slice() {
        ["subjects"] | ["schemas"] | ["contexts"] => Target::Listing,
        ["schemas", "ids", _, "subjects" | "versions"] => Target::Listing,
        ["_admin"] | ["_admin", ""] | ["_admin", "api", "overview"] => Target::Listing,
        ["subjects", s, ..] => subject(s),
        ["compatibility", "subjects", s, ..] => subject(s),
        ["config" | "mode", s, ..] => subject(s),
        ["contexts", c, ..] => Target::Context(crate::context::normalize_context(&crate::api::percent_decode(c)).unwrap_or_default()),
        ["_admin", "api", "subjects", s, ..] => subject(s),
        _ => Target::Global,
    }
}

/// Is this request allowed?
pub fn authorized(p: &Principal, method: &axum::http::Method, path: &str) -> bool {
    let roles = match target_of(path) {
        Target::Subject(q) => p.roles_for_subject(&q),
        Target::Context(ctx) => p.roles_for_context(&ctx),
        // Listings are allowed for anyone holding the role that path needs;
        // the handler then filters the response to what the caller may see.
        Target::Listing => {
            return if path.starts_with("/_admin") { p.has_role_anywhere(Role::Admin) } else { p.has_any_role() };
        }
        Target::Global => p.roles.clone(),
    };
    allows(&roles, method, path)
}

/// The role rules themselves, once we know which roles apply here.
fn allows(roles: &[Role], method: &axum::http::Method, path: &str) -> bool {
    use axum::http::Method;
    // The admin UI shows every subject, schema and setting at once: admins only.
    if path.starts_with("/_admin") {
        return roles.contains(&Role::Admin);
    }
    if roles.contains(&Role::Admin) {
        return true;
    }
    let read_only_request = matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
        || (*method == Method::POST
            && (path.starts_with("/compatibility/") || (path.starts_with("/subjects/") && !path.ends_with("/versions"))));
    if read_only_request {
        return !roles.is_empty();
    }
    if roles.contains(&Role::Write) {
        // Writers manage schemas and subject-scoped settings, not global ones or exporters.
        // `/config/{subject}` is subject-scoped; `/config` (global) and `/config/:.ctx:` (context) are not.
        let subject_scoped = (path.starts_with("/config/") || path.starts_with("/mode/")) && !path.ends_with(':');
        return path.starts_with("/subjects/") || subject_scoped;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    fn user(roles: &[Role], bindings: &[(Role, &[&str])]) -> Principal {
        Principal {
            roles: roles.to_vec(),
            bindings: bindings
                .iter()
                .map(|(r, ps)| (*r, ps.iter().map(|p| Pattern::parse(p).expect("pattern")).collect()))
                .collect(),
        }
    }

    #[test]
    fn patterns_match_context_and_subject() {
        let p = |s: &str| Pattern::parse(s).unwrap();
        assert!(p(".::test-subject").matches(".", "test-subject"));
        assert!(!p(".::test-subject").matches(".eu", "test-subject"));
        assert!(p(".::test*").matches(".", "test-1"));
        assert!(p(".::test*").matches(".", "test"));
        assert!(!p(".::test*").matches(".", "atest"));
        assert!(p("*::*").matches(".eu", "anything"));
        assert!(p(".eu::*").matches(".eu", "orders"));
        assert!(!p(".eu::*").matches(".", "orders"));
        // A bare pattern is the default context.
        assert!(p("orders-*").matches(".", "orders-value"));
        assert!(!p("orders-*").matches(".eu", "orders-value"));
        // Wildcards in the middle and at the front.
        assert!(p(".::*-value").matches(".", "orders-value"));
        assert!(p(".::a*z").matches(".", "abcz"));
        assert!(!p(".::a*z").matches(".", "abc"));
        // Only a whole-context pattern reaches the context's own settings.
        assert!(p(".eu::*").covers_context(".eu"));
        assert!(!p(".eu::orders*").covers_context(".eu"));
    }

    #[test]
    fn patterns_reject_nonsense() {
        assert!(Pattern::parse(".eu::").is_err());
        assert!(Pattern::parse("").is_err());
    }

    #[test]
    fn a_binding_grants_its_role_only_on_matching_subjects() {
        // readonly everywhere, admin over abc*/def*, write over xyz-*.
        let p = user(&[Role::Readonly], &[(Role::Admin, &[".::abc*", ".::def*"]), (Role::Write, &[".::xyz-*"])]);
        assert!(authorized(&p, &Method::DELETE, "/subjects/abc-1"));
        assert!(authorized(&p, &Method::PUT, "/config/def-2"));
        assert!(authorized(&p, &Method::POST, "/subjects/xyz-1/versions"));
        // write does not reach deletes of other people's subjects...
        assert!(!authorized(&p, &Method::DELETE, "/subjects/other"));
        assert!(!authorized(&p, &Method::POST, "/subjects/other/versions"));
        // ...but readonly still reads them.
        assert!(authorized(&p, &Method::GET, "/subjects/other/versions"));
        // Global settings and exporters need a global admin role.
        assert!(!authorized(&p, &Method::PUT, "/config"));
        assert!(!authorized(&p, &Method::POST, "/exporters"));
        // The admin UI opens for an admin of anything, and shows that much.
        assert!(authorized(&p, &Method::GET, "/_admin"));
        assert!(authorized(&p, &Method::GET, "/_admin/api/subjects/abc-1"));
        assert!(!authorized(&p, &Method::GET, "/_admin/api/subjects/other"));
        assert!(p.is_admin_of(".", "abc-1"));
        assert!(!p.is_admin_of(".", "xyz-1"), "a write binding is not an admin one");
    }

    #[test]
    fn a_context_wide_binding_owns_that_contexts_settings() {
        let p = user(&[], &[(Role::Admin, &[".eu::*"])]);
        assert!(authorized(&p, &Method::PUT, "/config/:.eu:"));
        assert!(authorized(&p, &Method::PUT, "/mode/:.eu:"));
        assert!(authorized(&p, &Method::DELETE, "/contexts/.eu"));
        assert!(authorized(&p, &Method::POST, "/subjects/:.eu:orders/versions"));
        // Not the default context, and not the global settings.
        assert!(!authorized(&p, &Method::PUT, "/config/:.us:"));
        assert!(!authorized(&p, &Method::PUT, "/config"));
        assert!(!authorized(&p, &Method::POST, "/subjects/orders/versions"));

        // A prefix binding inside a context does not own the context.
        let q = user(&[], &[(Role::Admin, &[".eu::orders-*"])]);
        assert!(!authorized(&q, &Method::PUT, "/config/:.eu:"));
        assert!(authorized(&q, &Method::PUT, "/config/:.eu:orders-1"));
    }

    #[test]
    fn listings_are_allowed_and_filtered_rather_than_refused() {
        let p = user(&[], &[(Role::Write, &[".::abc*"])]);
        assert!(authorized(&p, &Method::GET, "/subjects"));
        assert!(authorized(&p, &Method::GET, "/schemas"));
        assert!(authorized(&p, &Method::GET, "/contexts"));
        assert!(!p.sees_everything());
        assert!(p.can_see_subject(".", "abc-1"));
        assert!(!p.can_see_subject(".", "other"));
        assert!(p.can_see_context("."));
        assert!(!p.can_see_context(".eu"));
        // A user with a global role sees everything, filtering included.
        let g = user(&[Role::Readonly], &[]);
        assert!(g.sees_everything());
        assert!(g.can_see_subject(".eu", "anything"));
    }

    #[test]
    fn paths_resolve_to_the_thing_they_are_about() {
        assert_eq!(target_of("/subjects"), Target::Listing);
        assert_eq!(target_of("/subjects/foo/versions"), Target::Subject(QualifiedSubject::new(".", "foo")));
        assert_eq!(target_of("/subjects/%3A.eu%3Afoo"), Target::Subject(QualifiedSubject::new(".eu", "foo")));
        assert_eq!(target_of("/compatibility/subjects/foo/versions/1"), Target::Subject(QualifiedSubject::new(".", "foo")));
        assert_eq!(target_of("/config"), Target::Global);
        assert_eq!(target_of("/config/foo"), Target::Subject(QualifiedSubject::new(".", "foo")));
        assert_eq!(target_of("/config/:.eu:"), Target::Context(".eu".into()));
        assert_eq!(target_of("/mode/:.eu:"), Target::Context(".eu".into()));
        assert_eq!(target_of("/contexts"), Target::Listing);
        assert_eq!(target_of("/contexts/.eu"), Target::Context(".eu".into()));
        assert_eq!(target_of("/schemas/ids/7"), Target::Global);
        assert_eq!(target_of("/schemas/ids/7/subjects"), Target::Listing);
        assert_eq!(target_of("/exporters/x/pause"), Target::Global);
        assert_eq!(target_of("/_admin/api/overview"), Target::Listing);
        assert_eq!(target_of("/_admin/api/subjects/foo"), Target::Subject(QualifiedSubject::new(".", "foo")));
    }

    #[test]
    fn plain_global_roles_behave_exactly_as_before() {
        let ro = user(&[Role::Readonly], &[]);
        let w = user(&[Role::Write], &[]);
        let admin = user(&[Role::Admin], &[]);
        assert!(authorized(&ro, &Method::GET, "/subjects"));
        assert!(authorized(&ro, &Method::POST, "/subjects/foo"));
        assert!(authorized(&ro, &Method::POST, "/compatibility/subjects/foo/versions/latest"));
        assert!(!authorized(&ro, &Method::POST, "/subjects/foo/versions"));
        assert!(authorized(&w, &Method::POST, "/subjects/foo/versions"));
        assert!(authorized(&w, &Method::PUT, "/config/foo"));
        assert!(!authorized(&w, &Method::PUT, "/config"));
        assert!(!authorized(&w, &Method::PUT, "/config/:.eu:"));
        assert!(!authorized(&w, &Method::POST, "/exporters"));
        assert!(!authorized(&ro, &Method::GET, "/_admin"));
        assert!(!authorized(&w, &Method::GET, "/_admin/api/overview"));
        assert!(authorized(&admin, &Method::GET, "/_admin"));
        assert!(authorized(&admin, &Method::POST, "/exporters"));
    }
}
