//! Context-qualified subject names.
//!
//! Confluent encodes the context inside the subject string:
//!
//! * `orders-value`            -> context `.`,       subject `orders-value`
//! * `:.:orders-value`         -> context `.`,       subject `orders-value`
//! * `:.staging:orders-value`  -> context `.staging`, subject `orders-value`
//! * `:.staging:`              -> context `.staging`, no subject (context-level config/mode)
//! * `:*:`                     -> wildcard over every context (listing only)
//!
//! Each context is a fully separate namespace, including its own schema ID space.

use crate::error::ApiError;

pub const DEFAULT_CONTEXT: &str = ".";
pub const WILDCARD_CONTEXT: &str = "*";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedSubject {
    pub context: String,
    /// Empty when the string only named a context (`:.ctx:`).
    pub subject: String,
}

impl QualifiedSubject {
    /// Confluent's `QualifiedSubject(tenant, qualifiedSubject)`: only a `:.`
    /// prefix starts a context (`:abc:x` is a plain subject name); the context
    /// runs to the next `:` or to the end (`:.ctx` names the context itself).
    /// Context names are not validated.
    pub fn parse(raw: &str) -> Result<Self, ApiError> {
        Ok(if let Some(rest) = raw.strip_prefix(":.") {
            match rest.find(':') {
                Some(ix) => Self { context: normalize_parsed(&raw[1..2 + ix]), subject: rest[ix + 1..].to_string() },
                None => Self { context: normalize_parsed(&raw[1..]), subject: String::new() },
            }
        } else if let Some(rest) = raw.strip_prefix(":*:") {
            Self { context: WILDCARD_CONTEXT.to_string(), subject: rest.to_string() }
        } else {
            Self { context: DEFAULT_CONTEXT.to_string(), subject: raw.to_string() }
        })
    }

    /// Like [`parse`]; reads never validate names (Confluent only rejects
    /// invalid subjects on writes, see [`is_valid_subject`]).
    pub fn parse_subject(raw: &str) -> Result<Self, ApiError> {
        Self::parse(raw)
    }

    pub fn new(context: &str, subject: &str) -> Self {
        Self { context: context.to_string(), subject: subject.to_string() }
    }

    pub fn is_wildcard(&self) -> bool {
        self.context == WILDCARD_CONTEXT
    }

    pub fn is_context_only(&self) -> bool {
        self.subject.is_empty()
    }

    /// The form clients see: unqualified in the default context.
    pub fn qualified(&self) -> String {
        qualify(&self.context, &self.subject)
    }
}

pub fn qualify(context: &str, subject: &str) -> String {
    if context == DEFAULT_CONTEXT {
        subject.to_string()
    } else {
        format!(":{context}:{subject}")
    }
}

/// Canonical context name: always starts with a dot; `""` and `"."` are the default.
pub fn normalize_context(ctx: &str) -> Option<String> {
    if ctx.is_empty() || ctx == DEFAULT_CONTEXT {
        return Some(DEFAULT_CONTEXT.to_string());
    }
    if ctx == WILDCARD_CONTEXT {
        return Some(WILDCARD_CONTEXT.to_string());
    }
    let name = if ctx.starts_with('.') { ctx.to_string() } else { format!(".{ctx}") };
    let valid = name.len() > 1
        && name.len() <= 256
        && name[1..].chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
    valid.then_some(name)
}

/// `.` for the default context, else the context as written.
fn normalize_parsed(ctx: &str) -> String {
    if ctx.is_empty() { DEFAULT_CONTEXT.to_string() } else { ctx.to_string() }
}

/// Confluent's `QualifiedSubject.isValidSubject`: no ISO control characters,
/// and not one of the reserved names (after the context prefix).
pub fn is_valid_subject(raw: &str) -> bool {
    if raw.chars().any(|c| c.is_control()) {
        return false;
    }
    let q = QualifiedSubject::parse(raw).expect("infallible");
    q.subject != "__GLOBAL" && q.subject != "__EMPTY"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_qualified_subjects() {
        let q = QualifiedSubject::parse("foo").unwrap();
        assert_eq!((q.context.as_str(), q.subject.as_str()), (".", "foo"));
        let q = QualifiedSubject::parse(":.ctx:foo").unwrap();
        assert_eq!((q.context.as_str(), q.subject.as_str()), (".ctx", "foo"));
        assert_eq!(q.qualified(), ":.ctx:foo");
        let q = QualifiedSubject::parse(":.:foo").unwrap();
        assert_eq!(q.qualified(), "foo");
        let q = QualifiedSubject::parse(":.ctx:").unwrap();
        assert!(q.is_context_only());
        assert!(QualifiedSubject::parse(":*:").unwrap().is_wildcard());
        // Confluent: only ":." starts a context; ":.ctx" is the context itself.
        let q = QualifiedSubject::parse(":abc:x").unwrap();
        assert_eq!((q.context.as_str(), q.subject.as_str()), (".", ":abc:x"));
        let q = QualifiedSubject::parse(":.ctx").unwrap();
        assert_eq!((q.context.as_str(), q.subject.as_str()), (".ctx", ""));
        assert!(!is_valid_subject("tab\there"));
        assert!(!is_valid_subject(":.c:__GLOBAL"));
        assert!(is_valid_subject(""));
    }
}
