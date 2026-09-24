//! Host containers: several logically separate registries in one process and
//! one database.
//!
//! A container owns everything a standalone registry owns - its contexts, its
//! subjects, its schema ids, its global config and mode, its exporters and its
//! change log. Containers share nothing logically; physically they share the
//! store, separated by a prefix on every key (see `store`).
//!
//! The hierarchy is *container > context > subject*. A deployment that
//! configures none gets exactly one, named `default`, and behaves as if this
//! module did not exist.

use crate::error::ApiError;

/// The name of a host container. Guaranteed usable as a key prefix: no NUL, no
/// leading dot (so it can never be mistaken for a context name), bounded.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

pub const DEFAULT_TENANT: &str = "default";

impl TenantId {
    /// The container a deployment gets when it configures none.
    pub fn default_tenant() -> Self {
        Self(DEFAULT_TENANT.to_string())
    }

    pub fn parse(name: &str) -> Result<Self, ApiError> {
        let ok = !name.is_empty()
            && name.len() <= 64
            && !name.starts_with('.')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
        if !ok {
            return Err(ApiError::unprocessable(format!(
                "Invalid host container name '{name}': 1-64 characters of [A-Za-z0-9._-], not starting with '.'"
            )));
        }
        Ok(Self(name.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// What every key of this container starts with. The trailing NUL is what
    /// keeps one container's keys from being a prefix of another's
    /// (`prod` vs `prod-eu`), so prefix scans stay exact.
    pub fn key_prefix(&self) -> Vec<u8> {
        [self.0.as_bytes(), &[0]].concat()
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_that_could_confuse_a_key_are_refused() {
        assert!(TenantId::parse("default").is_ok());
        assert!(TenantId::parse("prod-eu_1.a").is_ok());
        // A leading dot is how context names start; a container must not look
        // like one, because old keys began with a context.
        assert!(TenantId::parse(".prod").is_err());
        assert!(TenantId::parse("").is_err());
        assert!(TenantId::parse("with space").is_err());
        assert!(TenantId::parse("with\0nul").is_err());
        assert!(TenantId::parse(&"x".repeat(65)).is_err());
    }

    #[test]
    fn one_container_is_never_a_prefix_of_another() {
        let prod = TenantId::parse("prod").unwrap().key_prefix();
        let prod_eu = TenantId::parse("prod-eu").unwrap().key_prefix();
        assert!(!prod_eu.starts_with(&prod), "a scan of prod would return prod-eu's rows");
        assert_eq!(prod, b"prod\0");
    }
}
