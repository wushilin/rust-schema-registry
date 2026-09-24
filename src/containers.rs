//! Which host container a request belongs to.
//!
//! A container is a whole registry - its own contexts, subjects, ids, config,
//! modes, exporters and change log - and they share one process and one store.
//! The request's `Host` header picks one, against patterns from the config.
//!
//! The `Host` header is the client's to choose, so this is **routing, not
//! authorization**. What keeps a container's data away from another's users is
//! that a user can be bound to containers (`containers = [...]` on the user),
//! checked in `authz` after the container is resolved. A deployment that wants
//! a harder boundary should also give each container its own listener and let
//! the network decide who reaches which port.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::ContainerConfig;
use crate::error::ApiError;
use crate::registry::Registry;
use crate::tenant::TenantId;

pub struct Containers {
    /// Patterns in configuration order; the first match wins.
    routes: Vec<(glob::Pattern, TenantId)>,
    by_name: HashMap<TenantId, Arc<Registry>>,
}

impl Containers {
    pub fn new(configured: &[ContainerConfig], registries: HashMap<TenantId, Arc<Registry>>) -> Self {
        let mut routes = Vec::new();
        for c in configured {
            let Ok(name) = TenantId::parse(&c.name) else { continue };
            for h in &c.hosts {
                if let Ok(p) = glob::Pattern::new(&h.to_ascii_lowercase()) {
                    routes.push((p, name.clone()));
                }
            }
        }
        // Nothing configured: one container, every host.
        if routes.is_empty() {
            routes.push((glob::Pattern::new("*").expect("valid"), TenantId::default_tenant()));
        }
        Self { routes, by_name: registries }
    }

    pub fn all(&self) -> impl Iterator<Item = (&TenantId, &Arc<Registry>)> {
        self.by_name.iter()
    }

    pub fn get(&self, name: &TenantId) -> Option<&Arc<Registry>> {
        self.by_name.get(name)
    }

    /// The container a `Host` header reaches, if any. A pattern without a port
    /// also matches the same host with one, because a client may or may not
    /// include it.
    pub fn route(&self, host: Option<&str>) -> Option<(&TenantId, &Arc<Registry>)> {
        let host = host.unwrap_or_default().to_ascii_lowercase();
        let without_port = host.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or_else(|| host.clone());
        let name = self
            .routes
            .iter()
            .find(|(p, _)| p.matches(&host) || (!p.as_str().contains(':') && p.matches(&without_port)))
            .map(|(_, n)| n)?;
        self.by_name.get_key_value(name)
    }

    /// What a request that reaches no container is told. 421 is the status for
    /// "you asked the wrong server for this host".
    pub fn no_such_host(host: Option<&str>) -> ApiError {
        ApiError::new(42101, format!("No host container is configured for host '{}'", host.unwrap_or("")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn containers(cfg: &[(&str, &[&str])]) -> Containers {
        let configured: Vec<ContainerConfig> = cfg
            .iter()
            .map(|(name, hosts)| ContainerConfig {
                name: name.to_string(),
                hosts: hosts.iter().map(|h| h.to_string()).collect(),
            })
            .collect();
        Containers::new(&configured, HashMap::new())
    }

    /// `route` needs a registry to return one, so these assert the name the
    /// patterns pick rather than the registry behind it.
    fn picked(c: &Containers, host: &str) -> Option<String> {
        let host = host.to_ascii_lowercase();
        let without_port = host.rsplit_once(':').map(|(h, _)| h.to_string()).unwrap_or_else(|| host.clone());
        c.routes
            .iter()
            .find(|(p, _)| p.matches(&host) || (!p.as_str().contains(':') && p.matches(&without_port)))
            .map(|(_, n)| n.to_string())
    }

    #[test]
    fn hosts_pick_a_container_in_configuration_order() {
        let c = containers(&[("prod", &["sr.example.com"]), ("dev", &["*.dev.example.com", "sr-dev.example.com:8081"])]);
        assert_eq!(picked(&c, "sr.example.com").as_deref(), Some("prod"));
        // A client may or may not send the port; a pattern without one takes both.
        assert_eq!(picked(&c, "sr.example.com:8081").as_deref(), Some("prod"));
        assert_eq!(picked(&c, "SR.example.com").as_deref(), Some("prod"), "hosts are case-insensitive");
        assert_eq!(picked(&c, "a.dev.example.com").as_deref(), Some("dev"));
        // A pattern that names a port matches only that port.
        assert_eq!(picked(&c, "sr-dev.example.com:8081").as_deref(), Some("dev"));
        assert_eq!(picked(&c, "sr-dev.example.com").as_deref(), None);
        assert_eq!(picked(&c, "unknown.example.com"), None);
    }

    #[test]
    fn without_configuration_everything_is_the_default_container() {
        let c = containers(&[]);
        assert_eq!(picked(&c, "anything").as_deref(), Some("default"));
        assert_eq!(picked(&c, "").as_deref(), Some("default"));
    }
}
