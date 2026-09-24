//! Server configuration (TOML file, overridable from the command line).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Address to listen on.
    pub listen: SocketAddr,
    /// RocksDB directory.
    pub data_dir: PathBuf,
    /// Compatibility level used when neither global, context nor subject config sets one.
    pub default_compatibility: String,
    /// Reported by `/v1/metadata/id` and used as the AUTO exporter context name.
    /// Generated and persisted on first start when unset.
    pub cluster_id: Option<String>,
    /// fsync the WAL on every write. Turning this off trades durability of the
    /// last few writes on power loss for lower write latency.
    pub sync_writes: bool,
    /// Maximum request body size in bytes.
    pub max_body_bytes: usize,
    /// Normalize every schema on registration and lookup unless a subject,
    /// context or global config says otherwise. On (the default), logically
    /// equal schemas (JSON key order, Protobuf field order or qualified type
    /// names, Avro property order, ...) share one id. `false` gives
    /// Confluent's default, where only identical canonical forms do.
    pub normalize: bool,
    /// `GET /schemas` result cap (Confluent `schema.search.default.limit` / `max.limit`).
    pub schema_search_default_limit: usize,
    pub schema_search_max_limit: usize,
    /// `GET /subjects` result cap (Confluent `subject.search.default.limit` / `max.limit`).
    pub subject_search_default_limit: usize,
    pub subject_search_max_limit: usize,
    /// Maximum entries in each in-memory schema cache (schema bodies, parsed
    /// stored schemas, parsed request schemas). Metadata (ids, subjects,
    /// versions, config) is always fully in memory; this bounds the big part.
    pub cache_max_entries: u64,
    /// How often exporters poll when idle (they are also woken on every change).
    pub exporter_poll_seconds: u64,
    pub auth: AuthConfig,
    /// Host containers: several logically separate registries in one process
    /// and one store. Without any, there is one named `default` that answers
    /// on every host, and nothing about the API changes.
    pub containers: Vec<ContainerConfig>,
}

/// `[[containers]]`: a container and the hosts that reach it.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ContainerConfig {
    pub name: String,
    /// `Host` header patterns, `*` allowed (`sr.example.com`, `*.dev.example.com`,
    /// `sr.example.com:8081`). Matching ignores case; a pattern without a port
    /// also matches the host with any port.
    pub hosts: Vec<String>,
}

impl Default for ContainerConfig {
    fn default() -> Self {
        Self { name: crate::tenant::DEFAULT_TENANT.to_string(), hosts: vec!["*".to_string()] }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:8081".parse().expect("valid address"),
            data_dir: PathBuf::from("./data"),
            default_compatibility: "BACKWARD".into(),
            cluster_id: None,
            sync_writes: true,
            max_body_bytes: 16 * 1024 * 1024,
            exporter_poll_seconds: 10,
            cache_max_entries: 20_000,
            normalize: true,
            schema_search_default_limit: 1000,
            schema_search_max_limit: 1000,
            subject_search_default_limit: 20_000,
            subject_search_max_limit: 20_000,
            auth: AuthConfig::default(),
            containers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AuthConfig {
    pub enabled: bool,
    pub realm: String,
    pub users: Vec<UserConfig>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { enabled: false, realm: "SchemaRegistry".into(), users: Vec::new() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    pub username: String,
    /// bcrypt hash (recommended, see `hash-password`) or plaintext.
    pub password: String,
    /// Roles that apply everywhere.
    #[serde(default = "default_roles")]
    pub roles: Vec<Role>,
    /// Roles that apply only to the subjects they name. A user can hold
    /// several, one per role, and they add to `roles` rather than limiting it.
    #[serde(default)]
    pub bindings: Vec<RoleBinding>,
    /// Host containers this user may use at all. Empty means every one of
    /// them. Since the `Host` header is the client's to choose, this - not the
    /// host - is what keeps one container's data away from another's users.
    #[serde(default)]
    pub containers: Vec<String>,
}

/// `[[auth.users.bindings]]`: one role, scoped to a set of subject patterns.
///
/// A pattern is `context::subject`, both sides accepting `*` as a wildcard:
/// `.::orders-value` (that subject in the default context), `.::test*`,
/// `.eu::*` (everything in `.eu`, and that context's own settings), `*::*`
/// (everywhere, i.e. the same as a global role). A pattern without `::` is
/// read as a subject in the default context.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleBinding {
    pub role: Role,
    pub subjects: Vec<String>,
}

fn default_roles() -> Vec<Role> {
    vec![Role::Readonly]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Write,
    Readonly,
}

impl ServerConfig {
    pub fn load(path: Option<&Path>) -> anyhow::Result<Self> {
        let cfg: ServerConfig = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p).map_err(|e| anyhow::anyhow!("reading {}: {e}", p.display()))?;
                toml::from_str(&text).map_err(|e| anyhow::anyhow!("parsing {}: {e}", p.display()))?
            }
            None => ServerConfig::default(),
        };
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        crate::model::CompatibilityLevel::parse(&self.default_compatibility)
            .map_err(|e| anyhow::anyhow!("default_compatibility: {}", e.message))?;
        if self.auth.enabled && self.auth.users.is_empty() {
            anyhow::bail!("auth.enabled = true but no [[auth.users]] are configured");
        }
        let mut names = std::collections::HashSet::new();
        for c in &self.containers {
            crate::tenant::TenantId::parse(&c.name).map_err(|e| anyhow::anyhow!("{}", e.message))?;
            if !names.insert(&c.name) {
                anyhow::bail!("duplicate host container '{}'", c.name);
            }
            if c.hosts.is_empty() {
                anyhow::bail!("host container '{}' has no hosts", c.name);
            }
            for h in &c.hosts {
                glob::Pattern::new(h).map_err(|e| anyhow::anyhow!("container '{}': host pattern '{h}': {e}", c.name))?;
            }
        }
        for u in &self.auth.users {
            for c in &u.containers {
                if !self.containers.is_empty() && !names.contains(c) {
                    anyhow::bail!("user '{}' is bound to unknown host container '{c}'", u.username);
                }
            }
        }
        let mut seen = std::collections::HashSet::new();
        for u in &self.auth.users {
            if u.username.is_empty() || u.username.contains(':') {
                anyhow::bail!("invalid username '{}'", u.username);
            }
            if !seen.insert(&u.username) {
                anyhow::bail!("duplicate user '{}'", u.username);
            }
            for b in &u.bindings {
                if b.subjects.is_empty() {
                    anyhow::bail!("user '{}': a binding needs at least one subject pattern", u.username);
                }
                for p in &b.subjects {
                    crate::authz::Pattern::parse(p)
                        .map_err(|e| anyhow::anyhow!("user '{}': subject pattern '{p}': {e}", u.username))?;
                }
            }
            if u.roles.is_empty() && u.bindings.is_empty() {
                anyhow::bail!("user '{}' has no roles and no bindings", u.username);
            }
        }
        Ok(())
    }
}
