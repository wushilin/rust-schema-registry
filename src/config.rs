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
    /// `text` (the default) or `json`. Both carry the same fields; `json` is
    /// the one a log collector can read without guessing.
    pub log_format: String,
    /// Log every request, not only writes and failures. Reads are the bulk of
    /// the traffic, so this is off unless someone is looking for something.
    pub log_reads: bool,
    pub proxy: ProxyConfig,
    pub auth: AuthConfig,
    /// Host containers: several logically separate registries in one process
    /// and one store. Without any, there is one named `default` that answers
    /// on every host, and nothing about the API changes.
    pub containers: Vec<ContainerConfig>,
}

/// `[proxy]`: what sits in front, if anything.
///
/// Off unless said otherwise. When something does sit in front, say which
/// version of the PROXY protocol it sends rather than leaving the server to
/// guess: a header of the other version is then refused loudly instead of
/// working by accident.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyConfig {
    /// Read the PROXY protocol header from the first bytes of a connection.
    pub proxy_on: bool,
    /// 1 (a line of text) or 2 (binary). Only read when `proxy_on`.
    pub proxy_version: u8,
    /// Whose headers are believed, as CIDRs - written as a list or as one
    /// string with `;` or `,` between them. Anyone can write those bytes, so a
    /// client on the internet must not be able to choose what the log says.
    /// Defaults to loopback and the private ranges.
    #[serde(deserialize_with = "one_or_many")]
    pub trust: Vec<String>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            proxy_on: false,
            proxy_version: 2,
            trust: crate::proxy_protocol::DEFAULT_TRUSTED.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl ProxyConfig {
    pub fn expect(&self) -> Result<crate::proxy_protocol::Expect, String> {
        Ok(crate::proxy_protocol::Expect {
            on: self.proxy_on,
            version: crate::proxy_protocol::Version::parse(self.proxy_version)?,
        })
    }
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
            proxy: ProxyConfig::default(),
            log_format: "text".into(),
            log_reads: false,
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

/// A setting that is naturally a list but is often written as one line:
/// `"192.168.44.0/24;127.0.0.1/32"` and `["192.168.44.0/24", "127.0.0.1/32"]`
/// mean the same thing.
fn one_or_many<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    let split = |s: &str| -> Vec<String> {
        s.split([';', ',']).map(str::trim).filter(|p| !p.is_empty()).map(String::from).collect()
    };
    Ok(match OneOrMany::deserialize(d)? {
        OneOrMany::One(s) => split(&s),
        OneOrMany::Many(v) => v.iter().flat_map(|s| split(s)).collect(),
    })
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
        crate::proxy_protocol::Version::parse(self.proxy.proxy_version).map_err(|e| anyhow::anyhow!("[proxy] {e}"))?;
        crate::proxy_protocol::Trusted::parse(&self.proxy.trust).map_err(|e| anyhow::anyhow!("[proxy] trust: {e}"))?;
        if !matches!(self.log_format.as_str(), "text" | "json") {
            anyhow::bail!("log_format must be 'text' or 'json', not '{}'", self.log_format);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn load(toml_text: &str) -> anyhow::Result<ServerConfig> {
        let cfg: ServerConfig = toml::from_str(toml_text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn the_example_config_is_valid() {
        // Every operator starts from this file, and until now no test had read
        // it: a renamed field or a `deny_unknown_fields` slip would ship.
        let cfg = load(include_str!("../config.example.toml")).expect("config.example.toml must parse and validate");
        assert!(!cfg.data_dir.as_os_str().is_empty());
    }

    #[test]
    fn a_configuration_that_cannot_work_is_refused_at_startup() {
        // Each of these is a way to get a server that runs but does not do what
        // the file says. Refusing beats starting and surprising someone later.
        let cases: &[(&str, &str)] = &[
            (r#"log_format = "pretty""#, "log_format"),
            (r#"default_compatibility = "SIDEWAYS""#, "default_compatibility"),
            ("[auth]\nenabled = true", "no [[auth.users]]"),
            (r#"[proxy]
proxy_version = 3"#, "proxy"),
            (r#"[proxy]
trust = "not-a-cidr""#, "trust"),
            (r#"[[containers]]
name = "a"
hosts = ["x"]

[[containers]]
name = "a"
hosts = ["y"]"#, "duplicate host container"),
            (r#"[[containers]]
name = "a"
hosts = []"#, "no hosts"),
            (r#"[[containers]]
name = "A B"
hosts = ["x"]"#, "A B"),
            (r#"[[containers]]
name = "a"
hosts = ["["]"#, "host pattern"),
            (r#"[auth]
enabled = true

[[auth.users]]
username = "u"
password = "p"
containers = ["nope"]

[[containers]]
name = "a"
hosts = ["x"]"#, "unknown host container"),
            (r#"[auth]
enabled = true

[[auth.users]]
username = "a:b"
password = "p""#, "invalid username"),
            (r#"[auth]
enabled = true

[[auth.users]]
username = "u"
password = "p"

[[auth.users]]
username = "u"
password = "q""#, "duplicate user"),
            (r#"[auth]
enabled = true

[[auth.users]]
username = "u"
password = "p"
roles = []"#, "no roles and no bindings"),
            (r#"[auth]
enabled = true

[[auth.users]]
username = "u"
password = "p"
[[auth.users.bindings]]
role = "admin"
subjects = []"#, "at least one subject pattern"),
            (r#"[auth]
enabled = true

[[auth.users]]
username = "u"
password = "p"
[[auth.users.bindings]]
role = "admin"
subjects = [".eu::"]"#, "subject pattern"),
            // A typo in a field name is a setting that silently does nothing.
            (r#"sync_write = true"#, "sync_write"),
        ];
        for (text, expected) in cases {
            let err = load(text).err().unwrap_or_else(|| panic!("this should not have been accepted:\n{text}"));
            let msg = format!("{err:#}");
            assert!(msg.contains(expected), "expected {expected:?} in the refusal, got: {msg}");
        }
    }

    #[test]
    fn a_list_setting_may_be_written_as_one_line_or_as_a_list() {
        let one = load("[proxy]\ntrust = \"10.0.0.0/8; 127.0.0.1/32\"").unwrap();
        let many = load("[proxy]\ntrust = [\"10.0.0.0/8\", \"127.0.0.1/32\"]").unwrap();
        assert_eq!(one.proxy.trust, many.proxy.trust);
        assert_eq!(one.proxy.trust, vec!["10.0.0.0/8".to_string(), "127.0.0.1/32".to_string()]);
    }
}
