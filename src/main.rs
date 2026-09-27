//! A Confluent-compatible Schema Registry: single node, RocksDB storage,
//! contexts, exporters and HTTP Basic auth.

mod api;
mod auth;
mod backup;
mod authz;
mod config;
mod containers;
#[cfg(test)]
mod compat_level_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod http_conformance_tests;
mod context;
mod engine;
mod error;
mod exporter;
mod metrics;
mod migrate;
mod model;
mod modegate;
mod mutations;
mod proxy_protocol;
mod registry;
mod schema;
mod snapshot;
mod store;
mod tenant;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};

use crate::auth::Auth;
use crate::config::ServerConfig;
use crate::model::CompatibilityLevel;
use crate::registry::Registry;

#[derive(Parser)]
#[command(name = "schema-registry", version, about = "Confluent-compatible schema registry backed by RocksDB")]
struct Cli {
    /// Path to a TOML config file.
    #[arg(short, long, env = "SR_CONFIG")]
    config: Option<PathBuf>,
    /// Override `listen` from the config file.
    #[arg(long, env = "SR_LISTEN")]
    listen: Option<std::net::SocketAddr>,
    /// Override `data_dir` from the config file.
    #[arg(long, env = "SR_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the server (default).
    Serve,
    /// Print a bcrypt hash for use as a user password in the config file.
    HashPassword {
        password: String,
        #[arg(long, default_value_t = 12)]
        cost: u32,
    },
    /// Copy every subject, version and setting from another registry into this
    /// one, keeping schema ids and version numbers.
    Migrate {
        /// Source registry, e.g. a Confluent Schema Registry.
        #[arg(long)]
        from: String,
        /// Destination registry (this one, usually).
        #[arg(long)]
        to: String,
        /// Basic auth for the source, as `user:password`.
        #[arg(long)]
        from_auth: Option<String>,
        /// Basic auth for the destination, as `user:password`.
        #[arg(long)]
        to_auth: Option<String>,
        /// Only subjects with this prefix (`:*:` - every context - by default).
        #[arg(long)]
        subject_prefix: Option<String>,
        /// Leave soft-deleted versions behind instead of recreating them.
        #[arg(long)]
        skip_deleted: bool,
        /// Report what would be copied without writing anything.
        #[arg(long)]
        dry_run: bool,
    },
    /// Write a whole registry to a newline-delimited JSON dump: subjects,
    /// versions, ids, references, metadata, rule sets, config, modes and
    /// exporters. Works against any Confluent-compatible registry.
    Backup {
        /// Registry to read, e.g. http://localhost:8081.
        #[arg(long)]
        from: String,
        /// Basic auth for it, as `user:password`.
        #[arg(long)]
        from_auth: Option<String>,
        /// Where to write the dump; `-` (the default) is standard output.
        #[arg(long, default_value = "-")]
        out: String,
        /// Only subjects with this prefix (`:*:` - every context - by default).
        #[arg(long)]
        subject_prefix: Option<String>,
        /// Read using only the Confluent API, as for a source that is not this
        /// software. The default asks this registry for its own dump first,
        /// which carries what that API cannot express - an alias, for one.
        #[arg(long)]
        confluent_api: bool,
    },
    /// Replay a dump into a registry, keeping ids and version numbers. The
    /// destination must allow IMPORT mode; re-running is safe.
    Restore {
        /// The dump to read; `-` is standard input.
        #[arg(long)]
        from: String,
        /// Registry to write into.
        #[arg(long)]
        to: String,
        /// Basic auth for the destination, as `user:password`.
        #[arg(long)]
        to_auth: Option<String>,
        /// Report what would be written without writing anything.
        #[arg(long)]
        dry_run: bool,
    },
}

/// Records go to stdout; warnings and errors go to stderr, so a shell can
/// separate "what happened" from "what went wrong" without parsing anything.
/// `log_format = "json"` swaps the human-readable layout for one line of JSON
/// per event, with the same fields either way.
fn init_logging(cfg: &ServerConfig) {
    use tracing_subscriber::fmt::writer::MakeWriterExt;
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info,tower_http=warn".into());
    let writer = std::io::stderr
        .with_max_level(tracing::Level::WARN)
        .or_else(std::io::stdout.with_max_level(tracing::Level::TRACE));
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(writer);
    if cfg.log_format == "json" {
        builder.json().flatten_event(true).init();
    } else {
        builder.init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if let Some(Command::HashPassword { password, cost }) = &cli.command {
        println!("{}", bcrypt::hash(password, *cost)?);
        return Ok(());
    }
    if let Some(Command::Backup { from, from_auth, out, subject_prefix, confluent_api }) = &cli.command {
        let src = migrate::Endpoint::new(from, from_auth.as_deref())?;
        let dump = backup::read(&src, subject_prefix.as_deref(), !*confluent_api).await?;
        let text = dump.to_ndjson();
        let (subjects, versions) = (dump.subject_order.len(), dump.version_count());
        if out == "-" {
            print!("{text}");
        } else {
            // Write beside the target and rename, so an interrupted backup
            // leaves yesterday's dump intact rather than half of today's.
            let tmp = format!("{out}.partial");
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().create(true).truncate(true).write(true).open(&tmp)
                .map_err(|e| anyhow::anyhow!("opening {tmp}: {e}"))?;
            // A dump carries the exporters' destination credentials, so it is
            // not world-readable. Only where a mode means something: Windows
            // has no `from_mode`, and asking for one there does not compile.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                file.set_permissions(std::fs::Permissions::from_mode(0o600))
                    .map_err(|e| anyhow::anyhow!("setting permissions on {tmp}: {e}"))?;
            }
            file.write_all(text.as_bytes()).map_err(|e| anyhow::anyhow!("writing {tmp}: {e}"))?;
            file.sync_all().map_err(|e| anyhow::anyhow!("syncing {tmp}: {e}"))?;
            std::fs::rename(&tmp, out).map_err(|e| anyhow::anyhow!("renaming {tmp} to {out}: {e}"))?;
            eprintln!("{subjects} subjects, {versions} versions, {} bytes -> {out}", text.len());
        }
        return Ok(());
    }
    if let Some(Command::Restore { from, to, to_auth, dry_run }) = &cli.command {
        let text = if from == "-" { std::io::read_to_string(std::io::stdin())? } else { std::fs::read_to_string(from)? };
        let dump = backup::Dump::parse(&text)?;
        let dst = migrate::Endpoint::new(to, to_auth.as_deref())?;
        let r = backup::restore(&dump, &dst, *dry_run).await?;
        let verb = if *dry_run { "would restore" } else { "restored" };
        println!(
            "{verb} {} subjects, {} versions ({} soft-deleted), {} configs, {} modes, {} exporters",
            r.subjects, r.versions, r.soft_deleted, r.configs, r.modes, r.exporters
        );
        return Ok(());
    }
    if let Some(Command::Migrate { from, to, from_auth, to_auth, subject_prefix, skip_deleted, dry_run }) = &cli.command {
        let src = migrate::Endpoint::new(from, from_auth.as_deref())?;
        let dst = migrate::Endpoint::new(to, to_auth.as_deref())?;
        let opts = migrate::Options {
            subject_prefix: subject_prefix.clone(),
            dry_run: *dry_run,
            include_deleted: !*skip_deleted,
        };
        let before = migrate::describe(&src).await?;
        println!("source has {} subjects, {} versions", before["subjects"], before["versions"]);
        let s = migrate::run(&src, &dst, &opts).await?;
        println!(
            "{} {} subjects, {} versions ({} soft-deleted), {} configs, {} modes",
            if *dry_run { "would copy" } else { "copied" },
            s.subjects,
            s.versions,
            s.soft_deleted,
            s.configs,
            s.modes
        );
        for skipped in &s.skipped {
            eprintln!("skipped {skipped}");
        }
        return Ok(if s.skipped.is_empty() { () } else { std::process::exit(1) });
    }

    let mut cfg = ServerConfig::load(cli.config.as_deref())?;
    init_logging(&cfg);

    if let Some(l) = cli.listen {
        cfg.listen = l;
    }
    if let Some(d) = cli.data_dir {
        cfg.data_dir = d;
    }
    cfg.validate()?;

    std::fs::create_dir_all(&cfg.data_dir)?;
    let db = store::PhysicalStore::open(&cfg.data_dir, cfg.sync_writes)?;
    let default_compat = CompatibilityLevel::parse(&cfg.default_compatibility).map_err(|e| anyhow::anyhow!(e.message))?;
    let limits = registry::SearchLimits {
        schema_default: cfg.schema_search_default_limit,
        schema_max: cfg.schema_search_max_limit,
        subject_default: cfg.subject_search_default_limit,
        subject_max: cfg.subject_search_max_limit,
    };

    // One registry per host container. Unconfigured means exactly one, named
    // `default`, answering on every host - the way it has always behaved.
    let wanted: Vec<tenant::TenantId> = if cfg.containers.is_empty() {
        vec![tenant::TenantId::default_tenant()]
    } else {
        cfg.containers.iter().map(|c| tenant::TenantId::parse(&c.name)).collect::<Result<_, _>>().map_err(|e| anyhow::anyhow!(e.message))?
    };
    let started = std::time::Instant::now();
    let mut registries = std::collections::HashMap::new();
    for name in &wanted {
        let store = db.container(name.clone());
        store.register()?;
        let (cluster_id, derived) = cluster_id_for(cfg.cluster_id.as_deref(), store.get_meta_string("cluster_id")?, name);
        if derived {
            store.put_meta_string("cluster_id", &cluster_id)?;
        }
        let snapshot = store.load_snapshot()?;
        let mut registry = Registry::new(store, snapshot, default_compat, cluster_id, cfg.cache_max_entries, cfg.normalize);
        registry.limits = limits;
        let registry = Arc::new(registry);
        tokio::spawn(exporter::run(registry.clone(), Duration::from_secs(cfg.exporter_poll_seconds.max(1))));
        registries.insert(name.clone(), registry);
    }
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        containers = registries.len(),
        "loaded metadata snapshots"
    );

    let auth = Arc::new(if cfg.auth.enabled { Auth::new(&cfg.auth) } else { Auth::disabled() });
    let container_names = registries.keys().map(|n| n.to_string()).collect::<Vec<_>>().join(", ");
    let containers = Arc::new(containers::Containers::new(&cfg.containers, registries));
    let app = api::service(api::Shared { containers, auth, log_reads: cfg.log_reads }, cfg.max_body_bytes);

    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    let expect = cfg.proxy.expect().map_err(|e| anyhow::anyhow!(e))?;
    let trusted = Arc::new(proxy_protocol::Trusted::parse(&cfg.proxy.trust).map_err(|e| anyhow::anyhow!(e))?);
    tracing::info!(
        listen = %cfg.listen,
        data_dir = %cfg.data_dir.display(),
        containers = %container_names,
        auth = cfg.auth.enabled,
        proxy = if cfg.proxy.proxy_on { format!("v{}", cfg.proxy.proxy_version) } else { "off".to_string() },
        "schema registry started"
    );
    serve(listener, app, expect, trusted).await
}

#[cfg(test)]
mod cli_tests {
    use super::*;

    #[test]
    fn migrate_accepts_deleted_prefix_and_destination_auth_options() {
        let cli = Cli::try_parse_from([
            "schema-registry", "migrate", "--from", "http://source",
            "--to", "http://destination", "--skip-deleted",
            "--subject-prefix", ":.eu:", "--to-auth", "admin:secret",
        ]).expect("migrate options parse");
        match cli.command {
            Some(Command::Migrate { skip_deleted, subject_prefix, to_auth, .. }) => {
                assert!(skip_deleted);
                assert_eq!(subject_prefix.as_deref(), Some(":.eu:"));
                assert_eq!(to_auth.as_deref(), Some("admin:secret"));
            }
            _ => panic!("expected migrate command"),
        }
    }
}

/// Accept connections ourselves rather than through `axum::serve`, because the
/// PROXY header is the first thing on the socket and has to be read before
/// anything else looks at the bytes. TLS, if it is ever terminated here, wraps
/// the stream this returns - after the header, never before.
/// The cluster id for one host container: what `/v1/metadata/id` reports, and
/// what an AUTO exporter names its destination context after. Returns the id
/// and whether it is new and should be stored.
///
/// Each container needs its own, or their AUTO contexts collide at a shared
/// destination. It also must not move once anything has been exported, so it
/// cannot depend on how many containers happen to be configured today: the
/// `default` container takes a configured id verbatim - which is what a
/// single-container deployment has always reported - and any other appends its
/// own name. Deciding that from `containers.len()` meant that configuring a
/// second container silently renamed the first one's id, and with it the
/// destination context of every AUTO exporter it had.
fn cluster_id_for(configured: Option<&str>, stored: Option<String>, name: &tenant::TenantId) -> (String, bool) {
    match (configured, stored) {
        (Some(base), _) if name.as_str() == crate::tenant::DEFAULT_TENANT => (base.to_string(), false),
        (Some(base), _) => (format!("{base}-{name}"), false),
        // Nothing configured: the first one derived is kept for ever.
        (None, Some(id)) => (id, false),
        (None, None) => (format!("sr-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]), true),
    }
}

async fn serve(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    expect: proxy_protocol::Expect,
    trusted: Arc<proxy_protocol::Trusted>,
) -> anyhow::Result<()> {
    let mut shutdown = Box::pin(tokio::signal::ctrl_c());
    loop {
        let (socket, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                // One failed accept (a file descriptor limit, a reset) is not
                // a reason to stop serving the rest.
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            },
            _ = &mut shutdown => {
                tracing::info!("shutting down");
                return Ok(());
            }
        };
        let app = app.clone();
        let trusted = trusted.clone();
        tokio::spawn(async move {
            let (stream, client) = match proxy_protocol::accept(socket, peer, expect, &trusted).await {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(%peer, error = %e, "rejected connection");
                    return;
                }
            };
            // Every request on this connection carries who asked, whether that
            // came from a header or from the socket.
            let svc = hyper::service::service_fn(move |mut req: hyper::Request<hyper::body::Incoming>| {
                req.extensions_mut().insert(api::ClientAddr(client));
                let mut app = app.clone();
                async move { tower::Service::call(&mut app, req).await }
            });
            let io = hyper_util::rt::TokioIo::new(stream);
            if let Err(e) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                .serve_connection_with_upgrades(io, svc)
                .await
            {
                tracing::debug!(%client, error = %e, "connection ended");
            }
        });
    }
}



#[cfg(test)]
mod cluster_id_tests {
    use super::cluster_id_for;
    use crate::tenant::TenantId;

    #[test]
    fn a_containers_cluster_id_does_not_move_when_another_is_added() {
        let t = |n: &str| TenantId::parse(n).expect("name");
        // Configured, single container today: the id is the configured one.
        assert_eq!(cluster_id_for(Some("base"), None, &t("default")).0, "base");
        // Adding `prod` tomorrow must not rename it - an AUTO exporter's
        // destination context is named after this.
        assert_eq!(cluster_id_for(Some("base"), None, &t("default")).0, "base");
        assert_eq!(cluster_id_for(Some("base"), None, &t("prod")).0, "base-prod");
        assert_ne!(
            cluster_id_for(Some("base"), None, &t("prod")).0,
            cluster_id_for(Some("base"), None, &t("dev")).0,
            "two containers sharing an id collide at a shared destination"
        );

        // Nothing configured: whatever was derived first is kept.
        let (id, derived) = cluster_id_for(None, None, &t("default"));
        assert!(derived && id.starts_with("sr-"));
        assert_eq!(cluster_id_for(None, Some("sr-abc".into()), &t("default")), ("sr-abc".to_string(), false));
        assert_eq!(cluster_id_for(None, Some("sr-abc".into()), &t("prod")), ("sr-abc".to_string(), false));
    }
}
