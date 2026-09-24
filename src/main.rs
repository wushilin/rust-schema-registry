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
    if let Some(Command::Backup { from, from_auth, out, subject_prefix }) = &cli.command {
        let src = migrate::Endpoint::new(from, from_auth.as_deref())?;
        let dump = backup::read(&src, subject_prefix.as_deref()).await?;
        let text = dump.to_ndjson();
        let (subjects, versions) = (dump.subject_order.len(), dump.version_count());
        if out == "-" {
            print!("{text}");
        } else {
            std::fs::write(out, &text)?;
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
        // Each container has its own cluster id: exporters name their AUTO
        // contexts after it, so a shared one would collide at a destination.
        let cluster_id = match (cfg.cluster_id.clone(), store.get_meta_string("cluster_id")?) {
            (Some(id), _) if wanted.len() == 1 => id,
            (_, Some(id)) => id,
            (configured, None) => {
                let id = match configured {
                    Some(base) => format!("{base}-{name}"),
                    None => format!("sr-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
                };
                store.put_meta_string("cluster_id", &id)?;
                id
            }
        };
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
    let mode = proxy_protocol::Mode::parse(&cfg.proxy_protocol).map_err(|e| anyhow::anyhow!(e))?;
    let trusted = Arc::new(proxy_protocol::Trusted::parse(&cfg.proxy_trust).map_err(|e| anyhow::anyhow!(e))?);
    tracing::info!(
        listen = %cfg.listen,
        data_dir = %cfg.data_dir.display(),
        containers = %container_names,
        auth = cfg.auth.enabled,
        proxy_protocol = %cfg.proxy_protocol,
        "schema registry started"
    );
    serve(listener, app, mode, trusted).await
}

/// Accept connections ourselves rather than through `axum::serve`, because the
/// PROXY header is the first thing on the socket and has to be read before
/// anything else looks at the bytes. TLS, if it is ever terminated here, wraps
/// the stream this returns - after the header, never before.
async fn serve(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    mode: proxy_protocol::Mode,
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
            let (stream, client) = match proxy_protocol::accept(socket, peer, mode, &trusted).await {
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


