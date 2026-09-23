//! A Confluent-compatible Schema Registry: single node, RocksDB storage,
//! contexts, exporters and HTTP Basic auth.

mod api;
mod auth;
mod authz;
mod config;
#[cfg(test)]
mod compat_level_tests;
#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod golden_tests;
#[cfg(test)]
mod http_conformance_tests;
mod context;
mod error;
mod exporter;
mod migrate;
mod model;
mod modegate;
mod registry;
mod schema;
mod snapshot;
mod store;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};

use crate::api::AppState;
use crate::auth::Auth;
use crate::config::ServerConfig;
use crate::model::CompatibilityLevel;
use crate::registry::Registry;
use crate::store::Store;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    if let Some(Command::HashPassword { password, cost }) = &cli.command {
        println!("{}", bcrypt::hash(password, *cost)?);
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

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=info".into()),
        )
        .init();

    let mut cfg = ServerConfig::load(cli.config.as_deref())?;
    if let Some(l) = cli.listen {
        cfg.listen = l;
    }
    if let Some(d) = cli.data_dir {
        cfg.data_dir = d;
    }
    cfg.validate()?;

    std::fs::create_dir_all(&cfg.data_dir)?;
    let store = Store::open(&cfg.data_dir, cfg.sync_writes)?;
    let cluster_id = match cfg.cluster_id.clone() {
        Some(id) => id,
        None => match store.get_meta_string("cluster_id")? {
            Some(id) => id,
            None => {
                let id = format!("sr-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
                store.put_meta_string("cluster_id", &id)?;
                id
            }
        },
    };
    let default_compat = CompatibilityLevel::parse(&cfg.default_compatibility).map_err(|e| anyhow::anyhow!(e.message))?;
    let started = std::time::Instant::now();
    let snapshot = store.load_snapshot()?;
    tracing::info!(elapsed_ms = started.elapsed().as_millis() as u64, "loaded metadata snapshot");
    let mut registry = Registry::new(store, snapshot, default_compat, cluster_id.clone(), cfg.cache_max_entries, cfg.normalize);
    registry.limits = registry::SearchLimits {
        schema_default: cfg.schema_search_default_limit,
        schema_max: cfg.schema_search_max_limit,
        subject_default: cfg.subject_search_default_limit,
        subject_max: cfg.subject_search_max_limit,
    };
    let registry = Arc::new(registry);
    let auth = Arc::new(if cfg.auth.enabled { Auth::new(&cfg.auth) } else { Auth::disabled() });

    tokio::spawn(exporter::run(registry.clone(), Duration::from_secs(cfg.exporter_poll_seconds.max(1))));

    let state = AppState { registry, auth };
    let app = api::service(state, cfg.max_body_bytes);

    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    tracing::info!(
        listen = %cfg.listen,
        data_dir = %cfg.data_dir.display(),
        cluster_id,
        auth = cfg.auth.enabled,
        "schema registry started"
    );
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    Ok(())
}
