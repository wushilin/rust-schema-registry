//! RocksDB persistence, in two pieces.
//!
//! [`PhysicalStore`] owns the database: column families, batches, key
//! encoding, on-disk format. It knows no rules.
//!
//! [`Store`] is one host container's view of it. Everything above this module
//! talks to a `Store` and never builds a key, so a container cannot read or
//! write another's rows by construction: every key it produces starts with
//! `<container> 0x00`.
//!
//! One column family per logical table. Keys are built so that the scans the
//! API needs are plain prefix scans in key order (`T` below is the container
//! prefix):
//!
//! | CF            | key                                           | value            |
//! |---------------|-----------------------------------------------|------------------|
//! | `schemas`     | T ctx 0x00 id(u32 BE)                         | SchemaRecord     |
//! | `fingerprints`| T ctx 0x00 sha256-hex                         | id(u32 BE)       |
//! | `versions`    | T ctx 0x00 subject 0x00 version(u32 BE)       | VersionRecord    |
//! | `refby`       | T ctx 0x00 subject 0x00 ver(u32 BE) id(u32 BE)| (empty)          |
//! | `config`      | T scope key                                   | ConfigRecord     |
//! | `mode`        | T scope key                                   | mode string      |
//! | `exporters`   | T name                                        | ExporterRecord   |
//! | `log`         | T seq(u64 BE)                                 | LogEvent         |
//! | `meta`        | T `next_id/<ctx>`, T `log_seq`, T `ctx/<ctx>` | counters, flags  |
//!
//! Big-endian integers make numeric order equal to byte order, so versions
//! come back sorted without any extra work. Subject and context names are
//! validated to contain no control characters, so 0x00 is a safe separator.
//! Container names may not start with 0x00 either, which leaves keys starting
//! with 0x00 free for the store's own bookkeeping (see `RESERVED`).
//!
//! All mutations go through [`Tx`], a thin wrapper over a `WriteBatch`, so each
//! API call is applied atomically (and fsync'd via the WAL).

use std::path::Path;
use std::sync::Arc;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::ApiResult;
use crate::model::*;
use crate::snapshot::{Op, Snapshot};
use crate::tenant::TenantId;

const CF_SCHEMAS: &str = "schemas";
const CF_FINGERPRINTS: &str = "fingerprints";
const CF_VERSIONS: &str = "versions";
const CF_REFBY: &str = "refby";
const CF_CONFIG: &str = "config";
const CF_MODE: &str = "mode";
const CF_EXPORTERS: &str = "exporters";
const CF_LOG: &str = "log";
const CF_META: &str = "meta";

const ALL_CFS: &[&str] = &[
    CF_SCHEMAS,
    CF_FINGERPRINTS,
    CF_VERSIONS,
    CF_REFBY,
    CF_CONFIG,
    CF_MODE,
    CF_EXPORTERS,
    CF_LOG,
    CF_META,
];

/// The store's own rows in `meta`, outside every container. A container prefix
/// can never start with 0x00, so these cannot collide with one.
const RESERVED: u8 = 0;
const FORMAT_VERSION_KEY: &[u8] = b"\0format_version";
const TENANT_KEY_PREFIX: &[u8] = b"\0container/";

/// On-disk format. 1: keys without a container prefix (before host
/// containers existed). 2: every key carries one.
const FORMAT_VERSION: u32 = 2;

/// Where a config or mode value lives. Lookups fall back Subject -> Context -> Global.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    Global,
    Context(String),
    Subject(String, String),
}

impl Scope {
    fn key(&self) -> Vec<u8> {
        match self {
            Scope::Global => b"G".to_vec(),
            Scope::Context(ctx) => [b"C", ctx.as_bytes()].concat(),
            Scope::Subject(ctx, s) => [b"S", ctx.as_bytes(), &[0], s.as_bytes()].concat(),
        }
    }

    fn from_key(k: &[u8]) -> Option<Scope> {
        let rest = String::from_utf8_lossy(k.get(1..)?).into_owned();
        match k.first()? {
            b'G' => Some(Scope::Global),
            b'C' => Some(Scope::Context(rest)),
            b'S' => {
                let (ctx, subject) = rest.split_once('\0')?;
                Some(Scope::Subject(ctx.to_string(), subject.to_string()))
            }
            _ => None,
        }
    }
}

/// Split `ctx 0x00 rest` keys.
fn split_ctx(k: &[u8]) -> Option<(String, &[u8])> {
    let i = k.iter().position(|b| *b == 0)?;
    Some((String::from_utf8_lossy(&k[..i]).into_owned(), &k[i + 1..]))
}

/// Split `subject 0x00 version(u32 BE) [tail]`.
fn split_subject_version(k: &[u8]) -> Option<(String, u32, &[u8])> {
    let i = k.iter().position(|b| *b == 0)?;
    let v = k.get(i + 1..i + 5)?;
    Some((String::from_utf8_lossy(&k[..i]).into_owned(), be_u32(v), &k[i + 5..]))
}

fn schema_key(ctx: &str, id: u32) -> Vec<u8> {
    [ctx.as_bytes(), &[0], &id.to_be_bytes()].concat()
}
fn fp_key(ctx: &str, fp: &str) -> Vec<u8> {
    [ctx.as_bytes(), &[0], fp.as_bytes()].concat()
}
fn subject_prefix(ctx: &str, subject: &str) -> Vec<u8> {
    [ctx.as_bytes(), &[0], subject.as_bytes(), &[0]].concat()
}
fn version_key(ctx: &str, subject: &str, version: u32) -> Vec<u8> {
    [subject_prefix(ctx, subject), version.to_be_bytes().to_vec()].concat()
}
fn refby_key(ctx: &str, subject: &str, version: u32, id: u32) -> Vec<u8> {
    [version_key(ctx, subject, version), id.to_be_bytes().to_vec()].concat()
}
fn be_u32(b: &[u8]) -> u32 {
    let mut a = [0u8; 4];
    a.copy_from_slice(&b[..4]);
    u32::from_be_bytes(a)
}
fn be_u64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8]);
    u64::from_be_bytes(a)
}

// ---------------------------------------------------------------------------
// The database
// ---------------------------------------------------------------------------

/// The physical store: RocksDB and nothing else. Obtain a container's view of
/// it with [`PhysicalStore::container`].
pub struct PhysicalStore {
    db: DB,
    sync_writes: bool,
}

impl PhysicalStore {
    pub fn open(path: &Path, sync_writes: bool) -> anyhow::Result<Arc<Self>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        // Reads are served from the in-memory snapshot and caches, so RocksDB
        // only needs modest memtables. The defaults (64MB x2 per column family,
        // 9 families) would reserve over 1GB for a registry that is typically tiny.
        opts.set_db_write_buffer_size(64 * 1024 * 1024);
        let cf_opts = || {
            let mut o = Options::default();
            o.set_compression_type(rocksdb::DBCompressionType::Lz4);
            o.set_write_buffer_size(8 * 1024 * 1024);
            o
        };
        let cfs = ALL_CFS.iter().map(|n| ColumnFamilyDescriptor::new(*n, cf_opts()));
        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        let store = Arc::new(Self { db, sync_writes });
        store.migrate_to_current_format()?;
        Ok(store)
    }

    /// One host container's view of this store.
    pub fn container(self: &Arc<Self>, tenant: TenantId) -> Store {
        Store {
            prefix: tenant.key_prefix(),
            tenant,
            db: self.clone(),
        }
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).expect("column family exists")
    }

    fn format_version(&self) -> ApiResult<Option<u32>> {
        Ok(self.db.get_cf(self.cf(CF_META), FORMAT_VERSION_KEY)?.map(|v| be_u32(&v)))
    }

    /// Every container this store holds, in name order.
    #[allow(dead_code)] // the host-routing slice lists them; tests cover it now
    pub fn containers(&self) -> ApiResult<Vec<TenantId>> {
        let mut out = Vec::new();
        for item in self
            .db
            .iterator_cf(self.cf(CF_META), IteratorMode::From(TENANT_KEY_PREFIX, Direction::Forward))
        {
            let (k, _) = item?;
            let Some(name) = k.strip_prefix(TENANT_KEY_PREFIX) else {
                break;
            };
            if let Ok(t) = TenantId::parse(&String::from_utf8_lossy(name)) {
                out.push(t);
            }
        }
        Ok(out)
    }

    /// Bring a data directory written before host containers existed up to the
    /// current format: every key moves into the `default` container.
    ///
    /// It runs once, in place, before anything reads. A directory that is
    /// already current, or that is empty, costs one point lookup.
    fn migrate_to_current_format(&self) -> anyhow::Result<()> {
        if self.format_version()? == Some(FORMAT_VERSION) {
            return Ok(());
        }
        let default = TenantId::default_tenant();
        let prefix = default.key_prefix();
        let mut moved = 0usize;
        for cf in ALL_CFS {
            let mut batch = WriteBatch::default();
            let mut in_batch = 0usize;
            for item in self.db.iterator_cf(self.cf(cf), IteratorMode::Start) {
                let (k, v) = item?;
                // Reserved rows are the store's own and stay where they are;
                // a key already in a container is left alone (an interrupted
                // migration can simply be run again).
                if k.first() == Some(&RESERVED) || k.starts_with(&prefix) {
                    continue;
                }
                batch.put_cf(self.cf(cf), [prefix.as_slice(), &k].concat(), &v);
                batch.delete_cf(self.cf(cf), &k);
                in_batch += 1;
                moved += 1;
                if in_batch >= 10_000 {
                    self.db.write(std::mem::take(&mut batch))?;
                    in_batch = 0;
                }
            }
            if in_batch > 0 {
                self.db.write(batch)?;
            }
        }
        let mut batch = WriteBatch::default();
        batch.put_cf(self.cf(CF_META), FORMAT_VERSION_KEY, FORMAT_VERSION.to_be_bytes());
        batch.put_cf(self.cf(CF_META), [TENANT_KEY_PREFIX, default.as_str().as_bytes()].concat(), b"");
        self.db.write(batch)?;
        if moved > 0 {
            tracing::info!(rows = moved, "migrated the store into the '{default}' host container");
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// One container's view
// ---------------------------------------------------------------------------

/// A host container's view of the physical store. Every key it touches carries
/// its prefix, so no call above this module can reach another container.
pub struct Store {
    db: Arc<PhysicalStore>,
    tenant: TenantId,
    prefix: Vec<u8>,
}

impl Store {
    /// Open a store and take the `default` container. The server builds its
    /// containers from configuration, so this is the tests' shortcut.
    #[cfg(test)]
    pub fn open(path: &Path, sync_writes: bool) -> anyhow::Result<Self> {
        Ok(PhysicalStore::open(path, sync_writes)?.container(TenantId::default_tenant()))
    }

    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Record that this container exists, so it survives a restart with no rows.
    pub fn register(&self) -> ApiResult<()> {
        let key = [TENANT_KEY_PREFIX, self.tenant.as_str().as_bytes()].concat();
        self.db.db.put_cf(self.db.cf(CF_META), key, b"")?;
        Ok(())
    }

    fn key(&self, rest: &[u8]) -> Vec<u8> {
        [self.prefix.as_slice(), rest].concat()
    }

    fn get_json<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> ApiResult<Option<T>> {
        match self.db.db.get_cf(self.db.cf(cf), self.key(key))? {
            Some(v) => Ok(Some(serde_json::from_slice(&v)?)),
            None => Ok(None),
        }
    }

    /// Iterate this container's (key, value) pairs whose key starts with
    /// `prefix`, with the container prefix already stripped from the keys.
    fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> ApiResult<Vec<(Box<[u8]>, Box<[u8]>)>> {
        let full = self.key(prefix);
        let mut out = Vec::new();
        let iter = self
            .db
            .db
            .iterator_cf(self.db.cf(cf), IteratorMode::From(&full, Direction::Forward));
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&full) {
                break;
            }
            out.push((k[self.prefix.len()..].into(), v));
        }
        Ok(out)
    }

    // ---------------- schemas ----------------

    pub fn get_schema(&self, ctx: &str, id: u32) -> ApiResult<Option<SchemaRecord>> {
        self.get_json(CF_SCHEMAS, &schema_key(ctx, id))
    }

    // ---------------- contexts & counters ----------------

    pub fn log_seq(&self) -> ApiResult<u64> {
        Ok(self.get_meta(b"log_seq")?.map(|v| be_u64(&v)).unwrap_or(0))
    }

    fn get_meta(&self, key: &[u8]) -> ApiResult<Option<Vec<u8>>> {
        Ok(self.db.db.get_cf(self.db.cf(CF_META), self.key(key))?)
    }

    pub fn get_meta_string(&self, key: &str) -> ApiResult<Option<String>> {
        Ok(self.get_meta(key.as_bytes())?.map(|v| String::from_utf8_lossy(&v).into_owned()))
    }

    pub fn put_meta_string(&self, key: &str, value: &str) -> ApiResult<()> {
        self.db.db.put_cf(self.db.cf(CF_META), self.key(key.as_bytes()), value)?;
        Ok(())
    }

    /// The oldest sequence number the log still holds; everything below it has
    /// been pruned and can only be recovered by re-reading the current state.
    pub fn log_floor(&self) -> ApiResult<u64> {
        Ok(self.get_meta(b"log_floor")?.map(|v| be_u64(&v)).unwrap_or(0))
    }

    /// Drop change-log entries below `before` (everything that every exporter
    /// of this container has already consumed). Without this the log grows for
    /// the lifetime of the server.
    pub fn prune_log(&self, before: u64) -> ApiResult<()> {
        if before <= self.log_floor()? {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(self.db.cf(CF_LOG), self.key(&0u64.to_be_bytes()), self.key(&before.to_be_bytes()));
        batch.put_cf(self.db.cf(CF_META), self.key(b"log_floor"), before.to_be_bytes());
        self.db.db.write(batch)?;
        Ok(())
    }

    /// Log events with sequence >= `from`, at most `limit`.
    pub fn read_log(&self, from: u64, limit: usize) -> ApiResult<Vec<(u64, LogEvent)>> {
        let start = self.key(&from.to_be_bytes());
        let mut out = Vec::new();
        for item in self
            .db
            .db
            .iterator_cf(self.db.cf(CF_LOG), IteratorMode::From(&start, Direction::Forward))
        {
            let (k, v) = item?;
            if !k.starts_with(&self.prefix) {
                break;
            }
            out.push((be_u64(&k[self.prefix.len()..]), serde_json::from_slice(&v)?));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    // ---------------- exporters ----------------

    pub fn list_exporters(&self) -> ApiResult<Vec<ExporterRecord>> {
        self.scan_prefix(CF_EXPORTERS, b"")?
            .into_iter()
            .map(|(_, v)| Ok(serde_json::from_slice(&v)?))
            .collect()
    }

    pub fn get_exporter(&self, name: &str) -> ApiResult<Option<ExporterRecord>> {
        self.get_json(CF_EXPORTERS, name.as_bytes())
    }

    pub fn put_exporter(&self, rec: &ExporterRecord, _: &crate::modegate::Allowed) -> ApiResult<()> {
        let key = self.key(rec.info.name.as_bytes());
        self.db.db.put_cf(self.db.cf(CF_EXPORTERS), key, serde_json::to_vec(rec)?)?;
        Ok(())
    }

    // ---------------- snapshot loading ----------------

    /// Build this container's in-memory metadata snapshot by scanning every
    /// table except schema bodies (those are loaded lazily into the bounded
    /// cache).
    pub fn load_snapshot(&self) -> anyhow::Result<Snapshot> {
        let mut snap = Snapshot::default();
        for (k, v) in self.scan_prefix(CF_VERSIONS, b"")? {
            let (ctx, rest) = split_ctx(&k).ok_or_else(|| anyhow::anyhow!("bad version key"))?;
            let (subject, version, _) = split_subject_version(rest).ok_or_else(|| anyhow::anyhow!("bad version key"))?;
            let rec: VersionRecord = serde_json::from_slice(&v)?;
            snap.apply(&Op::PutVersion {
                ctx,
                subject,
                version,
                rec,
            });
        }
        for (k, v) in self.scan_prefix(CF_FINGERPRINTS, b"")? {
            let (ctx, fp) = split_ctx(&k).ok_or_else(|| anyhow::anyhow!("bad fingerprint key"))?;
            let fp = String::from_utf8_lossy(fp).into_owned();
            let c = snap.ctxs.entry(ctx).or_default();
            c.fingerprints.insert(fp, be_u32(&v));
        }
        for (k, _) in self.scan_prefix(CF_REFBY, b"")? {
            let (ctx, rest) = split_ctx(&k).ok_or_else(|| anyhow::anyhow!("bad refby key"))?;
            let (subject, version, tail) = split_subject_version(rest).ok_or_else(|| anyhow::anyhow!("bad refby key"))?;
            snap.apply(&Op::PutRefby {
                ctx,
                subject,
                version,
                id: be_u32(tail),
            });
        }
        for (k, v) in self.scan_prefix(CF_CONFIG, b"")? {
            if let Some(scope) = Scope::from_key(&k) {
                snap.apply(&Op::PutConfig {
                    scope,
                    rec: serde_json::from_slice(&v)?,
                });
            }
        }
        for (k, v) in self.scan_prefix(CF_MODE, b"")? {
            if let Some(scope) = Scope::from_key(&k) {
                snap.apply(&Op::PutMode {
                    scope,
                    mode: serde_json::from_slice(&v)?,
                });
            }
        }
        // Contexts can exist without versions (e.g. after deletes), and
        // next_id counters, so read them from meta explicitly.
        snap.known_contexts = imbl::OrdSet::new();
        for (k, v) in self.scan_prefix(CF_META, b"")? {
            let key = String::from_utf8_lossy(&k).into_owned();
            if let Some(ctx) = key.strip_prefix("ctx/") {
                snap.known_contexts.insert(ctx.to_string());
            } else if let Some(ctx) = key.strip_prefix("next_id/") {
                snap.apply(&Op::SetNextId {
                    ctx: ctx.to_string(),
                    next: be_u32(&v),
                });
            }
        }
        snap.log_seq = self.log_seq()?;
        Ok(snap)
    }

    // ---------------- transactions ----------------

    pub fn tx(&self) -> ApiResult<Tx<'_>> {
        Ok(Tx {
            log_seq: self.log_seq()?,
            log_dirty: false,
            store: self,
            batch: WriteBatch::default(),
            ops: Vec::new(),
        })
    }
}

/// An atomic batch of writes, within one container. Callers must hold the
/// registry write lock while building and committing a `Tx` (counters are
/// read-modify-write).
pub struct Tx<'a> {
    store: &'a Store,
    batch: WriteBatch,
    log_seq: u64,
    log_dirty: bool,
    /// The same mutations, for the in-memory snapshot.
    ops: Vec<Op>,
}

impl Tx<'_> {
    fn put<T: Serialize>(&mut self, cf: &str, key: &[u8], value: &T) -> ApiResult<()> {
        let bytes = serde_json::to_vec(value)?;
        let key = self.store.key(key);
        self.batch.put_cf(self.store.db.cf(cf), key, bytes);
        Ok(())
    }

    /// Store schema content. `index` also maps its fingerprint to `id`
    /// (skipped when an imported id duplicates content that already has an id).
    /// Every write that defines registry state needs proof that the mode in
    /// scope allows it (see `modegate`): the token is the check, not a
    /// comment. Index rows (refby, next id, the change log) are derived from
    /// these and cannot exist without one.
    pub fn put_schema(&mut self, ctx: &str, id: u32, rec: &SchemaRecord, index: bool, _: &crate::modegate::Allowed) -> ApiResult<()> {
        self.put(CF_SCHEMAS, &schema_key(ctx, id), rec)?;
        self.ops.push(Op::PutSchema {
            ctx: ctx.into(),
            id,
            rec: Arc::new(rec.clone()),
            index,
        });
        if index {
            let key = self.store.key(&fp_key(ctx, &rec.fingerprint));
            self.batch.put_cf(self.store.db.cf(CF_FINGERPRINTS), key, id.to_be_bytes());
        }
        Ok(())
    }

    pub fn put_version(
        &mut self,
        ctx: &str,
        subject: &str,
        version: u32,
        rec: &VersionRecord,
        _: &crate::modegate::Allowed,
    ) -> ApiResult<()> {
        self.put(CF_VERSIONS, &version_key(ctx, subject, version), rec)?;
        let key = self.store.key(format!("ctx/{ctx}").as_bytes());
        self.batch.put_cf(self.store.db.cf(CF_META), key, b"");
        self.ops.push(Op::PutVersion {
            ctx: ctx.into(),
            subject: subject.into(),
            version,
            rec: rec.clone(),
        });
        Ok(())
    }

    pub fn delete_version(&mut self, ctx: &str, subject: &str, version: u32, id: u32, _: &crate::modegate::Allowed) {
        let key = self.store.key(&version_key(ctx, subject, version));
        self.batch.delete_cf(self.store.db.cf(CF_VERSIONS), key);
        self.ops.push(Op::DeleteVersion {
            ctx: ctx.into(),
            subject: subject.into(),
            version,
            id,
        });
    }

    pub fn put_refby(&mut self, ctx: &str, subject: &str, version: u32, id: u32) {
        let key = self.store.key(&refby_key(ctx, subject, version, id));
        self.batch.put_cf(self.store.db.cf(CF_REFBY), key, b"");
        self.ops.push(Op::PutRefby {
            ctx: ctx.into(),
            subject: subject.into(),
            version,
            id,
        });
    }

    pub fn delete_refby(&mut self, ctx: &str, subject: &str, version: u32, id: u32) {
        let key = self.store.key(&refby_key(ctx, subject, version, id));
        self.batch.delete_cf(self.store.db.cf(CF_REFBY), key);
        self.ops.push(Op::DeleteRefby {
            ctx: ctx.into(),
            subject: subject.into(),
            version,
            id,
        });
    }

    pub fn set_next_id(&mut self, ctx: &str, next: u32) {
        let key = self.store.key(format!("next_id/{ctx}").as_bytes());
        self.batch.put_cf(self.store.db.cf(CF_META), key, next.to_be_bytes());
        self.ops.push(Op::SetNextId { ctx: ctx.into(), next });
    }

    pub fn delete_context(&mut self, ctx: &str, _: &crate::modegate::Allowed) {
        let key = self.store.key(format!("ctx/{ctx}").as_bytes());
        self.batch.delete_cf(self.store.db.cf(CF_META), key);
        self.ops.push(Op::DeleteContext { ctx: ctx.into() });
    }

    pub fn put_config(&mut self, scope: &Scope, rec: &ConfigRecord, _: &crate::modegate::Allowed) -> ApiResult<()> {
        self.ops.push(Op::PutConfig {
            scope: scope.clone(),
            rec: rec.clone(),
        });
        self.put(CF_CONFIG, &scope.key(), rec)
    }

    pub fn delete_config(&mut self, scope: &Scope, _: &crate::modegate::Allowed) {
        let key = self.store.key(&scope.key());
        self.batch.delete_cf(self.store.db.cf(CF_CONFIG), key);
        self.ops.push(Op::DeleteConfig { scope: scope.clone() });
    }

    pub fn put_mode(&mut self, scope: &Scope, mode: Mode, _: &crate::modegate::Allowed) -> ApiResult<()> {
        self.ops.push(Op::PutMode {
            scope: scope.clone(),
            mode,
        });
        self.put(CF_MODE, &scope.key(), &mode)
    }

    pub fn delete_mode(&mut self, scope: &Scope, _: &crate::modegate::Allowed) {
        let key = self.store.key(&scope.key());
        self.batch.delete_cf(self.store.db.cf(CF_MODE), key);
        self.ops.push(Op::DeleteMode { scope: scope.clone() });
    }

    pub fn put_exporter(&mut self, rec: &ExporterRecord, _: &crate::modegate::Allowed) -> ApiResult<()> {
        self.put(CF_EXPORTERS, rec.info.name.as_bytes(), rec)
    }

    pub fn delete_exporter(&mut self, name: &str, _: &crate::modegate::Allowed) {
        let key = self.store.key(name.as_bytes());
        self.batch.delete_cf(self.store.db.cf(CF_EXPORTERS), key);
    }

    pub fn append_log(&mut self, ev: &LogEvent) -> ApiResult<()> {
        let seq = self.log_seq;
        self.put(CF_LOG, &seq.to_be_bytes(), ev)?;
        self.log_seq += 1;
        self.log_dirty = true;
        Ok(())
    }

    /// Durably commit the batch; returns the ops for the snapshot.
    pub fn commit(mut self) -> ApiResult<Vec<Op>> {
        if self.log_dirty {
            let key = self.store.key(b"log_seq");
            self.batch.put_cf(self.store.db.cf(CF_META), key, self.log_seq.to_be_bytes());
            self.ops.push(Op::SetLogSeq { seq: self.log_seq });
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(self.store.db.sync_writes);
        self.store.db.db.write_opt(self.batch, &wo)?;
        Ok(self.ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modegate::Allowed;

    fn version(id: u32) -> VersionRecord {
        VersionRecord { id, deleted: false, ts: 1 }
    }

    /// A write that is allowed because the test says so; the mode table is
    /// tested where it lives.
    fn allowed() -> Allowed {
        Allowed::not_schema_state()
    }

    #[test]
    fn containers_share_a_database_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let db = PhysicalStore::open(dir.path(), false).unwrap();
        let a = db.container(TenantId::parse("prod").unwrap());
        let b = db.container(TenantId::parse("prod-eu").unwrap());
        a.register().unwrap();
        b.register().unwrap();

        // The same subject name, in both, with different ids and versions.
        for (store, id) in [(&a, 7u32), (&b, 99u32)] {
            let mut tx = store.tx().unwrap();
            tx.put_version(".", "orders-value", 1, &version(id), &allowed()).unwrap();
            tx.put_config(
                &Scope::Global,
                &ConfigRecord {
                    normalize: Some(id == 7),
                    ..Default::default()
                },
                &allowed(),
            )
            .unwrap();
            tx.append_log(&LogEvent {
                ctx: ".".into(),
                subject: "orders-value".into(),
                version: 1,
                id,
                kind: LogEventKind::Register,
            })
            .unwrap();
            tx.commit().unwrap();
            store
                .put_exporter(
                    &ExporterRecord {
                        info: ExporterInfo {
                            name: format!("exp-{id}"),
                            subjects: vec!["*".into()],
                            context_type: "NONE".into(),
                            context: None,
                            subject_rename_format: None,
                            config: serde_json::Map::new(),
                        },
                        state: ExporterState::Running,
                        offset: 0,
                        ts: 0,
                        trace: String::new(),
                    },
                    &allowed(),
                )
                .unwrap();
        }

        let (sa, sb) = (a.load_snapshot().unwrap(), b.load_snapshot().unwrap());
        let id_of = |s: &Snapshot| s.ctxs.get(".").unwrap().subjects.get("orders-value").unwrap()[0].1.id;
        assert_eq!(id_of(&sa), 7);
        assert_eq!(id_of(&sb), 99, "the other container's row must not win");
        assert_eq!(sa.ctxs.get(".").unwrap().subjects.len(), 1, "one subject each, not two");
        assert_eq!(sa.config.get(&Scope::Global).unwrap().normalize, Some(true));
        assert_eq!(sb.config.get(&Scope::Global).unwrap().normalize, Some(false));

        // Exporters and change logs are per container, including sequences.
        assert_eq!(a.list_exporters().unwrap().len(), 1);
        assert_eq!(a.list_exporters().unwrap()[0].info.name, "exp-7");
        assert_eq!(b.list_exporters().unwrap()[0].info.name, "exp-99");
        assert_eq!(a.log_seq().unwrap(), 1);
        assert_eq!(b.log_seq().unwrap(), 1);
        assert_eq!(a.read_log(0, 10).unwrap().len(), 1);
        assert_eq!(a.read_log(0, 10).unwrap()[0].1.id, 7);
        assert_eq!(b.read_log(0, 10).unwrap()[0].1.id, 99);

        // Pruning one container's log leaves the other's alone.
        a.prune_log(1).unwrap();
        assert!(a.read_log(0, 10).unwrap().is_empty());
        assert_eq!(b.read_log(0, 10).unwrap().len(), 1);

        // `default` is always there - it is the container a deployment gets
        // when it configures none - and the two registered ones join it.
        assert_eq!(
            db.containers().unwrap(),
            vec![
                TenantId::default_tenant(),
                TenantId::parse("prod").unwrap(),
                TenantId::parse("prod-eu").unwrap()
            ],
            "a container is listed even with no rows of its own"
        );
        assert_eq!(a.tenant().as_str(), "prod");
    }

    #[test]
    fn a_store_written_before_containers_existed_is_migrated_in_place() {
        let dir = tempfile::tempdir().unwrap();
        // Write the old layout directly: keys with no container prefix.
        {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            let cfs = ALL_CFS.iter().map(|n| ColumnFamilyDescriptor::new(*n, Options::default()));
            let db = DB::open_cf_descriptors(&opts, dir.path(), cfs).unwrap();
            let cf = |n: &str| db.cf_handle(n).unwrap();
            db.put_cf(
                cf(CF_VERSIONS),
                version_key(".", "old-value", 1),
                serde_json::to_vec(&version(3)).unwrap(),
            )
            .unwrap();
            db.put_cf(cf(CF_FINGERPRINTS), fp_key(".", "abc"), 3u32.to_be_bytes()).unwrap();
            db.put_cf(
                cf(CF_CONFIG),
                Scope::Global.key(),
                serde_json::to_vec(&ConfigRecord::default()).unwrap(),
            )
            .unwrap();
            db.put_cf(cf(CF_META), b"ctx/.", b"").unwrap();
            db.put_cf(cf(CF_META), b"next_id/.", 4u32.to_be_bytes()).unwrap();
            db.put_cf(cf(CF_META), b"log_seq", 5u64.to_be_bytes()).unwrap();
        }

        let store = Store::open(dir.path(), false).unwrap();
        let snap = store.load_snapshot().unwrap();
        assert_eq!(snap.ctxs.get(".").unwrap().subjects.get("old-value").unwrap()[0].1.id, 3);
        assert_eq!(snap.ctxs.get(".").unwrap().fingerprints.get("abc"), Some(&3));
        assert_eq!(snap.ctxs.get(".").unwrap().next_id, Some(4));
        assert!(snap.config.contains_key(&Scope::Global));
        assert_eq!(store.log_seq().unwrap(), 5);

        // Running it again is a no-op, and a second open finds nothing to move.
        drop(store);
        let store = Store::open(dir.path(), false).unwrap();
        assert_eq!(store.load_snapshot().unwrap().ctxs.get(".").unwrap().subjects.len(), 1);
        assert_eq!(store.log_seq().unwrap(), 5);
    }
}
