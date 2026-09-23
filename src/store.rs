//! RocksDB persistence.
//!
//! One column family per logical table. Keys are built so that the scans the
//! API needs are plain prefix scans in key order:
//!
//! | CF            | key                                         | value            |
//! |---------------|---------------------------------------------|------------------|
//! | `schemas`     | ctx 0x00 id(u32 BE)                         | SchemaRecord     |
//! | `fingerprints`| ctx 0x00 sha256-hex                         | id(u32 BE)       |
//! | `versions`    | ctx 0x00 subject 0x00 version(u32 BE)       | VersionRecord    |
//! | `refby`       | ctx 0x00 subject 0x00 ver(u32 BE) id(u32 BE)| (empty)          |
//! | `config`      | scope key                                   | ConfigRecord     |
//! | `mode`        | scope key                                   | mode string      |
//! | `exporters`   | name                                        | ExporterRecord   |
//! | `log`         | seq(u64 BE)                                 | LogEvent         |
//! | `meta`        | `next_id/<ctx>`, `log_seq`, `ctx/<ctx>`     | counters, flags  |
//!
//! Big-endian integers make numeric order equal to byte order, so versions
//! come back sorted without any extra work. Subject and context names are
//! validated to contain no control characters, so 0x00 is a safe separator.
//!
//! All mutations go through [`Tx`], a thin wrapper over a `WriteBatch`, so each
//! API call is applied atomically (and fsync'd via the WAL).

use std::path::Path;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::ApiResult;
use crate::model::*;
use crate::snapshot::{Op, Snapshot};
use std::sync::Arc;

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

pub struct Store {
    db: DB,
    sync_writes: bool,
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

impl Store {
    pub fn open(path: &Path, sync_writes: bool) -> anyhow::Result<Self> {
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
        Ok(Self { db, sync_writes })
    }

    fn cf(&self, name: &str) -> &ColumnFamily {
        self.db.cf_handle(name).expect("column family exists")
    }

    fn get_json<T: DeserializeOwned>(&self, cf: &str, key: &[u8]) -> ApiResult<Option<T>> {
        match self.db.get_cf(self.cf(cf), key)? {
            Some(v) => Ok(Some(serde_json::from_slice(&v)?)),
            None => Ok(None),
        }
    }

    /// Iterate all (key, value) pairs whose key starts with `prefix`.
    fn scan_prefix(&self, cf: &str, prefix: &[u8]) -> ApiResult<Vec<(Box<[u8]>, Box<[u8]>)>> {
        let mut out = Vec::new();
        let iter = self.db.iterator_cf(self.cf(cf), IteratorMode::From(prefix, Direction::Forward));
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            out.push((k, v));
        }
        Ok(out)
    }

    // ---------------- schemas ----------------

    pub fn get_schema(&self, ctx: &str, id: u32) -> ApiResult<Option<SchemaRecord>> {
        self.get_json(CF_SCHEMAS, &schema_key(ctx, id))
    }

    // ---------------- versions ----------------

    // ---------------- contexts & counters ----------------

    pub fn log_seq(&self) -> ApiResult<u64> {
        Ok(self.db.get_cf(self.cf(CF_META), b"log_seq")?.map(|v| be_u64(&v)).unwrap_or(0))
    }

    pub fn get_meta_string(&self, key: &str) -> ApiResult<Option<String>> {
        Ok(self.db.get_cf(self.cf(CF_META), key)?.map(|v| String::from_utf8_lossy(&v).into_owned()))
    }

    pub fn put_meta_string(&self, key: &str, value: &str) -> ApiResult<()> {
        self.db.put_cf(self.cf(CF_META), key, value)?;
        Ok(())
    }

    /// Log events with sequence >= `from`, at most `limit`.
    /// The oldest sequence number the log still holds; everything below it has
    /// been pruned and can only be recovered by re-reading the current state.
    pub fn log_floor(&self) -> ApiResult<u64> {
        Ok(self.db.get_cf(self.cf(CF_META), b"log_floor")?.map(|v| be_u64(&v)).unwrap_or(0))
    }

    /// Drop change-log entries below `before` (everything that every exporter
    /// has already consumed). Without this the log grows for the lifetime of
    /// the server.
    pub fn prune_log(&self, before: u64) -> ApiResult<()> {
        if before <= self.log_floor()? {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(self.cf(CF_LOG), 0u64.to_be_bytes(), before.to_be_bytes());
        batch.put_cf(self.cf(CF_META), b"log_floor", before.to_be_bytes());
        self.db.write(batch)?;
        Ok(())
    }

    pub fn read_log(&self, from: u64, limit: usize) -> ApiResult<Vec<(u64, LogEvent)>> {
        let mut out = Vec::new();
        let start = from.to_be_bytes();
        for item in self.db.iterator_cf(self.cf(CF_LOG), IteratorMode::From(&start, Direction::Forward)) {
            let (k, v) = item?;
            out.push((be_u64(&k), serde_json::from_slice(&v)?));
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

    pub fn put_exporter(&self, rec: &ExporterRecord) -> ApiResult<()> {
        self.db.put_cf(self.cf(CF_EXPORTERS), rec.info.name.as_bytes(), serde_json::to_vec(rec)?)?;
        Ok(())
    }

    pub fn delete_exporter(&self, name: &str) -> ApiResult<()> {
        self.db.delete_cf(self.cf(CF_EXPORTERS), name.as_bytes())?;
        Ok(())
    }

    // ---------------- snapshot loading ----------------

    /// Build the in-memory metadata snapshot by scanning every table except
    /// schema bodies (those are loaded lazily into the bounded cache).
    pub fn load_snapshot(&self) -> anyhow::Result<Snapshot> {
        let mut snap = Snapshot::default();
        for item in self.db.iterator_cf(self.cf(CF_VERSIONS), IteratorMode::Start) {
            let (k, v) = item?;
            let (ctx, rest) = split_ctx(&k).ok_or_else(|| anyhow::anyhow!("bad version key"))?;
            let (subject, version, _) = split_subject_version(rest).ok_or_else(|| anyhow::anyhow!("bad version key"))?;
            let rec: VersionRecord = serde_json::from_slice(&v)?;
            snap.apply(&Op::PutVersion { ctx, subject, version, rec });
        }
        for item in self.db.iterator_cf(self.cf(CF_FINGERPRINTS), IteratorMode::Start) {
            let (k, v) = item?;
            let (ctx, fp) = split_ctx(&k).ok_or_else(|| anyhow::anyhow!("bad fingerprint key"))?;
            let fp = String::from_utf8_lossy(fp).into_owned();
            let c = snap.ctxs.entry(ctx).or_default();
            c.fingerprints.insert(fp, be_u32(&v));
        }
        for item in self.db.iterator_cf(self.cf(CF_REFBY), IteratorMode::Start) {
            let (k, _) = item?;
            let (ctx, rest) = split_ctx(&k).ok_or_else(|| anyhow::anyhow!("bad refby key"))?;
            let (subject, version, tail) = split_subject_version(rest).ok_or_else(|| anyhow::anyhow!("bad refby key"))?;
            snap.apply(&Op::PutRefby { ctx, subject, version, id: be_u32(tail) });
        }
        for item in self.db.iterator_cf(self.cf(CF_CONFIG), IteratorMode::Start) {
            let (k, v) = item?;
            if let Some(scope) = Scope::from_key(&k) {
                snap.apply(&Op::PutConfig { scope, rec: serde_json::from_slice(&v)? });
            }
        }
        for item in self.db.iterator_cf(self.cf(CF_MODE), IteratorMode::Start) {
            let (k, v) = item?;
            if let Some(scope) = Scope::from_key(&k) {
                snap.apply(&Op::PutMode { scope, mode: serde_json::from_slice(&v)? });
            }
        }
        // Contexts can exist without versions (e.g. after deletes), and
        // next_id counters, so read them from meta explicitly.
        snap.known_contexts = imbl::OrdSet::new();
        for item in self.db.iterator_cf(self.cf(CF_META), IteratorMode::Start) {
            let (k, v) = item?;
            let key = String::from_utf8_lossy(&k).into_owned();
            if let Some(ctx) = key.strip_prefix("ctx/") {
                snap.known_contexts.insert(ctx.to_string());
            } else if let Some(ctx) = key.strip_prefix("next_id/") {
                snap.apply(&Op::SetNextId { ctx: ctx.to_string(), next: be_u32(&v) });
            }
        }
        snap.log_seq = self.log_seq()?;
        Ok(snap)
    }

    // ---------------- transactions ----------------

    pub fn tx(&self) -> ApiResult<Tx<'_>> {
        Ok(Tx { log_seq: self.log_seq()?, log_dirty: false, store: self, batch: WriteBatch::default(), ops: Vec::new() })
    }
}

/// An atomic batch of writes. Callers must hold the registry write lock while
/// building and committing a `Tx` (counters are read-modify-write).
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
        self.batch.put_cf(self.store.cf(cf), key, bytes);
        Ok(())
    }

    /// Store schema content. `index` also maps its fingerprint to `id`
    /// (skipped when an imported id duplicates content that already has an id).
    pub fn put_schema(&mut self, ctx: &str, id: u32, rec: &SchemaRecord, index: bool) -> ApiResult<()> {
        self.put(CF_SCHEMAS, &schema_key(ctx, id), rec)?;
        self.ops.push(Op::PutSchema { ctx: ctx.into(), id, rec: Arc::new(rec.clone()), index });
        if index {
            self.batch.put_cf(self.store.cf(CF_FINGERPRINTS), fp_key(ctx, &rec.fingerprint), id.to_be_bytes());
        }
        Ok(())
    }

    pub fn put_version(&mut self, ctx: &str, subject: &str, version: u32, rec: &VersionRecord) -> ApiResult<()> {
        self.put(CF_VERSIONS, &version_key(ctx, subject, version), rec)?;
        self.batch.put_cf(self.store.cf(CF_META), format!("ctx/{ctx}"), b"");
        self.ops.push(Op::PutVersion { ctx: ctx.into(), subject: subject.into(), version, rec: rec.clone() });
        Ok(())
    }

    pub fn delete_version(&mut self, ctx: &str, subject: &str, version: u32, id: u32) {
        self.batch.delete_cf(self.store.cf(CF_VERSIONS), version_key(ctx, subject, version));
        self.ops.push(Op::DeleteVersion { ctx: ctx.into(), subject: subject.into(), version, id });
    }

    pub fn put_refby(&mut self, ctx: &str, subject: &str, version: u32, id: u32) {
        self.batch.put_cf(self.store.cf(CF_REFBY), refby_key(ctx, subject, version, id), b"");
        self.ops.push(Op::PutRefby { ctx: ctx.into(), subject: subject.into(), version, id });
    }

    pub fn delete_refby(&mut self, ctx: &str, subject: &str, version: u32, id: u32) {
        self.batch.delete_cf(self.store.cf(CF_REFBY), refby_key(ctx, subject, version, id));
        self.ops.push(Op::DeleteRefby { ctx: ctx.into(), subject: subject.into(), version, id });
    }

    pub fn set_next_id(&mut self, ctx: &str, next: u32) {
        self.batch.put_cf(self.store.cf(CF_META), format!("next_id/{ctx}"), next.to_be_bytes());
        self.ops.push(Op::SetNextId { ctx: ctx.into(), next });
    }

    pub fn delete_context(&mut self, ctx: &str) {
        self.batch.delete_cf(self.store.cf(CF_META), format!("ctx/{ctx}"));
        self.ops.push(Op::DeleteContext { ctx: ctx.into() });
    }

    pub fn put_config(&mut self, scope: &Scope, rec: &ConfigRecord) -> ApiResult<()> {
        self.ops.push(Op::PutConfig { scope: scope.clone(), rec: rec.clone() });
        self.put(CF_CONFIG, &scope.key(), rec)
    }

    pub fn delete_config(&mut self, scope: &Scope) {
        self.batch.delete_cf(self.store.cf(CF_CONFIG), scope.key());
        self.ops.push(Op::DeleteConfig { scope: scope.clone() });
    }

    pub fn put_mode(&mut self, scope: &Scope, mode: Mode) -> ApiResult<()> {
        self.ops.push(Op::PutMode { scope: scope.clone(), mode });
        self.put(CF_MODE, &scope.key(), &mode)
    }

    pub fn delete_mode(&mut self, scope: &Scope) {
        self.batch.delete_cf(self.store.cf(CF_MODE), scope.key());
        self.ops.push(Op::DeleteMode { scope: scope.clone() });
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
            self.batch.put_cf(self.store.cf(CF_META), b"log_seq", self.log_seq.to_be_bytes());
            self.ops.push(Op::SetLogSeq { seq: self.log_seq });
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(self.store.sync_writes);
        self.store.db.write_opt(self.batch, &wo)?;
        Ok(self.ops)
    }
}
