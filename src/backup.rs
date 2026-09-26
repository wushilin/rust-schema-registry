//! Backup and restore, as a logical dump.
//!
//! The format is newline-delimited JSON, one record per line, and it describes
//! the registry rather than the store: subjects, versions, ids, references,
//! metadata, rule sets, config, modes and exporters. Nothing about RocksDB,
//! key layout or on-disk format appears in it, so a dump restores into a
//! different build. Registry records use public API shapes and replay into
//! Confluent; exporter progress is this implementation's optional extension.
//!
//! ```text
//! {"type":"registry","format":1,"container":"prod","clusterId":"sr-…","at":…}
//! {"type":"config","subject":null,"config":{"compatibilityLevel":"BACKWARD"}}
//! {"type":"mode","subject":":.eu:","mode":"READONLY"}
//! {"type":"schema","subject":"a","version":1,"id":5,"deleted":false,…}
//! {"type":"exporter","info":{…},"state":"RUNNING","offset":…}
//! ```
//!
//! Restoring replays it through IMPORT mode, which is what `migrate` already
//! does over the wire: ids and versions are preserved, referenced schemas go
//! first, and soft deletes are re-applied after everything is written, because
//! deleting the last live version of a subject drops that subject's settings.
//!
//! Reading a dump is deliberately forgiving of unknown record types and
//! unknown fields: a newer build's dump should still restore what an older one
//! understands.

use std::collections::{HashMap, HashSet};

use serde_json::{Value, json};

use crate::api::percent_encode_segment;
use crate::context::QualifiedSubject;
use crate::migrate::Endpoint;

pub const FORMAT: u32 = 1;

/// Everything a dump holds, in memory. Dumps are the size of the registry's
/// metadata, which is small - the benchmark registry with 20,000 subjects is a
/// few tens of megabytes.
#[derive(Default)]
pub struct Dump {
    pub header: Option<Value>,
    /// Subject -> its versions, oldest first.
    pub subjects: HashMap<String, Vec<Value>>,
    /// In the order they were read, so a restore preserves it.
    pub subject_order: Vec<String>,
    /// `None` is the registry as a whole.
    pub configs: Vec<(Option<String>, Value)>,
    pub modes: Vec<(Option<String>, String)>,
    pub exporters: Vec<Value>,
}

impl Dump {
    /// Which versions of a subject are live (everything else was soft-deleted).
    #[cfg(test)]
    fn live(&self, subject: &str) -> Vec<u32> {
        self.subjects
            .get(subject)
            .into_iter()
            .flatten()
            .filter(|v| !v.get("deleted").and_then(Value::as_bool).unwrap_or(false))
            .filter_map(|v| v.get("version").and_then(Value::as_u64).map(|x| x as u32))
            .collect()
    }

    pub fn version_count(&self) -> usize {
        self.subjects.values().map(Vec::len).sum()
    }

    /// Parse newline-delimited JSON. Unknown record types are skipped, so a
    /// dump from a newer build still restores what this one knows.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let mut d = Dump::default();
        for (n, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: Value = serde_json::from_str(line).map_err(|e| anyhow::anyhow!("line {}: {e}", n + 1))?;
            let subject = |v: &Value| v.get("subject").and_then(Value::as_str).map(String::from);
            match v.get("type").and_then(Value::as_str).unwrap_or("") {
                "registry" => d.header = Some(v),
                "config" => {
                    let c = v.get("config").cloned().unwrap_or_else(|| json!({}));
                    d.configs.push((subject(&v), c));
                }
                "mode" => {
                    if let Some(m) = v.get("mode").and_then(Value::as_str) {
                        d.modes.push((subject(&v), m.to_string()));
                    }
                }
                "schema" => {
                    let Some(s) = subject(&v) else { continue };
                    if !d.subjects.contains_key(&s) {
                        d.subject_order.push(s.clone());
                    }
                    d.subjects.entry(s).or_default().push(v);
                }
                "exporter" => {
                    let info = v.get("info").cloned().unwrap_or_else(|| {
                        let mut value = v.clone();
                        if let Some(obj) = value.as_object_mut() { obj.remove("type"); }
                        value
                    });
                    d.exporters.push(json!({
                        "info": info,
                        "state": v.get("state").and_then(Value::as_str).unwrap_or("RUNNING"),
                        "offset": v.get("offset").and_then(Value::as_u64).unwrap_or(0),
                        "ts": v.get("ts").and_then(Value::as_i64).unwrap_or(0),
                        "trace": v.get("trace").and_then(Value::as_str).unwrap_or(""),
                    }));
                }
                _ => {}
            }
        }
        for versions in d.subjects.values_mut() {
            versions.sort_by_key(|v| v.get("version").and_then(Value::as_u64).unwrap_or(0));
        }
        Ok(d)
    }

    pub fn to_ndjson(&self) -> String {
        let mut out = String::new();
        if let Some(h) = &self.header {
            out.push_str(&h.to_string());
            out.push('\n');
        }
        for (subject, config) in &self.configs {
            out.push_str(&json!({"type": "config", "subject": subject, "config": config}).to_string());
            out.push('\n');
        }
        for (subject, mode) in &self.modes {
            out.push_str(&json!({"type": "mode", "subject": subject, "mode": mode}).to_string());
            out.push('\n');
        }
        for subject in &self.subject_order {
            for v in self.subjects.get(subject).into_iter().flatten() {
                out.push_str(&v.to_string());
                out.push('\n');
            }
        }
        for e in &self.exporters {
            let mut record = e.clone();
            record["type"] = json!("exporter");
            out.push_str(&record.to_string());
            out.push('\n');
        }
        out
    }
}

/// Read a whole registry over its REST API. Works against any Confluent-
/// compatible server, this one included.
pub async fn read(src: &Endpoint, subject_prefix: Option<&str>, prefer_own: bool) -> anyhow::Result<Dump> {
    // Our own registries serve the dump directly, and that one is complete:
    // the walk below can only ask the Confluent API, which has no way to
    // enumerate a subject that exists as nothing but a setting - an alias has
    // no versions, so it is in no listing, and `GET /config/{it}` is a 404.
    // Anything else falls through to the walk, which is what makes a dump from
    // a foreign registry, and from Confluent itself, possible at all.
    let admin = match subject_prefix {
        Some(p) => format!("/admin/api/backup?subjectPrefix={}", percent_encode_segment(p)),
        None => "/admin/api/backup".to_string(),
    };
    if prefer_own
        && let Some(text) = src.get_text(&admin).await
        && let Ok(d) = Dump::parse(&text)
        && d.header.is_some()
    {
        return Ok(d);
    }

    let mut d = Dump { header: Some(json!({"type": "registry", "format": FORMAT, "at": crate::model::now_millis()})), ..Default::default() };
    let prefix = subject_prefix.unwrap_or(":*:");
    let subjects: Vec<String> = serde_json::from_value(
        src.get(&format!("/subjects?subjectPrefix={}&deleted=true", percent_encode_segment(prefix)), &[]).await?,
    )?;

    // Global first, then each context's own, then each subject's.
    if let Some(c) = scope_config(src, None).await? {
        d.configs.push((None, c));
    }
    if let Some(m) = scope_mode(src, None).await? {
        d.modes.push((None, m));
    }
    let contexts: Vec<String> = serde_json::from_value(src.get("/contexts", &[]).await?).unwrap_or_default();
    for ctx in contexts.iter().filter(|c| *c != crate::context::DEFAULT_CONTEXT) {
        let name = format!(":{ctx}:");
        if let Some(c) = scope_config(src, Some(&name)).await? {
            d.configs.push((Some(name.clone()), c));
        }
        if let Some(m) = scope_mode(src, Some(&name)).await? {
            d.modes.push((Some(name), m));
        }
    }

    for subject in subjects {
        let enc = percent_encode_segment(&subject);
        let all: Vec<u32> = serde_json::from_value(src.get(&format!("/subjects/{enc}/versions?deleted=true"), &[]).await?)?;
        let live: Vec<u32> =
            serde_json::from_value(src.get(&format!("/subjects/{enc}/versions"), &[40401]).await.unwrap_or(json!([]))).unwrap_or_default();
        let mut rows = Vec::new();
        for v in all {
            let mut row = src.get(&format!("/subjects/{enc}/versions/{v}?deleted=true"), &[]).await?;
            row["type"] = json!("schema");
            row["deleted"] = json!(!live.contains(&v));
            rows.push(row);
        }
        if !rows.is_empty() {
            d.subject_order.push(subject.clone());
            d.subjects.insert(subject.clone(), rows);
        }
        if let Some(c) = scope_config(src, Some(&subject)).await? {
            d.configs.push((Some(subject.clone()), c));
        }
        if let Some(m) = scope_mode(src, Some(&subject)).await? {
            d.modes.push((Some(subject), m));
        }
    }

    // Exporters are ours, not Confluent's community API: absent is fine.
    if let Ok(names) = src.get("/exporters", &[404, 40403]).await {
        for name in names.as_array().into_iter().flatten().filter_map(Value::as_str) {
            if let Ok(info) = src.get(&format!("/exporters/{}", percent_encode_segment(name)), &[404]).await
                && info.is_object()
            {
                d.exporters.push(json!({"info": info, "state": "RUNNING", "offset": 0, "ts": 0, "trace": ""}));
            }
        }
    }
    Ok(d)
}

async fn scope_config(src: &Endpoint, subject: Option<&str>) -> anyhow::Result<Option<Value>> {
    let path = match subject {
        Some(s) => format!("/config/{}", percent_encode_segment(s)),
        None => "/config".to_string(),
    };
    // A tolerated 404/40408 comes back as the error body, not as data.
    let v = src.get(&path, &[40401, 40408]).await?;
    Ok(configured(v))
}

/// `None` unless this is a real configuration: an empty object, or the error
/// body of a tolerated "nothing configured here", is not one.
fn configured(v: Value) -> Option<Value> {
    let o = v.as_object()?;
    (!o.is_empty() && !o.contains_key("error_code")).then_some(v)
}

async fn scope_mode(src: &Endpoint, subject: Option<&str>) -> anyhow::Result<Option<String>> {
    let path = match subject {
        Some(s) => format!("/mode/{}", percent_encode_segment(s)),
        None => "/mode".to_string(),
    };
    let v = src.get(&path, &[40401, 40409]).await?;
    Ok(v.get("mode").and_then(Value::as_str).map(String::from))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Restored {
    pub subjects: usize,
    pub versions: usize,
    pub soft_deleted: usize,
    pub configs: usize,
    pub modes: usize,
    pub exporters: usize,
}

/// Write a dump into a registry, preserving ids and version numbers.
pub async fn restore(dump: &Dump, dst: &Endpoint, dry_run: bool) -> anyhow::Result<Restored> {
    let mut out = Restored { subjects: 0, versions: 0, soft_deleted: 0, configs: 0, modes: 0, exporters: 0 };
    // Our admin restore can install exporter cursors and states in the same
    // operation as the rest of the dump. Other registries do not expose that
    // extension, so they fall through to the public API replay below.
    if let Some(result) = dst.post_text("/admin/api/restore", &dump.to_ndjson()).await? {
        return Ok(serde_json::from_value(result)?);
    }
    if dry_run {
        out.subjects = dump.subject_order.len();
        out.versions = dump.version_count();
        out.configs = dump.configs.len();
        out.modes = dump.modes.len();
        out.exporters = dump.exporters.len();
        return Ok(out);
    }

    // Global and context settings first, so registrations see them.
    for (subject, config) in dump.configs.iter().filter(|(s, _)| s.as_deref().is_none_or(is_scope)) {
        crate::migrate::put_config(dst, subject.as_deref(), config).await?;
        out.configs += 1;
    }

    let mut done: HashSet<(String, u32)> = HashSet::new();
    for subject in &dump.subject_order {
        let rows = dump.subjects.get(subject).expect("listed");
        let enc = percent_encode_segment(subject);
        // Keep the schemas exactly as they were: this registry normalizes by
        // default, and normalizing would rewrite the text under the same id.
        crate::migrate::put_mode(dst, Some(subject), "IMPORT").await?;
        dst.put(&format!("/config/{enc}"), &json!({"normalize": false}), &[]).await?;
        for row in rows {
            write_version(dump, dst, subject, row, &mut done, &mut out).await?;
        }
        // Deletes come after every version is written: soft-deleting the last
        // live version of a subject drops its config and mode.
        for row in rows.iter().filter(|r| r.get("deleted").and_then(Value::as_bool).unwrap_or(false)) {
            let v = row.get("version").and_then(Value::as_u64).unwrap_or(0);
            dst.delete(&format!("/subjects/{enc}/versions/{v}"), &[40402, 40406]).await?;
            out.soft_deleted += 1;
        }
        out.subjects += 1;
    }

    // The `normalize` we set for each import is ours, not the dump's, and a
    // referenced subject has it set again while its referrer is written - so
    // clearing happens once everything has been written.
    for subject in &dump.subject_order {
        if !dump.configs.iter().any(|(s, _)| s.as_deref() == Some(subject.as_str())) {
            dst.delete(&format!("/config/{}", percent_encode_segment(subject)), &[40401, 40408]).await?;
        }
    }

    // Then each subject's own settings, and finally the modes (a subject's
    // mode is what ends its import).
    for (subject, config) in dump.configs.iter().filter(|(s, _)| s.as_deref().is_some_and(|s| !is_scope(s))) {
        crate::migrate::put_config(dst, subject.as_deref(), config).await?;
        out.configs += 1;
    }
    for (subject, mode) in &dump.modes {
        crate::migrate::put_mode(dst, subject.as_deref(), mode).await?;
        out.modes += 1;
    }
    // A subject that had no mode of its own must not keep the IMPORT we set.
    for subject in &dump.subject_order {
        if !dump.modes.iter().any(|(s, _)| s.as_deref() == Some(subject.as_str())) {
            dst.delete(&format!("/mode/{}", percent_encode_segment(subject)), &[40401, 40409]).await?;
        }
    }

    if !dump.exporters.is_empty() {
        let exporters_only = Dump { exporters: dump.exporters.clone(), ..Default::default() };
        if let Some(result) = dst.post_text("/admin/api/restore", &exporters_only.to_ndjson()).await? {
            out.exporters += result.get("exporters").and_then(Value::as_u64).unwrap_or(0) as usize;
        } else {
            for record in &dump.exporters {
                let info = &record["info"];
                if info.get("name").and_then(Value::as_str).is_none() { continue; }
                // 40950: an exporter by that name is already there - leave
                // the running one alone, a restore must be repeatable.
                dst.post("/exporters", info, &[40950]).await?;
                out.exporters += 1;
            }
        }
    }
    Ok(out)
}

/// `:.eu:` and the like: a context, not a subject.
fn is_scope(subject: &str) -> bool {
    QualifiedSubject::parse(subject).map(|q| q.is_context_only()).unwrap_or(false)
}

/// One version, with whatever it references written first.
async fn write_version(
    dump: &Dump,
    dst: &Endpoint,
    subject: &str,
    row: &Value,
    done: &mut HashSet<(String, u32)>,
    out: &mut Restored,
) -> anyhow::Result<()> {
    let version = row.get("version").and_then(Value::as_u64).unwrap_or(0) as u32;
    if !done.insert((subject.to_string(), version)) {
        return Ok(());
    }
    for r in row.get("references").and_then(Value::as_array).into_iter().flatten() {
        let (Some(rs), Some(rv)) = (r.get("subject").and_then(Value::as_str), r.get("version").and_then(Value::as_i64)) else {
            continue;
        };
        let target = qualify_like(subject, rs);
        if let Some(rows) = dump.subjects.get(&target)
            && let Some(referenced) = rows.iter().find(|x| x.get("version").and_then(Value::as_u64) == Some(rv.max(1) as u64))
        {
            let enc = percent_encode_segment(&target);
            crate::migrate::put_mode(dst, Some(&target), "IMPORT").await?;
            dst.put(&format!("/config/{enc}"), &json!({"normalize": false}), &[]).await?;
            Box::pin(write_version(dump, dst, &target, referenced, done, out)).await?;
        }
    }
    let mut body = json!({
        "schema": row.get("schema").cloned().unwrap_or(Value::Null),
        "references": row.get("references").cloned().unwrap_or_else(|| json!([])),
        "id": row.get("id").cloned().unwrap_or(Value::Null),
        "version": version,
    });
    for key in ["schemaType", "metadata", "ruleSet"] {
        if let Some(v) = row.get(key) {
            body[key] = v.clone();
        }
    }
    dst.post(&format!("/subjects/{}/versions", percent_encode_segment(subject)), &body, &[]).await?;
    out.versions += 1;
    Ok(())
}

/// A reference written unqualified means "in the referrer's context".
fn qualify_like(referrer: &str, referenced: &str) -> String {
    if referenced.starts_with(":.") {
        return referenced.to_string();
    }
    match (QualifiedSubject::parse(referrer), QualifiedSubject::parse(referenced)) {
        (Ok(parent), Ok(target)) => QualifiedSubject::new(&parent.context, &target.subject).qualified(),
        _ => referenced.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exporter_survives_the_round_trip() {
        let d = Dump::parse(r#"{"type":"exporter","info":{"name":"to-dr","subjects":["*"]}}"#).unwrap();
        assert_eq!(d.exporters.len(), 1);
        assert_eq!(d.exporters[0]["info"]["name"], "to-dr");
        assert_eq!(d.exporters[0]["state"], "RUNNING");
        assert_eq!(d.exporters[0]["offset"], 0);
        let again = Dump::parse(&d.to_ndjson()).unwrap();
        assert_eq!(again.exporters[0], d.exporters[0]);
    }

    #[test]
    fn exporter_state_and_offset_survive_a_dump_round_trip() {
        let d = Dump::parse(r#"{"type":"exporter","info":{"name":"to-dr","subjects":["*"]},"state":"PAUSED","offset":27,"ts":1234,"trace":"operator pause"}"#).unwrap();
        let again = Dump::parse(&d.to_ndjson()).unwrap();
        assert_eq!(again.exporters[0]["state"], "PAUSED");
        assert_eq!(again.exporters[0]["offset"], 27);
        assert_eq!(again.exporters[0]["ts"], 1234);
        assert_eq!(again.exporters[0]["trace"], "operator pause");
    }

    #[test]
    fn a_dump_survives_a_round_trip_through_text() {
        let text = concat!(
            r#"{"type":"registry","format":1,"container":"prod"}"#,
            "\n",
            r#"{"type":"config","subject":null,"config":{"compatibilityLevel":"FULL"}}"#,
            "\n",
            r#"{"type":"mode","subject":":.eu:","mode":"READONLY"}"#,
            "\n",
            r#"{"type":"schema","subject":"a","version":2,"id":7,"deleted":false,"schema":"\"string\""}"#,
            "\n",
            r#"{"type":"schema","subject":"a","version":1,"id":5,"deleted":true,"schema":"\"int\""}"#,
            "\n",
            r#"{"type":"unknown-to-this-build","whatever":true}"#,
            "\n",
        );
        let d = Dump::parse(text).unwrap();
        assert_eq!(d.subject_order, vec!["a"], "an unknown record type is skipped, not fatal");
        assert_eq!(d.version_count(), 2);
        // Versions come back oldest first whatever order they were written in.
        let versions: Vec<u64> =
            d.subjects["a"].iter().map(|v| v["version"].as_u64().expect("version")).collect();
        assert_eq!(versions, vec![1, 2]);
        assert_eq!(d.live("a"), vec![2]);
        assert_eq!(d.configs.len(), 1);
        assert_eq!(d.modes, vec![(Some(":.eu:".to_string()), "READONLY".to_string())]);

        // And writing it back out parses to the same thing.
        let again = Dump::parse(&d.to_ndjson()).unwrap();
        assert_eq!(again.version_count(), d.version_count());
        assert_eq!(again.subject_order, d.subject_order);
        assert_eq!(again.modes, d.modes);
    }

    #[test]
    fn references_without_a_context_belong_to_the_referrers() {
        assert_eq!(qualify_like(":.eu:holder", "dep"), ":.eu:dep");
        assert_eq!(qualify_like(":.eu:holder", ":.us:dep"), ":.us:dep");
        assert_eq!(qualify_like("holder", "dep"), "dep");
    }
}

// ---------------------------------------------------------------------------
// The same dump, without the network: what `/admin/api/backup` serves and
// `/admin/api/restore` reads, for the container the request reached.
// ---------------------------------------------------------------------------

use crate::error::ApiResult;
use crate::model::{ConfigRecord, Mode, RegisterSchemaRequest};
use crate::registry::{Registry, VersionSpec};

/// Read this registry straight from its snapshot.
pub fn dump_local(reg: &Registry, prefix: Option<&str>) -> ApiResult<Dump> {
    let mut d = Dump {
        header: Some(json!({
            "type": "registry",
            "format": FORMAT,
            "container": reg.container().as_str(),
            "clusterId": reg.cluster_id,
            "at": crate::model::now_millis(),
        })),
        ..Default::default()
    };
    let scope_config = |subject: Option<&str>| -> ApiResult<Option<Value>> {
        match reg.get_config(subject, false) {
            Ok(c) => Ok(Some(serde_json::to_value(c)?)),
            // Nothing configured at that scope.
            Err(e) if e.code == 40401 || e.code == 40408 => Ok(None),
            Err(e) => Err(e),
        }
    };
    let scope_mode = |subject: &str| -> ApiResult<Option<String>> {
        match reg.get_mode(Some(subject), false) {
            Ok(m) => Ok(Some(m.as_str().to_string())),
            Err(e) if e.code == 40401 || e.code == 40409 => Ok(None),
            Err(e) => Err(e),
        }
    };

    if let Some(c) = scope_config(None)? {
        d.configs.push((None, c));
    }
    d.modes.push((None, reg.get_mode(None, false)?.as_str().to_string()));
    for ctx in reg.list_contexts()?.into_iter().filter(|c| c != crate::context::DEFAULT_CONTEXT) {
        let name = format!(":{ctx}:");
        if let Some(c) = scope_config(Some(&name))? {
            d.configs.push((Some(name.clone()), c));
        }
        if let Some(m) = scope_mode(&name)? {
            d.modes.push((Some(name), m));
        }
    }

    let subjects = reg.list_subjects(prefix.or(Some(":*:")), true, false)?;
    let listed: std::collections::HashSet<String> = subjects.iter().cloned().collect();
    for subject in &subjects {
        let live = reg.list_versions(subject, false, false).unwrap_or_default();
        let mut rows = Vec::new();
        for v in reg.list_versions(subject, true, false)? {
            let view = reg.get_version(subject, VersionSpec::Exact(v), true)?;
            let mut row = serde_json::to_value(&view)?;
            row["type"] = json!("schema");
            row["deleted"] = json!(!live.contains(&v));
            rows.push(row);
        }
        if rows.is_empty() {
            continue;
        }
        d.subject_order.push(subject.clone());
        d.subjects.insert(subject.clone(), rows);
        if let Some(c) = scope_config(Some(subject))? {
            d.configs.push((Some(subject.clone()), c));
        }
        if let Some(m) = scope_mode(subject)? {
            d.modes.push((Some(subject.clone()), m));
        }
    }

    // A subject can exist as nothing but a setting: an alias is a
    // subject-level config with no versions of its own, and `list_subjects`
    // reports subjects by their versions, so walking those alone lost every
    // alias in the registry. Their config comes from the snapshot directly,
    // because `get_config` answers 40401 for a subject that has no versions.
    let mut settings_only: Vec<(String, Value)> = Vec::new();
    for (ctx, s, cfg) in reg.reader().subject_configs() {
        let qualified = crate::context::qualify(&ctx, &s);
        if listed.contains(&qualified) {
            continue;
        }
        if let Some(p) = prefix
            && !qualified.starts_with(p.trim_end_matches('*'))
        {
            continue;
        }
        settings_only.push((qualified, serde_json::to_value(&cfg)?));
    }
    settings_only.sort_by(|a, b| a.0.cmp(&b.0));
    for (subject, cfg) in settings_only {
        d.configs.push((Some(subject), cfg));
    }

    for name in reg.list_exporters()? {
        d.exporters.push(serde_json::to_value(reg.get_exporter(&name)?)?);
    }
    Ok(d)
}

/// Replay a dump into this registry. Same order as the remote restore: every
/// version first, then the deletes, then the settings - because soft-deleting
/// the last live version of a subject drops that subject's settings.
pub fn restore_local(reg: &Registry, dump: &Dump, dry_run: bool) -> ApiResult<Restored> {
    let mut out = Restored { subjects: 0, versions: 0, soft_deleted: 0, configs: 0, modes: 0, exporters: 0 };
    if dry_run {
        out.subjects = dump.subject_order.len();
        out.versions = dump.version_count();
        out.configs = dump.configs.len();
        out.modes = dump.modes.len();
        out.exporters = dump.exporters.len();
        return Ok(out);
    }
    let put_config = |subject: Option<&str>, config: &Value| -> ApiResult<()> {
        let mut rec: ConfigRecord = serde_json::from_value(config.clone()).unwrap_or_default();
        // `compatibilityLevel` on the way out is `compatibility` on the way in;
        // ConfigRecord reads both, so only the missing level needs care.
        if rec.compatibility_level.is_none()
            && let Some(l) = config.get("compatibilityLevel").and_then(Value::as_str)
        {
            rec.compatibility_level = Some(crate::model::CompatibilityLevel::parse(l)?);
        }
        reg.set_config(subject, rec)
    };

    for (subject, config) in dump.configs.iter().filter(|(s, _)| s.as_deref().is_none_or(is_scope)) {
        put_config(subject.as_deref(), config)?;
        out.configs += 1;
    }

    let mut done: HashSet<(String, u32)> = HashSet::new();
    for subject in &dump.subject_order {
        let rows = dump.subjects.get(subject).expect("listed");
        reg.set_mode(Some(subject), Mode::Import, true)?;
        reg.set_config(Some(subject), ConfigRecord { normalize: Some(false), ..Default::default() })?;
        for row in rows {
            write_version_local(reg, dump, subject, row, &mut done, &mut out)?;
        }
        for row in rows.iter().filter(|r| r.get("deleted").and_then(Value::as_bool).unwrap_or(false)) {
            let v = row.get("version").and_then(Value::as_u64).unwrap_or(0) as u32;
            match reg.delete_version(subject, VersionSpec::Exact(v), false) {
                Ok(_) => out.soft_deleted += 1,
                // Already soft-deleted, or gone: a replay is idempotent.
                Err(e) if e.code == 40402 || e.code == 40406 || e.code == 40401 => {}
                Err(e) => return Err(e),
            }
        }
        out.subjects += 1;
    }

    // Ours, not the dump's - and set again on a referenced subject while its
    // referrer was written, so it is cleared once everything is in.
    for subject in &dump.subject_order {
        if !dump.configs.iter().any(|(s, _)| s.as_deref() == Some(subject.as_str())) {
            let _ = reg.delete_config(Some(subject));
        }
    }
    for (subject, config) in dump.configs.iter().filter(|(s, _)| s.as_deref().is_some_and(|s| !is_scope(s))) {
        put_config(subject.as_deref(), config)?;
        out.configs += 1;
    }
    for (subject, mode) in &dump.modes {
        let m = Mode::parse(mode)?;
        reg.set_mode(subject.as_deref(), m, true)?;
        out.modes += 1;
    }
    for subject in &dump.subject_order {
        if !dump.modes.iter().any(|(s, _)| s.as_deref() == Some(subject.as_str())) {
            let _ = reg.delete_mode(subject);
        }
    }

    for record in &dump.exporters {
        let rec: crate::model::ExporterRecord = serde_json::from_value(record.clone())
            .map_err(|e| crate::error::ApiError::unprocessable(format!("exporter: {e}")))?;
        if reg.restore_exporter(rec)? { out.exporters += 1; }
    }
    Ok(out)
}

fn write_version_local(
    reg: &Registry,
    dump: &Dump,
    subject: &str,
    row: &Value,
    done: &mut HashSet<(String, u32)>,
    out: &mut Restored,
) -> ApiResult<()> {
    let version = row.get("version").and_then(Value::as_u64).unwrap_or(0) as u32;
    if !done.insert((subject.to_string(), version)) {
        return Ok(());
    }
    for r in row.get("references").and_then(Value::as_array).into_iter().flatten() {
        let (Some(rs), Some(rv)) = (r.get("subject").and_then(Value::as_str), r.get("version").and_then(Value::as_i64)) else {
            continue;
        };
        let target = qualify_like(subject, rs);
        if let Some(rows) = dump.subjects.get(&target)
            && let Some(referenced) = rows.iter().find(|x| x.get("version").and_then(Value::as_u64) == Some(rv.max(1) as u64))
        {
            reg.set_mode(Some(&target), Mode::Import, true)?;
            reg.set_config(Some(&target), ConfigRecord { normalize: Some(false), ..Default::default() })?;
            write_version_local(reg, dump, &target, referenced, done, out)?;
        }
    }
    let req = RegisterSchemaRequest {
        schema: row.get("schema").and_then(Value::as_str).map(String::from),
        schema_type: row.get("schemaType").and_then(Value::as_str).map(String::from),
        references: row
            .get("references")
            .and_then(Value::as_array)
            .map(|rs| rs.iter().map(|r| crate::model::RefIn::from_value(r)).collect()),
        metadata: row.get("metadata").cloned().filter(|v| !v.is_null()),
        rule_set: row.get("ruleSet").cloned().filter(|v| !v.is_null()),
        version: Some(version as i32),
        id: row.get("id").and_then(Value::as_i64).map(|v| v as i32),
    };
    reg.register(subject, req, false)?;
    out.versions += 1;
    Ok(())
}
