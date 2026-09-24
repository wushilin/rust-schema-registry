//! What the server has been doing, in Prometheus' text format.
//!
//! One global set of counters, updated where the work happens - the engine for
//! mutations, the HTTP layer for requests - plus gauges read from the
//! containers when someone scrapes. Counters are plain atomics: a scrape is
//! rare, a request is not, so the cost that matters is the increment.
//!
//! Label sets are kept small on purpose. A subject or a path would make the
//! cardinality grow with the registry, which is how monitoring becomes the
//! thing that falls over; routes are reported as their shape (`/subjects/*`),
//! and never as the subject.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// Buckets in seconds. Reads are tens of microseconds and a registration with
/// an fsync is a few milliseconds, so the interesting range is narrow.
const BUCKETS: [f64; 9] = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.1, 0.5, 2.0];

#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS.len()],
    count: AtomicU64,
    /// Microseconds, to stay integral; divided on the way out.
    sum_us: AtomicU64,
}

impl Histogram {
    fn observe(&self, seconds: f64, micros: u64) {
        for (i, edge) in BUCKETS.iter().enumerate() {
            if seconds <= *edge {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(micros, Ordering::Relaxed);
    }

    fn render(&self, name: &str, labels: &str, out: &mut String) {
        let sep = if labels.is_empty() { "" } else { "," };
        for (i, edge) in BUCKETS.iter().enumerate() {
            let v = self.buckets[i].load(Ordering::Relaxed);
            out.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"{edge}\"}} {v}\n"));
        }
        let count = self.count.load(Ordering::Relaxed);
        out.push_str(&format!("{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {count}\n"));
        out.push_str(&format!("{name}_sum{{{labels}}} {:.6}\n", self.sum_us.load(Ordering::Relaxed) as f64 / 1e6));
        out.push_str(&format!("{name}_count{{{labels}}} {count}\n"));
    }
}

#[derive(Default)]
struct Series {
    counters: Mutex<HashMap<(String, String), u64>>,
    histograms: Mutex<HashMap<String, Histogram>>,
}

#[derive(Default)]
pub struct Metrics {
    requests: Series,
    mutations: Series,
    started: OnceLock<Instant>,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

pub fn metrics() -> &'static Metrics {
    let m = METRICS.get_or_init(Metrics::default);
    m.started.get_or_init(Instant::now);
    m
}

/// A path as a label: its shape, never its subject. `/subjects/orders-value/
/// versions` is `/subjects/*/versions`, so one label covers every subject
/// instead of one label per subject.
pub fn route_shape(path: &str) -> String {
    let mut out = String::new();
    let mut after = None;
    for seg in path.trim_matches('/').split('/') {
        out.push('/');
        // The segment after `subjects`, `config`, `mode`, `contexts`,
        // `exporters` or `ids` is a name; everything else is structure.
        let is_name = matches!(after, Some("subjects" | "config" | "mode" | "contexts" | "exporters" | "ids"))
            || (after == Some("versions") && seg.parse::<u32>().is_ok())
            || (after == Some("versions") && seg == "latest");
        out.push_str(if is_name { "*" } else { seg });
        after = Some(seg);
    }
    if out.is_empty() { "/".into() } else { out }
}

impl Metrics {
    fn bump(series: &Series, name: &str, labels: String) {
        let mut c = series.counters.lock().unwrap_or_else(|e| e.into_inner());
        *c.entry((name.to_string(), labels)).or_insert(0) += 1;
    }

    fn observe(series: &Series, labels: String, elapsed_us: u64) {
        let hs = series.histograms.lock().unwrap_or_else(|e| e.into_inner());
        // The common path: the series already exists.
        if let Some(h) = hs.get(&labels) {
            h.observe(elapsed_us as f64 / 1e6, elapsed_us);
            return;
        }
        drop(hs);
        let mut hs = series.histograms.lock().unwrap_or_else(|e| e.into_inner());
        hs.entry(labels).or_default().observe(elapsed_us as f64 / 1e6, elapsed_us);
    }

    /// One finished request.
    pub fn request(&self, method: &str, route: &str, status: u16, container: &str, elapsed_us: u64) {
        let labels = format!("method=\"{method}\",route=\"{route}\",container=\"{container}\"");
        Self::bump(&self.requests, "sr_requests_total", format!("{labels},status=\"{status}\""));
        Self::observe(&self.requests, labels, elapsed_us);
    }

    /// One attempted mutation, whatever the engine made of it.
    pub fn mutation(&self, verb: &str, container: &str, outcome: &str, elapsed_us: u64) {
        let labels = format!("verb=\"{verb}\",container=\"{container}\"");
        Self::bump(&self.mutations, "sr_mutations_total", format!("{labels},outcome=\"{outcome}\""));
        Self::observe(&self.mutations, labels, elapsed_us);
    }

    fn render_series(series: &Series, counter: &str, histogram: &str, out: &mut String) {
        let counters = series.counters.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows: Vec<_> = counters.iter().filter(|((n, _), _)| n == counter).collect();
        rows.sort_by(|a, b| a.0.1.cmp(&b.0.1));
        for ((_, labels), v) in rows {
            out.push_str(&format!("{counter}{{{labels}}} {v}\n"));
        }
        drop(counters);
        let hs = series.histograms.lock().unwrap_or_else(|e| e.into_inner());
        let mut keys: Vec<_> = hs.keys().collect();
        keys.sort();
        for k in keys {
            hs[k].render(histogram, k, out);
        }
    }

    /// The whole exposition, including what the containers can say about
    /// themselves right now.
    pub fn render(&self, containers: &crate::containers::Containers) -> String {
        let mut out = String::new();
        out.push_str("# HELP sr_requests_total HTTP requests by method, route shape and status.\n# TYPE sr_requests_total counter\n");
        Self::render_series(&self.requests, "sr_requests_total", "sr_request_duration_seconds", &mut out);
        out.push_str("# HELP sr_mutations_total Attempted changes by verb and outcome.\n# TYPE sr_mutations_total counter\n");
        Self::render_series(&self.mutations, "sr_mutations_total", "sr_mutation_duration_seconds", &mut out);

        out.push_str("# HELP sr_subjects Subjects with at least one live version.\n# TYPE sr_subjects gauge\n");
        let mut rows: Vec<(String, u64, u64, u64, u64, Vec<(String, String, u64)>)> = Vec::new();
        for (name, reg) in containers.all() {
            let (mut subjects, mut versions, mut deleted) = (0u64, 0u64, 0u64);
            if let Ok(all) = reg.all_subject_versions() {
                versions = all.len() as u64;
            }
            if let Ok(live) = reg.list_subjects(Some(":*:"), false, false) {
                subjects = live.len() as u64;
            }
            if let Ok(with_deleted) = reg.list_subjects(Some(":*:"), true, false) {
                deleted = with_deleted.len() as u64 - subjects;
            }
            let contexts = reg.list_contexts().map(|c| c.len() as u64).unwrap_or(0);
            let mut exporters = Vec::new();
            for e in reg.store.list_exporters().unwrap_or_default() {
                exporters.push((e.info.name.clone(), format!("{:?}", e.state).to_uppercase(), e.offset));
            }
            rows.push((name.to_string(), subjects, versions, deleted, contexts, exporters));
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        for (c, subjects, ..) in &rows {
            out.push_str(&format!("sr_subjects{{container=\"{c}\"}} {subjects}\n"));
        }
        out.push_str("# HELP sr_subjects_soft_deleted Subjects whose every version is soft-deleted.\n# TYPE sr_subjects_soft_deleted gauge\n");
        for (c, _, _, deleted, ..) in &rows {
            out.push_str(&format!("sr_subjects_soft_deleted{{container=\"{c}\"}} {deleted}\n"));
        }
        out.push_str("# HELP sr_versions Stored versions, soft-deleted ones included.\n# TYPE sr_versions gauge\n");
        for (c, _, versions, ..) in &rows {
            out.push_str(&format!("sr_versions{{container=\"{c}\"}} {versions}\n"));
        }
        out.push_str("# HELP sr_contexts Contexts in a container.\n# TYPE sr_contexts gauge\n");
        for (c, _, _, _, contexts, _) in &rows {
            out.push_str(&format!("sr_contexts{{container=\"{c}\"}} {contexts}\n"));
        }
        out.push_str("# HELP sr_exporter_offset The change-log position an exporter has reached.\n# TYPE sr_exporter_offset gauge\n");
        for (c, .., exporters) in &rows {
            for (name, _, offset) in exporters {
                out.push_str(&format!("sr_exporter_offset{{container=\"{c}\",exporter=\"{name}\"}} {offset}\n"));
            }
        }
        out.push_str("# HELP sr_exporter_up 1 while an exporter is running; 0 when it is paused, retrying or failed.\n# TYPE sr_exporter_up gauge\n");
        for (c, .., exporters) in &rows {
            for (name, state, _) in exporters {
                let up = u8::from(state == "RUNNING");
                out.push_str(&format!("sr_exporter_up{{container=\"{c}\",exporter=\"{name}\",state=\"{state}\"}} {up}\n"));
            }
        }

        let uptime = self.started.get().map(|s| s.elapsed().as_secs_f64()).unwrap_or(0.0);
        out.push_str("# HELP sr_uptime_seconds Time since this process started serving.\n# TYPE sr_uptime_seconds gauge\n");
        out.push_str(&format!("sr_uptime_seconds {uptime:.3}\n"));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_route_label_never_carries_a_subject() {
        // One label per shape, not one per subject - otherwise the label set
        // grows with the registry.
        assert_eq!(route_shape("/subjects/orders-value/versions"), "/subjects/*/versions");
        assert_eq!(route_shape("/subjects/:.eu:orders-value/versions/3"), "/subjects/*/versions/*");
        assert_eq!(route_shape("/subjects/a/versions/latest/referencedby"), "/subjects/*/versions/*/referencedby");
        assert_eq!(route_shape("/schemas/ids/7/subjects"), "/schemas/ids/*/subjects");
        assert_eq!(route_shape("/config/orders-value"), "/config/*");
        assert_eq!(route_shape("/mode"), "/mode");
        assert_eq!(route_shape("/exporters/to-dr/status"), "/exporters/*/status");
        assert_eq!(route_shape("/contexts/.eu"), "/contexts/*");
        assert_eq!(route_shape("/subjects"), "/subjects");
    }

    #[test]
    fn counts_and_timings_come_back_in_the_exposition() {
        let m = Metrics::default();
        m.request("GET", "/subjects", 200, "default", 300);
        m.request("GET", "/subjects", 200, "default", 900);
        m.request("POST", "/subjects/*/versions", 409, "default", 4000);
        m.mutation("RegisterSchema", "default", "ok", 2500);
        m.mutation("RegisterSchema", "default", "refused", 100);

        let mut out = String::new();
        Metrics::render_series(&m.requests, "sr_requests_total", "sr_request_duration_seconds", &mut out);
        Metrics::render_series(&m.mutations, "sr_mutations_total", "sr_mutation_duration_seconds", &mut out);
        assert!(out.contains(r#"sr_requests_total{method="GET",route="/subjects",container="default",status="200"} 2"#), "{out}");
        assert!(out.contains(r#"sr_requests_total{method="POST",route="/subjects/*/versions",container="default",status="409"} 1"#));
        assert!(out.contains(r#"sr_mutations_total{verb="RegisterSchema",container="default",outcome="refused"} 1"#));
        // 0.3ms and 0.9ms fall in the first two buckets, 4ms does not.
        assert!(out.contains(r#"sr_request_duration_seconds_bucket{method="GET",route="/subjects",container="default",le="0.0005"} 1"#), "{out}");
        assert!(out.contains(r#"sr_request_duration_seconds_count{method="GET",route="/subjects",container="default"} 2"#));
        assert!(out.contains(r#"sr_request_duration_seconds_sum{method="GET",route="/subjects",container="default"} 0.001200"#));
    }
}
