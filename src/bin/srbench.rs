//! Load generator for Confluent-compatible schema registries.
//!
//! srbench --url http://127.0.0.1:8081 [--subjects 1000] [--concurrency 64] [--seconds 10]
//!
//! Seeds `--subjects` Avro subjects (one schema each), then runs each workload
//! for a warmup period followed by a measured period, reporting throughput and
//! latency percentiles. Workloads mirror what real clients do:
//!
//! * get-by-id        GET  /schemas/ids/{id}               (every deserializer, first sight of an id)
//! * latest           GET  /subjects/{s}/versions/latest   (use.latest.version serializers)
//! * lookup           POST /subjects/{s}                   (auto.register=false serializers)
//! * register-exist   POST /subjects/{s}/versions          (auto.register=true serializers, idempotent)
//! * compat           POST /compatibility/subjects/{s}/versions/latest   (CI checks)
//! * register-new     POST /subjects/{new}/versions        (writes)

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::json;

struct Args {
    url: String,
    subjects: usize,
    concurrency: usize,
    seconds: u64,
    warmup: u64,
    only: Option<String>,
    prefix: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        url: "http://127.0.0.1:8081".into(),
        subjects: 1000,
        concurrency: 64,
        seconds: 10,
        warmup: 3,
        only: None,
        prefix: format!("bench-{}", std::process::id()),
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut v = || it.next().expect("missing value");
        match k.as_str() {
            "--url" => a.url = v(),
            "--subjects" => a.subjects = v().parse().expect("number"),
            "--concurrency" => a.concurrency = v().parse().expect("number"),
            "--seconds" => a.seconds = v().parse().expect("number"),
            "--warmup" => a.warmup = v().parse().expect("number"),
            "--only" => a.only = Some(v()),
            "--prefix" => a.prefix = v(),
            other => panic!("unknown argument {other}"),
        }
    }
    a
}

fn schema_for(i: usize) -> String {
    json!({
        "type": "record",
        "name": format!("Rec{i}"),
        "namespace": "bench",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "tags", "type": {"type": "array", "items": "string"}, "default": []},
            {"name": "amount", "type": ["null", "double"], "default": null},
            {"name": "kind", "type": {"type": "enum", "name": format!("Kind{i}"), "symbols": ["A", "B", "C"]}},
        ]
    })
    .to_string()
}

#[derive(Clone, Copy)]
enum Workload {
    GetById,
    Latest,
    Lookup,
    RegisterExisting,
    Compat,
    RegisterNew,
}

impl Workload {
    fn name(&self) -> &'static str {
        match self {
            Workload::GetById => "get-by-id",
            Workload::Latest => "latest",
            Workload::Lookup => "lookup",
            Workload::RegisterExisting => "register-exist",
            Workload::Compat => "compat",
            Workload::RegisterNew => "register-new",
        }
    }
}

struct Ctx {
    client: reqwest::Client,
    url: String,
    subjects: Vec<String>,
    ids: Vec<u32>,
    bodies: Vec<String>,
    new_counter: AtomicU64,
    prefix: String,
}

async fn one(ctx: &Ctx, w: Workload, n: u64) -> bool {
    let i = (n as usize).wrapping_mul(2654435761) % ctx.subjects.len();
    let ct = "application/vnd.schemaregistry.v1+json";
    let req = match w {
        Workload::GetById => ctx.client.get(format!("{}/schemas/ids/{}", ctx.url, ctx.ids[i])),
        Workload::Latest => ctx.client.get(format!("{}/subjects/{}/versions/latest", ctx.url, ctx.subjects[i])),
        Workload::Lookup => ctx
            .client
            .post(format!("{}/subjects/{}", ctx.url, ctx.subjects[i]))
            .header("Content-Type", ct)
            .body(ctx.bodies[i].clone()),
        Workload::RegisterExisting => ctx
            .client
            .post(format!("{}/subjects/{}/versions", ctx.url, ctx.subjects[i]))
            .header("Content-Type", ct)
            .body(ctx.bodies[i].clone()),
        Workload::Compat => ctx
            .client
            .post(format!("{}/compatibility/subjects/{}/versions/latest", ctx.url, ctx.subjects[i]))
            .header("Content-Type", ct)
            .body(ctx.bodies[i].clone()),
        Workload::RegisterNew => {
            let k = ctx.new_counter.fetch_add(1, Ordering::Relaxed) as usize;
            ctx.client
                .post(format!("{}/subjects/{}-new-{k}/versions", ctx.url, ctx.prefix))
                .header("Content-Type", ct)
                .body(json!({ "schema": schema_for(1_000_000 + k) }).to_string())
        }
    };
    match req.send().await {
        Ok(r) => {
            let ok = r.status().is_success();
            let _ = r.bytes().await;
            ok
        }
        Err(_) => false,
    }
}

struct Stats {
    ops: u64,
    errors: u64,
    elapsed: Duration,
    lat_us: Vec<u64>,
}

async fn run(ctx: Arc<Ctx>, w: Workload, concurrency: usize, dur: Duration, record: bool) -> Stats {
    let stop = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    let start = Instant::now();
    for _ in 0..concurrency {
        let (ctx, stop, counter) = (ctx.clone(), stop.clone(), counter.clone());
        handles.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut errors = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                let t = Instant::now();
                if !one(&ctx, w, n).await {
                    errors += 1;
                }
                if record {
                    lat.push(t.elapsed().as_micros() as u64);
                }
            }
            (lat, errors)
        }));
    }
    tokio::time::sleep(dur).await;
    stop.store(true, Ordering::Relaxed);
    let mut lat_us = Vec::new();
    let mut errors = 0;
    for h in handles {
        let (l, e) = h.await.expect("task");
        lat_us.extend(l);
        errors += e;
    }
    let elapsed = start.elapsed();
    lat_us.sort_unstable();
    Stats { ops: lat_us.len() as u64, errors, elapsed, lat_us }
}

fn pct(v: &[u64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[idx] as f64 / 1000.0
}

#[tokio::main]
async fn main() {
    let a = parse_args();
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(a.concurrency * 2)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("client");

    eprintln!("seeding {} subjects on {} ...", a.subjects, a.url);
    let mut subjects = Vec::with_capacity(a.subjects);
    let mut ids = Vec::with_capacity(a.subjects);
    let mut bodies = Vec::with_capacity(a.subjects);
    let seed_start = Instant::now();
    for i in 0..a.subjects {
        let s = format!("{}-{i}-value", a.prefix);
        let body = json!({ "schema": schema_for(i) }).to_string();
        let r = client
            .post(format!("{}/subjects/{s}/versions", a.url))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json")
            .body(body.clone())
            .send()
            .await
            .expect("seed request");
        let status = r.status();
        let v: serde_json::Value = r.json().await.expect("seed json");
        let id = v["id"].as_u64().unwrap_or_else(|| panic!("seed failed ({status}): {v}")) as u32;
        subjects.push(s);
        ids.push(id);
        bodies.push(body);
    }
    eprintln!("seeded in {:.1}s", seed_start.elapsed().as_secs_f64());

    let ctx = Arc::new(Ctx {
        client,
        url: a.url.clone(),
        subjects,
        ids,
        bodies,
        new_counter: AtomicU64::new(0),
        prefix: a.prefix.clone(),
    });
    let workloads = [
        Workload::GetById,
        Workload::Latest,
        Workload::Lookup,
        Workload::RegisterExisting,
        Workload::Compat,
        Workload::RegisterNew,
    ];
    println!("{:<15} {:>10} {:>9} {:>9} {:>9} {:>9} {:>7}", "workload", "req/s", "p50 ms", "p99 ms", "p99.9 ms", "max ms", "errors");
    for w in workloads {
        if let Some(only) = &a.only
            && !only.split(',').any(|o| o == w.name())
        {
            continue;
        }
        let _ = run(ctx.clone(), w, a.concurrency, Duration::from_secs(a.warmup), false).await;
        let s = run(ctx.clone(), w, a.concurrency, Duration::from_secs(a.seconds), true).await;
        println!(
            "{:<15} {:>10.0} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>7}",
            w.name(),
            s.ops as f64 / s.elapsed.as_secs_f64(),
            pct(&s.lat_us, 0.50),
            pct(&s.lat_us, 0.99),
            pct(&s.lat_us, 0.999),
            s.lat_us.last().copied().unwrap_or(0) as f64 / 1000.0,
            s.errors
        );
    }
}
