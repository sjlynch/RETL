//! Property tests for two end-to-end invariants:
//!
//! 1. **Round-trip + dedupe over scan/export**. For an arbitrary set of records,
//!    an arbitrary `QuerySpec`, and either JSONL or JSON-array output, the set
//!    of exported records (modulo whitelist projection) equals the set of input
//!    records that satisfy the same query. This jointly exercises
//!    `streaming::stream_job` (the on-line filter/transform) and the byte-level
//!    fast-path rewriter, so a regression in either shows up here.
//!
//! 2. **Dedupe is idempotent**. `build_runs_sorted` + `merge_runs_sorted` over
//!    arbitrary record inputs collapse duplicates by key, and re-running the
//!    pipeline on its own output is a no-op (and the keys in the output are
//!    unique).

#[path = "common/mod.rs"]
mod common;

use proptest::prelude::*;
use retl::{
    build_runs_sorted, merge_runs_sorted, DedupeCfg, ExportFormat, KeyExtractor, RedditETL,
    Sources, YearMonth,
};
use serde_json::{json, Value};
use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared generators / helpers
// ---------------------------------------------------------------------------

const SUBREDDITS: &[&str] = &["programming", "rust", "python", "linux"];
const AUTHORS: &[&str] = &["alice", "bob", "carol", "dave", "eve"];

#[derive(Clone, Debug)]
struct TestRecord {
    id: String,
    author: String,
    subreddit: String,
    score: i64,
    created_utc: i64,
}

impl TestRecord {
    fn to_json_line(&self) -> String {
        json!({
            "id": self.id,
            "author": self.author,
            "subreddit": self.subreddit,
            "subreddit_id": "t5_x",
            "score": self.score,
            "ups": self.score,
            "controversiality": 0,
            "stickied": false,
            "edited": false,
            "gilded": 0,
            "distinguished": null,
            "link_id": "t3_s1",
            "parent_id": "t3_s1",
            "body": "x",
            "created_utc": self.created_utc,
            "retrieved_on": self.created_utc + 100,
        })
        .to_string()
    }
}

fn record_strategy() -> impl Strategy<Value = TestRecord> {
    // 2006-01-01 00:00:00 UTC .. + ~30 days
    const TS_START: i64 = 1_136_073_600;
    const TS_END: i64 = TS_START + 30 * 24 * 3600;

    (
        prop::sample::select(SUBREDDITS),
        prop::sample::select(AUTHORS),
        -10i64..=100,
        TS_START..=TS_END,
    )
        .prop_map(|(sub, author, score, ts)| TestRecord {
            id: String::new(),
            author: author.to_string(),
            subreddit: sub.to_string(),
            score,
            created_utc: ts,
        })
}

#[derive(Clone, Debug)]
struct GenQuery {
    subreddits: Option<Vec<String>>,
    authors_in: Option<Vec<String>>,
    min_score: Option<i64>,
    max_score: Option<i64>,
    whitelist: Option<Vec<String>>,
}

fn query_strategy() -> impl Strategy<Value = GenQuery> {
    (
        prop::option::of(prop::collection::vec(
            prop::sample::select(SUBREDDITS).prop_map(|s| s.to_string()),
            1..=3,
        )),
        prop::option::of(prop::collection::vec(
            prop::sample::select(AUTHORS).prop_map(|s| s.to_string()),
            1..=3,
        )),
        prop::option::of(-20i64..=120),
        prop::option::of(-20i64..=120),
        prop::option::of(prop_oneof![
            Just(vec!["author".to_string(), "id".to_string()]),
            Just(vec!["id".to_string(), "subreddit".to_string()]),
        ]),
    )
        .prop_map(|(subs, auths, mut min_s, mut max_s, wl)| {
            if let (Some(min), Some(max)) = (min_s, max_s) {
                if min > max {
                    min_s = Some(max);
                    max_s = Some(min);
                }
            }
            GenQuery {
                subreddits: subs,
                authors_in: auths,
                min_score: min_s,
                max_score: max_s,
                whitelist: wl,
            }
        })
}

#[derive(Clone, Copy, Debug)]
enum OutFormat {
    Jsonl,
    JsonArray,
    PartitionedZst,
}

fn format_strategy() -> impl Strategy<Value = OutFormat> {
    prop_oneof![
        Just(OutFormat::Jsonl),
        Just(OutFormat::JsonArray),
        Just(OutFormat::PartitionedZst),
    ]
}

/// Mirror of the predicate used by `matches_minimal` for the subset of fields
/// that our generators populate (no pseudo users, no domain/keywords/regex).
/// Filters use `include_pseudo_users()` so the pseudo-user gate is off.
fn record_matches(r: &TestRecord, q: &GenQuery) -> bool {
    if let Some(subs) = &q.subreddits {
        if !subs.iter().any(|s| s.eq_ignore_ascii_case(&r.subreddit)) {
            return false;
        }
    }
    if let Some(auths) = &q.authors_in {
        if !auths.iter().any(|a| a.eq_ignore_ascii_case(&r.author)) {
            return false;
        }
    }
    if let Some(min_s) = q.min_score {
        if r.score < min_s {
            return false;
        }
    }
    if let Some(max_s) = q.max_score {
        if r.score > max_s {
            return false;
        }
    }
    true
}

/// Project a record (full JSON object) onto the whitelist's keys, mirroring
/// the projection that `streaming::stream_job` performs when whitelist is set.
fn project(v: &Value, wl: &Option<Vec<String>>) -> Value {
    match wl {
        None => v.clone(),
        Some(fields) => {
            let mut m = serde_json::Map::new();
            if let Some(obj) = v.as_object() {
                for k in fields {
                    if let Some(x) = obj.get(k) {
                        m.insert(k.clone(), x.clone());
                    }
                }
            }
            Value::Object(m)
        }
    }
}

/// Write `records` as a single one-month `.zst` (RC_2006-01.zst) using the
/// same encoder shape as `atomic_write::write_zst_atomic` (level 3 +
/// `include_checksum(true)`).
fn write_input_zst(path: &Path, records: &[TestRecord]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let f = File::create(path).unwrap();
    let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
    enc.include_checksum(true).unwrap();
    for r in records {
        writeln!(enc, "{}", r.to_json_line()).unwrap();
    }
    enc.finish().unwrap();
}

fn read_jsonl_values(path: &Path) -> Vec<Value> {
    let f = File::open(path).unwrap();
    BufReader::new(f)
        .lines()
        .map_while(Result::ok)
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str(&s).unwrap())
        .collect()
}

fn read_zst_values(path: &Path) -> Vec<Value> {
    let f = File::open(path).unwrap();
    let dec = zstd::stream::read::Decoder::new(f).unwrap();
    BufReader::new(dec)
        .lines()
        .map_while(Result::ok)
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str(&s).unwrap())
        .collect()
}

/// Drive the scan/extract pipeline for the chosen format and return the
/// exported records as `Vec<Value>`.
fn run_export(records: &[TestRecord], q: &GenQuery, fmt: OutFormat) -> Vec<Value> {
    let dir = TempDir::new().unwrap();
    let base = dir.path();
    let rc_path = base.join("comments").join("RC_2006-01.zst");
    write_input_zst(&rc_path, records);

    let mut etl = RedditETL::new()
        .base_dir(base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false);

    if let Some(wl) = &q.whitelist {
        etl = etl.whitelist_fields(wl.clone());
    }

    let mut plan = etl.scan().include_pseudo_users();
    if let Some(subs) = &q.subreddits {
        plan = plan.subreddits(subs.clone());
    }
    if let Some(auths) = &q.authors_in {
        plan = plan.authors(auths.clone());
    }
    if let Some(min_s) = q.min_score {
        plan = plan.min_score(min_s);
    }
    if let Some(max_s) = q.max_score {
        plan = plan.max_score(max_s);
    }

    match fmt {
        OutFormat::Jsonl => {
            let p = base.join("out.jsonl");
            plan.extract_to_jsonl(&p).unwrap();
            if !p.exists() {
                return Vec::new();
            }
            read_jsonl_values(&p)
        }
        OutFormat::JsonArray => {
            let p = base.join("out.json");
            plan.extract_to_json(&p, false).unwrap();
            if !p.exists() {
                return Vec::new();
            }
            let s = fs::read_to_string(&p).unwrap();
            let v: Value = serde_json::from_str(&s).unwrap();
            v.as_array().cloned().unwrap_or_default()
        }
        OutFormat::PartitionedZst => {
            let out_dir = base.join("export");
            plan.export_partitioned(&out_dir, ExportFormat::Zst)
                .unwrap();
            // export_partitioned removes empty zsts; fall back to "no records exported".
            let rc_out = out_dir.join("comments").join("RC_2006-01.zst");
            if !rc_out.exists() {
                return Vec::new();
            }
            read_zst_values(&rc_out)
        }
    }
}

// ---------------------------------------------------------------------------
// Round-trip property
// ---------------------------------------------------------------------------

proptest! {
    // Each case runs a full RedditETL pipeline end-to-end (zstd encode +
    // extract/export). Default 8 cases per run for daily iteration; bump with
    // `RETL_PROPTEST_CASES=128 cargo test --test properties` before merging
    // changes to the streaming/export hot path.
    #![proptest_config(ProptestConfig {
        cases: common::proptest_cases(8),
        ..ProptestConfig::default()
    })]

    /// For any (records, query, format), the set of exported records (projected
    /// through the whitelist if any) equals the set of input records satisfying
    /// the same query (projected the same way).
    #[test]
    fn round_trip_export_matches_filter(
        recs in prop::collection::vec(record_strategy(), 0..50),
        q in query_strategy(),
        fmt in format_strategy(),
    ) {
        // Stamp unique ids so individual records remain distinguishable.
        let records: Vec<TestRecord> = recs
            .into_iter()
            .enumerate()
            .map(|(i, mut r)| { r.id = format!("rec_{:06}", i); r })
            .collect();

        let exported = run_export(&records, &q, fmt);

        let expected: BTreeSet<String> = records
            .iter()
            .filter(|r| record_matches(r, &q))
            .map(|r| {
                let v: Value = serde_json::from_str(&r.to_json_line()).unwrap();
                serde_json::to_string(&project(&v, &q.whitelist)).unwrap()
            })
            .collect();

        let got: BTreeSet<String> = exported
            .iter()
            .map(|v| serde_json::to_string(&project(v, &q.whitelist)).unwrap())
            .collect();

        prop_assert_eq!(got, expected);
    }
}

// ---------------------------------------------------------------------------
// Dedupe idempotency property
// ---------------------------------------------------------------------------

/// Materialize `lines` as a single NDJSON file under `dir`, run
/// `build_runs_sorted` + `merge_runs_sorted` keyed on `author`, picking the
/// first line in each key-group as the representative. Return the output
/// lines (without trailing newlines).
fn run_dedupe_pipeline(lines: &[String], dir: &Path, label: &str) -> Vec<String> {
    let in_path = dir.join(format!("{}_in.ndjson", label));
    {
        let mut f = File::create(&in_path).unwrap();
        for l in lines {
            writeln!(f, "{}", l).unwrap();
        }
    }
    let runs_dir = dir.join(format!("{}_runs", label));
    let key = KeyExtractor::author_lowercase_fast();
    let cfg = DedupeCfg {
        // Tight buffers so even small inputs may produce >1 run, exercising
        // the k-way merge path. The actual default would coalesce everything
        // into a single run for tiny inputs.
        mem: retl::AdaptiveMemCfg {
            soft_low_frac: 0.0,
            high_frac: 1.0,
            adapt_cooldown_ms: 1,
        },
        min_buf_mb: 0,
        max_buf_mb: 0,
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        inflight_bytes: 0, // disable cap; tight min/max_buf_mb drives flushes
    };
    let runs = build_runs_sorted(&in_path, &runs_dir, &key, &cfg).unwrap();
    let out_path = dir.join(format!("{}_out.ndjson", label));
    merge_runs_sorted(&runs, &out_path, &key, &cfg, |_k, group, w| {
        // Keep the first occurrence as the canonical record for the key.
        let line = &group[0];
        w.write_all(line.as_bytes())?;
        if !line.ends_with('\n') {
            w.write_all(b"\n")?;
        }
        Ok(())
    })
    .unwrap();

    if !out_path.exists() {
        return Vec::new();
    }
    BufReader::new(File::open(&out_path).unwrap())
        .lines()
        .map_while(Result::ok)
        .filter(|s| !s.is_empty())
        .collect()
}

proptest! {
    // Each case runs build_runs_sorted + merge_runs_sorted twice over a
    // generated input. Default 8 cases per run for daily iteration; bump with
    // `RETL_PROPTEST_CASES=128 cargo test --test properties`.
    #![proptest_config(ProptestConfig {
        cases: common::proptest_cases(8),
        ..ProptestConfig::default()
    })]

    /// Running the dedupe pipeline twice yields the same output as running it
    /// once, and the once-deduped output has unique keys (and unique lines).
    #[test]
    fn dedupe_is_idempotent(
        seeds in prop::collection::vec(
            (
                prop::sample::select(AUTHORS).prop_map(|s| s.to_string()),
                0u32..1000,
            ),
            0..50,
        ),
    ) {
        // Build JSON-shaped lines so the key extractor can find an author key.
        // Using `Vec<(author, n)>` ensures duplicate authors collide on keys
        // (which is what makes idempotency non-trivial), while `n` lets two
        // records sharing an author still differ in body.
        let lines: Vec<String> = seeds
            .iter()
            .enumerate()
            .map(|(i, (author, n))| {
                json!({
                    "author": author,
                    "id": format!("rec_{}_{}", i, n),
                    "subreddit": "x",
                })
                .to_string()
            })
            .collect();

        let dir = TempDir::new().unwrap();
        let once = run_dedupe_pipeline(&lines, dir.path(), "once");
        let twice = run_dedupe_pipeline(&once, dir.path(), "twice");

        prop_assert_eq!(&once, &twice);

        // Output lines are byte-unique (one representative per key).
        let unique_lines: HashSet<&String> = once.iter().collect();
        prop_assert_eq!(unique_lines.len(), once.len());

        // Output keys are unique.
        let key = KeyExtractor::author_lowercase_fast();
        let keys: Vec<String> = once
            .iter()
            .filter_map(|line| key.key_from_line(line).expect("valid JSON in dedupe output"))
            .collect();
        let unique_keys: HashSet<&String> = keys.iter().collect();
        prop_assert_eq!(unique_keys.len(), keys.len());
    }
}
