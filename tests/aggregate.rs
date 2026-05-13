#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{Aggregator, RedditETL};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File};
use std::io::BufReader;

/// A tiny aggregator: counts how many JSON objects were ingested overall.
/// Demonstrates implementing `Aggregator` and running `aggregate_jsonls_parallel`.
#[derive(Default, Serialize, Deserialize)]
struct RecCount {
    count: u64,
}
impl Aggregator for RecCount {
    fn ingest(&mut self, _record: &Value) {
        self.count += 1;
    }
    fn merge(&mut self, other: Self) {
        self.count += other.count;
    }
}

#[derive(Default, Serialize, Deserialize)]
struct ThreadCountAgg {
    max_threads: usize,
}
impl Aggregator for ThreadCountAgg {
    fn ingest(&mut self, _record: &Value) {
        self.max_threads = self.max_threads.max(rayon::current_num_threads());
    }
    fn merge(&mut self, other: Self) {
        self.max_threads = self.max_threads.max(other.max_threads);
    }
}

/// Build inputs by spooling per-month parts first, then aggregate those JSONL files.
/// We spool for authors ["alice","bob","charlie","AutoModerator"].
/// The sample month has 5 rows total, but `[deleted]` is **not** in that allow-list,
/// so the spooled inputs contain **4** rows (2 submissions + 2 comments).
#[test]
fn aggregate_over_jsonl_inputs() {
    let base = make_corpus_basic();

    // First, produce small JSONL inputs via spooling.
    let spool_dir = base.join("spool_for_agg");
    let authors = vec![
        "alice".to_string(),
        "bob".to_string(),
        "charlie".to_string(),
        "AutoModerator".to_string(),
    ];

    let (parts, n) = RedditETL::new()
        .base_dir(&base)
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors(&authors)
        .extract_spool_monthly(&spool_dir)
        .unwrap();

    // Expect 4 records (everything except the [deleted] comment).
    assert_eq!(n, 4);

    // Now aggregate those inputs.
    let shards_dir = base.join("agg_shards");
    let final_out = base.join("agg_final.json");
    RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel::<RecCount>(parts, &shards_dir, &final_out, true, false)
        .unwrap();

    // Validate the final count matches the number of spooled rows.
    let f = File::open(&final_out).unwrap();
    let r = BufReader::new(f);
    let agg: RecCount = serde_json::from_reader(r).unwrap();
    assert_eq!(agg.count, 4);
}

#[test]
fn aggregate_same_basename_inputs_get_distinct_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let run_a = tmp.path().join("run_a");
    let run_b = tmp.path().join("run_b");
    fs::create_dir_all(&run_a).unwrap();
    fs::create_dir_all(&run_b).unwrap();
    let a = run_a.join("part_RC_2006-01.jsonl");
    let b = run_b.join("part_RC_2006-01.jsonl");
    fs::write(&a, "{\"id\":\"a1\"}\n{\"id\":\"a2\"}\n").unwrap();
    fs::write(&b, "{\"id\":\"b1\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (agg, built, errors) = RedditETL::new()
        .progress(false)
        .parallelism(2)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![a, b], &shards_dir)
        .unwrap();

    assert_eq!(agg.count, 3);
    assert_eq!(built, 2);
    assert_eq!(errors, 0);

    let mut shard_names: Vec<String> = fs::read_dir(&shards_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("agg_") && name.ends_with(".json"))
        .collect();
    shard_names.sort();
    assert_eq!(shard_names.len(), 2);
    assert_ne!(shard_names[0], shard_names[1]);
    assert!(shard_names.iter().all(|name| name.contains("RC_2006-01")));
}

#[test]
fn aggregate_malformed_json_line_counts_shard_error() {
    let tmp = tempfile::tempdir().unwrap();
    let bad = tmp.path().join("bad.jsonl");
    fs::write(&bad, "{\"id\":\"ok\"}\nnot-json\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (agg, built, errors) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![bad], &shards_dir)
        .unwrap();

    assert_eq!(agg.count, 0, "malformed shard should be dropped");
    assert_eq!(built, 0);
    assert_eq!(errors, 1);
    let shard_count = fs::read_dir(&shards_dir)
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".json")
        })
        .count();
    assert_eq!(shard_count, 0);
}

#[test]
fn aggregate_clears_stale_shards_between_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.jsonl");
    let b = tmp.path().join("b.jsonl");
    fs::write(&a, "{\"id\":\"a\"}\n").unwrap();
    fs::write(&b, "{\"id\":\"b\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (first, built, errors) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![a.clone(), b], &shards_dir)
        .unwrap();
    assert_eq!(first.count, 2);
    assert_eq!(built, 2);
    assert_eq!(errors, 0);

    let (second, built, errors) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![a], &shards_dir)
        .unwrap();
    assert_eq!(
        second.count, 1,
        "stale shard from first run leaked into second"
    );
    assert_eq!(built, 1);
    assert_eq!(errors, 0);
}

#[test]
fn aggregate_honors_parallelism_one() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input.jsonl");
    fs::write(&input, "{\"id\":\"a\"}\n{\"id\":\"b\"}\n").unwrap();

    let (agg, built, errors) = RedditETL::new()
        .progress(false)
        .parallelism(1)
        .aggregate_jsonls_parallel_collect::<ThreadCountAgg>(
            vec![input],
            &tmp.path().join("shards"),
        )
        .unwrap();

    assert_eq!(built, 1);
    assert_eq!(errors, 0);
    assert_eq!(agg.max_threads, 1);
}
