#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{Aggregator, RedditETL};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::io::BufReader;

/// A tiny aggregator: counts how many JSON objects were ingested overall.
/// Demonstrates implementing `Aggregator` and running `aggregate_jsonls_parallel`.
#[derive(Default, Serialize, Deserialize)]
struct RecCount { count: u64 }
impl Aggregator for RecCount {
    fn ingest(&mut self, _record: &Value) { self.count += 1; }
    fn merge(&mut self, other: Self) { self.count += other.count; }
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
    let authors = vec!["alice".to_string(), "bob".to_string(), "charlie".to_string(), "AutoModerator".to_string()];

    let (parts, n) = RedditETL::new()
        .base_dir(&base)
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors(&authors)
        .extract_spool_monthly(&spool_dir, true)
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
