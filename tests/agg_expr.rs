//! Integration coverage for the `ExprAggregator` / `--group-by` + `--agg` DSL.
//!
//! Three flavors:
//!   * `cli_group_by_subreddit_month_count_distinct_*` — fixture acceptance
//!     test that mirrors the worked example in `LATTICE_TASK.md`: runs
//!     `retl aggregate --group-by subreddit,month --agg
//!     'count(*),distinct(author)'` against a small JSONL fixture and
//!     compares the rows against a hand-computed expected output.
//!   * `library_default_count_matches_naive_groupby_*` — property test that
//!     compares `ExprAggregator` against an in-memory naive groupby on
//!     randomly generated tiny inputs.
//!   * `library_grouped_dsl_preserves_existing_collect_call_sites_*` — sanity
//!     that the new aggregator is wired into the same
//!     `aggregate_jsonls_parallel_collect_with` call site as the existing
//!     `GroupMetricAgg`, so the per-shard merge contract continues to hold
//!     for the new DSL.

#[path = "common/mod.rs"]
mod common;

use common::cli::{retl, write_jsonl};
use common::proptest_cases;
use proptest::prelude::*;
use retl::{
    AggOp, Aggregator, ExprAggregator, GroupKey, RedditETL,
};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// -----------------------------------------------------------------------------
// CLI acceptance: --group-by subreddit,month --agg count(*),distinct(author)
// -----------------------------------------------------------------------------

#[test]
fn cli_group_by_subreddit_month_count_distinct_author_matches_hand_computed() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2024-01.jsonl");
    // Fixture: two subreddits across two months. Hand-computed expected
    // counts/distinct-author live below.
    let rows = vec![
        json!({"subreddit": "rust",   "author": "alice",   "id": "a1", "created_utc": 1_704_067_200_i64}), // 2024-01-01
        json!({"subreddit": "rust",   "author": "alice",   "id": "a2", "created_utc": 1_704_067_300_i64}),
        json!({"subreddit": "rust",   "author": "bob",     "id": "a3", "created_utc": 1_704_067_400_i64}),
        json!({"subreddit": "rust",   "author": "carol",   "id": "a4", "created_utc": 1_706_745_600_i64}), // 2024-02-01
        json!({"subreddit": "rust",   "author": "alice",   "id": "a5", "created_utc": 1_706_745_700_i64}),
        json!({"subreddit": "golang", "author": "dave",    "id": "g1", "created_utc": 1_704_067_500_i64}),
        json!({"subreddit": "golang", "author": "eve",     "id": "g2", "created_utc": 1_704_067_600_i64}),
        json!({"subreddit": "golang", "author": "dave",    "id": "g3", "created_utc": 1_706_745_800_i64}),
    ];
    write_jsonl(&input, &rows);

    let out = tmp.path().join("agg.jsonl");
    let shards = tmp.path().join("shards");

    retl()
        .arg("aggregate")
        .arg("--no-manifest")
        .arg("--no-progress")
        .arg("--shards-dir")
        .arg(&shards)
        .arg("--group-by")
        .arg("subreddit,month")
        .arg("--agg")
        .arg("count(*),distinct(author)")
        .arg("--out")
        .arg(&out)
        .arg(&input)
        .assert()
        .success();

    let body = std::fs::read_to_string(&out).unwrap();
    let mut got: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
    for line in body.lines() {
        let row: Map<String, Value> = serde_json::from_str(line).unwrap();
        let sub = row.get("subreddit").and_then(Value::as_str).unwrap().to_string();
        let month = row.get("month").and_then(Value::as_str).unwrap().to_string();
        let count = row.get("count").and_then(Value::as_u64).unwrap();
        let distinct = row
            .get("distinct_author")
            .and_then(Value::as_u64)
            .unwrap();
        got.insert((sub, month), (count, distinct));
    }

    // Hand-computed expected:
    //   golang/2024-01: 2 rows, {dave, eve}    → count=2, distinct=2
    //   golang/2024-02: 1 row,  {dave}          → count=1, distinct=1
    //   rust  /2024-01: 3 rows, {alice, bob}    → count=3, distinct=2
    //   rust  /2024-02: 2 rows, {alice, carol}  → count=2, distinct=2
    let expected: BTreeMap<(String, String), (u64, u64)> = [
        (("golang".to_string(), "2024-01".to_string()), (2, 2)),
        (("golang".to_string(), "2024-02".to_string()), (1, 1)),
        (("rust".to_string(), "2024-01".to_string()), (3, 2)),
        (("rust".to_string(), "2024-02".to_string()), (2, 2)),
    ]
    .into_iter()
    .collect();
    assert_eq!(got, expected);
}

#[test]
fn cli_rejects_combining_group_by_with_legacy_by() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("a.jsonl");
    write_jsonl(&input, &[json!({"subreddit": "rust", "id": "a1"})]);

    let out = retl()
        .arg("aggregate")
        .arg("--no-manifest")
        .arg("--no-progress")
        .arg("--shards-dir")
        .arg(tmp.path().join("shards"))
        .arg("--group-by")
        .arg("subreddit")
        .arg("--by")
        .arg("subreddit")
        .arg("--out")
        .arg(tmp.path().join("agg.jsonl"))
        .arg(&input)
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(
        stderr.contains("generalized DSL") || stderr.contains("--group-by"),
        "expected CLI to reject mixing --group-by with --by; stderr was: {stderr}",
    );
}

// -----------------------------------------------------------------------------
// Library: ExprAggregator threaded through the same parallel-collect helper
// as the existing GroupMetricAgg.
// -----------------------------------------------------------------------------

#[test]
fn library_grouped_dsl_preserves_existing_collect_call_sites() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.jsonl");
    let b = tmp.path().join("b.jsonl");
    write_jsonl(
        &a,
        &[
            json!({"subreddit": "rust", "author": "alice", "score": 5}),
            json!({"subreddit": "rust", "author": "bob", "score": 10}),
        ],
    );
    write_jsonl(
        &b,
        &[
            json!({"subreddit": "rust", "author": "alice", "score": 2}),
            json!({"subreddit": "golang", "author": "carol", "score": 1}),
        ],
    );

    let group_keys = vec![GroupKey::Subreddit];
    let ops = vec![
        AggOp::Count,
        AggOp::CountDistinct("author".to_string()),
        AggOp::Sum("score".to_string()),
        AggOp::Max("score".to_string()),
    ];
    let gks = group_keys.clone();
    let opss = ops.clone();
    let (agg, report) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect_with::<ExprAggregator, _>(
            vec![a, b],
            &tmp.path().join("agg_shards"),
            move || ExprAggregator::new(gks.clone(), opss.clone()),
        )
        .unwrap();

    assert_eq!(report.merged_shards, 2);
    assert_eq!(report.problem_count(), 0);
    let rows = agg.into_rows();
    let by_sub: BTreeMap<String, Map<String, Value>> = rows
        .into_iter()
        .map(|r| {
            (
                r.get("subreddit")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_string(),
                r,
            )
        })
        .collect();
    let rust = &by_sub["rust"];
    assert_eq!(rust.get("count"), Some(&json!(3)));
    assert_eq!(rust.get("distinct_author"), Some(&json!(2)));
    assert_eq!(rust.get("sum_score"), Some(&json!(17)));
    assert_eq!(rust.get("max_score"), Some(&json!(10)));
    let go = &by_sub["golang"];
    assert_eq!(go.get("count"), Some(&json!(1)));
    assert_eq!(go.get("sum_score"), Some(&json!(1)));
}

// -----------------------------------------------------------------------------
// Property test: random records → ExprAggregator must match a naive groupby.
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Rec {
    subreddit: String,
    author: String,
    score: i64,
    id: u64,
}

fn arb_rec() -> impl Strategy<Value = Rec> {
    (
        "[a-c]{1,3}",   // subreddit ∈ small alphabet → guaranteed group overlap
        "[a-d]{1,3}",   // author ∈ small alphabet → distinct counts vary
        -10i64..=10i64, // small score range
        0u64..1000u64,  // id range
    )
        .prop_map(|(subreddit, author, score, id)| Rec {
            subreddit,
            author,
            score,
            id,
        })
}

fn naive_groupby(records: &[Rec]) -> BTreeMap<String, (u64, u64, i64, i64, i64)> {
    // (count, distinct_author, sum_score, min_score, max_score)
    let mut by_sub: BTreeMap<String, (u64, std::collections::HashSet<String>, i64, Option<i64>, Option<i64>)> =
        BTreeMap::new();
    for r in records {
        let e = by_sub
            .entry(r.subreddit.clone())
            .or_insert_with(|| (0, std::collections::HashSet::new(), 0, None, None));
        e.0 += 1;
        e.1.insert(r.author.clone());
        e.2 += r.score;
        e.3 = Some(e.3.map_or(r.score, |old| old.min(r.score)));
        e.4 = Some(e.4.map_or(r.score, |old| old.max(r.score)));
    }
    by_sub
        .into_iter()
        .map(|(k, (c, set, sum, mn, mx))| {
            (k, (c, set.len() as u64, sum, mn.unwrap_or(0), mx.unwrap_or(0)))
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: proptest_cases(64),
        ..Default::default()
    })]

    #[test]
    fn library_default_count_matches_naive_groupby(records in prop::collection::vec(arb_rec(), 0..40)) {
        let mut agg = ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![
                AggOp::Count,
                AggOp::CountDistinct("author".to_string()),
                AggOp::Sum("score".to_string()),
                AggOp::Min("score".to_string()),
                AggOp::Max("score".to_string()),
            ],
        );
        for r in &records {
            agg.ingest(&json!({
                "subreddit": r.subreddit,
                "author": r.author,
                "score": r.score,
                "id": r.id,
            }));
        }
        let rows = agg.into_rows();
        let actual: BTreeMap<String, (u64, u64, i64, i64, i64)> = rows
            .into_iter()
            .map(|row| {
                let sub = row.get("subreddit").and_then(Value::as_str).unwrap().to_string();
                let count = row.get("count").and_then(Value::as_i64).unwrap() as u64;
                let distinct = row
                    .get("distinct_author")
                    .and_then(Value::as_i64)
                    .unwrap() as u64;
                let sum = row.get("sum_score").and_then(Value::as_i64).unwrap();
                let min = row.get("min_score").and_then(Value::as_i64).unwrap();
                let max = row.get("max_score").and_then(Value::as_i64).unwrap();
                (sub, (count, distinct, sum, min, max))
            })
            .collect();
        let expected = naive_groupby(&records);
        prop_assert_eq!(actual, expected);
    }

    /// Splitting records into two shards and folding via `Aggregator::merge`
    /// must produce the same per-group output as ingesting them all into one
    /// shard. This is the associativity invariant the library's parallel
    /// shard-merge relies on.
    #[test]
    fn library_grouped_dsl_merge_matches_single_shard(records in prop::collection::vec(arb_rec(), 0..40)) {
        let make = || ExprAggregator::new(
            vec![GroupKey::Subreddit],
            vec![
                AggOp::Count,
                AggOp::CountDistinct("author".to_string()),
                AggOp::Sum("score".to_string()),
                AggOp::Min("score".to_string()),
                AggOp::Max("score".to_string()),
            ],
        );
        let mut single = make();
        for r in &records {
            single.ingest(&json!({
                "subreddit": r.subreddit,
                "author": r.author,
                "score": r.score,
                "id": r.id,
            }));
        }

        let mut left = make();
        let mut right = make();
        for (i, r) in records.iter().enumerate() {
            let target = if i % 2 == 0 { &mut left } else { &mut right };
            target.ingest(&json!({
                "subreddit": r.subreddit,
                "author": r.author,
                "score": r.score,
                "id": r.id,
            }));
        }
        left.merge(right);

        prop_assert_eq!(single.into_rows(), left.into_rows());
    }
}
