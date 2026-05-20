use crate::common::write_zst_lines;
use retl::{RedditETL, ScanPlan, Sources, YearMonth};
use serde_json::json;
use std::path::PathBuf;

fn corpus_with_scores() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let lines: Vec<String> = vec![
        json!({"subreddit":"programming","author":"u_neg","id":"c0","body":"x","parent_id":"t3_s1","created_utc":1136074600_i64,"score":-5_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_zero","id":"c1","body":"x","parent_id":"t3_s1","created_utc":1136074601_i64,"score":0_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_one","id":"c2","body":"x","parent_id":"t3_s1","created_utc":1136074602_i64,"score":1_i64}).to_string(),
        json!({"subreddit":"programming","author":"u_ten","id":"c3","body":"x","parent_id":"t3_s1","created_utc":1136074603_i64,"score":10_i64}).to_string(),
    ];
    write_zst_lines(&rc, &lines);
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

#[test]
fn min_score_is_inclusive_at_boundary() {
    let base = corpus_with_scores();
    // min_score(0) keeps {0, 1, 10}; rejects {-5}.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .min_score(0)
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(3));
}

#[test]
fn max_score_is_inclusive_at_boundary() {
    let base = corpus_with_scores();
    // max_score(0) keeps {-5, 0}; rejects {1, 10}.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .max_score(0)
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

#[test]
fn min_and_max_score_combine_as_a_closed_interval() {
    let base = corpus_with_scores();
    // [0, 1] keeps {0, 1}; rejects {-5, 10}.
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .min_score(0)
        .max_score(1)
        .count_by_month()
        .unwrap();
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(2));
}

/// Reddit dumps occasionally store `score` / `created_utc` as JSON strings or
/// whole-number floats rather than integers. Every record below carries the
/// identical numeric values (`score == 100`, `created_utc == 1_136_074_600`)
/// and differs *only* in how those two fields are encoded, so a correct
/// numeric filter must keep or drop all three together.
fn corpus_with_typed_numeric_fields() -> PathBuf {
    let dir = tempfile::tempdir().unwrap().keep();
    let rc = dir.join("comments").join("RC_2006-01.zst");
    let ts: i64 = 1_136_074_600;
    let lines: Vec<String> = vec![
        // Integer-typed `score` / `created_utc` — the baseline encoding.
        json!({"subreddit":"programming","author":"u_int","id":"c0","body":"x","parent_id":"t3_s1","created_utc":ts,"score":100_i64}).to_string(),
        // String-typed `score` / `created_utc`.
        json!({"subreddit":"programming","author":"u_str","id":"c1","body":"x","parent_id":"t3_s1","created_utc":ts.to_string(),"score":"100"}).to_string(),
        // Whole-number float `score` / `created_utc`.
        json!({"subreddit":"programming","author":"u_flt","id":"c2","body":"x","parent_id":"t3_s1","created_utc":ts as f64,"score":100.0_f64}).to_string(),
    ];
    write_zst_lines(&rc, &lines);
    std::fs::create_dir_all(dir.join("submissions")).unwrap();
    dir
}

/// Run a scan over the typed-numeric corpus and return the 2006-01 record
/// count (`0` when the month produced no matches).
fn typed_corpus_count(base: &PathBuf, configure: impl FnOnce(ScanPlan) -> ScanPlan) -> u64 {
    let plan = RedditETL::new()
        .base_dir(base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming");
    configure(plan)
        .count_by_month()
        .unwrap()
        .get(&YearMonth::new(2006, 1))
        .copied()
        .unwrap_or(0)
}

#[test]
fn min_score_treats_string_and_float_typed_score_like_integers() {
    let base = corpus_with_typed_numeric_fields();
    // score == 100 in every encoding: min_score(100) keeps all three,
    // min_score(101) drops all three. Before the MinimalRecord coercion fix
    // the string/float records yielded score=None and were silently dropped.
    assert_eq!(typed_corpus_count(&base, |s| s.min_score(100)), 3);
    assert_eq!(typed_corpus_count(&base, |s| s.min_score(101)), 0);
}

#[test]
fn after_filter_treats_string_and_float_typed_created_utc_like_integers() {
    let base = corpus_with_typed_numeric_fields();
    // created_utc == 1_136_074_600 in every encoding. `--after` is an
    // inclusive `created_utc >=` bound on the MinimalRecord fast path.
    assert_eq!(typed_corpus_count(&base, |s| s.after(1_136_074_600)), 3);
    assert_eq!(typed_corpus_count(&base, |s| s.after(1_136_074_601)), 0);
}

#[test]
fn json_eq_and_gte_agree_on_string_and_float_typed_score() {
    let base = corpus_with_typed_numeric_fields();
    // `--json '/score=100'` and `--json '/score>=100'` must keep the same
    // records. Before the fix, `scalar_values_equal` only compared numerically
    // when both sides were `Value::Number`, so `=100` dropped the string-typed
    // record while `>=100` (which always coerces) kept it.
    let eq = typed_corpus_count(&base, |s| s.json_eq("/score", 100));
    let gte = typed_corpus_count(&base, |s| s.json_number_gte("/score", 100.0));
    assert_eq!(eq, 3, "json '=100' dropped string/float-typed score");
    assert_eq!(gte, 3, "json '>=100' dropped string/float-typed score");
    assert_eq!(eq, gte, "json '=N' and '>=N' disagree on numeric coercion");
}
