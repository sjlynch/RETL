#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{RedditETL, Sources, YearMonth};

/// Demonstrates `count_by_month()` with combined filters:
///   - keywords_any("rust") (matches 2 rows in the tiny corpus)
///   - contains_url(true) (narrows to the comment which has http://)
/// Outcome: Only one comment remains in Jan 2006, so count == 1.
#[test]
fn count_by_month_keywords_and_url() {
    let base = make_corpus_basic();
    let counts = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .keywords_any(["rust"])
        .contains_url(true)
        .count_by_month()
        .unwrap();

    assert_eq!(counts.len(), 1);
    assert_eq!(counts.get(&YearMonth::new(2006, 1)).copied(), Some(1));
}

/// Demonstrates the TSV reducers:
///   - `author_counts_to_tsv()`: total number of matching records per author
///   - `build_first_seen_index_to_tsv()`: earliest timestamp per author
/// Outcome: verify counts (alice=1, bob=1, charlie=1) and earliest timestamps.
#[test]
fn author_counts_and_first_seen_tsv() {
    let base = make_corpus_basic();

    let counts_tsv = base.join("author_counts.tsv");
    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .author_counts_to_tsv(&counts_tsv)
        .unwrap();

    let first_seen_tsv = base.join("first_seen.tsv");
    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .build_first_seen_index_to_tsv(&first_seen_tsv)
        .unwrap();

    // Validate counts
    let mut counts = read_lines(&counts_tsv);
    counts.sort();
    let kv: std::collections::HashMap<String, i64> = counts
        .into_iter()
        .filter_map(|line| {
            let (k, v) = line.split_once('\t')?;
            Some((k.to_string(), v.parse::<i64>().ok()?))
        })
        .collect();

    assert_eq!(kv.get("alice").copied(), Some(1));
    assert_eq!(kv.get("bob").copied(), Some(1));
    assert_eq!(kv.get("charlie").copied(), Some(1));

    // Validate first-seen (created_utc) values
    let mut first_seen = read_lines(&first_seen_tsv);
    first_seen.sort();
    let kv2: std::collections::HashMap<String, i64> = first_seen
        .into_iter()
        .filter_map(|line| {
            let (k, v) = line.split_once('\t')?;
            Some((k.to_string(), v.parse::<i64>().ok()?))
        })
        .collect();

    assert_eq!(kv2.get("bob").copied(), Some(1136073600));     // submission s1
    assert_eq!(kv2.get("alice").copied(), Some(1136074600));   // comment c1
    assert_eq!(kv2.get("charlie").copied(), Some(1136074700)); // comment c2
}
