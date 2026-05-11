//! Cover `ScanPlan::extract_to_json` for both `pretty=false` and `pretty=true`.
//! The output must parse cleanly as `Vec<Value>` and contain exactly the records
//! that survived filtering.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{PlanningError, RedditETL, Sources, YearMonth};
use serde_json::Value;
use std::fs;

#[test]
fn extract_to_json_array_compact_parses_back_as_vec() {
    let base = make_corpus_basic();
    let out = base.join("out_compact.json");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users()
        .extract_to_json(&out, false)
        .unwrap();

    let s = fs::read_to_string(&out).unwrap();
    // Compact form should not contain a newline between the opening bracket and the first element.
    // (No assertion about whitespace between elements — focus is on validity, not formatting.)
    assert!(s.starts_with('['), "compact form starts with '['");
    assert!(s.ends_with(']') || s.trim_end().ends_with(']'));

    let arr: Vec<Value> = serde_json::from_str(&s).expect("compact array parses");
    assert_eq!(arr.len(), 5, "1 RS(2) + 1 RC(3) = 5 records");

    // Every element should have at least an "id" field (records have id).
    for v in &arr {
        assert!(v.get("id").is_some(), "record missing id: {:?}", v);
    }
}

#[test]
fn extract_to_json_array_pretty_parses_back_as_vec_and_has_indentation() {
    let base = make_corpus_basic();
    let out = base.join("out_pretty.json");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users()
        .extract_to_json(&out, true)
        .unwrap();

    let s = fs::read_to_string(&out).unwrap();
    assert!(s.contains('\n'), "pretty form should have line breaks");
    let arr: Vec<Value> = serde_json::from_str(&s).expect("pretty array parses");
    assert_eq!(arr.len(), 5);
    // Same content as compact, just formatted differently.
    let ids: std::collections::BTreeSet<&str> = arr
        .iter()
        .filter_map(|v| v.get("id").and_then(|x| x.as_str()))
        .collect();
    // Expect the basic corpus ids: s1, s2, c1, c2, c3
    let expected: std::collections::BTreeSet<&str> =
        ["s1", "s2", "c1", "c2", "c3"].into_iter().collect();
    assert_eq!(ids, expected);
}

#[test]
fn extract_to_json_array_errors_when_no_input_files_exist() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_path_buf();
    fs::create_dir_all(base.join("comments")).unwrap();
    fs::create_dir_all(base.join("submissions")).unwrap();

    let out = base.join("empty.json");
    let err = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(1999, 1)), Some(YearMonth::new(1999, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_to_json(&out, false)
        .unwrap_err();

    assert!(err.downcast_ref::<PlanningError>().is_some());
    assert!(!out.exists(), "no empty JSON array should be published without candidate input files");
}
