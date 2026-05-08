#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{validate_zst_full, ExportFormat, RedditETL, Sources, YearMonth};

/// Extract with a whitelist and human-readable timestamps:
/// - Focus "programming" in Jan 2006
/// - Whitelist only a few fields to keep records small
/// - Turn on human-readable timestamps (RFC3339) for numeric time fields
/// - Extract to a single JSONL file
/// Checks: file exists, records parse, `created_utc` becomes a string (when present)
#[test]
fn extract_with_whitelist_and_human_timestamps() {
    let base = make_corpus_basic();
    let out = base.join("whitelist_human.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .timestamps_human_readable(true)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .whitelist_fields(["author", "created_utc", "subreddit", "id"])
        .extract_to_jsonl(&out)
        .unwrap();

    assert!(out.exists(), "output JSONL should exist");

    let lines = read_jsonl_values(&out);
    assert!(!lines.is_empty());

    // If created_utc exists, it should now be a string (RFC3339).
    for v in lines {
        if let Some(cu) = v.get("created_utc") {
            assert!(cu.is_string(), "created_utc should be RFC3339 string when present");
        }
    }
}

/// Extract with **only** human-readable timestamps (no whitelist) — exercises the
/// byte-level rewrite fast-path that avoids round-tripping through `serde_json::Value`.
/// Checks: every numeric `created_utc`/`retrieved_on` is converted to RFC3339 strings,
/// every other field is preserved verbatim, and `edited:false` (bool) is left alone.
#[test]
fn extract_with_human_timestamps_only() {
    let base = make_corpus_basic();
    let out = base.join("human_only.jsonl");

    RedditETL::new()
        .base_dir(&base)
        .timestamps_human_readable(true)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out)
        .unwrap();

    let lines = read_jsonl_values(&out);
    assert!(!lines.is_empty(), "expected some output rows");

    let mut saw_created = false;
    let mut saw_retrieved = false;
    let mut saw_edited_bool = false;
    for v in &lines {
        if let Some(cu) = v.get("created_utc") {
            saw_created = true;
            let s = cu.as_str().expect("created_utc should be RFC3339 string");
            assert!(s.contains('T') && (s.ends_with('Z') || s.contains('+')),
                    "created_utc not RFC3339-shaped: {s}");
        }
        if let Some(ro) = v.get("retrieved_on") {
            saw_retrieved = true;
            assert!(ro.is_string(), "retrieved_on should be string when numeric");
        }
        if let Some(e) = v.get("edited") {
            // The corpus uses `edited:false` (bool) — must be preserved untouched.
            if e.is_boolean() {
                saw_edited_bool = true;
                assert_eq!(e.as_bool(), Some(false));
            }
        }
        // Non-timestamp fields must round-trip unchanged.
        if let Some(sub) = v.get("subreddit") {
            assert_eq!(sub.as_str(), Some("programming"));
        }
    }
    assert!(saw_created, "expected at least one row with created_utc");
    assert!(saw_retrieved, "expected at least one row with retrieved_on");
    assert!(saw_edited_bool, "expected at least one row with edited:false bool");
}

/// Partitioned export to ZST:
/// - Focus "programming" in Jan 2006
/// - **Allow pseudo users** to include the `[deleted]` record
/// - Export partitioned per source (comments/submissions) as ZST
/// Expectation: RC_2006-01.zst has 3 lines, RS_2006-01.zst has 2 lines (total 5)
#[test]
fn export_partitioned_zst() {
    let base = make_corpus_basic();
    let out_dir = base.join("export_zst");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users() // include `[deleted]`
        .export_partitioned(&out_dir, ExportFormat::Zst)
        .unwrap();

    let rc = out_dir.join("comments").join("RC_2006-01.zst");
    let rs = out_dir.join("submissions").join("RS_2006-01.zst");
    assert!(rc.exists(), "RC zst not found at {}", rc.display());
    assert!(rs.exists(), "RS zst not found at {}", rs.display());

    let rc_lines = decompress_zst_lines(&rc).len();
    let rs_lines = decompress_zst_lines(&rs).len();

    assert_eq!(rc_lines, 3, "comments (RC) should have 3 lines");
    assert_eq!(rs_lines, 2, "submissions (RS) should have 2 lines");
    assert_eq!(rc_lines + rs_lines, 5, "total lines should be 5");

    // The encoder now writes a content checksum and is properly finalized; the
    // full validator must accept both files (this previously failed because the
    // frame epilogue was never written).
    validate_zst_full(&rc).expect("RC zst must pass full validation");
    validate_zst_full(&rs).expect("RS zst must pass full validation");

    // No leftover .inprogress files should remain in the staging dir.
    let staging = out_dir.join("_staging");
    if staging.exists() {
        let leftovers: Vec<_> = std::fs::read_dir(&staging)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".inprogress"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "staging dir should be free of .inprogress on success; found: {:?}",
            leftovers.iter().map(|e| e.path()).collect::<Vec<_>>()
        );
    }
}
