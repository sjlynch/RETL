#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ParentAttachStats, ParentIds, ParentMaps, RedditETL, Sources, YearMonth};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

/// End-to-end parents pipeline over a tiny synthetic corpus:
/// 1) Build a small test corpus under a temp dir (RC/RS 2006-01 for r/programming).
/// 2) Spool *both* submissions and comments for r/programming **without** restricting authors,
///    but with `.include_pseudo_users()` so `[deleted]` is included.
/// 3) Collect parent IDs from the spooled JSONL files.
/// 4) Resolve the parent contents into a cache by scanning the tiny corpus over a ±1 month window.
/// 5) Attach parent payloads back onto the spooled records.
/// 6) Assert that the total number of lines with parents attached is **5**
///    (3 comments in RC_2006-01 + 2 submissions in RS_2006-01).
#[test]
fn spool_resolve_attach_parents_end_to_end() {
    // Create miniature corpus
    let base = make_corpus_basic();

    // Work dirs (under the same temp base)
    let work_dir = base.join("work");
    let spool_dir = work_dir.join("spool");
    let spool_with_parents_dir = work_dir.join("spool_with_parents");
    let parents_cache_dir = work_dir.join("parents_cache");
    let lib_tmp = work_dir.join("lib_tmp");

    fs::create_dir_all(&spool_dir).unwrap();
    fs::create_dir_all(&spool_with_parents_dir).unwrap();
    fs::create_dir_all(&parents_cache_dir).unwrap();
    fs::create_dir_all(&lib_tmp).unwrap();

    // Pipeline window
    let start = YearMonth::new(2006, 1);
    let end = YearMonth::new(2006, 1);

    // ------------- Step 1: Spool monthly parts (Both sources) -------------
    // NOTE: We **do not** constrain authors here. We only allow pseudo users so
    // `[deleted]` is included. This ensures we keep the entire RC(3) + RS(2) set.
    let (spool_parts, total_written) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .sources(Sources::Both)
        .date_range(Some(start), Some(end))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users() // crucial: include "[deleted]" in the spooled RC set
        .extract_spool_monthly(&spool_dir)
        .unwrap();

    assert!(
        spool_parts.len() >= 2,
        "expected at least RC_2006-01 and RS_2006-01 spooled; got {}",
        spool_parts.len()
    );
    assert_eq!(
        total_written, 5,
        "expected exactly 5 records written to spool (RC=3, RS=2); got {}",
        total_written
    );

    // ------------- Step 2: Collect parent IDs from spool -------------------
    let ids: ParentIds = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .progress(false)
        .collect_parent_ids_from_jsonls(spool_parts.clone())
        .unwrap();

    // ------------- Step 3: Resolve parent contents into cache --------------
    let parent_start = start.prev().unwrap_or(start);
    let parent_end = end.next().unwrap_or(end);

    let parents: ParentMaps = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .file_concurrency(4)
        .progress(false)
        .date_range(Some(parent_start), Some(parent_end))
        .resolve_parent_maps(&ids, &parents_cache_dir, /*resume=*/ true)
        .unwrap();

    // ------------- Step 4: Attach parents to spooled JSONL -----------------
    let attached_paths = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .progress(false)
        .attach_parents_jsonls_parallel(
            spool_parts.clone(),
            &spool_with_parents_dir,
            &parents,
            /*resume=*/ false,
        )
        .unwrap();

    assert!(
        !attached_paths.is_empty(),
        "attach_parents_jsonls_parallel produced no outputs"
    );

    // ------------- Step 5: Count total lines across attached files ---------
    let total_lines = count_jsonl_lines(&attached_paths);
    // Expectation for the tiny dataset:
    //   RC_2006-01.jsonl -> 3 comments (including `[deleted]`)
    //   RS_2006-01.jsonl -> 2 submissions
    // Total = 5
    assert_eq!(
        total_lines, 5,
        "expected 5 total lines after parent attachment"
    );
}

#[test]
fn parent_attach_stats_unresolved_rate_boundaries() {
    assert_eq!(ParentAttachStats::default().total(), 0);
    assert_eq!(ParentAttachStats::default().unresolved_rate(), 0.0);

    let all_resolved = ParentAttachStats {
        resolved: 20,
        unresolved: 0,
    };
    assert_eq!(all_resolved.total(), 20);
    assert_eq!(all_resolved.unresolved_rate(), 0.0);

    let threshold = ParentAttachStats {
        resolved: 95,
        unresolved: 5,
    };
    assert_eq!(threshold.total(), 100);
    assert!((threshold.unresolved_rate() - 0.05).abs() < f64::EPSILON);

    let high = ParentAttachStats {
        resolved: 1,
        unresolved: 1,
    };
    assert_eq!(high.total(), 2);
    assert_eq!(high.unresolved_rate(), 0.5);
}

#[test]
fn unresolved_parent_is_absent_and_counted() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        r#"{"id":"c_missing","body":"child","parent_id":"t1_missing","created_utc":1136073600}"#,
    )
    .unwrap();

    let parents = ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
    };

    let (attached_paths, stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![input],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap();

    assert_eq!(
        stats,
        ParentAttachStats {
            resolved: 0,
            unresolved: 1,
        }
    );
    let line = fs::read_to_string(&attached_paths[0]).unwrap();
    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert!(
        v.get("parent").is_none(),
        "unresolved parent must be absent"
    );
}

#[test]
fn attach_parents_resume_rebuilds_corrupt_published_output() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        "{\"id\":\"c1\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n",
    )
    .unwrap();

    let out_dir = tmp.path().join("attached");
    fs::create_dir_all(&out_dir).unwrap();
    fs::write(out_dir.join("part_RC_2006-01.jsonl"), "{\"id\":").unwrap();

    let mut comments = HashMap::new();
    comments.insert("p1".to_string(), "parent body".to_string());
    let parents = ParentMaps {
        comments,
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
    };

    let (attached_paths, stats) = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(vec![input], &out_dir, &parents, true)
        .unwrap();

    assert_eq!(stats.resolved, 1);
    assert_eq!(stats.unresolved, 0);
    let line = fs::read_to_string(&attached_paths[0]).unwrap();
    let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        v.pointer("/parent/body").and_then(|v| v.as_str()),
        Some("parent body")
    );
}

#[test]
fn attach_resume_rebuilds_when_resolution_window_expands() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let spool_dir = work_dir.join("spool");
    let attached_dir = work_dir.join("attached");
    let cache_dir = work_dir.join("parents_cache");
    let lib_tmp = work_dir.join("lib_tmp");

    let parent_month = YearMonth::new(2005, 12);
    let child_month = YearMonth::new(2006, 1);

    write_zst_lines(
        &base.join("submissions").join("RS_2005-12.zst"),
        &[serde_json::json!({
            "author": "parent_author",
            "created_utc": 1133395200,
            "domain": "example.com",
            "id": "s_dec",
            "score": 10,
            "selftext": "parent selftext",
            "title": "December parent",
            "subreddit": "programming"
        })
        .to_string()],
    );
    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[serde_json::json!({
            "author": "child_author",
            "body": "child body",
            "created_utc": 1136073600,
            "id": "c_child",
            "link_id": "t3_s_dec",
            "parent_id": "t3_s_dec",
            "score": 1,
            "subreddit": "programming"
        })
        .to_string()],
    );

    let (spool_parts, total_written) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .sources(Sources::Comments)
        .date_range(Some(child_month), Some(child_month))
        .progress(false)
        .scan()
        .subreddit("programming")
        .extract_spool_monthly(&spool_dir)
        .unwrap();
    assert_eq!(total_written, 1);

    let ids = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .progress(false)
        .collect_parent_ids_from_jsonls(spool_parts.clone())
        .unwrap();

    let narrow_parents = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(child_month), Some(child_month))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();

    let (first_paths, first_stats) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(child_month), Some(child_month))
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            spool_parts.clone(),
            &attached_dir,
            &narrow_parents,
            false,
        )
        .unwrap();
    assert_eq!(first_stats.resolved, 0);
    assert_eq!(first_stats.unresolved, 1);
    let first_line = fs::read_to_string(&first_paths[0]).unwrap();
    let first_value: serde_json::Value = serde_json::from_str(first_line.trim()).unwrap();
    assert!(first_value.get("parent").is_none());

    let wider_parents = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(parent_month), Some(child_month))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();

    let (second_paths, second_stats) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .date_range(Some(parent_month), Some(child_month))
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(spool_parts, &attached_dir, &wider_parents, true)
        .unwrap();

    assert_eq!(second_stats.resolved, 1);
    assert_eq!(second_stats.unresolved, 0);
    assert_eq!(second_paths[0], first_paths[0]);
    let second_line = fs::read_to_string(&second_paths[0]).unwrap();
    let second_value: serde_json::Value = serde_json::from_str(second_line.trim()).unwrap();
    assert_eq!(
        second_value
            .pointer("/parent/title")
            .and_then(|v| v.as_str()),
        Some("December parent")
    );
}

#[test]
fn resolve_resume_rebuilds_when_parent_ids_change() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent one"), ("p2", "parent two")]);
    let spool1 = tmp.path().join("spool1.jsonl");
    let spool2 = tmp.path().join("spool2.jsonl");
    write_parent_ref_spool(&spool1, "t1_p1");
    write_parent_ref_spool(&spool2, "t1_p2");

    let ids1 = collect_parent_ids_for_test(&base, &work_dir, &spool1);
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids1, &cache_dir, true)
        .unwrap();
    let first = read_comment_cache(&cache_dir, ym);
    assert_eq!(first.get("p1").map(String::as_str), Some("parent one"));
    assert!(!first.contains_key("p2"));

    let ids2 = collect_parent_ids_for_test(&base, &work_dir, &spool2);
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids2, &cache_dir, true)
        .unwrap();
    let second = read_comment_cache(&cache_dir, ym);
    assert_eq!(second.get("p2").map(String::as_str), Some("parent two"));
    assert!(
        !second.contains_key("p1"),
        "resume reused a parent-cache shard from the prior ParentIds"
    );
}

#[test]
fn resolve_resume_rebuilds_when_corpus_path_changes() {
    let tmp = tempfile::tempdir().unwrap();
    let base1 = tmp.path().join("corpus1");
    let base2 = tmp.path().join("corpus2");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base1, ym, &[("p1", "old corpus body")]);
    write_comment_parent_corpus(&base2, ym, &[("p1", "new corpus body")]);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base1, &work_dir, &spool);

    RedditETL::new()
        .base_dir(&base1)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    assert_eq!(
        read_comment_cache(&cache_dir, ym)
            .get("p1")
            .map(String::as_str),
        Some("old corpus body")
    );

    RedditETL::new()
        .base_dir(&base2)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
        .unwrap();
    assert_eq!(
        read_comment_cache(&cache_dir, ym)
            .get("p1")
            .map(String::as_str),
        Some("new corpus body"),
        "resume reused a parent-cache shard from a different corpus path"
    );
}

#[test]
fn resolve_parent_maps_errors_on_zero_planned_files() {
    let tmp = tempfile::tempdir().unwrap();
    let ym = YearMonth::new(2006, 1);
    let err = match RedditETL::new()
        .base_dir(tmp.path().join("empty_corpus"))
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ParentIds::new(), &tmp.path().join("cache"), true)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("planned zero corpus files") || msg.contains("no input files found"),
        "unexpected error: {msg}"
    );
}

#[test]
fn resolve_parent_maps_propagates_cache_write_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent body")]);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    fs::create_dir_all(cache_dir.join("comments").join("RC_2006-01.json")).unwrap();
    let err = match RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, false)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("write parent shard"),
        "unexpected error: {msg}"
    );
}

#[test]
fn collect_parent_ids_errors_on_malformed_spool_json_with_location() {
    let tmp = tempfile::tempdir().unwrap();
    let spool = tmp.path().join("spool.jsonl");
    fs::write(
        &spool,
        "{\"id\":\"child\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"link_id\":\"t3_s1\",\"created_utc\":1136073600}\n{bad json}\n",
    )
    .unwrap();

    let err = match RedditETL::new()
        .work_dir(tmp.path().join("work"))
        .progress(false)
        .collect_parent_ids_from_jsonls(vec![spool.clone()])
    {
        Ok(_) => panic!("collect_parent_ids_from_jsonls unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("malformed JSON"), "unexpected error: {msg}");
    assert!(
        msg.contains(&spool.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("line 2"), "unexpected error: {msg}");
}

#[test]
fn resolve_parent_maps_errors_on_malformed_corpus_json_with_location() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);
    let corpus_path = base.join("comments").join("RC_2006-01.zst");

    write_zst_lines(
        &corpus_path,
        &[
            serde_json::json!({
                "author": "parent_author",
                "body": "parent body",
                "created_utc": 1136073600,
                "id": "p1",
                "score": 1,
                "subreddit": "programming"
            })
            .to_string(),
            "{bad json}".to_string(),
        ],
    );
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    let err = match RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(msg.contains("malformed JSON"), "unexpected error: {msg}");
    assert!(
        msg.contains(&corpus_path.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("line 2"), "unexpected error: {msg}");
    assert!(
        !cache_dir.join("comments").join("RC_2006-01.json").exists(),
        "malformed resolver input must not publish a parent shard"
    );
}

#[test]
fn resolve_parent_maps_errors_on_truncated_corpus_and_does_not_publish_cache() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);
    let corpus_path = base.join("comments").join("RC_2006-01.zst");

    make_truncated_zst(&corpus_path, 2000, 16);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_rec000000");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    let err = match RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, true)
    {
        Ok(_) => panic!("resolve_parent_maps unexpectedly succeeded"),
        Err(err) => err,
    };
    let msg = format!("{err:#}");
    assert!(
        msg.contains("incomplete zstd decode") || msg.contains("zstd"),
        "unexpected error: {msg}"
    );
    assert!(
        msg.contains(&corpus_path.display().to_string()),
        "unexpected error: {msg}"
    );

    let shard = cache_dir.join("comments").join("RC_2006-01.json");
    let sidecar = cache_dir
        .join("comments")
        .join("RC_2006-01.json.parents-resolve.json");
    assert!(
        !shard.exists(),
        "truncated resolver input must not publish a parent shard"
    );
    assert!(
        !sidecar.exists(),
        "truncated resolver input must not mark a parent shard complete"
    );
}

#[test]
fn attach_parents_errors_on_malformed_spool_json_with_location() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("part_RC_2006-01.jsonl");
    fs::write(
        &input,
        "{\"id\":\"ok\",\"body\":\"child\",\"parent_id\":\"t1_p1\",\"created_utc\":1136073600}\n{bad json}\n",
    )
    .unwrap();
    let parents = ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
    };

    let err = RedditETL::new()
        .progress(false)
        .attach_parents_jsonls_parallel_with_stats(
            vec![input.clone()],
            &tmp.path().join("attached"),
            &parents,
            false,
        )
        .unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains(&input.display().to_string()),
        "unexpected error: {msg}"
    );
    assert!(msg.contains("line 2"), "unexpected error: {msg}");
}

#[test]
fn parent_id_collections_sharing_work_dir_keep_held_ids_alive() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(
        &base,
        ym,
        &[("p1", "first parent"), ("p2", "second parent")],
    );
    let spool1 = tmp.path().join("spool1.jsonl");
    let spool2 = tmp.path().join("spool2.jsonl");
    write_parent_ref_spool(&spool1, "t1_p1");
    write_parent_ref_spool(&spool2, "t1_p2");

    let ids1 = collect_parent_ids_for_test(&base, &work_dir, &spool1);
    let ids2 = collect_parent_ids_for_test(&base, &work_dir, &spool2);

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids1, &tmp.path().join("cache1"), false)
        .unwrap();
    let first = read_comment_cache(&tmp.path().join("cache1"), ym);
    assert_eq!(first.get("p1").map(String::as_str), Some("first parent"));
    assert!(!first.contains_key("p2"));

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids2, &tmp.path().join("cache2"), false)
        .unwrap();
    let second = read_comment_cache(&tmp.path().join("cache2"), ym);
    assert_eq!(second.get("p2").map(String::as_str), Some("second parent"));
    assert!(!second.contains_key("p1"));
}

/// Regression: before the atomic-write fix, the per-shard cache writer in
/// `resolve_parent_maps` staged via `out.with_extension("json.tmp")` — a
/// fixed sibling path with no PID/nonce — so a crashed run left a stale
/// `RC_<ym>.json.tmp` next to the published shards that nothing would
/// sweep. The fix routes shard writes through `<cache>/_staging/<basename>.retl-<pid>-<nonce>.inprogress`
/// and adds a sweep-on-entry to `resolve_parent_maps`. This test simulates a
/// crash leftover by planting a `*.inprogress` file under `_staging/` whose
/// owner PID is no longer running, then asserts `resolve_parent_maps`
/// sweeps it on entry and still writes the live shard.
#[test]
fn resolve_parent_maps_sweeps_crash_leftover_under_staging() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent body")]);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    // Plant a `_staging/` leftover that looks like the previous run crashed
    // mid-flush. The format matches `unique_inprogress_path`:
    //     <basename>.retl-<pid>-<counter>-<nanos>.inprogress
    // Use a PID that is extremely unlikely to be live (u32::MAX) so
    // `process_is_running` returns false and the sweep removes it.
    let comments_out = cache_dir.join("comments");
    let staging = comments_out.join("_staging");
    fs::create_dir_all(&staging).unwrap();
    let leftover_name = format!("RC_{ym}.json.retl-{}-0-0.inprogress", u32::MAX);
    let leftover = staging.join(&leftover_name);
    fs::write(&leftover, b"partial flush from a dead run").unwrap();
    assert!(leftover.exists(), "test failed to plant the leftover");

    // Now run resolver. Sweep-on-entry should remove the planted leftover,
    // and the shard write should publish a fresh `RC_<ym>.json` via a
    // distinct PID/nonce-suffixed staged file.
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, false)
        .unwrap();

    assert!(
        !leftover.exists(),
        "sweep-on-entry should have removed crash leftover {}",
        leftover.display()
    );

    let cache = read_comment_cache(&cache_dir, ym);
    assert_eq!(
        cache.get("p1").map(String::as_str),
        Some("parent body"),
        "subsequent run should publish the live shard after sweep"
    );

    // No stray `*.tmp` sibling at the legacy path either — that path is gone.
    let legacy_sibling = comments_out.join(format!("RC_{ym}.json.tmp"));
    assert!(
        !legacy_sibling.exists(),
        "legacy sibling temp `{}` should not be produced by the shard writer",
        legacy_sibling.display()
    );

    // After a successful publish, the staging dir contains no leftover
    // belonging to this run either.
    let live_leftovers: Vec<std::path::PathBuf> = fs::read_dir(&staging)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.ends_with(".inprogress"))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        live_leftovers.is_empty(),
        "no staged leftovers should remain after a successful resolve, found {:?}",
        live_leftovers
    );
}

fn write_comment_parent_corpus(base: &Path, ym: YearMonth, parents: &[(&str, &str)]) {
    let lines: Vec<String> = parents
        .iter()
        .map(|(id, body)| {
            serde_json::json!({
                "author": "parent_author",
                "body": body,
                "created_utc": 1136073600,
                "id": id,
                "score": 1,
                "subreddit": "programming"
            })
            .to_string()
        })
        .collect();
    write_zst_lines(
        &base.join("comments").join(format!("RC_{}.zst", ym)),
        &lines,
    );
}

fn write_parent_ref_spool(path: &Path, parent_id: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(
        path,
        format!(
            "{{\"id\":\"child\",\"body\":\"child\",\"parent_id\":\"{}\",\"link_id\":\"t3_s\",\"created_utc\":1136073600}}\n",
            parent_id
        ),
    )
    .unwrap();
}

fn collect_parent_ids_for_test(base: &Path, work_dir: &Path, spool: &Path) -> ParentIds {
    RedditETL::new()
        .base_dir(base)
        .work_dir(work_dir)
        .progress(false)
        .collect_parent_ids_from_jsonls(vec![spool.to_path_buf()])
        .unwrap()
}

fn read_comment_cache(cache_dir: &Path, ym: YearMonth) -> HashMap<String, String> {
    let path = cache_dir.join("comments").join(format!("RC_{}.json", ym));
    let file = File::open(path).unwrap();
    serde_json::from_reader(BufReader::new(file)).unwrap()
}

fn count_jsonl_lines(paths: &[std::path::PathBuf]) -> usize {
    let mut total = 0usize;
    for p in paths {
        let f = File::open(p).unwrap();
        let r = BufReader::new(f);
        for line in r.lines() {
            let s = line.unwrap();
            if !s.trim().is_empty() {
                total += 1;
            }
        }
    }
    total
}
