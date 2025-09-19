#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ParentIds, ParentMaps, RedditETL, Sources, YearMonth};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};

/// End-to-end parents pipeline over a tiny synthetic corpus:
/// 1) Build a small test corpus under a temp dir (RC/RS 2006-01 for r/programming).
/// 2) Spool *both* submissions and comments for r/programming **without** restricting authors,
///    but with `.allow_pseudo_users()` so `[deleted]` is included.
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
        .allow_pseudo_users() // crucial: include "[deleted]" in the spooled RC set
        .extract_spool_monthly(&spool_dir, /*resume=*/false)
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
        .resolve_parent_maps(&ids, &parents_cache_dir, /*resume=*/true)
        .unwrap();

    // ------------- Step 4: Attach parents to spooled JSONL -----------------
    let attached_paths = RedditETL::new()
        .base_dir(&base)
        .work_dir(&lib_tmp)
        .progress(false)
        .attach_parents_jsonls_parallel(spool_parts.clone(), &spool_with_parents_dir, &parents, /*resume=*/false)
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
    assert_eq!(total_lines, 5, "expected 5 total lines after parent attachment");
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
