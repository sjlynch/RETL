use crate::common::make_corpus_basic;
use crate::common::parents::*;
use retl::{ParentIds, ParentMaps, RedditETL, Sources, YearMonth};

#[test]
fn spool_resolve_attach_parents_end_to_end() {
    // Create miniature corpus
    let base = make_corpus_basic();

    // Work dirs (under the same temp base)
    let (_work_dir, spool_dir, spool_with_parents_dir, parents_cache_dir, lib_tmp) =
        make_parent_pipeline_dirs(&base);

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
fn direct_parent_ids_construction_matches_spool_collection_resolution() {
    let base = make_corpus_basic();
    let work_dir = base.join("work_direct_ids");
    let spool_dir = work_dir.join("spool");
    let ym = YearMonth::new(2006, 1);

    let (spool_parts, _n) = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Both)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .extract_spool_monthly(&spool_dir)
        .unwrap();
    let spool_ids = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .progress(false)
        .collect_parent_ids_from_jsonls(spool_parts)
        .unwrap();

    let mut direct_ids = ParentIds::new();
    assert_eq!(
        direct_ids.extend_prefixed(["t1_c1", " t3_s1 ", "t2_bad", "t1_"]),
        2
    );
    assert!(!direct_ids.insert_prefixed("c1"));
    assert!(!direct_ids.insert_t1("t3_s1"));
    assert!(!direct_ids.insert_t3("t1_c1"));
    assert!(
        !direct_ids.insert_t1("c1"),
        "duplicate bare ID should be ignored"
    );

    let spool_cache = work_dir.join("cache_spool");
    let direct_cache = work_dir.join("cache_direct");
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&spool_ids, &spool_cache, false)
        .unwrap();
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&direct_ids, &direct_cache, false)
        .unwrap();

    assert_eq!(
        read_comment_cache(&spool_cache, ym),
        read_comment_cache(&direct_cache, ym)
    );
    assert_eq!(
        read_submission_cache(&spool_cache, ym),
        read_submission_cache(&direct_cache, ym)
    );
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
