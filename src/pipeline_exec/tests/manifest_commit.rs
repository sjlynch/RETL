use super::fixtures::*;
use super::*;

/// Count published `<prefix>*.jsonl` files in `dir` (the partitioned and spool
/// outputs both land as plain JSONL named with a stable prefix).
fn count_published_jsonl(dir: &Path, prefix: &str) -> usize {
    if !dir.exists() {
        return 0;
    }
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(prefix) && n.ends_with(".jsonl"))
        })
        .count()
}

#[test]
fn partitioned_export_stops_publishing_after_a_manifest_commit_failure() {
    // 12 months, two workers in flight, eight Rayon threads: without the abort
    // flag the workers blocked on the file-concurrency semaphore would each
    // run to completion and publish their partition even after the run is
    // already doomed by a manifest save failure.
    let base = make_n_month_comment_corpus(12);
    let out_dir = base.join("partitioned_abort");
    let file_concurrency = 2usize;

    let err = {
        // Let only the initial pruned-manifest pre-save land, then fail every
        // per-month commit. The first failed commit poisons the run.
        let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 999);
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 12)))
            .progress(false)
            .resume(true)
            .parallelism(8)
            .file_concurrency(file_concurrency)
            .scan()
            .subreddit("programming")
            .export_partitioned(&out_dir, ExportFormat::Jsonl)
            .expect_err("a manifest commit failure must fail the partitioned export")
    };
    assert!(
        err.to_string()
            .contains("durably update partitioned export progress manifest"),
        "unexpected error: {err:#}"
    );

    let published = count_published_jsonl(&out_dir.join("comments"), "RC_");
    let recorded = manifest_json(&out_dir)["months"]
        .as_object()
        .unwrap()
        .len();
    // The on-disk output set must never run past what `_progress.json`
    // durably records by more than the bounded set of workers that were
    // already mid-publish when the first commit failed.
    assert!(
        published <= recorded + file_concurrency,
        "published {published} partitions but `_progress.json` records only \
         {recorded}; at most {file_concurrency} workers can be mid-publish — \
         the run kept publishing after the manifest failure",
    );
    assert!(
        published < 12,
        "abort flag must stop the run well short of publishing every month \
         (published {published})",
    );
}

#[test]
fn spool_stops_publishing_after_a_manifest_commit_failure() {
    let base = make_n_month_comment_corpus(12);
    let out_dir = base.join("spool_abort");
    let file_concurrency = 2usize;

    let err = {
        let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 999);
        RedditETL::new()
            .base_dir(&base)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 12)))
            .progress(false)
            .resume(true)
            .parallelism(8)
            .file_concurrency(file_concurrency)
            .scan()
            .subreddit("programming")
            .extract_spool_monthly(&out_dir)
            .expect_err("a manifest commit failure must fail the spool run")
    };
    assert!(
        err.to_string()
            .contains("durably update resume progress manifest"),
        "unexpected error: {err:#}"
    );

    let published = count_published_jsonl(&out_dir, "part_RC_");
    let recorded = manifest_json(&out_dir)["months"]
        .as_object()
        .unwrap()
        .len();
    assert!(
        published <= recorded + file_concurrency,
        "published {published} spool parts but `_progress.json` records only \
         {recorded}; at most {file_concurrency} workers can be mid-publish — \
         the run kept publishing after the manifest failure",
    );
    assert!(
        published < 12,
        "abort flag must stop the run well short of publishing every month \
         (published {published})",
    );
}

#[test]
fn spool_resume_commit_failure_after_publish_returns_error_and_next_run_succeeds() {
    let base = make_one_comment_corpus();
    let out_dir = base.join("spool_manifest_failure");

    let err = {
        // The first save is the initial pruned manifest; fail the per-month
        // commit that happens only after the spool part has been published.
        let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 1);
        one_month_scan(&base)
            .extract_spool_monthly(&out_dir)
            .expect_err("manifest commit failure must fail the run")
    };
    assert!(
        err.to_string()
            .contains("durably update resume progress manifest"),
        "unexpected error: {err:#}"
    );

    let part = out_dir.join("part_RC_2006-01.jsonl");
    assert!(
        part.exists(),
        "data publish should remain atomic and durable"
    );
    let manifest = manifest_json(&out_dir);
    assert!(
        manifest["months"].as_object().unwrap().is_empty(),
        "failed commit must not claim the month is resumable: {manifest}"
    );

    let (_parts, written) = one_month_scan(&base)
        .extract_spool_monthly(&out_dir)
        .expect("subsequent run without manifest failure should succeed");
    assert_eq!(written, 1);
    let manifest = manifest_json(&out_dir);
    assert!(manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-01"));
}

#[test]
fn partitioned_resume_commit_failure_after_publish_returns_error() {
    let base = make_one_comment_corpus();
    let out_dir = base.join("partitioned_manifest_failure");

    let err = {
        let _guard = fail_saves_after_attempts_for_tests(&out_dir, 1, 1);
        one_month_scan(&base)
            .export_partitioned(&out_dir, ExportFormat::Jsonl)
            .expect_err("manifest commit failure must fail the partitioned export")
    };
    assert!(
        err.to_string()
            .contains("durably update partitioned export progress manifest"),
        "unexpected error: {err:#}"
    );

    let part = out_dir.join("comments").join("RC_2006-01.jsonl");
    assert!(
        part.exists(),
        "partition should remain published after commit failure"
    );
    let manifest = manifest_json(&out_dir);
    assert!(
        manifest["months"].as_object().unwrap().is_empty(),
        "failed commit must not mark the partition resumable: {manifest}"
    );

    one_month_scan(&base)
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("subsequent partitioned export should succeed");
    let manifest = manifest_json(&out_dir);
    assert!(manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-01"));
}

#[test]
fn extract_resume_commit_failure_after_temp_part_publish_returns_error_and_next_run_stitches() {
    let base = make_one_comment_corpus();
    let work_dir = base.join("work_manifest_failure");
    let out = base.join("out.jsonl");

    let plan = RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .build()
        .unwrap();
    let files = plan_pipeline_files(&plan.etl, Some(&plan.query)).unwrap();
    let fingerprint =
        build_resume_fingerprint(&plan.etl, &plan.query, "extract", plan.limit, &files).unwrap();
    let tmp_dir = extract_scratch_dir(
        &work_dir,
        "extract_jsonl_q_tmp",
        &out,
        true,
        Some(&fingerprint),
    )
    .unwrap();

    let err = {
        let _guard = fail_saves_after_attempts_for_tests(&tmp_dir, 1, 1);
        RedditETL::new()
            .base_dir(&base)
            .work_dir(&work_dir)
            .sources(Sources::Comments)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .resume(true)
            .scan()
            .subreddit("programming")
            .extract_to_jsonl(&out)
            .expect_err("manifest commit failure must fail extract_to_jsonl")
    };
    assert!(
        err.to_string()
            .contains("durably update extract progress manifest"),
        "unexpected error: {err:#}"
    );

    let tmp_part = tmp_dir.join(".part_RC_2006-01.jsonl");
    assert!(
        tmp_part.exists(),
        "resume temp part should remain published"
    );
    assert!(
        !out.exists(),
        "final extract output should not be stitched after a failed checkpoint"
    );
    let manifest = manifest_json(&tmp_dir);
    assert!(
        manifest["months"].as_object().unwrap().is_empty(),
        "failed commit must not mark the temp part resumable: {manifest}"
    );

    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
        .extract_to_jsonl(&out)
        .expect("subsequent extract should commit the temp part and stitch output");
    assert!(out.exists());
    let manifest = manifest_json(&tmp_dir);
    assert!(manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-01"));
}
