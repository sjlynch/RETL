use super::fixtures::*;
use super::*;

#[test]
fn resume_fingerprint_includes_new_text_and_url_filters() {
    let base = make_one_comment_corpus();
    let base_fp = spool_fingerprint_for(one_month_scan(&base));

    let keyword_all_fp = spool_fingerprint_for(one_month_scan(&base).keywords_all(["hello"]));
    let exclude_keyword_fp =
        spool_fingerprint_for(one_month_scan(&base).exclude_keywords(["spam"]));
    let text_regex_fp = spool_fingerprint_for(one_month_scan(&base).text_regex("hello"));
    let no_url_fp = spool_fingerprint_for(one_month_scan(&base).no_url());

    assert_ne!(base_fp, keyword_all_fp);
    assert_ne!(base_fp, exclude_keyword_fp);
    assert_ne!(base_fp, text_regex_fp);
    assert_ne!(base_fp, no_url_fp);
}

#[test]
fn resume_fingerprint_changes_when_timestamp_bounds_change() {
    let base = make_one_comment_corpus();
    let plan_a = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .scan()
        .created_utc_gte(1_136_073_600)
        .created_utc_lt(1_136_160_000)
        .build()
        .unwrap();
    let plan_b = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .progress(false)
        .scan()
        .created_utc_gte(1_136_073_601)
        .created_utc_lt(1_136_160_000)
        .build()
        .unwrap();

    let files_a = plan_pipeline_files(&plan_a.etl, Some(&plan_a.query)).unwrap();
    let files_b = plan_pipeline_files(&plan_b.etl, Some(&plan_b.query)).unwrap();
    let fp_a = build_resume_fingerprint(
        &plan_a.etl,
        &plan_a.query,
        "extract",
        plan_a.limit,
        &files_a,
    )
    .expect("fingerprint a");
    let fp_b = build_resume_fingerprint(
        &plan_b.etl,
        &plan_b.query,
        "extract",
        plan_b.limit,
        &files_b,
    )
    .expect("fingerprint b");
    assert_ne!(fp_a, fp_b);
}

#[test]
fn resume_fingerprint_changes_when_record_id_filter_changes() {
    let base = make_one_comment_corpus();
    let plan_c1 = one_month_scan(&base).ids(["c1"]).build().unwrap();
    let plan_c2 = one_month_scan(&base).ids(["c2"]).build().unwrap();

    let files_c1 = plan_pipeline_files(&plan_c1.etl, Some(&plan_c1.query)).unwrap();
    let files_c2 = plan_pipeline_files(&plan_c2.etl, Some(&plan_c2.query)).unwrap();
    let fp_c1 = build_resume_fingerprint(
        &plan_c1.etl,
        &plan_c1.query,
        "spool",
        plan_c1.limit,
        &files_c1,
    )
    .unwrap();
    let fp_c2 = build_resume_fingerprint(
        &plan_c2.etl,
        &plan_c2.query,
        "spool",
        plan_c2.limit,
        &files_c2,
    )
    .unwrap();

    assert_ne!(fp_c1, fp_c2);
}

#[test]
fn resume_fingerprint_includes_record_limit() {
    let base = make_one_comment_corpus();
    let no_limit_plan = one_month_scan(&base).build().unwrap();
    let limit_one_plan = one_month_scan(&base).limit(1).build().unwrap();
    let limit_two_plan = one_month_scan(&base).limit(2).build().unwrap();

    let no_limit_files =
        plan_pipeline_files(&no_limit_plan.etl, Some(&no_limit_plan.query)).unwrap();
    let limit_one_files =
        plan_pipeline_files(&limit_one_plan.etl, Some(&limit_one_plan.query)).unwrap();
    let limit_two_files =
        plan_pipeline_files(&limit_two_plan.etl, Some(&limit_two_plan.query)).unwrap();

    let no_limit = build_resume_fingerprint(
        &no_limit_plan.etl,
        &no_limit_plan.query,
        "extract",
        no_limit_plan.limit,
        &no_limit_files,
    )
    .unwrap();
    let limit_one = build_resume_fingerprint(
        &limit_one_plan.etl,
        &limit_one_plan.query,
        "extract",
        limit_one_plan.limit,
        &limit_one_files,
    )
    .unwrap();
    let limit_two = build_resume_fingerprint(
        &limit_two_plan.etl,
        &limit_two_plan.query,
        "extract",
        limit_two_plan.limit,
        &limit_two_files,
    )
    .unwrap();

    assert_ne!(no_limit, limit_one);
    assert_ne!(limit_one, limit_two);
}
