use retl::{QueryBuildError, RedditETL, ScanPlan};

#[test]
fn external_code_can_name_scan_plan_and_query_build_error() {
    let plan: ScanPlan = RedditETL::new().scan();
    let result: Result<ScanPlan, QueryBuildError> = plan.author_regex("[").build();

    let err = match result {
        Ok(_) => panic!("invalid regex should fail ScanPlan::build"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("author_regex"));
}
