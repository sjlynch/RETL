use super::fixtures::make_basic_corpus_inline;
use super::*;

// -----------------------------------------------------------------------
// Provenance: the `.retl-manifest.json` sidecar emitted by `retl scan`
// (and `count --mode month` file outputs) must record every query filter
// that was actually wired into the run. A manifest that omits an active
// filter makes a filtered run look unfiltered — see the README's promise
// that the manifest records the normalized query/options.
// -----------------------------------------------------------------------

fn scan_args(argv: &[&str]) -> crate::bin_args::ScanArgs {
    let mut v = vec!["retl", "scan"];
    v.extend_from_slice(argv);
    match Cli::try_parse_from(&v).expect("clap parse").command {
        Command::Scan(a) => a,
        other => panic!("expected scan command, got {other:?}"),
    }
}

#[test]
fn scan_manifest_records_all_active_query_filters() {
    let base = make_basic_corpus_inline();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out = cwd.path().join("usernames.txt");

    let args = scan_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--work-dir",
        work_dir.to_str().unwrap(),
        "--start",
        "2006-01",
        "--end",
        "2006-01",
        "--keyword-all",
        "rust",
        "--exclude-keyword",
        "spam",
        "--text-regex",
        "(?i)love",
        "--no-url",
        "--after",
        "2006-01-01",
        "--before",
        "2007-01-01",
        "--id",
        "t1_c1",
        "--no-progress",
        "--out",
        out.to_str().unwrap(),
    ]);
    run_scan(args).expect("scan must succeed");

    let manifest_path = retl::manifest_path_for_file(&out);
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read scan manifest"))
            .expect("parse scan manifest");
    let query = &manifest["query"];

    // Each filter formerly omitted from `cli_query_value` must now appear.
    assert_eq!(
        query["keywords_all"],
        serde_json::json!(["rust"]),
        "--keyword-all missing from manifest: {query}"
    );
    assert_eq!(
        query["keywords_exclude"],
        serde_json::json!(["spam"]),
        "--exclude-keyword missing from manifest: {query}"
    );
    assert_eq!(
        query["text_regex"],
        serde_json::json!("(?i)love"),
        "--text-regex missing from manifest: {query}"
    );
    assert_eq!(
        query["no_url"],
        serde_json::json!(true),
        "--no-url missing from manifest: {query}"
    );
    assert!(
        query["after"].is_number(),
        "--after missing from manifest: {query}"
    );
    assert!(
        query["before"].is_number(),
        "--before missing from manifest: {query}"
    );
    assert_eq!(
        query["ids"],
        serde_json::json!(["t1_c1"]),
        "--id missing from manifest: {query}"
    );
}

#[test]
fn scan_manifest_omits_inactive_query_filters_as_empty() {
    // With no filters set, the new keys are present but empty/null — the
    // manifest stays a faithful record rather than implying a filter ran.
    let base = make_basic_corpus_inline();
    let cwd = tempfile::tempdir().unwrap();
    let work_dir = cwd.path().join("work");
    let out = cwd.path().join("usernames.txt");

    let args = scan_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--work-dir",
        work_dir.to_str().unwrap(),
        "--start",
        "2006-01",
        "--end",
        "2006-01",
        "--no-progress",
        "--out",
        out.to_str().unwrap(),
    ]);
    run_scan(args).expect("scan must succeed");

    let manifest_path = retl::manifest_path_for_file(&out);
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read scan manifest"))
            .expect("parse scan manifest");
    let query = &manifest["query"];

    assert_eq!(query["keywords_all"], serde_json::json!([]));
    assert_eq!(query["keywords_exclude"], serde_json::json!([]));
    assert_eq!(query["ids"], serde_json::json!([]));
    assert_eq!(query["ids_files"], serde_json::json!([]));
    assert!(query["text_regex"].is_null());
    assert!(query["no_url"].is_null());
    assert!(query["after"].is_null());
    assert!(query["before"].is_null());
}
