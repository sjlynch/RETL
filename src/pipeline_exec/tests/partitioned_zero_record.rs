//! Regression tests for partitioned export over an all-filtered-out
//! (zero-record) month: it must not leave a stale partition file, and its
//! `partition_files` count must agree between a fresh run and a resumed run.

use super::fixtures::*;
use super::*;

/// Corpus with two months: `RC_2006-01` carries a `programming` comment, while
/// `RC_2006-02` carries only an `other`-subreddit comment that the
/// `programming` filter drops — so that month yields zero records.
fn make_zero_record_month_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[comment_line("jan_real", "alice", 1_136_073_600)],
    );
    let other = json!({
        "id": "feb_other",
        "author": "bob",
        "subreddit": "other",
        "created_utc": 1_138_752_000_i64,
        "score": 1,
        "body": "hello",
    })
    .to_string();
    write_zst_lines(&base.join("comments").join("RC_2006-02.zst"), &[other]);
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

/// Read the `partition_files` count from the run manifest sidecar.
fn run_manifest_partition_files(out_dir: &Path) -> u64 {
    let manifest: Value =
        serde_json::from_slice(&fs::read(out_dir.join("_retl_manifest.json")).unwrap()).unwrap();
    manifest["counts"]["partition_files"]
        .as_u64()
        .expect("partition_files count")
}

fn zero_record_scan(base: &Path) -> ScanPlan {
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
}

#[test]
fn partitioned_zero_record_month_count_is_stable_fresh_vs_resumed() {
    let base = make_zero_record_month_corpus();
    let out_dir = base.join("partitioned_zero_record");
    let zero_path = out_dir.join("comments").join("RC_2006-02.jsonl");
    let real_path = out_dir.join("comments").join("RC_2006-01.jsonl");

    // Fresh run: only the one non-empty month produces a partition file.
    zero_record_scan(&base)
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("fresh partitioned export should succeed");
    let fresh_count = run_manifest_partition_files(&out_dir);
    assert_eq!(
        fresh_count, 1,
        "only the non-empty month should count toward partition_files",
    );
    assert!(real_path.exists(), "the non-empty month must be published");
    assert!(
        !zero_path.exists(),
        "a zero-record month must not leave a partition file at the published path",
    );

    // Resumed run over identical data: both months resume-skip. The resumed
    // `partition_files` count must match the fresh run — seeding it from
    // `completed_keys` (which counts the zero-record month) would over-count.
    zero_record_scan(&base)
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .expect("resumed partitioned export should succeed");
    let resumed_count = run_manifest_partition_files(&out_dir);
    assert_eq!(
        resumed_count, fresh_count,
        "resumed partition_files ({resumed_count}) must match fresh ({fresh_count}); \
         the zero-record month must not inflate the resumed count",
    );
    assert!(
        !zero_path.exists(),
        "resume must not resurrect the zero-record partition file",
    );
}
