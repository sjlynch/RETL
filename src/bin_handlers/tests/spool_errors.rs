use super::fixtures::{export_args, make_basic_corpus_inline};
use super::*;

/// Migrated from tests/streaming_error_hardening.rs::cli_spool_publish_failure_exits_nonzero.
/// The original spawned `retl export --format spool` against a corpus
/// whose publish destination had been pre-created as a directory, and
/// asserted on (a) non-zero exit code and (b) stderr containing the
/// blocked filename. In-process, we drive the same `run_export` handler
/// and assert on the returned `anyhow::Error`'s Display — that's the
/// same string the subprocess would have written to stderr.
///
/// Gated on `feature = "test-utils"` because it depends on the
/// retry-budget cap (`retl::cap_backoff_budget_for_test`). Without the
/// feature the library-level
/// `spool_publish_failure_is_fatal_and_does_not_commit_manifest`
/// in `tests/streaming_error_hardening.rs` provides equivalent coverage
/// (just slower, ~14 s of retry-budget exhaustion).
#[cfg(feature = "test-utils")]
#[test]
fn export_spool_publish_failure_surfaces_helpful_error() {
    let base = make_basic_corpus_inline();
    let cwd = tempfile::tempdir().unwrap();
    let out_dir = cwd.path().join("spool");
    fs::create_dir_all(out_dir.join("part_RC_2006-01.jsonl")).unwrap();

    // Cap retry budget so the publish failure surfaces immediately
    // instead of waiting out the production ~10–20 s on
    // ERROR_ACCESS_DENIED. The invariant ("export bails with an error
    // mentioning the blocked filename") is independent of timing.
    let _backoff_cap = retl::cap_backoff_budget_for_test(1, 0);

    let args = export_args(&[
        "--data-dir",
        base.to_str().unwrap(),
        "--work-dir",
        cwd.path().join("work").to_str().unwrap(),
        "--source",
        "rc",
        "--start",
        "2006-01",
        "--end",
        "2006-01",
        "--format",
        "spool",
        "--no-progress",
        "--resume",
        "--out",
        out_dir.to_str().unwrap(),
    ]);
    let err = run_export(args).expect_err("spool publish over a directory must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("part_RC_2006-01.jsonl"),
        "expected blocked filename in error, got: {msg}"
    );
}
