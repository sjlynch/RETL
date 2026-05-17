//! Validation tests that genuinely need a fresh subprocess for env-var
//! isolation. The other 10 tests in this file were converted to in-process
//! calls into `run_scan` under `src/bin_handlers/tests.rs` (no binary spawn,
//! same assertions on validation messages).

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn corrupt_corpus() -> tempfile::TempDir {
    let base = tempfile::tempdir().unwrap();
    fs::create_dir_all(base.path().join("comments")).unwrap();
    fs::create_dir_all(base.path().join("submissions")).unwrap();
    fs::write(
        base.path().join("comments").join("RC_2006-01.zst"),
        b"not a zstd frame",
    )
    .unwrap();
    base
}

fn base_scan_cmd(base: &tempfile::TempDir) -> Command {
    let mut cmd = retl();
    cmd.arg("scan")
        .arg("--data-dir")
        .arg(base.path())
        .arg("--work-dir")
        .arg(base.path().join("work"))
        .args(["--source", "rc", "--start", "2006-01", "--end", "2006-01"])
        .arg("--no-progress");
    cmd
}

/// Needs a fresh subprocess because it sets `ETL_EXCLUDE_AUTHORS_FILE` to a
/// path with non-UTF8 content and asserts the resulting error mentions
/// `line 1`. The env-mutation can't safely happen inside an in-process test
/// because cargo runs tests in parallel within a process.
#[test]
fn cli_exclude_common_bots_invalid_file_fails_before_scanning() {
    let base = corrupt_corpus();
    let excludes = base.path().join("bad_excludes.txt");
    fs::write(&excludes, b"\xff\n").unwrap();

    let mut cmd = base_scan_cmd(&base);
    cmd.arg("--exclude-common-bots")
        .env_remove("ETL_EXCLUDE_AUTHORS")
        .env("ETL_EXCLUDE_AUTHORS_FILE", &excludes)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("ETL_EXCLUDE_AUTHORS_FILE")
                .and(predicate::str::contains("line 1"))
                .and(predicate::str::contains("zstd").not()),
        );
}
