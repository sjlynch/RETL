//! End-to-end smoke that the `retl quickstart` binary runs without a real
//! corpus and prints the expected demo output. The previous 17 sibling tests
//! covered the public CLI surface by spawning the binary just to grep `--help`
//! output — those moved into in-process `clap` rendering checks under
//! `src/bin_args/mod.rs::tests` and `src/bin_handlers/*::tests` for the
//! handler-side validation messages.
//!
//! This file keeps only the one test that genuinely exercises the binary's
//! stdout-printing handler end-to-end.

use assert_cmd::Command;
use predicates::prelude::*;
use predicates::str::contains;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

#[test]
fn quickstart_runs_without_corpus() {
    let dir = tempfile::tempdir().unwrap();
    retl()
        .arg("quickstart")
        .arg("--out-dir")
        .arg(dir.path())
        .assert()
        .success()
        .stdout(contains("Feature demo").and(contains("Found 4 unique authors")));
}
