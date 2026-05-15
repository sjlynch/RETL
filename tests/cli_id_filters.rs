mod common;

use assert_cmd::Command;
use common::read_jsonl_values;
use predicates::prelude::*;
use std::fs;

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn export_cmd(base: &std::path::Path, out: &std::path::Path) -> Command {
    let mut cmd = retl();
    cmd.arg("export")
        .arg("--data-dir")
        .arg(base)
        .arg("--work-dir")
        .arg(base.join("work"))
        .args(["--source", "both", "--start", "2006-01", "--end", "2006-01"])
        .arg("--no-progress")
        .args(["--format", "jsonl"])
        .arg("--out")
        .arg(out);
    cmd
}

#[test]
fn cli_export_filters_single_record_id() {
    let base = common::make_corpus_basic();
    let out = base.join("cli_single_id.jsonl");

    export_cmd(&base, &out)
        .args(["--id", "c2"])
        .assert()
        .success();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], "c2");
}

#[test]
fn cli_export_filters_ids_file_with_prefixed_id() {
    let base = common::make_corpus_basic();
    let ids_file = base.join("ids.txt");
    fs::write(&ids_file, "# comments and blanks are ignored\n\n t3_s1 \n").unwrap();
    let out = base.join("cli_ids_file.jsonl");

    export_cmd(&base, &out)
        .arg("--ids-file")
        .arg(&ids_file)
        .assert()
        .success();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1);
    assert_eq!(values[0]["id"], "s1");
}

#[test]
fn cli_id_filter_suppresses_full_corpus_warning() {
    let base = common::make_corpus_basic();
    let out = base.join("cli_no_full_corpus_warning.jsonl");

    let mut cmd = retl();
    cmd.arg("export")
        .arg("--data-dir")
        .arg(&base)
        .arg("--work-dir")
        .arg(base.join("work_warning"))
        .args(["--source", "both"])
        .arg("--no-progress")
        .args(["--format", "jsonl"])
        .arg("--out")
        .arg(&out)
        .args(["--id", "c2"])
        .assert()
        .success()
        .stderr(predicate::str::contains("unfiltered, undated").not());
}

#[test]
fn cli_rejects_duplicate_ids_before_scanning() {
    let base = tempfile::tempdir().unwrap();
    fs::create_dir_all(base.path().join("comments")).unwrap();
    fs::create_dir_all(base.path().join("submissions")).unwrap();
    fs::write(
        base.path().join("comments").join("RC_2006-01.zst"),
        b"not zstd",
    )
    .unwrap();
    let out = base.path().join("out.jsonl");

    export_cmd(base.path(), &out)
        .args(["--id", "t1_abc", "--id", "T1_ABC"])
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("ids_in")
                .and(predicate::str::contains("duplicate ID"))
                .and(predicate::str::contains("zstd").not()),
        );
}
