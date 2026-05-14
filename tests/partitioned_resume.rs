#[path = "common/mod.rs"]
mod common;

use common::{decompress_zst_lines, make_truncated_zst, read_lines, write_zst_lines};
use retl::{ExportFormat, RedditETL, Sources, YearMonth};
use serde_json::{json, Value};
use std::fs;
use std::path::{Path, PathBuf};

fn valid_comment(id: &str, body: &str) -> String {
    json!({
        "id": id,
        "author": "alice",
        "subreddit": "programming",
        "created_utc": 1136073600_i64,
        "score": 1,
        "body": body,
    })
    .to_string()
}

fn setup_two_month_corpus() -> (PathBuf, PathBuf, PathBuf) {
    let base = tempfile::tempdir().unwrap().keep();
    let jan = base.join("comments").join("RC_2006-01.zst");
    let feb = base.join("comments").join("RC_2006-02.zst");
    write_zst_lines(&jan, &[valid_comment("jan", "complete")]);
    make_truncated_zst(&feb, 500, 128);
    fs::create_dir_all(base.join("submissions")).unwrap();
    (base, jan, feb)
}

fn manifest(out_dir: &Path) -> Value {
    serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap()
}

fn run_export(base: &Path, out_dir: &Path, format: ExportFormat, allow_partial: bool) {
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .resume(true)
        .allow_partial(allow_partial)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .export_partitioned(out_dir, format)
        .unwrap();
}

fn setup_one_month_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    let jan = base.join("comments").join("RC_2006-01.zst");
    write_zst_lines(&jan, &[valid_comment("jan", "complete")]);
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

fn assert_partitioned_resume(format: ExportFormat) {
    let (base, jan_source, feb_source) = setup_two_month_corpus();
    let out_dir = base.join(match format {
        ExportFormat::Jsonl => "out_jsonl",
        ExportFormat::Zst => "out_zst",
    });
    let ext = match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Zst => "zst",
    };
    let jan_out = out_dir.join("comments").join(format!("RC_2006-01.{ext}"));
    let feb_out = out_dir.join("comments").join(format!("RC_2006-02.{ext}"));

    run_export(&base, &out_dir, format, true);
    assert!(jan_out.exists(), "good month should publish");
    assert!(!feb_out.exists(), "truncated month must not publish");
    let first_manifest = manifest(&out_dir);
    assert!(first_manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-01"));
    assert!(!first_manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-02"));

    // If resume fails to skip the completed January output, this invalid source
    // would make the second strict run fail. February is repaired and should be
    // the only month processed on the resume run.
    fs::write(&jan_source, b"not a zstd stream").unwrap();
    write_zst_lines(&feb_source, &[valid_comment("feb", "repaired")]);

    run_export(&base, &out_dir, format, false);
    assert!(feb_out.exists(), "repaired month should publish on resume");
    let second_manifest = manifest(&out_dir);
    assert!(second_manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-01"));
    assert!(second_manifest["months"]
        .as_object()
        .unwrap()
        .contains_key("RC_2006-02"));

    let feb_lines = match format {
        ExportFormat::Jsonl => read_lines(&feb_out),
        ExportFormat::Zst => decompress_zst_lines(&feb_out),
    };
    assert_eq!(feb_lines.len(), 1);
    assert!(feb_lines[0].contains("feb"), "{feb_lines:?}");
}

fn assert_partitioned_resume_rebuilds_corrupt_output(format: ExportFormat) {
    let base = setup_one_month_corpus();
    let out_dir = base.join(match format {
        ExportFormat::Jsonl => "out_jsonl_rebuild",
        ExportFormat::Zst => "out_zst_rebuild",
    });
    run_export(&base, &out_dir, format, false);

    let ext = match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Zst => "zst",
    };
    let out = out_dir.join("comments").join(format!("RC_2006-01.{ext}"));
    fs::write(&out, b"not a valid completed partition").unwrap();

    run_export(&base, &out_dir, format, false);
    let lines = match format {
        ExportFormat::Jsonl => read_lines(&out),
        ExportFormat::Zst => decompress_zst_lines(&out),
    };
    assert_eq!(lines.len(), 1);
    assert!(lines[0].contains("jan"), "{lines:?}");
}

#[test]
fn partitioned_jsonl_resume_retries_incomplete_month_and_skips_completed() {
    assert_partitioned_resume(ExportFormat::Jsonl);
}

#[test]
fn partitioned_zst_resume_retries_incomplete_month_and_skips_completed() {
    assert_partitioned_resume(ExportFormat::Zst);
}

#[test]
fn partitioned_jsonl_resume_rebuilds_corrupt_published_output() {
    assert_partitioned_resume_rebuilds_corrupt_output(ExportFormat::Jsonl);
}

#[test]
fn partitioned_zst_resume_rebuilds_corrupt_published_output() {
    assert_partitioned_resume_rebuilds_corrupt_output(ExportFormat::Zst);
}
