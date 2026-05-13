#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::write_zst_lines;
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn make_shard_stability_corpus() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();

    let months = [
        ("2006-01", 1_136_073_600_i64),
        ("2006-02", 1_138_752_000_i64),
        ("2006-03", 1_141_171_200_i64),
    ];
    let records_per_file = 220usize;
    let distinct_authors = 320usize; // > default shard_count collisions exercise per-shard ordering.

    for (month_idx, (label, ts0)) in months.iter().copied().enumerate() {
        let mut rc_lines = Vec::with_capacity(records_per_file);
        let mut rs_lines = Vec::with_capacity(records_per_file);

        for i in 0..records_per_file {
            // Deliberately repeat authors across files/months while distributing
            // adjacent records across the keyspace. The parallel scan can race on
            // shard mutex acquisition, but the final text outputs must not depend
            // on that race.
            let author_idx = (i * 17 + month_idx * 43) % distinct_authors;
            let author = format!("user_{author_idx:04}");
            rc_lines.push(
                json!({
                    "author": author,
                    "body": format!("comment {label} {i}"),
                    "created_utc": ts0 + i as i64,
                    "id": format!("c_{label}_{i:04}"),
                    "parent_id": "t3_root",
                    "score": (i % 100) as i64,
                    "subreddit": "programming"
                })
                .to_string(),
            );

            let author_idx = (i * 31 + month_idx * 29 + 7) % distinct_authors;
            let author = format!("user_{author_idx:04}");
            rs_lines.push(
                json!({
                    "author": author,
                    "created_utc": ts0 + 10_000 + i as i64,
                    "domain": "example.com",
                    "id": format!("s_{label}_{i:04}"),
                    "score": (i % 100) as i64,
                    "subreddit": "programming",
                    "title": format!("submission {label} {i}"),
                    "url": "http://example.com/"
                })
                .to_string(),
            );
        }

        write_zst_lines(
            &base.join("comments").join(format!("RC_{label}.zst")),
            &rc_lines,
        );
        write_zst_lines(
            &base.join("submissions").join(format!("RS_{label}.zst")),
            &rs_lines,
        );
    }

    base
}

fn run_text_output(
    cwd: &Path,
    base: &Path,
    run_name: &str,
    subcommand: &str,
    extra_args: &[&str],
    parallelism: usize,
    file_concurrency: usize,
) -> Vec<u8> {
    let work_dir = cwd.join(format!("work_{run_name}"));
    let out = cwd.join(format!("{run_name}.txt"));

    retl()
        .current_dir(cwd)
        .arg(subcommand)
        .arg("--data-dir")
        .arg(base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--start",
            "2006-01",
            "--end",
            "2006-03",
            "--subreddit",
            "programming",
            "--parallelism",
        ])
        .arg(parallelism.to_string())
        .arg("--file-concurrency")
        .arg(file_concurrency.to_string())
        .arg("--no-progress")
        .args(extra_args)
        .arg("--out")
        .arg(&out)
        .assert()
        .success();

    fs::read(out).unwrap()
}

#[test]
fn scan_count_author_and_first_seen_outputs_are_byte_stable_across_parallelism() {
    let base = make_shard_stability_corpus();
    let cwd = tempfile::tempdir().unwrap();

    let scan_serial = run_text_output(cwd.path(), &base, "scan_serial", "scan", &[], 1, 1);
    let scan_parallel = run_text_output(cwd.path(), &base, "scan_parallel", "scan", &[], 4, 4);
    assert!(!scan_serial.is_empty());
    assert_eq!(scan_serial, scan_parallel, "scan output changed bytes");

    let count_serial = run_text_output(
        cwd.path(),
        &base,
        "count_serial",
        "count",
        &["--mode", "author"],
        1,
        1,
    );
    let count_parallel = run_text_output(
        cwd.path(),
        &base,
        "count_parallel",
        "count",
        &["--mode", "author"],
        4,
        4,
    );
    assert!(!count_serial.is_empty());
    assert_eq!(
        count_serial, count_parallel,
        "count --mode author output changed bytes"
    );

    let first_seen_serial = run_text_output(
        cwd.path(),
        &base,
        "first_seen_serial",
        "first-seen",
        &[],
        1,
        1,
    );
    let first_seen_parallel = run_text_output(
        cwd.path(),
        &base,
        "first_seen_parallel",
        "first-seen",
        &[],
        4,
        4,
    );
    assert!(!first_seen_serial.is_empty());
    assert_eq!(
        first_seen_serial, first_seen_parallel,
        "first-seen output changed bytes"
    );
}
