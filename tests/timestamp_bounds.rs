#[path = "common/mod.rs"]
mod common;

use assert_cmd::Command;
use common::{read_jsonl_values, write_zst_lines};
use serde_json::json;
use std::fs;
use std::io::Write;
use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

fn epoch(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> i64 {
    let date = Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap();
    let time = Time::from_hms(hour, minute, second).unwrap();
    OffsetDateTime::new_in_offset(date, time, UtcOffset::UTC).unix_timestamp()
}

#[test]
fn cli_timestamp_bounds_filter_exactly_and_derive_month_planning() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    fs::create_dir_all(base.join("comments")).unwrap();

    // If --after/--before do not derive a month planning range, this corrupt
    // out-of-window month is scanned and the command fails before filtering.
    let mut corrupt = fs::File::create(base.join("comments").join("RC_2020-10.zst")).unwrap();
    writeln!(corrupt, "not a zstd stream").unwrap();

    let lower = epoch(2020, 11, 30, 12, 0, 0);
    let upper = epoch(2020, 12, 1, 12, 0, 0);

    write_zst_lines(
        &base.join("comments").join("RC_2020-11.zst"),
        &[
            json!({
                "id": "below", "author": "alice", "subreddit": "programming",
                "created_utc": lower - 1, "body": "too early"
            })
            .to_string(),
            json!({
                "id": "at_lower", "author": "alice", "subreddit": "programming",
                "created_utc": lower, "body": "inclusive lower"
            })
            .to_string(),
            json!({
                "id": "missing_ts", "author": "alice", "subreddit": "programming",
                "body": "missing created_utc"
            })
            .to_string(),
            json!({
                "id": "string_ts", "author": "alice", "subreddit": "programming",
                "created_utc": lower.to_string(), "body": "string-encoded timestamp"
            })
            .to_string(),
        ],
    );

    write_zst_lines(
        &base.join("comments").join("RC_2020-12.zst"),
        &[
            json!({
                "id": "before_upper", "author": "alice", "subreddit": "programming",
                "created_utc": upper - 1, "body": "inside"
            })
            .to_string(),
            json!({
                "id": "at_upper", "author": "alice", "subreddit": "programming",
                "created_utc": upper, "body": "exclusive upper"
            })
            .to_string(),
        ],
    );

    // Another corrupt out-of-window file, this time after the exclusive upper
    // bound, further defends that both derived planning endpoints are used.
    let mut corrupt = fs::File::create(base.join("comments").join("RC_2021-01.zst")).unwrap();
    writeln!(corrupt, "not a zstd stream either").unwrap();

    let work_dir = base.join("work");
    let out = base.join("out.jsonl");
    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--source",
            "rc",
            "--after",
            "2020-11-30T12:00Z",
            "--before",
            "2020-12-01T12:00Z",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success();

    let ids: Vec<String> = read_jsonl_values(&out)
        .into_iter()
        .map(|value| value["id"].as_str().unwrap().to_string())
        .collect();
    // `string_ts` carries a string-encoded `created_utc`; the fast path coerces
    // it like a number, so it is kept (it sits exactly on the inclusive lower
    // bound). `missing_ts` (no `created_utc`) and the out-of-window records
    // stay dropped.
    assert_eq!(ids, vec!["at_lower", "string_ts", "before_upper"]);
}

/// A tighter `--after`/`--before` must NARROW an explicit `--start`/`--end`
/// for file planning: the planned month set is the intersection of the two
/// ranges, not the explicit range alone.
#[test]
fn cli_timestamp_bounds_narrow_explicit_date_range_for_planning() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();
    fs::create_dir_all(base.join("comments")).unwrap();

    let lower = epoch(2020, 11, 30, 12, 0, 0);
    let upper = epoch(2020, 12, 1, 12, 0, 0);

    // These months sit inside the explicit `--start 2020-09 --end 2021-02`
    // window but outside the tighter `--after`/`--before` window (which derives
    // 2020-11..2020-12). If planning used the explicit range alone, these
    // corrupt files would be opened and fail the run. Intersecting the two
    // ranges for planning skips them entirely.
    for month in [
        "RC_2020-09.zst",
        "RC_2020-10.zst",
        "RC_2021-01.zst",
        "RC_2021-02.zst",
    ] {
        let mut corrupt = fs::File::create(base.join("comments").join(month)).unwrap();
        writeln!(corrupt, "not a zstd stream").unwrap();
    }

    write_zst_lines(
        &base.join("comments").join("RC_2020-11.zst"),
        &[json!({
            "id": "kept_lower", "author": "alice", "subreddit": "programming",
            "created_utc": lower, "body": "inclusive lower"
        })
        .to_string()],
    );
    write_zst_lines(
        &base.join("comments").join("RC_2020-12.zst"),
        &[json!({
            "id": "kept_inside", "author": "alice", "subreddit": "programming",
            "created_utc": upper - 1, "body": "inside"
        })
        .to_string()],
    );

    let work_dir = base.join("work");
    let out = base.join("out.jsonl");
    retl()
        .arg("export")
        .arg("--data-dir")
        .arg(base)
        .arg("--work-dir")
        .arg(&work_dir)
        .args([
            "--source",
            "rc",
            "--start",
            "2020-09",
            "--end",
            "2021-02",
            "--after",
            "2020-11-30T12:00Z",
            "--before",
            "2020-12-01T12:00Z",
            "--format",
            "jsonl",
            "--no-progress",
            "--out",
        ])
        .arg(&out)
        .assert()
        .success();

    let mut ids: Vec<String> = read_jsonl_values(&out)
        .into_iter()
        .map(|value| value["id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["kept_inside", "kept_lower"]);
}
