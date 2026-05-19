use super::*;

/// Build a `DescribeArgs` from CLI-shaped argv. Mirrors what `main.rs`
/// would do for a `retl describe ...` invocation.
pub(super) fn describe_args(argv: &[&str]) -> crate::bin_args::DescribeArgs {
    let mut v = vec!["retl", "describe"];
    v.extend_from_slice(argv);
    let cli = Cli::try_parse_from(&v).expect("clap parse");
    match cli.command {
        Command::Describe(a) => a,
        other => panic!("expected describe command, got {other:?}"),
    }
}

/// Build a `SchemaArgs` from CLI-shaped argv.
pub(super) fn schema_args(argv: &[&str]) -> crate::bin_args::SchemaArgs {
    let mut v = vec!["retl", "schema"];
    v.extend_from_slice(argv);
    let cli = Cli::try_parse_from(&v).expect("clap parse");
    match cli.command {
        Command::Schema(a) => a,
        other => panic!("expected schema command, got {other:?}"),
    }
}

pub(super) fn export_args(argv: &[&str]) -> crate::bin_args::ExportArgs {
    let mut v = vec!["retl", "export"];
    v.extend_from_slice(argv);
    match Cli::try_parse_from(&v).expect("clap parse").command {
        Command::Export(a) => a,
        other => panic!("expected export command, got {other:?}"),
    }
}

pub(super) fn count_args(argv: &[&str]) -> crate::bin_args::CountArgs {
    let mut v = vec!["retl", "count"];
    v.extend_from_slice(argv);
    match Cli::try_parse_from(&v).expect("clap parse").command {
        Command::Count(a) => a,
        other => panic!("expected count command, got {other:?}"),
    }
}

pub(super) fn sample_args(argv: &[&str]) -> crate::bin_args::SampleArgs {
    let mut v = vec!["retl", "sample"];
    v.extend_from_slice(argv);
    match Cli::try_parse_from(&v).expect("clap parse").command {
        Command::Sample(a) => a,
        other => panic!("expected sample command, got {other:?}"),
    }
}

pub(super) fn convert_args(argv: &[&str]) -> crate::bin_args::ConvertArgs {
    let mut v = vec!["retl", "convert"];
    v.extend_from_slice(argv);
    match Cli::try_parse_from(&v).expect("clap parse").command {
        Command::Convert(a) => a,
        other => panic!("expected convert command, got {other:?}"),
    }
}

pub(super) fn dedupe_args(argv: &[&str]) -> crate::bin_args::DedupeArgs {
    let mut v = vec!["retl", "dedupe"];
    v.extend_from_slice(argv);
    match Cli::try_parse_from(&v).expect("clap parse").command {
        Command::Dedupe(a) => a,
        other => panic!("expected dedupe command, got {other:?}"),
    }
}

/// Build the tiny 3-month corpus from the integration-test helpers, so
/// in-process describe tests share the same fixtures the binary-spawn
/// tests used. `tests/common/mod.rs` isn't visible to the bin crate, so
/// reconstruct just what these tests need inline.
pub(super) fn make_multi_month_corpus(months: &[YearMonth]) -> std::path::PathBuf {
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    for ym in months {
        let label = format!("{:04}-{:02}", ym.year, ym.month);
        let ts = ym_to_epoch_first_day(ym.year, ym.month);
        let rs_lines = [
            json!({
                "archived": false, "author": format!("user_{}", label),
                "created_utc": ts, "domain":"example.com", "id": format!("s_{}", label),
                "is_self":false, "is_video":false, "num_comments":1, "over_18":false,
                "score":10, "selftext":"", "title":"hi", "subreddit":"programming",
                "subreddit_id":"t5_x", "url":"http://example.com/x"
            })
            .to_string(),
            json!({
                "archived": false, "author":"AutoModerator",
                "created_utc": ts + 1, "domain":"reddit.com", "id": format!("sb_{}", label),
                "is_self":false, "is_video":false, "num_comments":1, "over_18":false,
                "score":1, "selftext":"", "title":"meta", "subreddit":"programming",
                "subreddit_id":"t5_x", "url":"http://reddit.com/rules"
            })
            .to_string(),
        ];
        let rs_path = base.join("submissions").join(format!("RS_{}.zst", label));
        write_zst_at(&rs_path, &rs_lines);
        let rc_lines = [
            json!({
                "controversiality":0, "body":"hi", "subreddit_id":"t5_x",
                "link_id": format!("t3_s_{}", label), "stickied":false,
                "subreddit":"programming", "score":2, "ups":2,
                "author": format!("user_{}", label), "id": format!("c1_{}", label),
                "edited":false, "parent_id": format!("t3_s_{}", label),
                "gilded":0, "distinguished":null,
                "created_utc": ts + 100, "retrieved_on": ts + 200
            })
            .to_string(),
            json!({
                "controversiality":0, "body":"yo", "subreddit_id":"t5_x",
                "link_id": format!("t3_s_{}", label), "stickied":false,
                "subreddit":"programming", "score":3, "ups":3,
                "author": format!("commenter_{}", label), "id": format!("c2_{}", label),
                "edited":false, "parent_id": format!("t3_s_{}", label),
                "gilded":0, "distinguished":null,
                "created_utc": ts + 200, "retrieved_on": ts + 300
            })
            .to_string(),
        ];
        let rc_path = base.join("comments").join(format!("RC_{}.zst", label));
        write_zst_at(&rc_path, &rc_lines);
    }
    base
}

/// Build the same tiny `make_corpus_basic`-shaped fixture that
/// `tests/common/mod.rs` produces. The integration-test `common` module
/// isn't visible to the bin crate, so reconstruct the small set of
/// records here. Cached encoding isn't needed — each test in this module
/// runs once per `cargo test` invocation.
pub(super) fn make_basic_corpus_inline() -> std::path::PathBuf {
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    let rs_lines = [
        json!({
            "archived": false, "author":"bob", "created_utc":1136073600,
            "domain":"example.com", "id":"s1", "is_self":false, "is_video":false,
            "num_comments":10, "over_18":false, "score":183, "selftext":"",
            "title":"Rust news", "subreddit":"programming", "subreddit_id":"t5_x",
            "url":"http://example.com/x"
        })
        .to_string(),
        json!({
            "archived": false, "author":"AutoModerator", "created_utc":1136073601,
            "domain":"nytimes.com", "id":"s2", "is_self":false, "is_video":false,
            "num_comments":1, "over_18":false, "score":1, "selftext":"",
            "title":"Meta: rules", "subreddit":"programming", "subreddit_id":"t5_x",
            "url":"http://reddit.com/rules"
        })
        .to_string(),
    ];
    write_zst_at(&base.join("submissions").join("RS_2006-01.zst"), &rs_lines);
    let rc_lines = [
        json!({
            "controversiality":0, "body":"I love Rust http://rust-lang.org", "subreddit_id":"t5_x",
            "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":2,
            "ups":2, "author":"alice", "id":"c1", "edited":false, "parent_id":"t3_s1",
            "gilded":0, "distinguished":null, "created_utc":1136074600, "retrieved_on":1136075600
        })
        .to_string(),
        json!({
            "controversiality":0, "body":"reply to alice", "subreddit_id":"t5_x",
            "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":5,
            "ups":5, "author":"charlie", "id":"c2", "edited":false, "parent_id":"t1_c1",
            "gilded":0, "distinguished":null, "created_utc":1136074700, "retrieved_on":1136075700
        })
        .to_string(),
        json!({
            "controversiality":0, "body":"[deleted msg]", "subreddit_id":"t5_x",
            "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":0,
            "ups":0, "author":"[deleted]", "id":"c3", "edited":false, "parent_id":"t3_s1",
            "gilded":0, "distinguished":null, "created_utc":1136074800, "retrieved_on":1136075800
        })
        .to_string(),
    ];
    write_zst_at(&base.join("comments").join("RC_2006-01.zst"), &rc_lines);
    base
}

pub(super) fn write_zst_at(path: &Path, lines: &[String]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let f = fs::File::create(path).unwrap();
    let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
    for l in lines {
        writeln!(enc, "{l}").unwrap();
    }
    enc.finish().unwrap();
}

fn ym_to_epoch_first_day(year: u16, month: u8) -> i64 {
    use time::{Date, Month, OffsetDateTime, Time, UtcOffset};
    let m = Month::try_from(month).unwrap();
    let d = Date::from_calendar_date(year as i32, m, 1).unwrap();
    OffsetDateTime::new_in_offset(d, Time::MIDNIGHT, UtcOffset::UTC).unix_timestamp()
}

/// Decompress a `.zst` file into JSONL lines, mirroring the helper in
/// `tests/common/mod.rs::decompress_zst_lines`.
pub(super) fn decompress_zst(path: &Path) -> Vec<String> {
    use std::io::{BufRead, BufReader};
    let f = fs::File::open(path).unwrap();
    let dec = zstd::stream::read::Decoder::new(f).unwrap();
    BufReader::new(dec)
        .lines()
        .filter_map(|l| l.ok())
        .filter(|s| !s.is_empty())
        .collect()
}
