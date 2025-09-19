use serde_json::json;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

/// Write a compressed `.zst` file containing the provided JSONL lines.
/// This mirrors the corpus's RC_/RS_ monthly files but with tiny content.
pub fn write_zst_lines(path: &Path, lines: &[String]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let f = File::create(path).unwrap();
    let mut enc = zstd::stream::write::Encoder::new(f, 3).unwrap();
    for l in lines {
        writeln!(&mut enc, "{}", l).unwrap();
    }
    enc.finish().unwrap();
}

/// Read a JSONL file into a vector of `serde_json::Value` (skips empty lines).
pub fn read_jsonl_values(path: &Path) -> Vec<serde_json::Value> {
    let f = File::open(path).unwrap();
    let r = BufReader::new(f);
    r.lines()
        .map(|l| l.unwrap())
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str(&s).unwrap())
        .collect()
}

/// Read a text file line-by-line into strings (useful for .ndjson, .tsv).
pub fn read_lines(path: &Path) -> Vec<String> {
    let f = File::open(path).unwrap();
    let r = BufReader::new(f);
    r.lines().map(|l| l.unwrap()).filter(|s| !s.is_empty()).collect()
}

/// Build a tiny **valid** corpus with:
/// - 1 submission (RS_2006-01) in r/programming, id=s1 by "bob".
/// - 3 comments (RC_2006-01) in r/programming:
///     c1 by "alice" replying to submission s1 (t3_s1), includes an http:// URL
///     c2 by "charlie" replying to comment c1 (t1_c1) — tests comment-parent resolution
///     c3 by "[deleted]" (pseudo user) — tests pseudo user filtering
/// - 1 additional submission by "AutoModerator" (bot) on "nytimes.com" domain — for bot/`domains_in` tests.
///
/// All timestamps are in Jan 2006 to keep tests small and deterministic.
pub fn make_corpus_basic() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.into_path();

    // Submissions (RS_2006-01): s1 (bob), s2 (AutoModerator on nytimes.com)
    let rs_2006_01 = base.join("submissions").join("RS_2006-01.zst");
    let rs_lines = vec![
        json!({
            "archived": false, "author":"bob", "created_utc":1136073600,
            "domain":"example.com", "id":"s1", "is_self":false, "is_video":false,
            "num_comments":10, "over_18":false, "score":183, "selftext":"",
            "title":"Rust news", "subreddit":"programming", "subreddit_id":"t5_x",
            "url":"http://example.com/x"
        }).to_string(),
        json!({
            "archived": false, "author":"AutoModerator", "created_utc":1136073601,
            "domain":"nytimes.com", "id":"s2", "is_self":false, "is_video":false,
            "num_comments":1, "over_18":false, "score":1, "selftext":"",
            "title":"Meta: rules", "subreddit":"programming", "subreddit_id":"t5_x",
            "url":"http://reddit.com/rules"
        }).to_string(),
    ];
    write_zst_lines(&rs_2006_01, &rs_lines);

    // Comments (RC_2006-01): c1 (alice -> s1), c2 (charlie -> c1), c3 ([deleted])
    let rc_2006_01 = base.join("comments").join("RC_2006-01.zst");
    let rc_lines = vec![
        json!({
            "controversiality":0, "body":"I love Rust http://rust-lang.org", "subreddit_id":"t5_x",
            "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":2,
            "ups":2, "author":"alice", "id":"c1", "edited":false, "parent_id":"t3_s1",
            "gilded":0, "distinguished":null, "created_utc":1136074600, "retrieved_on":1136075600
        }).to_string(),
        json!({
            "controversiality":0, "body":"reply to alice", "subreddit_id":"t5_x",
            "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":5,
            "ups":5, "author":"charlie", "id":"c2", "edited":false, "parent_id":"t1_c1",
            "gilded":0, "distinguished":null, "created_utc":1136074700, "retrieved_on":1136075700
        }).to_string(),
        json!({
            "controversiality":0, "body":"[deleted msg]", "subreddit_id":"t5_x",
            "link_id":"t3_s1", "stickied":false, "subreddit":"programming", "score":0,
            "ups":0, "author":"[deleted]", "id":"c3", "edited":false, "parent_id":"t3_s1",
            "gilded":0, "distinguished":null, "created_utc":1136074800, "retrieved_on":1136075800
        }).to_string(),
    ];
    write_zst_lines(&rc_2006_01, &rc_lines);

    base
}

/// Build a second, **corrupt** monthly file (RC_2006-02.zst) alongside the basic corpus
/// to exercise the integrity checker. The file has a correct name but invalid contents
/// (plain text, not zstd), so decoding must fail in integrity checks.
pub fn add_corrupt_month(base: &Path) {
    let corrupt = base.join("comments").join("RC_2006-02.zst");
    fs::create_dir_all(corrupt.parent().unwrap()).unwrap();
    let mut f = File::create(corrupt).unwrap();
    // Not a zstd stream:
    writeln!(
        &mut f,
        "{{\"id\":\"bad\",\"author\":\"mallory\",\"subreddit\":\"programming\"}}"
    )
    .unwrap();
}

/// Decompress a `.zst` file and collect lines (strings). Handy for export(Zst) verification.
pub fn decompress_zst_lines(path: &Path) -> Vec<String> {
    let f = File::open(path).unwrap();
    let dec = zstd::stream::read::Decoder::new(f).unwrap();
    let r = BufReader::new(dec);
    r.lines().map(|l| l.unwrap()).filter(|s| !s.is_empty()).collect()
}
