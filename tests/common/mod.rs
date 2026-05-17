#![allow(dead_code)]

pub mod cli;
pub mod parents;
pub mod spool;

use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

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

/// Return the proptest case count to use for a property, honoring the
/// `RETL_PROPTEST_CASES` env var if set.
///
/// Daily iteration uses small case counts so the property suites don't
/// dominate `cargo test` wall time; pre-merge / release runs set
/// `RETL_PROPTEST_CASES` to a much larger number for thorough exploration.
/// Override beats `default_for_iteration` when the user wants to dial up
/// exploration explicitly.
pub fn proptest_cases(default_for_iteration: u32) -> u32 {
    std::env::var("RETL_PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse::<u32>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default_for_iteration)
}

/// Encode JSONL lines as a zstd byte buffer (no file I/O). Used by the cached
/// corpus builders below so the dominant cost (zstd encoding at level 3) is
/// amortized across every test in a binary instead of paid per call.
fn encode_zst_bytes(lines: &[String]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    {
        let mut enc = zstd::stream::write::Encoder::new(&mut buf, 3).unwrap();
        for l in lines {
            writeln!(&mut enc, "{}", l).unwrap();
        }
        enc.finish().unwrap();
    }
    buf
}

/// Materialize a cached corpus into a fresh tempdir. Files are routed by
/// relative path under the returned base. Always called with paths whose
/// parent dirs are `comments` or `submissions`.
fn materialize_corpus(files: &[(PathBuf, Vec<u8>)]) -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();
    for (rel, bytes) in files {
        let full = base.join(rel);
        fs::create_dir_all(full.parent().unwrap()).unwrap();
        fs::write(&full, bytes).unwrap();
    }
    base
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
    static CACHE: OnceLock<Vec<(PathBuf, Vec<u8>)>> = OnceLock::new();
    let files = CACHE.get_or_init(|| {
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
        vec![
            (PathBuf::from("submissions").join("RS_2006-01.zst"), encode_zst_bytes(&rs_lines)),
            (PathBuf::from("comments").join("RC_2006-01.zst"), encode_zst_bytes(&rc_lines)),
        ]
    });
    materialize_corpus(files)
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

// -----------------------------------------------------------------------------
// Helpers added by T11 (tests for zstd corruption / interruption / round-trip).
// Append-only — do not modify helpers above this line.
// -----------------------------------------------------------------------------

/// Build a valid `.zst` containing `valid_records` JSONL records (RC-style) and then
/// **truncate the file** by `truncate_by_bytes` from the END.
///
/// The result is a file that is well-formed at the start but missing trailing bytes:
/// a streaming decoder will succeed for a while and then fail before EOF. Useful for
/// exercising late/trailing corruption that `IntegrityMode::Quick` sampling can miss.
///
/// Records are intentionally chunky (~250 byte payloads) so that even after high
/// compression the file is large enough that `truncate_by_bytes` (e.g. 256) does not
/// destroy the entire frame.
pub fn make_truncated_zst(path: &Path, valid_records: usize, truncate_by_bytes: u64) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut lines = Vec::with_capacity(valid_records);
    for i in 0..valid_records {
        lines.push(json!({
            "id": format!("rec{:06}", i),
            "author": format!("user_{:04}", i % 256),
            "subreddit": "programming",
            "subreddit_id": "t5_x",
            "link_id": "t3_s1",
            "parent_id": "t3_s1",
            "created_utc": 1136073600_i64 + i as i64,
            "score": (i as i64) % 1000,
            "ups": (i as i64) % 1000,
            "controversiality": 0,
            "stickied": false,
            "edited": false,
            "gilded": 0,
            "distinguished": serde_json::Value::Null,
            "retrieved_on": 1136075600_i64 + i as i64,
            "body": format!(
                "record {} payload — keep this string long enough that even after \
                 zstd compression the resulting file is comfortably larger than the \
                 caller's truncation amount, so the truncated tail can be observed \
                 by a full streaming decode without obliterating the entire frame.",
                i
            ),
        }).to_string());
    }
    write_zst_lines(path, &lines);

    let cur_len = fs::metadata(path).unwrap().len();
    let new_len = cur_len.saturating_sub(truncate_by_bytes);
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(new_len).unwrap();
}

/// Open a `.zst` file and flip the byte at `byte_offset` (XOR with 0xFF).
/// Useful for simulating an arbitrary single-bit/byte corruption inside the
/// compressed stream so that decode errors surface mid-file rather than at EOF.
pub fn bit_flip_zst(path: &Path, byte_offset: u64) {
    use std::io::{Read, Seek, SeekFrom, Write as IoWrite};
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    f.seek(SeekFrom::Start(byte_offset)).unwrap();
    let mut buf = [0u8; 1];
    f.read_exact(&mut buf).unwrap();
    buf[0] ^= 0xFF;
    f.seek(SeekFrom::Start(byte_offset)).unwrap();
    f.write_all(&buf).unwrap();
    f.flush().unwrap();
}

// =============================================================================
// Shared helpers appended for T12 behavioral coverage backfill (#T12).
// Append-only — coexist with #T11 helpers above.
// =============================================================================

use retl::YearMonth;

/// Build a corpus spanning multiple months. For each `(year, month)` in `months`,
/// writes one comment file (RC_YYYY-MM.zst) and one submission file (RS_YYYY-MM.zst)
/// with **two records each** in r/programming. Authors are deterministic per month
/// so callers can assert exact dedupe results.
///
/// Records carry `created_utc` aligned to the first day of the named month so
/// record-level `within_bounds` checks match plan-level filenames.
pub fn make_corpus_multi_month(months: &[YearMonth]) -> PathBuf {
    static CACHE: OnceLock<Mutex<HashMap<Vec<(u16, u8)>, Vec<(PathBuf, Vec<u8>)>>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    let key: Vec<(u16, u8)> = months.iter().map(|ym| (ym.year, ym.month)).collect();

    // Compute (and cache) the encoded payloads under a brief lock, then drop
    // the lock before file I/O. The clone is cheap (~handful of small Vecs).
    let files: Vec<(PathBuf, Vec<u8>)> = {
        let mut map = cache.lock().unwrap();
        map.entry(key)
            .or_insert_with(|| {
                let mut out = Vec::with_capacity(months.len() * 2);
                for ym in months {
                    let label = format!("{:04}-{:02}", ym.year, ym.month);
                    let ts = ym_to_epoch_first_day(ym.year, ym.month);

                    let rs_lines = vec![
                        json!({
                            "archived": false, "author": format!("user_{}", label),
                            "created_utc": ts, "domain":"example.com", "id": format!("s_{}", label),
                            "is_self":false, "is_video":false, "num_comments":1, "over_18":false,
                            "score":10, "selftext":"", "title":"hi", "subreddit":"programming",
                            "subreddit_id":"t5_x", "url":"http://example.com/x"
                        }).to_string(),
                        json!({
                            "archived": false, "author":"AutoModerator",
                            "created_utc": ts + 1, "domain":"reddit.com", "id": format!("sb_{}", label),
                            "is_self":false, "is_video":false, "num_comments":1, "over_18":false,
                            "score":1, "selftext":"", "title":"meta", "subreddit":"programming",
                            "subreddit_id":"t5_x", "url":"http://reddit.com/rules"
                        }).to_string(),
                    ];
                    out.push((
                        PathBuf::from("submissions").join(format!("RS_{}.zst", label)),
                        encode_zst_bytes(&rs_lines),
                    ));

                    let rc_lines = vec![
                        json!({
                            "controversiality":0, "body":"hi", "subreddit_id":"t5_x",
                            "link_id": format!("t3_s_{}", label), "stickied":false,
                            "subreddit":"programming", "score":2, "ups":2,
                            "author": format!("user_{}", label), "id": format!("c1_{}", label),
                            "edited":false, "parent_id": format!("t3_s_{}", label),
                            "gilded":0, "distinguished":null,
                            "created_utc": ts + 100, "retrieved_on": ts + 200
                        }).to_string(),
                        json!({
                            "controversiality":0, "body":"yo", "subreddit_id":"t5_x",
                            "link_id": format!("t3_s_{}", label), "stickied":false,
                            "subreddit":"programming", "score":3, "ups":3,
                            "author": format!("commenter_{}", label), "id": format!("c2_{}", label),
                            "edited":false, "parent_id": format!("t3_s_{}", label),
                            "gilded":0, "distinguished":null,
                            "created_utc": ts + 200, "retrieved_on": ts + 300
                        }).to_string(),
                    ];
                    out.push((
                        PathBuf::from("comments").join(format!("RC_{}.zst", label)),
                        encode_zst_bytes(&rc_lines),
                    ));
                }
                out
            })
            .clone()
    };

    materialize_corpus(&files)
}

/// Build a single-month corpus (RC_2006-01.zst) with `n` synthetic comment records
/// in r/programming. Authors cycle through ~`n/4` distinct names so dedupe/bucketing
/// has interesting groups; ids are unique. Used to drive bucketing's adaptive flush
/// threshold and to stress dedupe with non-trivial run sizes.
pub fn make_corpus_n_records(n: usize) -> PathBuf {
    static CACHE: OnceLock<Mutex<HashMap<usize, Vec<(PathBuf, Vec<u8>)>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    let files: Vec<(PathBuf, Vec<u8>)> = {
        let mut map = cache.lock().unwrap();
        map.entry(n)
            .or_insert_with(|| {
                let distinct = (n / 4).max(1);
                let ts0: i64 = 1136073600; // 2006-01-01 00:00:00 UTC
                let mut lines: Vec<String> = Vec::with_capacity(n);
                for i in 0..n {
                    let author = format!("user_{:05}", i % distinct);
                    lines.push(
                        json!({
                            "controversiality":0,
                            "body": format!("body line {}", i),
                            "subreddit_id":"t5_x",
                            "link_id":"t3_s1",
                            "stickied":false,
                            "subreddit":"programming",
                            "score": (i as i64) % 100,
                            "ups": (i as i64) % 100,
                            "author": author,
                            "id": format!("rc{:08}", i),
                            "edited":false,
                            "parent_id":"t3_s1",
                            "gilded":0,
                            "distinguished":null,
                            "created_utc": ts0 + (i as i64),
                            "retrieved_on": ts0 + (i as i64) + 100
                        })
                        .to_string(),
                    );
                }
                vec![(
                    PathBuf::from("comments").join("RC_2006-01.zst"),
                    encode_zst_bytes(&lines),
                )]
            })
            .clone()
    };

    materialize_corpus(&files)
}

/// Compute a unix timestamp for the first day of a (year, month) at 00:00:00 UTC.
/// Uses the `time` crate (already a transitive dep). Days-since-epoch math.
fn ym_to_epoch_first_day(year: u16, month: u8) -> i64 {
    use time::{Date, Month, OffsetDateTime, Time, UtcOffset};
    let m = Month::try_from(month).unwrap();
    let d = Date::from_calendar_date(year as i32, m, 1).unwrap();
    let dt = OffsetDateTime::new_in_offset(d, Time::MIDNIGHT, UtcOffset::UTC);
    dt.unix_timestamp()
}
