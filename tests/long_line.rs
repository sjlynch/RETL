//! Regression tests for the `max_line_bytes` guard on JSONL/text-shard readers.
//!
//! Pre-fix, `NdjsonReader::read_line` and the parents/extract validators
//! delegated to `BufRead::read_line(&mut String)`, which grew its buffer
//! without bound — a single 5 GiB JSONL line OOMed the rayon worker
//! silently. These tests pin the new behavior: cap exceeded → structured
//! `io::ErrorKind::InvalidData` with the offending path in the message.

use std::fs::File;
use std::io::{BufReader, Write};

use retl::{
    build_runs_sorted, for_each_line_cfg, read_line_capped, DedupeCfg, KeyExtractor, NdjsonReader,
    DEFAULT_MAX_LINE_BYTES,
};

/// Cap small enough to keep the test fast while still being larger than the
/// payload required to demonstrate streaming reads work below the cap.
const SMALL_CAP: usize = 64 * 1024; // 64 KiB

fn write_jsonl(path: &std::path::Path, lines: &[&str]) {
    let mut f = File::create(path).unwrap();
    for l in lines {
        f.write_all(l.as_bytes()).unwrap();
        f.write_all(b"\n").unwrap();
    }
}

#[test]
fn ndjson_reader_rejects_oversized_line() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("oversized.ndjson");

    let big = "x".repeat(SMALL_CAP * 4);
    write_jsonl(&p, &[r#"{"id":"ok"}"#, &big]);

    let mut r = NdjsonReader::open_with_max(&p, 8 * 1024, SMALL_CAP).unwrap();
    let mut buf = String::new();

    // First line: under cap, reads cleanly.
    let n = r.read_line(&mut buf).unwrap();
    assert!(n > 0);
    assert_eq!(buf, r#"{"id":"ok"}"#);

    // Second line: exceeds cap, surfaces InvalidData rather than allocating
    // unbounded memory.
    let err = r
        .read_line(&mut buf)
        .expect_err("oversized line must error");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    let msg = err.to_string();
    assert!(
        msg.contains("max_line_bytes"),
        "error message should mention the cap: {msg}"
    );
    assert!(
        msg.contains("oversized.ndjson"),
        "error message should name the offending path: {msg}"
    );
}

#[test]
fn zstd_reader_rejects_oversized_line() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("oversized.zst");

    let f = File::create(&p).unwrap();
    let mut enc = zstd::stream::write::Encoder::new(f, 1).unwrap();
    enc.write_all(&vec![b'x'; DEFAULT_MAX_LINE_BYTES + 1])
        .unwrap();
    enc.write_all(b"\n").unwrap();
    enc.finish().unwrap();

    let err = for_each_line_cfg(&p, 8 * 1024, |_line| Ok(()))
        .expect_err("oversized zstd JSONL line must error");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("max_line_bytes"),
        "error message should mention the cap: {msg}"
    );
    assert!(
        msg.contains("oversized.zst"),
        "error message should name the offending path: {msg}"
    );
    let has_invalid_data = err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(|e| e.kind() == std::io::ErrorKind::InvalidData)
            .unwrap_or(false)
    });
    assert!(
        has_invalid_data,
        "error chain should preserve io::ErrorKind::InvalidData: {msg}"
    );
}

#[test]
fn dedupe_build_runs_rejects_oversized_intermediate_line() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("oversized.ndjson");
    let runs = dir.path().join("runs");

    let mut f = File::create(&input).unwrap();
    f.write_all(&vec![b'x'; DEFAULT_MAX_LINE_BYTES + 1])
        .unwrap();
    f.write_all(b"\n").unwrap();
    drop(f);

    let cfg = DedupeCfg {
        read_buf_bytes: 8 * 1024,
        write_buf_bytes: 8 * 1024,
        ..Default::default()
    };
    let err = build_runs_sorted(&input, &runs, &KeyExtractor::author_lowercase_fast(), &cfg)
        .expect_err("oversized dedupe intermediate line must error");
    let msg = format!("{err:#}");
    assert!(msg.contains("max_line_bytes"), "unexpected error: {msg}");
    assert!(
        msg.contains("oversized.ndjson"),
        "error should name the input path: {msg}"
    );
}

#[test]
fn ndjson_reader_with_max_line_bytes_builder_overrides_cap() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("override.ndjson");
    let payload = "y".repeat(SMALL_CAP * 2);
    write_jsonl(&p, &[&payload]);

    // With the small cap, this line errors.
    let mut r = NdjsonReader::open(&p, 8 * 1024)
        .unwrap()
        .with_max_line_bytes(SMALL_CAP);
    let mut buf = String::new();
    assert!(r.read_line(&mut buf).is_err());

    // Bumping the cap above the line length recovers normal reads.
    let mut r2 = NdjsonReader::open(&p, 8 * 1024)
        .unwrap()
        .with_max_line_bytes(SMALL_CAP * 4);
    let mut buf2 = String::new();
    let n = r2.read_line(&mut buf2).unwrap();
    assert!(n > 0);
    assert_eq!(buf2.len(), payload.len());
}

#[test]
fn read_line_capped_returns_zero_on_eof() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("empty.ndjson");
    File::create(&p).unwrap();
    let f = File::open(&p).unwrap();
    let mut r = BufReader::new(f);
    let mut buf = String::new();
    let n = read_line_capped(&mut r, &mut buf, SMALL_CAP, &p).unwrap();
    assert_eq!(n, 0);
    assert!(buf.is_empty());
}

#[test]
fn read_line_capped_strips_crlf_and_lf() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("crlf.ndjson");
    let mut f = File::create(&p).unwrap();
    f.write_all(b"one\r\ntwo\nthree").unwrap();
    drop(f);

    let f = File::open(&p).unwrap();
    let mut r = BufReader::new(f);
    let mut buf = String::new();

    assert!(read_line_capped(&mut r, &mut buf, SMALL_CAP, &p).unwrap() > 0);
    assert_eq!(buf, "one");
    assert!(read_line_capped(&mut r, &mut buf, SMALL_CAP, &p).unwrap() > 0);
    assert_eq!(buf, "two");
    // Final record without a trailing newline still reads.
    assert!(read_line_capped(&mut r, &mut buf, SMALL_CAP, &p).unwrap() > 0);
    assert_eq!(buf, "three");
    // EOF.
    assert_eq!(
        read_line_capped(&mut r, &mut buf, SMALL_CAP, &p).unwrap(),
        0
    );
}

#[test]
fn read_line_capped_rejects_unterminated_oversized_line() {
    // A "line" that never ends in `\n` and exceeds the cap must still error
    // rather than read the whole rest of the file into memory.
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("unterminated.ndjson");
    let mut f = File::create(&p).unwrap();
    // No newline anywhere.
    f.write_all(&vec![b'z'; SMALL_CAP * 2]).unwrap();
    drop(f);

    let f = File::open(&p).unwrap();
    let mut r = BufReader::new(f);
    let mut buf = String::new();
    let err = read_line_capped(&mut r, &mut buf, SMALL_CAP, &p)
        .expect_err("unterminated oversized line must error");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}
