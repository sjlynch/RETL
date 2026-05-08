//! Regression test for `replace_file_atomic_backoff`.
//!
//! The function previously did `remove(dest)` then `rename(tmp, dest)`, which
//! opened a window during which a concurrent reader saw `NotFound` at `dest` —
//! a regression from the "atomic" contract its name advertises (and a real
//! source of flaky test/output behavior on Windows).
//!
//! This test races a reader against a writer performing many
//! `replace_file_atomic_backoff` calls and asserts the reader NEVER observes a
//! "missing file" error. It does not assert which version of the contents the
//! reader sees — only that *some* version is always present at `dest`.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use retl::replace_file_atomic_backoff;

fn write_tmp(path: &PathBuf, contents: &[u8]) {
    fs::write(path, contents).expect("write tmp");
}

#[test]
fn replace_is_atomic_under_concurrent_reader() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let dest: PathBuf = tmpdir.path().join("dest.bin");

    // Seed the destination so the reader has something to open before the
    // writer starts replacing.
    fs::write(&dest, b"v0").expect("seed dest");

    let stop = Arc::new(AtomicBool::new(false));

    // Reader thread: continually opens `dest` and reads it. Records any
    // `NotFound` (or context-wrapping equivalent) — those would be the
    // regression we're guarding against.
    let reader_dest = dest.clone();
    let reader_stop = Arc::clone(&stop);
    let reader = thread::spawn(move || -> Result<u64, String> {
        let mut reads: u64 = 0;
        while !reader_stop.load(Ordering::Relaxed) {
            match fs::read(&reader_dest) {
                Ok(_) => reads += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(format!(
                        "reader observed NotFound at {} after {} successful reads",
                        reader_dest.display(),
                        reads
                    ));
                }
                Err(e) => {
                    // Other transient errors (e.g. sharing violations on Windows
                    // when the writer is mid-rename) are acceptable — the file
                    // *exists*, we just couldn't read it this instant. Retry.
                    let _ = e;
                }
            }
        }
        Ok(reads)
    });

    // Writer: build many distinct tmp files in the same directory (so rename
    // is a same-volume move) and atomically replace `dest` with each in turn.
    // Run for a fixed wall-clock budget OR a minimum iteration count,
    // whichever takes longer, to give the reader plenty of opportunities to
    // catch a missing-file window if one exists.
    let writer_dir = tmpdir.path().to_path_buf();
    let writer_dest = dest.clone();
    let min_iters: usize = 200;
    let min_duration = Duration::from_millis(750);
    let writer = thread::spawn(move || {
        let start = Instant::now();
        let mut i: usize = 0;
        loop {
            let tmp = writer_dir.join(format!("dest.bin.tmp.{}", i));
            // Vary contents so we know the writer is actually doing work.
            let payload = format!("v{:08}", i).into_bytes();
            write_tmp(&tmp, &payload);
            replace_file_atomic_backoff(&tmp, &writer_dest)
                .expect("replace_file_atomic_backoff failed");
            i += 1;
            if i >= min_iters && start.elapsed() >= min_duration {
                break;
            }
        }
        i
    });

    let writes = writer.join().expect("writer panicked");
    stop.store(true, Ordering::Relaxed);
    let reads_result = reader.join().expect("reader panicked");

    let reads = reads_result.unwrap_or_else(|msg| panic!("{}", msg));

    // Sanity: the test is meaningless if neither side did real work.
    assert!(
        writes >= 200,
        "writer only managed {} replace cycles — environment is too slow for this test to be meaningful",
        writes
    );
    assert!(
        reads >= 1,
        "reader didn't manage a single successful read in {} writer cycles",
        writes
    );

    // Final state: dest still exists and contains the last value the writer
    // produced.
    let final_contents = fs::read(&dest).expect("dest gone after writer finished");
    let expected_last = format!("v{:08}", writes - 1);
    assert_eq!(
        final_contents,
        expected_last.as_bytes(),
        "dest should hold the last payload the writer produced"
    );
}

/// `replace_file_atomic_backoff` must overwrite an existing destination — not
/// fail because `dest` already exists. Pinned because the "atomic" fix removes
/// the explicit pre-remove and now relies on `rename` (and the copy fallback)
/// doing the overwrite themselves.
#[test]
fn replace_overwrites_existing_dest() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let dest = tmpdir.path().join("dest.bin");
    let tmp = tmpdir.path().join("dest.bin.tmp");

    fs::write(&dest, b"OLD CONTENTS, MUST BE REPLACED").expect("seed dest");
    fs::write(&tmp, b"NEW").expect("seed tmp");

    replace_file_atomic_backoff(&tmp, &dest).expect("replace");

    assert_eq!(fs::read(&dest).expect("read dest"), b"NEW");
    assert!(!tmp.exists(), "tmp should be gone after replace");
}
