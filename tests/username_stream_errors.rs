//! Verify `UsernameStream` terminates gracefully when underlying reads error.
//!
//! The iterator's matched `Err(_) => continue` branch is the failure mode: a
//! Read that returns Err on every call would spin forever. With real files
//! the OS-level read advances past invalid UTF-8 byte sequences anyway, so
//! we drive the iterator over:
//!   - a missing file path (open_next() returns Err -> lossy iterator logs
//!     a warning, advances past the bad file, and returns None when the file
//!     list is exhausted; callers using try_next() can observe the Err)
//!   - a file containing invalid UTF-8 (read_line returns Err but bytes are
//!     consumed; iterator must reach EOF and terminate)
//!   - a deduped file containing valid lines + a UTF-8 error in the middle.
//!
//! The test harness uses a thread + timeout to bound the test duration so a
//! regression that re-introduces an infinite loop fails the suite cleanly
//! instead of hanging it.

use retl::UsernameStream;
use std::fs::File;
use std::io::Write;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

/// Run `f` on a worker thread, fail with a clear message if it does not finish
/// within `timeout`.
fn run_with_timeout<F, T>(name: &str, timeout: Duration, f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let r = f();
        let _ = tx.send(r);
    });
    rx.recv_timeout(timeout)
        .unwrap_or_else(|_| panic!("{name} did not terminate within {:?}", timeout))
}

#[test]
fn username_stream_terminates_when_file_does_not_exist() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("not_there.txt");

    let result: Vec<String> = run_with_timeout(
        "username_stream_missing_file",
        Duration::from_secs(5),
        move || {
            let it = UsernameStream::from_deduped_files(vec![missing]).unwrap();
            it.collect()
        },
    );
    // open_next() returns Err for the missing file; the lossy Iterator impl
    // logs a warning and advances past it, then returns None when the file
    // list is exhausted. Net result: an empty stream, no panic, no hang.
    assert!(result.is_empty(), "got: {:?}", result);
}

#[test]
fn username_stream_terminates_on_invalid_utf8_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("bad_utf8.txt");

    // Mix of valid lines + a bare invalid UTF-8 sequence in the middle.
    // 0x80 by itself is an invalid UTF-8 continuation byte.
    {
        let mut f = File::create(&p).unwrap();
        f.write_all(b"alice\n").unwrap();
        f.write_all(&[0x80, 0xC3, 0x28]).unwrap(); // invalid sequence
        f.write_all(b"\n").unwrap();
        f.write_all(b"bob\n").unwrap();
    }

    let p_clone = p.clone();
    let names: Vec<String> = run_with_timeout(
        "username_stream_bad_utf8",
        Duration::from_secs(5),
        move || {
            let it = UsernameStream::from_deduped_files(vec![p_clone]).unwrap();
            it.collect()
        },
    );
    // The iterator must terminate. Valid lines should still appear; the bad line is dropped.
    assert!(names.contains(&"alice".to_string()), "got: {:?}", names);
    assert!(names.contains(&"bob".to_string()), "got: {:?}", names);
}

#[test]
fn username_stream_handles_multiple_files_including_a_broken_one() {
    let dir = tempfile::tempdir().unwrap();
    let good1 = dir.path().join("a.txt");
    let bad = dir.path().join("b.txt");
    let good2 = dir.path().join("c.txt");

    File::create(&good1).unwrap().write_all(b"x\ny\n").unwrap();
    {
        let mut f = File::create(&bad).unwrap();
        f.write_all(&[0xFF, 0xFE]).unwrap(); // invalid UTF-8
        f.write_all(b"\n").unwrap();
    }
    File::create(&good2).unwrap().write_all(b"z\n").unwrap();

    let files = vec![good1, bad, good2];
    let names: Vec<String> = run_with_timeout(
        "username_stream_mixed_files",
        Duration::from_secs(5),
        move || {
            let it = UsernameStream::from_deduped_files(files).unwrap();
            it.collect()
        },
    );
    // All good lines are surfaced; iterator terminates.
    for expected in ["x", "y", "z"] {
        assert!(
            names.contains(&expected.to_string()),
            "missing {} in {:?}",
            expected,
            names
        );
    }
}
