//! Exercises the copy+rename fallback inside `replace_file_atomic_backoff`.
//!
//! `replace_file_atomic_backoff` retries `MoveFileExW(REPLACE_EXISTING)` for
//! ~10s before falling back to: copy `tmp` into a private sibling temp, then
//! `rename` that sibling over `dest`. That fallback runs on Windows when a
//! sharing violation on `dest` refuses to clear within the retry budget — and
//! the sibling rename keeps the swap atomic instead of truncating `dest`.
//!
//! Reproducing this path deterministically requires holding an open `File`
//! handle on `dest` with a restrictive share mode that blocks write/delete
//! access. That blocks both `MoveFileExW(REPLACE_EXISTING)` swaps (the primary
//! rename and the sibling rename); the copy itself targets an unblocked
//! sibling path. The handle is released after the primary rename retry budget
//! elapses so the sibling rename can succeed.
//!
//! Linux's `rename(2)` doesn't have this sharing-violation behavior, so this
//! test is `#[cfg(windows)]`-only.

#![cfg(windows)]

use std::fs;
use std::fs::OpenOptions;
use std::os::windows::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use retl::replace_file_atomic_backoff;

/// `dwShareMode` bit allowing concurrent readers but blocking writers and
/// deleters. Held on `dest` to force `MoveFileExW`/`CopyFileExW` to fail with
/// ERROR_SHARING_VIOLATION (32) — which `is_retriable_io_error` matches, so
/// the function retries instead of bailing.
const FILE_SHARE_READ: u32 = 0x0000_0001;

/// Total backoff budget consumed if all 20 rename retries fail at 50ms base
/// delay × (i+1): 50 * (1+2+...+20) = 10_500ms. We hold the blocking handle
/// just past this so the rename loop is guaranteed to exhaust before the
/// handle drops and the copy fallback can succeed.
const RENAME_RETRY_BUDGET_MS: u64 = 10_500;
const HOLD_HANDLE_MS: u64 = RENAME_RETRY_BUDGET_MS + 600;

/// This test intentionally waits out the full ~10.5 s rename retry budget
/// to verify the copy-fallback path runs only after retries exhaust. That
/// 11 s wall time isn't pathology — it IS the contract under test. Marked
/// `#[ignore]` for daily iteration; run before merging changes to
/// `replace_file_atomic_backoff` with:
///
///     cargo test --test atomic_replace_fallback -- --ignored --nocapture
#[test]
#[ignore = "wide-budget exhaustion verification; run with --ignored before retry-budget changes"]
fn copy_fallback_runs_when_rename_retries_exhaust_then_dest_unlocks() {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let dest: PathBuf = tmpdir.path().join("dest.bin");
    let tmp: PathBuf = tmpdir.path().join("dest.bin.tmp");

    fs::write(&dest, b"OLD CONTENTS").expect("seed dest");
    fs::write(&tmp, b"NEW CONTENTS VIA COPY FALLBACK").expect("seed tmp");

    let blocker_ready = Arc::new(AtomicBool::new(false));
    let blocker_dest = dest.clone();
    let blocker_ready_for_thread = Arc::clone(&blocker_ready);

    // Acquire a restrictive handle on `dest` and hold it just past the rename
    // retry budget. While held, no MoveFileExW(REPLACE_EXISTING) can swap
    // `dest` — both the primary rename and the sibling rename get sharing
    // violations and the function's retry loops engage. The fallback copy
    // targets an unblocked sibling temp path, so it still succeeds.
    let blocker = thread::spawn(move || {
        let handle = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .open(&blocker_dest)
            .expect("open dest with restrictive share_mode");
        blocker_ready_for_thread.store(true, Ordering::Release);
        thread::sleep(Duration::from_millis(HOLD_HANDLE_MS));
        drop(handle);
    });

    // Wait until the blocker has the handle before kicking the writer off.
    let spin_start = Instant::now();
    while !blocker_ready.load(Ordering::Acquire) {
        assert!(
            spin_start.elapsed() < Duration::from_secs(2),
            "blocker thread never acquired its dest handle"
        );
        thread::sleep(Duration::from_millis(5));
    }

    let start = Instant::now();
    replace_file_atomic_backoff(&tmp, &dest)
        .expect("replace_file_atomic_backoff must eventually succeed via copy fallback");
    let elapsed = start.elapsed();

    blocker.join().expect("blocker thread panicked");

    // Sanity guard: if the call returned in well under the rename retry
    // budget, the rename loop didn't actually exhaust and we never exercised
    // the copy fallback. Treat that as a test-environment failure rather than
    // silently passing.
    assert!(
        elapsed >= Duration::from_millis(RENAME_RETRY_BUDGET_MS - 1_500),
        "replace returned in {:?} — too fast for the rename loop to have actually \
         exhausted, so the copy fallback path may not have run",
        elapsed
    );

    // Destination ends up with the new contents...
    let final_contents = fs::read(&dest).expect("read dest after fallback");
    assert_eq!(
        final_contents, b"NEW CONTENTS VIA COPY FALLBACK",
        "dest must hold the tmp file's contents after copy fallback"
    );

    // ...and the staged tmp file is gone (the fallback removes it after the
    // sibling rename publishes its copy).
    assert!(
        !tmp.exists(),
        "tmp file must be cleaned up by the copy+rename fallback path"
    );
}
