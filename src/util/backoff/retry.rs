use std::io;
use std::thread::sleep;
use std::time::Duration;

/// Default retry count for Windows-friendly file operations.
///
/// These retry budgets cover transient filter-driver, antivirus, sharing, USB,
/// and removable-volume failures matched by the retry classifier (Win32 error
/// codes 5, 21, 32, 33, 225, 433, 1006, 1117, and 1224). Keep the values stable
/// unless all atomic-write/backoff call sites are intentionally retuned.
pub const DEFAULT_BACKOFF_TRIES: usize = 16;

/// Short retry count for best-effort cleanup/removal paths.
pub const SHORT_BACKOFF_TRIES: usize = 8;

/// Default linear backoff delay, in milliseconds, between retry attempts.
pub const DEFAULT_BACKOFF_DELAY_MS: u64 = 50;

// Named Windows error codes commonly seen on filter-driver / sharing /
// removable-volume hiccups. Used by `is_retriable_io_error` below; named so
// the retry classification reads as documentation rather than a wall of magic
// integers. Values match the Win32 `ERROR_*` constants of the same name.
const WIN_ERR_ACCESS_DENIED: i32 = 5; // ERROR_ACCESS_DENIED — often AV/share
const WIN_ERR_NOT_READY: i32 = 21; // ERROR_NOT_READY — device not ready
pub(super) const WIN_ERR_SHARING_VIOLATION: i32 = 32; // ERROR_SHARING_VIOLATION
const WIN_ERR_LOCK_VIOLATION: i32 = 33; // ERROR_LOCK_VIOLATION
const WIN_ERR_VIRUS_INFECTED: i32 = 225; // ERROR_VIRUS_INFECTED — AV/PUA blocked
const WIN_ERR_NO_SUCH_DEVICE: i32 = 433; // ERROR_NO_SUCH_DEVICE
const WIN_ERR_FILE_INVALID: i32 = 1006; // ERROR_FILE_INVALID — volume changed
const WIN_ERR_IO_DEVICE: i32 = 1117; // ERROR_IO_DEVICE
const WIN_ERR_USER_MAPPED_FILE: i32 = 1224; // ERROR_USER_MAPPED_FILE

/// Return true for transient/retriable I/O errors often seen on Windows when
/// filter drivers (AV/backup), USB/NAS volumes, or sharing violations occur.
fn is_retriable_io_error(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(WIN_ERR_ACCESS_DENIED)
            | Some(WIN_ERR_NOT_READY)
            | Some(WIN_ERR_SHARING_VIOLATION)
            | Some(WIN_ERR_LOCK_VIOLATION)
            | Some(WIN_ERR_VIRUS_INFECTED)
            | Some(WIN_ERR_NO_SUCH_DEVICE)
            | Some(WIN_ERR_FILE_INVALID)
            | Some(WIN_ERR_IO_DEVICE)
            | Some(WIN_ERR_USER_MAPPED_FILE)
    )
}

/// Generic retry-with-backoff loop used by the `*_with_backoff` family.
/// Calls `f` up to `tries` times; on a retriable I/O error, sleeps
/// `delay_ms * attempt_number` milliseconds before retrying. Non-retriable
/// errors return immediately. After exhausting retries, returns the last
/// retriable error seen.
pub(super) fn with_backoff<T, F>(tries: usize, delay_ms: u64, mut f: F) -> io::Result<T>
where
    F: FnMut() -> io::Result<T>,
{
    // When a test sets a budget cap via `cap_backoff_budget_for_test`, all
    // `*_with_backoff` callers on that thread get clamped to the cap. This
    // lets tests that deliberately trigger a non-recoverable failure (e.g.
    // publishing onto a directory) skip the full multi-second retry budget
    // while still exercising the production code path verbatim. Zero overhead
    // in production builds: the TLS read is absent unless either `cfg(test)`
    // (lib unit tests) or `feature = "test-utils"` (integration tests) is on.
    #[cfg(any(test, feature = "test-utils"))]
    let (tries, delay_ms) = test_backoff_override(tries, delay_ms);

    let tries = tries.max(1);
    let mut last_err: Option<io::Error> = None;
    for i in 0..tries {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(
                    delay_ms.saturating_mul((i + 1) as u64),
                ));
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err
        .unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "with_backoff: retries exhausted")))
}

#[cfg(any(test, feature = "test-utils"))]
thread_local! {
    /// Test-only per-thread cap on `(tries, delay_ms)` consulted by
    /// `with_backoff`. `None` means "use the caller's budget unchanged."
    static TEST_BACKOFF_BUDGET: std::cell::Cell<Option<(usize, u64)>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(any(test, feature = "test-utils"))]
fn test_backoff_override(tries: usize, delay_ms: u64) -> (usize, u64) {
    TEST_BACKOFF_BUDGET.with(|cell| {
        cell.get()
            .map(|(t, d)| (tries.min(t), delay_ms.min(d)))
            .unwrap_or((tries, delay_ms))
    })
}

/// RAII guard that caps the per-thread retry budget for `with_backoff` to
/// `(max_tries, max_delay_ms)`. Used by tests that deliberately drive a
/// non-recoverable failure and don't want to wait out the full ~10–20 s
/// production retry budget. Resets on drop.
///
/// Available under either `cfg(test)` (the crate's own lib unit tests) or
/// `feature = "test-utils"` (so integration tests in `tests/` can also opt
/// in). Production builds without the feature compile this out entirely.
#[cfg(any(test, feature = "test-utils"))]
pub struct TestBackoffBudgetGuard;

#[cfg(any(test, feature = "test-utils"))]
impl Drop for TestBackoffBudgetGuard {
    fn drop(&mut self) {
        TEST_BACKOFF_BUDGET.with(|cell| cell.set(None));
    }
}

/// Cap the per-thread `with_backoff` budget for the lifetime of the returned
/// guard. Used by tests that drive a deliberately non-recoverable failure
/// (e.g. publishing onto a directory) — without the cap, the production
/// 20-try × 50 ms linear backoff burns ~10–20 s before the failure surfaces.
///
/// The cap is *clamped*, not replaced: callers using a smaller production
/// budget keep theirs. Available under `cfg(test)` (lib unit tests) or
/// `feature = "test-utils"` (integration tests opting in).
#[cfg(any(test, feature = "test-utils"))]
pub fn cap_backoff_budget_for_test(max_tries: usize, max_delay_ms: u64) -> TestBackoffBudgetGuard {
    TEST_BACKOFF_BUDGET.with(|cell| cell.set(Some((max_tries, max_delay_ms))));
    TestBackoffBudgetGuard
}
