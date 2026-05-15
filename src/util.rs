use crate::config::clamp_parallelism_threads;
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::query::{normalize_str, QueryBuildError};
use anyhow::{Context, Result};

static INIT_ONCE: std::sync::Once = std::sync::Once::new();

/// Initialize tracing for the binary. Call this once at program startup from
/// `main`. Library code must NOT call this — the binary owns the tracing
/// subscriber.
pub fn init_tracing_for_binary() {
    INIT_ONCE.call_once(|| {
        let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
        let _ = tracing_subscriber::fmt().with_env_filter(env_filter).try_init();
    });
}

// -------- NEW: default bot/service accounts + merging from env/file --------

/// Returns a normalized (lowercase) default list of bot/service authors to exclude.
/// This is a conservative set focused on high-volume/systemic accounts.
/// Feel free to extend as needed; try_merge_extra_exclusions() will add env/file entries.
pub fn default_bot_authors() -> Vec<String> {
    // Hand-curated defaults (normalized to lowercase)
    let defaults = [
        "automoderator",
        "imguralbumbot",
        "autowikibot",
        "remindmebot",
        "totesmessenger",
        "tweet_poster",
        "video_link_bot",
        "gifvbot",
        "helper-bot",
        "github-actions[bot]",
        "slackbot",
        "discordbot",
    ];
    let mut v: Vec<String> = defaults.iter().map(|s| normalize_str(s)).collect();
    v.sort();
    v.dedup();
    v
}

/// Merge extra exclusions from env/file into the provided vector (in-place).
/// - ETL_EXCLUDE_AUTHORS: comma/semicolon/space separated names
/// - ETL_EXCLUDE_AUTHORS_FILE: path to newline-separated file of names
///
/// All entries are normalized (lowercase), then the list is sort+dedup. If
/// `ETL_EXCLUDE_AUTHORS_FILE` is set to a non-blank path, failure to open or
/// fully read it is fatal because the file is part of the query semantics.
/// Per-line reads are bounded by [`DEFAULT_MAX_LINE_BYTES`] so an adversarial
/// exclusions file cannot OOM the process.
pub(crate) fn try_merge_extra_exclusions(
    target: &mut Vec<String>,
) -> std::result::Result<(), QueryBuildError> {
    use std::io::BufReader;

    let mut extras = Vec::new();

    if let Ok(s) = std::env::var("ETL_EXCLUDE_AUTHORS") {
        for raw in s.split(|c: char| c == ',' || c == ';' || c.is_whitespace()) {
            let n = normalize_str(raw);
            if !n.is_empty() {
                extras.push(n);
            }
        }
    }

    if let Some(path_os) = std::env::var_os("ETL_EXCLUDE_AUTHORS_FILE") {
        let path = std::path::PathBuf::from(path_os);
        if !path.to_string_lossy().trim().is_empty() {
            let f = open_with_backoff(&path, 16, 50).map_err(|e| {
                QueryBuildError::new(format!(
                    "ETL_EXCLUDE_AUTHORS_FILE {} cannot be opened: {e}",
                    path.display()
                ))
            })?;
            let mut r = BufReader::new(f);
            let mut line = String::with_capacity(1024);
            let mut line_no: usize = 0;
            loop {
                line_no += 1;
                match read_line_capped(&mut r, &mut line, DEFAULT_MAX_LINE_BYTES, &path) {
                    Ok(0) => break,
                    Ok(_) => {
                        let n = normalize_str(&line);
                        if !n.is_empty() {
                            extras.push(n);
                        }
                    }
                    Err(e) => {
                        return Err(QueryBuildError::new(format!(
                            "ETL_EXCLUDE_AUTHORS_FILE {} could not be read at line {line_no}: {e}",
                            path.display()
                        )));
                    }
                }
            }
        }
    }

    target.extend(extras);

    // normalize + sort + dedup
    for s in target.iter_mut() {
        *s = normalize_str(s);
    }
    target.sort();
    target.dedup();
    Ok(())
}

// -------- NEW: scoped Rayon thread pool helper --------

/// Run `f` inside a scoped Rayon thread pool sized to `n` threads. If `n` is
/// `None` (or `Some(0)`), run on the global default pool. Positive values are
/// clamped through [`crate::config::max_parallelism_limit`]; if Rayon still
/// rejects the pool, RETL logs a warning and safely falls back to the global
/// pool instead of panicking.
///
/// Prefer this over `rayon::ThreadPoolBuilder::build_global()`, which mutates
/// process-wide state, only succeeds for the first caller, and prevents
/// different stages from picking different thread counts.
pub fn with_thread_pool<R, F>(n: Option<usize>, f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    match n {
        Some(k) if k > 0 => {
            let clamped = clamp_parallelism_threads(k, "with_thread_pool");
            match rayon::ThreadPoolBuilder::new().num_threads(clamped).build() {
                Ok(pool) => pool.install(f),
                Err(e) => {
                    tracing::warn!(
                        requested = k,
                        clamped,
                        error = %e,
                        "failed to build scoped Rayon thread pool; falling back to global pool"
                    );
                    f()
                }
            }
        }
        _ => f(),
    }
}

// -------- NEW: robust open/create with backoff (Windows-friendly) --------

use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Named Windows error codes commonly seen on filter-driver / sharing /
// removable-volume hiccups. Used by `is_retriable_io_error` below; named so
// the retry classification reads as documentation rather than a wall of magic
// integers. Values match the Win32 `ERROR_*` constants of the same name.
const WIN_ERR_ACCESS_DENIED: i32 = 5;       // ERROR_ACCESS_DENIED — often AV/share
const WIN_ERR_NOT_READY: i32 = 21;          // ERROR_NOT_READY — device not ready
const WIN_ERR_SHARING_VIOLATION: i32 = 32;  // ERROR_SHARING_VIOLATION
const WIN_ERR_LOCK_VIOLATION: i32 = 33;     // ERROR_LOCK_VIOLATION
const WIN_ERR_VIRUS_INFECTED: i32 = 225;    // ERROR_VIRUS_INFECTED — AV/PUA blocked
const WIN_ERR_NO_SUCH_DEVICE: i32 = 433;    // ERROR_NO_SUCH_DEVICE
const WIN_ERR_FILE_INVALID: i32 = 1006;     // ERROR_FILE_INVALID — volume changed
const WIN_ERR_IO_DEVICE: i32 = 1117;        // ERROR_IO_DEVICE
const WIN_ERR_USER_MAPPED_FILE: i32 = 1224; // ERROR_USER_MAPPED_FILE

static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Return a process-unique scratch directory path under `work_dir`.
///
/// The directory name carries the logical `prefix`, scratch `kind`, PID,
/// process-local counter, and current nanoseconds so separate `retl` processes
/// sharing the same work dir do not reuse fixed shard roots.
pub(crate) fn unique_scratch_dir(work_dir: &Path, prefix: &str, kind: &str) -> PathBuf {
    let counter = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    work_dir.join(format!(
        "{prefix}_{kind}_{}_{}_{}",
        std::process::id(),
        counter,
        nanos
    ))
}

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
fn with_backoff<T, F>(tries: usize, delay_ms: u64, mut f: F) -> io::Result<T>
where
    F: FnMut() -> io::Result<T>,
{
    let tries = tries.max(1);
    let mut last_err: Option<io::Error> = None;
    for i in 0..tries {
        match f() {
            Ok(v) => return Ok(v),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(delay_ms.saturating_mul((i + 1) as u64)));
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "with_backoff: retries exhausted")))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestIoOp {
    Open,
    Create,
    CreateDir,
    CreateDirAll,
    ReadDir,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
enum TestIoPathMatch {
    Exact(PathBuf),
    FileName(String),
}

#[cfg(test)]
impl TestIoPathMatch {
    fn matches(&self, path: &Path) -> bool {
        match self {
            Self::Exact(expected) => expected == path,
            Self::FileName(expected) => path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == expected),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct TestIoFailure {
    op: TestIoOp,
    path_match: TestIoPathMatch,
    remaining: usize,
    raw_os_error: i32,
}

#[cfg(test)]
static TEST_IO_FAILURES: std::sync::Mutex<Vec<TestIoFailure>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) struct TestIoFailureGuard {
    op: TestIoOp,
    path_match: TestIoPathMatch,
}

#[cfg(test)]
impl Drop for TestIoFailureGuard {
    fn drop(&mut self) {
        let mut failures = TEST_IO_FAILURES
            .lock()
            .expect("test I/O failure mutex poisoned");
        failures.retain(|f| !(f.op == self.op && f.path_match == self.path_match));
    }
}

#[cfg(test)]
pub(crate) fn inject_retriable_io_errors_for_tests(
    op: TestIoOp,
    path: impl Into<PathBuf>,
    failures: usize,
) -> TestIoFailureGuard {
    inject_retriable_io_errors_for_match_tests(op, TestIoPathMatch::Exact(path.into()), failures)
}

#[cfg(test)]
pub(crate) fn inject_retriable_io_errors_for_file_name_tests(
    op: TestIoOp,
    file_name: impl Into<String>,
    failures: usize,
) -> TestIoFailureGuard {
    inject_retriable_io_errors_for_match_tests(
        op,
        TestIoPathMatch::FileName(file_name.into()),
        failures,
    )
}

#[cfg(test)]
fn inject_retriable_io_errors_for_match_tests(
    op: TestIoOp,
    path_match: TestIoPathMatch,
    failures: usize,
) -> TestIoFailureGuard {
    let guard = TestIoFailureGuard {
        op,
        path_match: path_match.clone(),
    };
    TEST_IO_FAILURES
        .lock()
        .expect("test I/O failure mutex poisoned")
        .push(TestIoFailure {
            op,
            path_match,
            remaining: failures,
            raw_os_error: WIN_ERR_SHARING_VIOLATION,
        });
    guard
}

#[cfg(test)]
fn maybe_inject_retriable_io_error_for_tests(op: TestIoOp, path: &Path) -> io::Result<()> {
    let mut failures = TEST_IO_FAILURES
        .lock()
        .expect("test I/O failure mutex poisoned");
    if let Some(failure) = failures.iter_mut().find(|failure| {
        failure.op == op && failure.remaining > 0 && failure.path_match.matches(path)
    }) {
        failure.remaining -= 1;
        return Err(io::Error::from_raw_os_error(failure.raw_os_error));
    }
    Ok(())
}

/// Open a file with retries/backoff for transient errors.
pub fn open_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::Open, path)?;
        File::open(path)
    })
}

/// Create or truncate a file with retries/backoff for transient errors.
pub fn create_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::Create, path)?;
        File::create(path)
    })
}

/// Create a single directory with retries/backoff for transient errors.
pub(crate) fn create_dir_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<()> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::CreateDir, path)?;
        fs::create_dir(path)
    })
}

/// Recursively create directories with retries/backoff for transient errors.
pub fn create_dir_all_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<()> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::CreateDirAll, path)?;
        fs::create_dir_all(path)
    })
}

/// Read a directory with retries/backoff for transient errors.
///
/// Entries are collected inside the retry loop so transient errors produced
/// while iterating the `ReadDir` also retry the whole enumeration.
pub fn read_dir_with_backoff(
    path: &Path,
    tries: usize,
    delay_ms: u64,
) -> io::Result<Vec<fs::DirEntry>> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::ReadDir, path)?;
        fs::read_dir(path)?.collect()
    })
}

/// Create a brand-new file with retries/backoff for transient errors.
///
/// Unlike [`create_with_backoff`], this uses `create_new(true)` so an
/// unexpected path collision returns `AlreadyExists` instead of truncating an
/// existing staged file.
pub fn create_new_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    })
}

/// Remove a file with retries/backoff for transient errors.
/// Succeeds if the file doesn't exist.
pub fn remove_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    })
    .with_context(|| format!("remove {}", path.display()))
}

/// Recursively remove a scratch directory with retries/backoff.
/// Succeeds if the directory doesn't exist.
pub(crate) fn remove_dir_all_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    })
    .with_context(|| format!("remove_dir_all {}", path.display()))
}

/// Rename a file with retries/backoff for transient errors.
fn rename_with_backoff(src: &Path, dest: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || fs::rename(src, dest))
        .with_context(|| format!("rename {} -> {}", src.display(), dest.display()))
}

/// Copy a file with retries/backoff for transient errors.
fn copy_with_backoff(src: &Path, dest: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || fs::copy(src, dest))
        .map(|_| ())
        .with_context(|| format!("copy {} -> {}", src.display(), dest.display()))
}

/// Smoothstep curve mapping a measured free-memory fraction onto `[0.0, 1.0]`,
/// used by adaptive buffer sizing in dedupe and bucketing.
///
/// - At or below `soft_low`, returns `0.0` (favor smaller buffers).
/// - At or above `high`, returns `1.0` (favor larger buffers).
/// - Between the two, applies the cubic smoothstep `s*s*(3 - 2*s)` to soften
///   the transition.
///
/// `(high - soft_low)` is floored at `0.05` to keep the divisor non-degenerate
/// when callers are configured with overlapping thresholds.
pub(crate) fn smoothstep_memory_fraction(free: f64, soft_low: f64, high: f64) -> f64 {
    let span = (high - soft_low).max(0.05);
    let scale = ((free - soft_low) / span).clamp(0.0, 1.0);
    scale * scale * (3.0 - 2.0 * scale)
}


/// Atomically replace `dest` with `tmp` (Windows-friendly).
///
/// `std::fs::rename` on Windows is implemented via
/// `MoveFileExW(MOVEFILE_REPLACE_EXISTING)`, which atomically swaps the
/// destination if it exists — concurrent readers see either the old contents
/// or the new contents, never a missing file. We therefore do NOT pre-remove
/// `dest`: a remove-then-rename sequence opens a window where readers observe
/// `NotFound`, breaking the "atomic" contract this function advertises.
///
/// If rename fails (e.g., due to a sharing violation that doesn't clear within
/// the retry budget), fall back to copy+remove. `fs::copy` opens the
/// destination with `CREATE_ALWAYS` on Windows, so it overwrites in place.
pub fn replace_file_atomic_backoff(tmp: &Path, dest: &Path) -> Result<()> {
    let tries = 20usize;
    let delay_ms = 50u64;
    match rename_with_backoff(tmp, dest, tries, delay_ms) {
        Ok(_) => Ok(()),
        Err(_) => {
            copy_with_backoff(tmp, dest, tries, delay_ms)?;
            remove_with_backoff(tmp, tries, delay_ms)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_dir_all_with_backoff_retries_transient_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("nested").join("leaf");
        let _guard = inject_retriable_io_errors_for_tests(TestIoOp::CreateDirAll, &dir, 2);

        create_dir_all_with_backoff(&dir, 3, 0).expect("create dir with retry");

        assert!(
            dir.is_dir(),
            "directory should exist after retried create_dir_all"
        );
    }

    #[test]
    fn read_dir_with_backoff_retries_transient_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let file = tmp.path().join("entry.txt");
        fs::write(&file, b"x").expect("seed file");
        let _guard = inject_retriable_io_errors_for_tests(TestIoOp::ReadDir, tmp.path(), 1);

        let entries = read_dir_with_backoff(tmp.path(), 2, 0).expect("read_dir with retry");
        let names: Vec<_> = entries
            .into_iter()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();

        assert_eq!(names, vec!["entry.txt"]);
    }
}
