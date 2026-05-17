use anyhow::{Context, Result};
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;
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
const WIN_ERR_SHARING_VIOLATION: i32 = 32; // ERROR_SHARING_VIOLATION
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
/// Prefer [`open_with_default_backoff`] unless a caller needs a custom budget.
pub fn open_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::Open, path)?;
        File::open(path)
    })
}

pub fn open_with_default_backoff(path: &Path) -> io::Result<File> {
    open_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

/// Create or truncate a file with retries/backoff for transient errors.
/// Prefer [`create_with_default_backoff`] unless a caller needs a custom budget.
pub fn create_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::Create, path)?;
        File::create(path)
    })
}

pub fn create_with_default_backoff(path: &Path) -> io::Result<File> {
    create_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

/// Create a single directory with retries/backoff for transient errors.
/// Prefer [`create_dir_with_default_backoff`] unless a caller needs a custom budget.
pub(crate) fn create_dir_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<()> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::CreateDir, path)?;
        fs::create_dir(path)
    })
}

pub(crate) fn create_dir_with_default_backoff(path: &Path) -> io::Result<()> {
    create_dir_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

/// Recursively create directories with retries/backoff for transient errors.
/// Prefer [`create_dir_all_with_default_backoff`] unless a caller needs a custom budget.
pub fn create_dir_all_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<()> {
    with_backoff(tries, delay_ms, || {
        #[cfg(test)]
        maybe_inject_retriable_io_error_for_tests(TestIoOp::CreateDirAll, path)?;
        fs::create_dir_all(path)
    })
}

pub fn create_dir_all_with_default_backoff(path: &Path) -> io::Result<()> {
    create_dir_all_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
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

pub fn read_dir_with_default_backoff(path: &Path) -> io::Result<Vec<fs::DirEntry>> {
    read_dir_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

/// Create a brand-new file with retries/backoff for transient errors.
///
/// Unlike [`create_with_backoff`], this uses `create_new(true)` so an
/// unexpected path collision returns `AlreadyExists` instead of truncating an
/// existing staged file.
/// Prefer [`create_new_with_default_backoff`] unless a caller needs a custom budget.
pub fn create_new_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    })
}

pub fn create_new_with_default_backoff(path: &Path) -> io::Result<File> {
    create_new_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

/// Remove a file with retries/backoff for transient errors.
/// Succeeds if the file doesn't exist. Prefer [`remove_with_default_backoff`]
/// or [`remove_with_short_backoff`] unless a caller needs a custom budget.
pub fn remove_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    })
    .with_context(|| format!("remove {}", path.display()))
}

pub fn remove_with_default_backoff(path: &Path) -> Result<()> {
    remove_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

pub fn remove_with_short_backoff(path: &Path) -> Result<()> {
    remove_with_backoff(path, SHORT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

/// Recursively remove a scratch directory with retries/backoff.
/// Succeeds if the directory doesn't exist. Prefer
/// [`remove_dir_all_with_default_backoff`] or [`remove_dir_all_with_short_backoff`]
/// unless a caller needs a custom budget.
pub fn remove_dir_all_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    with_backoff(tries, delay_ms, || match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    })
    .with_context(|| format!("remove_dir_all {}", path.display()))
}

pub fn remove_dir_all_with_default_backoff(path: &Path) -> Result<()> {
    remove_dir_all_with_backoff(path, DEFAULT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
}

pub fn remove_dir_all_with_short_backoff(path: &Path) -> Result<()> {
    remove_dir_all_with_backoff(path, SHORT_BACKOFF_TRIES, DEFAULT_BACKOFF_DELAY_MS)
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
