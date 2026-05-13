use crate::query::normalize_str;
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
/// Feel free to extend as needed; merge_extra_exclusions() will add env/file entries.
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
/// All entries are normalized (lowercase), then the list is sort+dedup.
pub fn merge_extra_exclusions(target: &mut Vec<String>) {
    use std::fs::File;
    use std::io::{BufRead, BufReader};

    if let Ok(s) = std::env::var("ETL_EXCLUDE_AUTHORS") {
        for raw in s.split(|c: char| c == ',' || c == ';' || c.is_whitespace()) {
            let n = normalize_str(raw);
            if !n.is_empty() {
                target.push(n);
            }
        }
    }

    if let Ok(path) = std::env::var("ETL_EXCLUDE_AUTHORS_FILE") {
        if !path.trim().is_empty() {
            if let Ok(f) = File::open(&path) {
                let r = BufReader::new(f);
                for line in r.lines().flatten() {
                    let n = normalize_str(&line);
                    if !n.is_empty() {
                        target.push(n);
                    }
                }
            } else {
                tracing::warn!("ETL_EXCLUDE_AUTHORS_FILE is set but cannot be opened: {}", path);
            }
        }
    }

    // normalize + sort + dedup
    for s in target.iter_mut() {
        *s = normalize_str(s);
    }
    target.sort();
    target.dedup();
}

// -------- NEW: scoped Rayon thread pool helper --------

/// Run `f` inside a scoped Rayon thread pool sized to `n` threads. If `n` is
/// `None` (or `Some(0)`), run on the global default pool.
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
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(k)
                .build()
                .expect("failed to build rayon thread pool");
            pool.install(f)
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

/// Open a file with retries/backoff for transient errors.
pub fn open_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || File::open(path))
}

/// Create a file with retries/backoff for transient errors.
pub fn create_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    with_backoff(tries, delay_ms, || File::create(path))
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
