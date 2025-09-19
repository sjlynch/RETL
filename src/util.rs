use crate::query::{normalize_str, QuerySpec};
use anyhow::{Context, Result};

// Existing helpers
pub fn basic_query_allow_all() -> QuerySpec {
    QuerySpec { filter_pseudo_users: true, ..Default::default() }.normalize()
}

static INIT_ONCE: std::sync::Once = std::sync::Once::new();
pub fn init_tracing_once() {
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

// -------- NEW: robust open/create with backoff (Windows-friendly) --------

use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

/// Return true for transient/retriable I/O errors often seen on Windows when
/// filter drivers (AV/backup), USB/NAS volumes, or sharing violations occur.
fn is_retriable_io_error(e: &io::Error) -> bool {
    match e.raw_os_error() {
        // Common Windows transient codes:
        //   5   = Access is denied (often AV/share)
        //   32  = Sharing violation
        //   33  = Lock violation
        //   225 = AV/PUA blocked file
        //   433 = A device which does not exist was specified
        //   1006= Volume externally altered; handle invalid
        //   1117= I/O device error
        //   1224= The requested operation cannot be performed on a file with a user-mapped section open
        //   21  = Device not ready
        Some(5)   | Some(32)  | Some(33)  | Some(225) |
        Some(433) | Some(1006)| Some(1117)| Some(1224)|
        Some(21) => true,
        _ => false,
    }
}

/// Open a file with retries/backoff for transient errors.
pub fn open_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    let mut last_err: Option<io::Error> = None;
    let tries = tries.max(1);
    for i in 0..tries {
        match File::open(path) {
            Ok(f) => return Ok(f),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(delay_ms.saturating_mul((i + 1) as u64)));
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "open failed")))
}

/// Create a file with retries/backoff for transient errors.
pub fn create_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> io::Result<File> {
    let mut last_err: Option<io::Error> = None;
    let tries = tries.max(1);
    for i in 0..tries {
        match File::create(path) {
            Ok(f) => return Ok(f),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(delay_ms.saturating_mul((i + 1) as u64)));
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "create failed")))
}

/// Remove a file with retries/backoff for transient errors.
/// Succeeds if the file doesn't exist.
pub fn remove_with_backoff(path: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    let mut last_err: Option<io::Error> = None;
    for i in 0..tries.max(1) {
        match fs::remove_file(path) {
            Ok(_) => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(delay_ms.saturating_mul((i + 1) as u64)));
                continue;
            }
            Err(e) => return Err(e).with_context(|| format!("remove {}", path.display())),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "remove failed")))
        .with_context(|| format!("remove (retries) {}", path.display()))
}

/// Rename a file with retries/backoff for transient errors.
fn rename_with_backoff(src: &Path, dest: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    let mut last_err: Option<io::Error> = None;
    for i in 0..tries.max(1) {
        match fs::rename(src, dest) {
            Ok(_) => return Ok(()),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(delay_ms.saturating_mul((i + 1) as u64)));
                continue;
            }
            Err(e) => return Err(e).with_context(|| format!("rename {} -> {}", src.display(), dest.display())),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "rename failed")))
        .with_context(|| format!("rename (retries) {} -> {}", src.display(), dest.display()))
}

/// Copy a file with retries/backoff for transient errors.
fn copy_with_backoff(src: &Path, dest: &Path, tries: usize, delay_ms: u64) -> Result<()> {
    let mut last_err: Option<io::Error> = None;
    for i in 0..tries.max(1) {
        match fs::copy(src, dest) {
            Ok(_) => return Ok(()),
            Err(e) if is_retriable_io_error(&e) => {
                last_err = Some(e);
                sleep(Duration::from_millis(delay_ms.saturating_mul((i + 1) as u64)));
                continue;
            }
            Err(e) => return Err(e).with_context(|| format!("copy {} -> {}", src.display(), dest.display())),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "copy failed")))
        .with_context(|| format!("copy (retries) {} -> {}", src.display(), dest.display()))
}

/// Atomically replace `dest` with `tmp` (Windows-friendly).
/// If rename fails (e.g., due to sharing), fall back to copy+remove.
pub fn replace_file_atomic_backoff(tmp: &Path, dest: &Path) -> Result<()> {
    let tries = 20usize;
    let delay_ms = 50u64;
    if dest.exists() {
        remove_with_backoff(dest, tries, delay_ms)?;
    }
    match rename_with_backoff(tmp, dest, tries, delay_ms) {
        Ok(_) => Ok(()),
        Err(_) => {
            copy_with_backoff(tmp, dest, tries, delay_ms)?;
            remove_with_backoff(tmp, tries, delay_ms)?;
            Ok(())
        }
    }
}
