use anyhow::{Context, Result};
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;

use super::retry::{
    with_backoff, DEFAULT_BACKOFF_DELAY_MS, DEFAULT_BACKOFF_TRIES, SHORT_BACKOFF_TRIES,
};
#[cfg(test)]
use super::testing::{maybe_inject_retriable_io_error_for_tests, TestIoOp};

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
