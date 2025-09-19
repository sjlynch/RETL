//! Progress reporting utilities: global byte-based progress bar and total size helper.

use crate::paths::FileJob;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fs;
use std::sync::{Arc, OnceLock};

/// Optional global MultiProgress that allows multiple bars to render concurrently.
/// If unset, progress bars draw to the default terminal target.
static GLOBAL_MP: OnceLock<Arc<MultiProgress>> = OnceLock::new();

/// Install a global MultiProgress used by all subsequently created progress bars.
/// Safe to call once; additional calls are ignored.
pub fn set_global_multiprogress(mp: Arc<MultiProgress>) {
    let _ = GLOBAL_MP.set(mp);
}

fn new_bar(total: u64) -> ProgressBar {
    if let Some(mp) = GLOBAL_MP.get() {
        mp.add(ProgressBar::new(total))
    } else {
        ProgressBar::new(total)
    }
}

#[allow(dead_code)]
pub fn make_progress_bar(total_bytes: u64) -> ProgressBar {
    make_progress_bar_labeled(total_bytes, None)
}

pub fn make_progress_bar_labeled(total_bytes: u64, label: Option<&str>) -> ProgressBar {
    let pb = new_bar(total_bytes);
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg} {bytes:>10}/{total_bytes:<10} [{bar:.cyan/blue}] {percent:>3}%  \
         {bytes_per_sec}  elapsed: {elapsed_precise}  eta: {eta_precise}"
    )
    .unwrap()
    .progress_chars("█▉▊▋▌▍▎▏  ");
    pb.set_style(style);
    if let Some(msg) = label {
        pb.set_message(msg.to_string());
    }
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

/// Count-style progress bar (items processed out of total), with an optional label.
pub fn make_count_progress(total: u64, label: &str) -> ProgressBar {
    let pb = new_bar(total);
    let style = ProgressStyle::with_template(
        "{spinner:.green} {msg} {pos}/{len} [{bar:.cyan/blue}] {percent:>3}%  \
         it/s: {per_sec}  elapsed: {elapsed_precise}  eta: {eta_precise}"
    )
    .unwrap()
    .progress_chars("█▉▊▋▌▍▎▏  ");
    pb.set_style(style);
    if !label.is_empty() {
        pb.set_message(label.to_string());
    }
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}

pub fn total_compressed_size(files: &[FileJob]) -> u64 {
    files
        .iter()
        .map(|j| fs::metadata(&j.path).map(|m| m.len()).unwrap_or(0))
        .sum()
}

/// A small, ergonomic wrapper around `indicatif` progress bars.
/// Use either `ProgressScope::bytes(..)` or `ProgressScope::count(..)`.
/// - `inc_bytes(delta)` / `inc_items(delta)` increments progress
/// - `finish(msg)` finalizes the bar with a message
pub struct ProgressScope {
    pb: ProgressBar,
    mode: Mode,
}

enum Mode { Bytes, Count }

impl ProgressScope {
    pub fn bytes<T: Into<String>>(label: T, total_bytes: u64) -> Self {
        let pb = make_progress_bar_labeled(total_bytes, Some(&label.into()));
        Self { pb, mode: Mode::Bytes }
    }
    pub fn count<T: Into<String>>(label: T, total: u64) -> Self {
        let pb = {
            let pb = new_bar(total);
            let style = ProgressStyle::with_template(
                "{spinner:.green} {msg} {pos}/{len} [{bar:.cyan/blue}] {percent:>3}%  \
                 it/s: {per_sec}  elapsed: {elapsed_precise}  eta: {eta_precise}"
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  ");
            pb.set_style(style);
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            pb
        };
        let label_str = label.into();
        if !label_str.is_empty() {
            pb.set_message(label_str);
        }
        Self { pb, mode: Mode::Count }
    }
    #[inline] pub fn inc_bytes(&self, delta: u64) { let _ = &self.mode; self.pb.inc(delta); }
    #[inline] pub fn inc_items(&self, delta: u64) { let _ = &self.mode; self.pb.inc(delta); }
    pub fn finish<T: Into<String>>(&self, msg: T) { self.pb.finish_with_message(msg.into()); }
}
