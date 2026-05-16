mod cfg;
mod merge;
mod runs;

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

pub use cfg::DedupeCfg;
pub use merge::merge_runs_sorted;
pub use runs::build_runs_sorted;

pub(crate) use merge::merge_runs_sorted_with_key_stats;
pub(crate) use runs::build_runs_sorted_with_key_stats;

#[inline]
fn note_key_extraction_failed(counter: Option<&AtomicU64>) {
    if let Some(counter) = counter {
        counter.fetch_add(1, AtomicOrdering::Relaxed);
    }
}
