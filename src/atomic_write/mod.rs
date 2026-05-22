//! Atomic file writers for partitioned outputs.
//!
//! Pattern: write to a unique
//! `<staging_dir>/<filename>.retl-<pid>-<nonce>.inprogress`, finalize the
//! writer (flushing buffers and finishing the zstd frame), then
//! atomically replace the final destination with the staged file. A run that
//! crashes mid-write leaves a uniquely owned `.inprogress` artifact in the
//! staging directory — never a partial, unreadable file at the published path.
//!
//! Staged names include the writer process ID and a per-process nonce so
//! concurrent RETL processes sharing an output root cannot open/truncate each
//! other's scratch files. Stale sweeping only deletes staged files whose owner
//! PID is no longer running; live or legacy/unowned staged files are left in
//! place with a warning.

mod naming;
mod sweep;
#[cfg(test)]
mod testing;
mod writer;

/// Directory used to stage `*.inprogress` files under an output root.
pub const STAGING_DIR_NAME: &str = "_staging";

pub(crate) use naming::{unique_inprogress_path, INPROGRESS_EXT};
pub use sweep::{sweep_stale_atomic_replace_tmp, sweep_stale_inprogress};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use testing::{set_stage_path_observer_for_tests, StagePathObserverGuard};
pub(crate) use writer::write_json_pretty_atomic;
pub use writer::{
    ensure_staging_dir, write_at_path_atomic, write_jsonl_atomic, write_jsonl_atomic_if,
    write_text_atomic, write_zst_atomic_if,
};
