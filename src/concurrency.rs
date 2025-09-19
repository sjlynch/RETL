//! Concurrency helper: limit the number of monthly files processed in parallel.

use crate::paths::FileJob;
use anyhow::Result;
use rayon::prelude::*;

/// Limit parallelism across monthly files: at most `limit` decoders in flight.
pub fn for_each_file_limited<F>(files: &[FileJob], limit: usize, f: F) -> Result<()>
where
    F: Sync + Fn(&FileJob) -> Result<()>,
{
    if limit <= 1 {
        for job in files {
            f(job)?;
        }
        return Ok(());
    }
    for chunk in files.chunks(limit) {
        chunk.par_iter().try_for_each(|job| f(job))?;
    }
    Ok(())
}
