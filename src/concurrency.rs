//! Concurrency helper: limit the number of monthly files processed in parallel.

use crate::paths::FileJob;
use anyhow::Result;
use parking_lot::{Condvar, Mutex};
use rayon::prelude::*;

/// Counting semaphore built on parking_lot primitives. Used to gate how many
/// rayon workers may be inside `f` at once without forcing them to wait at
/// chunk boundaries.
struct Semaphore {
    available: Mutex<usize>,
    cv: Condvar,
}

impl Semaphore {
    fn new(permits: usize) -> Self {
        Self {
            available: Mutex::new(permits),
            cv: Condvar::new(),
        }
    }

    fn acquire(&self) -> Permit<'_> {
        let mut guard = self.available.lock();
        while *guard == 0 {
            self.cv.wait(&mut guard);
        }
        *guard -= 1;
        Permit { sem: self }
    }

    fn release(&self) {
        let mut guard = self.available.lock();
        *guard += 1;
        self.cv.notify_one();
    }
}

/// RAII guard that returns its permit on drop so an early return or panic
/// inside `f` cannot leak slots and stall the rest of the pool.
struct Permit<'a> {
    sem: &'a Semaphore,
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        self.sem.release();
    }
}

/// Limit parallelism across monthly files: at most `limit` decoders in flight.
///
/// Uses a rayon parallel iterator gated by a counting semaphore so workers
/// pick up the next file as soon as a slot frees. The previous chunk-then-
/// par_iter version had workers idle at every chunk boundary waiting for the
/// slowest in-chunk member to finish; this version keeps the pipeline full.
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
    let sem = Semaphore::new(limit);
    files.par_iter().try_for_each(|job| -> Result<()> {
        let _permit = sem.acquire();
        f(job)
    })
}
