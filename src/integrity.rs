use crate::paths::{discover_all, plan_files};
use crate::progress::make_count_progress;
use crate::util::{open_with_backoff, with_thread_pool};
use crate::RedditETL;
use anyhow::Result;
use rayon::prelude::*;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use zstd::stream::read::Decoder;

/// Prevents "Frame requires too much memory" on large Reddit dumps.
const ZSTD_WINDOW_LOG_MAX: u32 = 31;

/// QUICK check: attempt to decode up to `max_decompressed_bytes` and stop.
pub fn quick_validate_zst(path: &Path, max_decompressed_bytes: u64) -> Result<()> {
    let file = open_with_backoff(path, 16, 50)?;
    let mut decoder = Decoder::new(file)?;
    decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
    let mut limited = decoder.take(max_decompressed_bytes);
    io::copy(&mut limited, &mut io::sink())?;
    Ok(())
}

/// FULL check: decode the entire stream to EOF.
///
/// When the file was produced with `Encoder::include_checksum(true)` (zstd
/// frames carry a trailing XXH64 over the original uncompressed data), the
/// stock `zstd::Decoder` validates that checksum during decode and surfaces
/// a mismatch as an error from `read`/`io::copy`. So decoding to EOF here
/// is sufficient to verify the trailing checksum — no separate XXH64 pass
/// is needed. (Bit-flips inside compressed payloads typically also fail
/// earlier with a frame/entropy decode error.)
pub fn validate_zst_full(path: &Path) -> Result<()> {
    let file = open_with_backoff(path, 16, 50)?;
    let mut decoder = Decoder::new(file)?;
    decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
    io::copy(&mut decoder, &mut io::sink())?;
    Ok(())
}

/// Mode for integrity checks.
#[derive(Clone, Copy, Debug)]
pub enum IntegrityMode {
    /// Decode only the first `sample_bytes` (decompressed) per file.
    /// Fast and catches early corruption; cannot detect late/trailing corruption.
    Quick { sample_bytes: u64 },
    /// Decode entire stream; slowest but most thorough (validates checksums).
    Full,
}

impl RedditETL {
    /// Check all monthly `.zst` files (RC/RS depending on `sources`) within the date range
    /// configured on this `RedditETL` instance. Returns a list of `(path, error_message)` for
    /// files that failed the selected integrity check.
    ///
    /// - Set `.sources()` (Comments / Submissions / Both) and `.date_range()` on the builder
    ///   before calling this.
    /// - Progress displays one tick per file (not per byte) to avoid noisy output.
    /// - Use `IntegrityMode::Quick { sample_bytes }` for a fast, best-effort pass, or
    ///   `IntegrityMode::Full` to validate the entire stream.
    ///
    /// Parallelism is controlled by `.with_parallelism(n)` (passed to a scoped Rayon pool).
    /// Integrity checks are CPU-bound (decompression), so we run all files through a single
    /// `par_iter()` and let Rayon schedule them across the pool.
    pub fn check_corpus_integrity(self, mode: IntegrityMode) -> Result<Vec<(PathBuf, String)>> {
        let discovered = discover_all(&self.opts.comments_dir, &self.opts.submissions_dir);
        let files = plan_files(&discovered, self.opts.sources, self.opts.start, self.opts.end);

        let label = match mode {
            IntegrityMode::Quick { .. } => "Integrity (quick)",
            IntegrityMode::Full => "Integrity (full)",
        };
        let pb = if self.opts.progress {
            Some(make_count_progress(files.len() as u64, label))
        } else {
            None
        };

        let errors = Mutex::new(Vec::<(PathBuf, String)>::new());

        let validate = |job: &crate::paths::FileJob| {
            let res = match mode {
                IntegrityMode::Quick { sample_bytes } => quick_validate_zst(&job.path, sample_bytes),
                IntegrityMode::Full => validate_zst_full(&job.path),
            };
            if let Err(e) = res {
                errors.lock().unwrap().push((job.path.clone(), e.to_string()));
            }
            if let Some(pb) = &pb {
                pb.inc(1);
            }
        };

        if self.opts.file_concurrency <= 1 {
            for job in &files {
                validate(job);
            }
        } else {
            with_thread_pool(self.opts.parallelism, || {
                files.par_iter().for_each(&validate);
            });
        }

        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }
        Ok(errors.into_inner().unwrap())
    }
}
