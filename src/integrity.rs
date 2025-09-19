use crate::paths::{discover_all, plan_files};
use crate::progress::make_count_progress;
use crate::util::init_tracing_once;
use crate::zstd_jsonl::{quick_validate_zst, validate_zst_full};
use crate::RedditETL;
use anyhow::Result;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::Mutex;

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
    pub fn check_corpus_integrity(self, mode: IntegrityMode) -> Result<Vec<(PathBuf, String)>> {
        init_tracing_once();
        if let Some(n) = self.opts.parallelism {
            if n > 0 {
                let _ = rayon::ThreadPoolBuilder::new().num_threads(n).build_global();
            }
        }

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

        // Bound parallelism to file_concurrency to avoid excessive I/O pressure.
        if self.opts.file_concurrency <= 1 {
            for job in &files {
                let res = match mode {
                    IntegrityMode::Quick { sample_bytes } => quick_validate_zst(&job.path, sample_bytes),
                    IntegrityMode::Full => validate_zst_full(&job.path),
                };
                if let Err(e) = res {
                    errors.lock().unwrap().push((job.path.clone(), e.to_string()));
                }
                if let Some(pb) = &pb { pb.inc(1); }
            }
        } else {
            for chunk in files.chunks(self.opts.file_concurrency) {
                chunk.par_iter().for_each(|job| {
                    let res = match mode {
                        IntegrityMode::Quick { sample_bytes } => quick_validate_zst(&job.path, sample_bytes),
                        IntegrityMode::Full => validate_zst_full(&job.path),
                    };
                    if let Err(e) = res {
                        errors.lock().unwrap().push((job.path.clone(), e.to_string()));
                    }
                    if let Some(pb) = &pb { pb.inc(1); }
                });
            }
        }

        if let Some(pb) = pb {
            pb.finish_with_message("done");
        }
        Ok(errors.into_inner().unwrap())
    }
}
