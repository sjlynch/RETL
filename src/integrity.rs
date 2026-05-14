use crate::concurrency::for_each_file_limited;
use crate::paths::{discover_all, log_missing_month_warnings, plan_files_checked, FileJob};
use crate::progress::make_count_progress;
use crate::util::{open_with_backoff, with_thread_pool};
use crate::RedditETL;
use anyhow::{Context, Result};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
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

fn validate_integrity_job(job: &FileJob, mode: IntegrityMode) -> Result<()> {
    match mode {
        IntegrityMode::Quick { sample_bytes } => quick_validate_zst(&job.path, sample_bytes),
        IntegrityMode::Full => validate_zst_full(&job.path),
    }
}

fn run_integrity_checks<F, V>(
    files: &[FileJob],
    mode: IntegrityMode,
    file_concurrency: usize,
    parallelism: Option<usize>,
    progress: bool,
    on_failure: &F,
    validate_job: &V,
) -> Result<Vec<(PathBuf, String)>>
where
    F: Fn(&Path, &str) -> Result<()> + Send + Sync,
    V: Fn(&FileJob, IntegrityMode) -> Result<()> + Send + Sync,
{
    let label = match mode {
        IntegrityMode::Quick { .. } => "Integrity (quick)",
        IntegrityMode::Full => "Integrity (full)",
    };
    let pb = if progress {
        Some(make_count_progress(files.len() as u64, label))
    } else {
        None
    };

    let errors = Mutex::new(Vec::<(PathBuf, String)>::new());
    let failed_so_far = AtomicUsize::new(0);
    let heartbeat_reported = AtomicUsize::new(0);

    let fanout = with_thread_pool(parallelism, || {
        for_each_file_limited(files, file_concurrency, |job| -> Result<()> {
            let res = validate_job(job, mode);
            if let Err(e) = res {
                let path = job.path.clone();
                let err = e.to_string();
                {
                    let mut guard = errors.lock().unwrap();
                    guard.push((path.clone(), err.clone()));
                    failed_so_far.store(guard.len(), Ordering::Relaxed);
                }
                on_failure(&path, &err).with_context(|| {
                    format!("streaming integrity failure for {}", path.display())
                })?;
            }

            if let Some(pb) = &pb {
                pb.inc(1);
                let n = failed_so_far.load(Ordering::Relaxed);
                let mut last = heartbeat_reported.load(Ordering::Relaxed);
                while n > last {
                    match heartbeat_reported.compare_exchange(
                        last,
                        n,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            pb.suspend(|| {
                                eprintln!("INTEGRITY: {n} file(s) failed so far");
                            });
                            break;
                        }
                        Err(actual) => last = actual,
                    }
                }
            }

            Ok(())
        })
    });

    if let Some(pb) = pb {
        pb.finish_with_message(if fanout.is_ok() { "done" } else { "failed" });
    }
    fanout?;

    Ok(errors.into_inner().unwrap())
}

impl RedditETL {
    /// Check all monthly `.zst` files (RC/RS depending on `sources`) within the date range
    /// configured on this `RedditETL` instance. Returns a materialized list of
    /// `(path, error_message)` for files that failed the selected integrity check.
    ///
    /// - Set `.sources()` (Comments / Submissions / Both) and `.date_range()` on the builder
    ///   before calling this.
    /// - Progress displays one tick per file (not per byte) to avoid noisy output.
    /// - Use `IntegrityMode::Quick { sample_bytes }` for a fast, best-effort pass, or
    ///   `IntegrityMode::Full` to validate the entire stream.
    /// - Parallelism is controlled by `.parallelism(n)`, while `.file_concurrency(n)`
    ///   bounds the number of zstd decoders in flight.
    ///
    /// This convenience method only returns failures after the run completes. Use
    /// [`RedditETL::check_corpus_integrity_with_failure_sink`] to observe failures
    /// incrementally while still receiving the final collected list.
    pub fn check_corpus_integrity(self, mode: IntegrityMode) -> Result<Vec<(PathBuf, String)>> {
        self.check_corpus_integrity_with_failure_sink(mode, |_path, _err| Ok(()))
    }

    /// Check corpus integrity and call `on_failure` as each bad file is discovered.
    ///
    /// The callback may be invoked concurrently when `.file_concurrency(n) > 1`, so
    /// callers that write to stdout/stderr or an output file should synchronize and
    /// flush inside the callback. The returned `Vec` still materializes the full
    /// failure list for compatibility with existing library callers.
    pub fn check_corpus_integrity_with_failure_sink<F>(
        self,
        mode: IntegrityMode,
        on_failure: F,
    ) -> Result<Vec<(PathBuf, String)>>
    where
        F: Fn(&Path, &str) -> Result<()> + Send + Sync,
    {
        let discovered = discover_all(&self.opts.comments_dir, &self.opts.submissions_dir);
        let files = plan_files_checked(
            &discovered,
            &self.opts.comments_dir,
            &self.opts.submissions_dir,
            self.opts.sources,
            self.opts.start,
            self.opts.end,
        )?;
        log_missing_month_warnings(
            &discovered,
            self.opts.sources,
            self.opts.start,
            self.opts.end,
        );

        run_integrity_checks(
            &files,
            mode,
            self.opts.file_concurrency,
            self.opts.parallelism,
            self.opts.progress,
            &on_failure,
            &validate_integrity_job,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::{FileJob, FileKind};
    use crate::YearMonth;
    use anyhow::anyhow;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    fn fake_jobs(n: usize) -> Vec<FileJob> {
        (0..n)
            .map(|i| FileJob {
                kind: FileKind::Comment,
                ym: YearMonth::new(2006, (i % 12 + 1) as u8),
                path: PathBuf::from(format!("RC_2006-{:02}_{i}.zst", i % 12 + 1)),
            })
            .collect()
    }

    #[test]
    fn integrity_runner_honors_file_concurrency_limit() {
        let jobs = fake_jobs(16);
        let in_flight = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);

        run_integrity_checks(
            &jobs,
            IntegrityMode::Full,
            3,
            Some(8),
            false,
            &|_path, _err| Ok(()),
            &|_job, _mode| {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .unwrap();

        let peak = peak.load(Ordering::SeqCst);
        assert!(
            peak <= 3,
            "peak in-flight validations {peak} exceeded limit 3"
        );
        assert!(peak > 1, "test did not exercise parallel validation");
    }

    #[test]
    fn integrity_failure_sink_observes_failure_before_run_returns() {
        let jobs = fake_jobs(2);
        let bad_path = jobs[0].path.clone();
        let release_slow_job = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel();

        let release_in_thread = release_slow_job.clone();
        let done_in_thread = done.clone();
        let handle = std::thread::spawn(move || {
            let tx = Mutex::new(tx);
            let bad_path_for_validator = bad_path.clone();
            let res = run_integrity_checks(
                &jobs,
                IntegrityMode::Full,
                2,
                Some(2),
                false,
                &|path, err| {
                    tx.lock()
                        .unwrap()
                        .send((path.to_path_buf(), err.to_string()))
                        .unwrap();
                    Ok(())
                },
                &|job, _mode| {
                    if job.path == bad_path_for_validator {
                        return Err(anyhow!("boom"));
                    }
                    while !release_in_thread.load(Ordering::Acquire) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Ok(())
                },
            );
            done_in_thread.store(true, Ordering::Release);
            res
        });

        let (streamed_path, streamed_err) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("failure should stream before integrity run returns");
        assert_eq!(streamed_path, PathBuf::from("RC_2006-01_0.zst"));
        assert!(streamed_err.contains("boom"));
        assert!(
            !done.load(Ordering::Acquire),
            "failure was not observed until after the run completed"
        );

        release_slow_job.store(true, Ordering::Release);
        let bad = handle.join().unwrap().unwrap();
        assert_eq!(bad.len(), 1);
    }
}
