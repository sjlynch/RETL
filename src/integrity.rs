use crate::concurrency::for_each_file_limited;
use crate::paths::{
    discover_sources_checked, log_missing_month_warnings, plan_files_checked, FileJob,
};
use crate::progress::make_count_progress;
use crate::util::with_thread_pool;
use crate::RedditETL;
use anyhow::{Context, Result};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use zstd::stream::read::Decoder;

/// Prevents "Frame requires too much memory" on large Reddit dumps.
const ZSTD_WINDOW_LOG_MAX: u32 = 31;
const ZERO_SAMPLE_BYTES_ERROR: &str =
    "--sample-bytes must be > 0; use --mode full for complete validation";

/// Whether a [`quick_validate_zst`] sample covered the whole file or only a
/// decompressed prefix.
///
/// Quick mode caps decode at `max_decompressed_bytes`. When a file's
/// decompressed size is *smaller* than that budget the cap is never hit, the
/// decode reaches EOF, and the trailing zstd checksum is verified — so quick
/// mode on a small file is equivalent to a full check. `QuickOutcome` lets a
/// caller tell that case apart from a genuine prefix-only sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuickOutcome {
    /// The decompressed stream reached EOF before the sample budget was
    /// exhausted. The entire frame — including its trailing checksum — was
    /// decoded, so this is equivalent to a [`validate_zst_full`] check.
    FullyDecoded,
    /// The sample budget was exhausted before EOF; only a decompressed prefix
    /// was validated. Corruption past the sampled prefix (including a bad
    /// trailing checksum) is *not* detected — use [`validate_zst_full`] for
    /// that.
    PrefixOnly,
}

/// QUICK check: attempt to decode up to `max_decompressed_bytes` and stop.
///
/// `max_decompressed_bytes` must be positive. The returned [`QuickOutcome`]
/// reports whether the sample covered the whole file ([`QuickOutcome::FullyDecoded`])
/// or only a prefix ([`QuickOutcome::PrefixOnly`]): a file whose decompressed
/// size is at or below `max_decompressed_bytes` is decoded to EOF and is
/// therefore validated as thoroughly as [`validate_zst_full`] would. Use
/// [`validate_zst_full`] directly when you need that guarantee unconditionally.
pub fn quick_validate_zst(path: &Path, max_decompressed_bytes: u64) -> Result<QuickOutcome> {
    if max_decompressed_bytes == 0 {
        anyhow::bail!(ZERO_SAMPLE_BYTES_ERROR);
    }

    let file = crate::util::open_with_default_backoff(path)?;
    let mut decoder = Decoder::new(file)?;
    decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
    let mut limited = decoder.take(max_decompressed_bytes);
    let decoded = io::copy(&mut limited, &mut io::sink())?;
    if decoded < max_decompressed_bytes {
        // The decompressed stream hit EOF before the sample budget — the
        // whole frame (and its trailing checksum) was decoded and verified.
        return Ok(QuickOutcome::FullyDecoded);
    }
    // The sample budget was exhausted. Probe one more byte to distinguish a
    // file whose decompressed size is *exactly* the budget (fully decoded,
    // checksum verified by the EOF read) from one that still has data — and
    // therefore unverified trailing content — past the sampled prefix.
    let mut decoder = limited.into_inner();
    if decoder.read(&mut [0u8; 1])? == 0 {
        Ok(QuickOutcome::FullyDecoded)
    } else {
        Ok(QuickOutcome::PrefixOnly)
    }
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
    let file = crate::util::open_with_default_backoff(path)?;
    let mut decoder = Decoder::new(file)?;
    decoder.window_log_max(ZSTD_WINDOW_LOG_MAX)?;
    io::copy(&mut decoder, &mut io::sink())?;
    Ok(())
}

/// Mode for integrity checks.
#[derive(Clone, Copy, Debug)]
pub enum IntegrityMode {
    /// Decode only the first `sample_bytes` (decompressed) per file.
    ///
    /// `sample_bytes` must be positive. Quick mode is fast and catches early
    /// corruption; it cannot detect late/trailing corruption beyond the sampled
    /// prefix.
    Quick { sample_bytes: u64 },
    /// Decode entire stream; slowest but most thorough (validates checksums).
    Full,
}

fn validate_integrity_mode(mode: IntegrityMode) -> Result<()> {
    if let IntegrityMode::Quick { sample_bytes: 0 } = mode {
        anyhow::bail!(ZERO_SAMPLE_BYTES_ERROR);
    }
    Ok(())
}

fn validate_integrity_job(job: &FileJob, mode: IntegrityMode) -> Result<()> {
    match mode {
        // Quick mode's prefix-vs-full distinction is surfaced by
        // [`quick_validate_zst`] for direct callers; the corpus runner only
        // cares whether the file decoded without error.
        IntegrityMode::Quick { sample_bytes } => {
            quick_validate_zst(&job.path, sample_bytes).map(|_| ())
        }
        IntegrityMode::Full => validate_zst_full(&job.path),
    }
}

/// Upper bound on `(path, error)` pairs retained by a corpus integrity run.
///
/// On a corpus where many or all files are corrupt — a wrong volume, a
/// wrong-format directory — retaining one error string per file (each
/// carrying a long zstd/anyhow cause chain) would grow memory without bound.
/// The runner therefore keeps at most this many failures in
/// [`IntegrityReport::failures`]; failures beyond the cap are still delivered
/// to the failure sink as they happen and counted in
/// [`IntegrityReport::failure_count`], but are not held in memory.
pub const MAX_RETAINED_FAILURES: usize = 1024;

/// Outcome of a corpus integrity run.
///
/// `failures` is **capped** at [`MAX_RETAINED_FAILURES`] entries so a run over
/// an all-corrupt corpus does not accumulate an unbounded list of error
/// strings; `dropped` counts failures observed past that cap. Always use
/// [`failure_count`](Self::failure_count) — not `failures.len()` — for the
/// true number of bad files. When a streaming failure sink is supplied to
/// [`RedditETL::check_corpus_integrity_with_failure_sink`], every failure
/// (including dropped ones) is still reported through that sink as it occurs.
#[derive(Debug, Default, Clone)]
pub struct IntegrityReport {
    /// `(path, error_message)` for failed files in completion order, capped at
    /// [`MAX_RETAINED_FAILURES`] entries.
    pub failures: Vec<(PathBuf, String)>,
    /// Failures observed beyond [`MAX_RETAINED_FAILURES`] and dropped from
    /// `failures` to keep memory bounded. `0` when nothing was dropped.
    pub dropped: usize,
}

impl IntegrityReport {
    /// Total number of files that failed the integrity check, including any
    /// dropped from the retained `failures` list.
    pub fn failure_count(&self) -> usize {
        self.failures.len() + self.dropped
    }

    /// `true` when every checked file passed the integrity check.
    pub fn is_ok(&self) -> bool {
        self.failure_count() == 0
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
) -> Result<IntegrityReport>
where
    F: Fn(&Path, &str) -> Result<()> + Send + Sync,
    V: Fn(&FileJob, IntegrityMode) -> Result<()> + Send + Sync,
{
    validate_integrity_mode(mode)?;

    let label = match mode {
        IntegrityMode::Quick { .. } => "Integrity (quick)",
        IntegrityMode::Full => "Integrity (full)",
    };
    let pb = if progress {
        Some(make_count_progress(files.len() as u64, label))
    } else {
        None
    };

    // `failures` is capped at MAX_RETAINED_FAILURES to keep memory bounded on
    // all-corrupt corpora; `total_failures` counts every failure regardless of
    // the cap so the heartbeat and the returned `failure_count` stay accurate.
    let failures = Mutex::new(Vec::<(PathBuf, String)>::new());
    let total_failures = AtomicUsize::new(0);
    let heartbeat_reported = AtomicUsize::new(0);

    let fanout = with_thread_pool(parallelism, || {
        for_each_file_limited(files, file_concurrency, |job| -> Result<()> {
            let res = validate_job(job, mode);
            if let Err(e) = res {
                let path = &job.path;
                let err = e.to_string();
                total_failures.fetch_add(1, Ordering::Relaxed);
                {
                    let mut guard = failures.lock().unwrap();
                    if guard.len() < MAX_RETAINED_FAILURES {
                        guard.push((path.clone(), err.clone()));
                    }
                }
                // The sink observes *every* failure, even those past the
                // retention cap, so a long all-corrupt run still streams.
                on_failure(path, &err).with_context(|| {
                    format!("streaming integrity failure for {}", path.display())
                })?;
            }

            if let Some(pb) = &pb {
                pb.inc(1);
                let n = total_failures.load(Ordering::Relaxed);
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

    let failures = failures.into_inner().unwrap();
    let dropped = total_failures
        .load(Ordering::Relaxed)
        .saturating_sub(failures.len());
    Ok(IntegrityReport { failures, dropped })
}

impl RedditETL {
    /// Check all monthly `.zst` files (RC/RS depending on `sources`) within the date range
    /// configured on this `RedditETL` instance. Returns an [`IntegrityReport`]
    /// describing the files that failed the selected integrity check.
    ///
    /// - Set `.sources()` (Comments / Submissions / Both) and `.date_range()` on the builder
    ///   before calling this.
    /// - Progress displays one tick per file (not per byte) to avoid noisy output.
    /// - Use `IntegrityMode::Quick { sample_bytes }` with a positive sample size for
    ///   a fast, best-effort prefix pass, or `IntegrityMode::Full` to validate the
    ///   entire stream.
    /// - Parallelism is controlled by `.parallelism(n)`, while `.file_concurrency(n)`
    ///   bounds the number of zstd decoders in flight.
    ///
    /// The report's `failures` list is capped at [`MAX_RETAINED_FAILURES`] so an
    /// all-corrupt corpus cannot grow memory without bound; consult
    /// [`IntegrityReport::failure_count`] for the true total. This convenience
    /// method only returns failures after the run completes — use
    /// [`RedditETL::check_corpus_integrity_with_failure_sink`] to observe failures
    /// incrementally.
    pub fn check_corpus_integrity(self, mode: IntegrityMode) -> Result<IntegrityReport> {
        self.check_corpus_integrity_with_failure_sink(mode, |_path, _err| Ok(()))
    }

    /// Check corpus integrity and call `on_failure` as each bad file is discovered.
    ///
    /// The callback may be invoked concurrently when `.file_concurrency(n) > 1`, so
    /// callers that write to stdout/stderr or an output file should synchronize and
    /// flush inside the callback.
    ///
    /// `on_failure` observes **every** failure as it happens — that is the point
    /// of the streaming API. The returned [`IntegrityReport`], by contrast,
    /// retains at most [`MAX_RETAINED_FAILURES`] `(path, error)` pairs and counts
    /// the rest in [`IntegrityReport::dropped`]: on a corpus where every file is
    /// corrupt this keeps the run's memory bounded instead of accumulating one
    /// long zstd/anyhow error string per file. A caller that needs every failure
    /// detail should record it from the sink rather than from the returned list.
    pub fn check_corpus_integrity_with_failure_sink<F>(
        self,
        mode: IntegrityMode,
        on_failure: F,
    ) -> Result<IntegrityReport>
    where
        F: Fn(&Path, &str) -> Result<()> + Send + Sync,
    {
        validate_integrity_mode(mode)?;

        let discovered = discover_sources_checked(
            &self.opts.comments_dir,
            &self.opts.submissions_dir,
            self.opts.sources,
        )?;
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
        let report = handle.join().unwrap().unwrap();
        assert_eq!(report.failure_count(), 1);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.dropped, 0);
    }

    /// Streaming mode must not retain one error string per file on an
    /// all-corrupt corpus: the retained list is capped at
    /// `MAX_RETAINED_FAILURES` while the sink still observes every failure and
    /// `failure_count()` still reports the true total. This is the memory
    /// guard from the integrity capability-gap fix.
    #[test]
    fn streaming_failure_sink_caps_retained_list_on_all_corrupt_corpus() {
        let file_count = MAX_RETAINED_FAILURES * 3;
        let jobs = fake_jobs(file_count);
        let streamed = AtomicUsize::new(0);

        let report = run_integrity_checks(
            &jobs,
            IntegrityMode::Full,
            4,
            Some(4),
            false,
            &|_path, _err| {
                streamed.fetch_add(1, Ordering::Relaxed);
                Ok(())
            },
            &|_job, _mode| Err(anyhow!("every file is corrupt")),
        )
        .unwrap();

        // The sink saw every failure...
        assert_eq!(streamed.load(Ordering::Relaxed), file_count);
        // ...and so does the reported total...
        assert_eq!(report.failure_count(), file_count);
        // ...but the retained list stays bounded regardless of file count.
        assert_eq!(report.failures.len(), MAX_RETAINED_FAILURES);
        assert_eq!(report.dropped, file_count - MAX_RETAINED_FAILURES);
    }
}
