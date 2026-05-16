
/// Policy for zstd decode errors that occur after a file has been opened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartialReadPolicy {
    /// Decode errors are fatal. This is the default for corpus scans/exports.
    Strict,
    /// Decode errors are logged and reported through `on_skip`, and the file
    /// is treated as incomplete (`Ok(false)` from status APIs).
    AllowPartial,
}

/// Options for [`for_each_line_with_opts`].
///
/// Every field has a sensible default (`None`/`true`/strict); callers fill in
/// only the knobs they actually use. Lifetime `'a` ties the trait-object
/// callbacks to the caller's stack.
pub struct LineStreamOpts<'a> {
    /// `BufReader` capacity in bytes. `None` → [`DEFAULT_READ_BUF_BYTES`].
    pub read_buf_bytes: Option<usize>,
    /// If `Some`, called with the delta of compressed bytes read after each
    /// line, plus a final flush at EOF. On an allowed partial-read skip, only
    /// the remaining compressed bytes are reported so totals do not exceed the
    /// file length.
    pub progress: Option<&'a mut dyn FnMut(u64)>,
    /// If `Some`, called once when the file is skipped due to a decode error
    /// in [`PartialReadPolicy::AllowPartial`] mode. The error is also logged
    /// via tracing/stderr; strict mode returns the error instead.
    pub on_skip: Option<&'a mut dyn FnMut(&Path, &anyhow::Error)>,
    /// Strict by default. Set to [`PartialReadPolicy::AllowPartial`] only when
    /// the caller is deliberately accepting lossy results and records skipped
    /// paths somewhere machine-readable.
    pub partial_read_policy: PartialReadPolicy,
    /// Sample [`maybe_throttle_low_memory`] every
    /// [`THROTTLE_SAMPLE_MASK`]+1 lines. Set `false` for stages that briefly
    /// allocate a lot (e.g., parent-cache builds) where the backoff would
    /// otherwise dominate runtime.
    pub throttle: bool,
}

impl<'a> Default for LineStreamOpts<'a> {
    fn default() -> Self {
        Self {
            read_buf_bytes: None,
            progress: None,
            on_skip: None,
            partial_read_policy: PartialReadPolicy::Strict,
            throttle: true,
        }
    }
}

/// Stream a zstd JSONL file line-by-line using `opts`, calling `on_line`
/// with each raw `&str` (newline already stripped).
///
/// We request `window_log_max(ZSTD_WINDOW_LOG_MAX)` up front to avoid
/// "Frame requires too much memory" on very large frames. If decoding
/// still fails (e.g., checksum/corruption), strict mode returns a contextual
/// error. In [`PartialReadPolicy::AllowPartial`] mode we log a single warning,
/// invoke `opts.on_skip` (if set), report only the remaining compressed-byte
/// progress needed to reach the file's size (if a progress callback is set),
/// and return `Ok(false)` so callers can keep resume manifests from marking
/// the month complete.
pub fn for_each_line_with_opts_status(
    path: &Path,
    opts: LineStreamOpts<'_>,
    mut on_line: impl FnMut(&str) -> Result<()>,
) -> Result<bool> {
    let LineStreamOpts {
        read_buf_bytes,
        mut progress,
        mut on_skip,
        partial_read_policy,
        throttle,
    } = opts;
    let result = for_each_line_attempt(
        path,
        read_buf_bytes,
        progress.as_deref_mut(),
        throttle,
        &mut on_line,
    );
    match result {
        Ok(()) => Ok(true),
        Err(LineStreamAttemptError::Open(e)) => Err(e),
        Err(LineStreamAttemptError::Decode {
            source,
            bytes_reported,
        }) => {
            let e = zstd_decode_error(path, source);
            match partial_read_policy {
                PartialReadPolicy::Strict => Err(e),
                PartialReadPolicy::AllowPartial => {
                    warn_decode_skip(path, &e);
                    if let Some(cb) = on_skip.as_deref_mut() {
                        cb(path, &e);
                    }
                    // Keep progress bars monotonic on skip without double-counting
                    // bytes already reported by the inner line loop.
                    if let Some(cb) = progress.as_deref_mut() {
                        if let Ok(meta) = fs::metadata(path) {
                            cb(meta.len().saturating_sub(bytes_reported));
                        }
                    }
                    Ok(false)
                }
            }
        }
        Err(LineStreamAttemptError::InvalidLine(e)) => Err(e),
        Err(LineStreamAttemptError::Callback(e)) => Err(e),
    }
}

/// Wrapper for callers that do not need the complete/incomplete boolean.
/// Decode-error behavior is controlled by [`LineStreamOpts::partial_read_policy`]
/// and is strict by default.
pub fn for_each_line_with_opts(
    path: &Path,
    opts: LineStreamOpts<'_>,
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts_status(path, opts, on_line).map(|_| ())
}

/// Tunable line-stream entry point used throughout the library.
///
/// Thin shim over `for_each_line_with_opts` for callers that only need a
/// custom `BufReader` capacity.
pub fn for_each_line_cfg(
    path: &Path,
    read_buf_bytes: usize,
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    for_each_line_with_opts(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            ..Default::default()
        },
        on_line,
    )
}

/// Progress-aware allow-partial streaming **without** the per-line memory
/// throttle. Returns `Ok(false)` when a zstd decode error was tolerated.
///
/// Useful for stages that briefly use more RAM (e.g., building parent
/// caches) where the backoff would otherwise dominate runtime.
pub fn for_each_line_with_progress_cfg_no_throttle_status(
    path: &Path,
    read_buf_bytes: usize,
    mut on_progress: impl FnMut(u64),
    on_line: impl FnMut(&str) -> Result<()>,
) -> Result<bool> {
    for_each_line_with_opts_status(
        path,
        LineStreamOpts {
            read_buf_bytes: Some(read_buf_bytes),
            progress: Some(&mut on_progress),
            throttle: false,
            partial_read_policy: PartialReadPolicy::AllowPartial,
            ..Default::default()
        },
        on_line,
    )
}

/// A `Read` wrapper that counts compressed bytes read.
struct CountingReader<R: Read> {
    inner: R,
    counter: Arc<AtomicU64>,
}
impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.counter.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

enum LineStreamAttemptError {
    Open(anyhow::Error),
    Decode {
        source: anyhow::Error,
        /// Sum of compressed-byte deltas already delivered to the progress
        /// callback. Allow-partial skip handling reports only the remaining
        /// metadata bytes so progress never exceeds 100%.
        bytes_reported: u64,
    },
    /// A decoded JSONL line exceeded the configured cap or was not valid UTF-8.
    /// This is record-level invalid data, not a zstd-frame skip, so it remains
    /// fatal even when `PartialReadPolicy::AllowPartial` is set.
    InvalidLine(anyhow::Error),
    Callback(anyhow::Error),
}

/// Single inner read loop. The two former `_attempt` paths only differed on
/// whether the file was wrapped in `CountingReader` to drive a progress
/// callback — we always wrap it now (the atomic add lands once per
/// `BufReader` refill, ~every 16 KiB, which is noise next to zstd decode
/// cost), and only invoke the callback when one is set.
///
/// The two lifetime parameters decouple the *borrow* lifetime from the
/// trait-object lifetime; without that, lifetime elision would tie the
/// trait object's lifetime to the borrow, which then forces the caller's
/// borrow to span the entire owning struct's lifetime and conflicts with
/// reusing the same callback on the post-error fallback path.
fn for_each_line_attempt<'borrow, 'cb: 'borrow>(
    path: &Path,
    read_buf_bytes: Option<usize>,
    mut on_progress: Option<&'borrow mut (dyn FnMut(u64) + 'cb)>,
    throttle: bool,
    on_line: &mut impl FnMut(&str) -> Result<()>,
) -> std::result::Result<(), LineStreamAttemptError> {
    let file = crate::util::open_with_default_backoff(path).map_err(|e| {
        LineStreamAttemptError::Open(
            anyhow::Error::new(e).context(format!("open zstd input {}", path.display())),
        )
    })?;
    let counter = Arc::new(AtomicU64::new(0));
    let cnt = CountingReader {
        inner: file,
        counter: counter.clone(),
    };

    let mut decoder = Decoder::new(cnt).map_err(|e| LineStreamAttemptError::Decode {
        source: e.into(),
        bytes_reported: 0,
    })?;
    decoder
        .window_log_max(ZSTD_WINDOW_LOG_MAX)
        .map_err(|e| LineStreamAttemptError::Decode {
            source: e.into(),
            bytes_reported: 0,
        })?;

    let cap = read_buf_bytes.unwrap_or(DEFAULT_READ_BUF_BYTES);
    let mut reader = BufReader::with_capacity(cap, decoder);

    let mut buf = String::with_capacity(DEFAULT_READ_BUF_BYTES);
    let mut last: u64 = 0;
    // Sampled cooperative memory backoff: maybe_throttle_low_memory acquires a
    // global Mutex (see src/mem.rs). Now that bucketing/dedupe carry their own
    // bounded backpressure channels, this throttle is a coarse safety net —
    // sample every THROTTLE_SAMPLE_MASK+1 lines to keep mutex contention out
    // of the hot read loop.
    let mut tick: u32 = 0;
    loop {
        let n = match read_line_capped(&mut reader, &mut buf, DEFAULT_MAX_LINE_BYTES, path) {
            Ok(n) => n,
            Err(e) => {
                let is_line_cap = e.kind() == io::ErrorKind::InvalidData
                    && e.to_string().contains("max_line_bytes");
                if is_line_cap {
                    return Err(LineStreamAttemptError::InvalidLine(
                        anyhow::Error::new(e).context(format!(
                            "read zstd JSONL line from {} with max_line_bytes={}",
                            path.display(),
                            DEFAULT_MAX_LINE_BYTES
                        )),
                    ));
                }
                return Err(LineStreamAttemptError::Decode {
                    source: e.into(),
                    bytes_reported: last,
                });
            }
        };
        if n == 0 {
            // Final progress flush at EOF.
            if let Some(cb) = on_progress.as_deref_mut() {
                let cur = counter.load(Ordering::Relaxed);
                if cur > last {
                    cb(cur - last);
                }
            }
            break;
        }
        // Per-line progress delta (preserves prior cadence: drained before
        // the user's `on_line` callback runs so "bytes read" never lags
        // "lines seen").
        if let Some(cb) = on_progress.as_deref_mut() {
            let cur = counter.load(Ordering::Relaxed);
            if cur > last {
                cb(cur - last);
                last = cur;
            }
        }
        on_line(&buf).map_err(LineStreamAttemptError::Callback)?;
        if throttle {
            tick = tick.wrapping_add(1);
            if tick & THROTTLE_SAMPLE_MASK == 0 {
                maybe_throttle_low_memory(0.10);
            }
        }
    }
    Ok(())
}

pub use crate::integrity::{quick_validate_zst, validate_zst_full};
