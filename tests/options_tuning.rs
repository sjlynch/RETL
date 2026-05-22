use retl::{AdaptiveMemCfg, BucketingCfg, DedupeCfg, ETLOptions};
use std::io;
use std::sync::{Arc, Mutex};
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct CaptureLogs {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl CaptureLogs {
    fn contents(&self) -> String {
        String::from_utf8(self.buf.lock().unwrap().clone()).unwrap()
    }
}

struct CaptureWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl io::Write for CaptureWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CaptureLogs {
    type Writer = CaptureWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CaptureWriter {
            buf: self.buf.clone(),
        }
    }
}

/// Run `f` with a thread-local `WARN`-level subscriber and return its result
/// plus everything that subscriber captured.
fn capture_warnings<R>(f: impl FnOnce() -> R) -> (R, String) {
    let logs = CaptureLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let result = tracing::subscriber::with_default(subscriber, f);
    (result, logs.contents())
}

#[test]
fn passing_zero_to_an_inflight_setter_produces_an_observable_signal() {
    // `inflight_bytes(0)` is the disable-the-cap sentinel, NOT a zero-byte
    // ceiling — it falls back to memory-fraction sampling and risks OOM. A
    // budget computed as `available / n` that rounds to 0 would silently
    // unbound the run, so the setter must warn.
    let (opts, log) = capture_warnings(|| ETLOptions::default().with_inflight_bytes(0));
    assert_eq!(opts.inflight_bytes, 0);
    assert!(
        log.contains("DISABLES the explicit memory cap"),
        "expected a warning when 0 is passed to an inflight setter, got:\n{log}"
    );
    assert!(
        log.contains("disable_inflight_cap"),
        "the warning should point at the explicit opt-out, got:\n{log}"
    );
}

#[test]
fn disable_inflight_cap_is_the_warning_free_opt_out() {
    // The deliberate path for the same sentinel must NOT warn.
    let (opts, log) = capture_warnings(|| ETLOptions::default().disable_inflight_cap());
    assert_eq!(opts.inflight_bytes, 0);
    assert!(
        log.is_empty(),
        "disable_inflight_cap() is the intentional opt-out and must stay quiet, got:\n{log}"
    );
}

#[test]
fn out_of_range_io_buffer_values_are_logged_like_other_clamped_knobs() {
    // Sub-floor request: clamped up to the 8-KiB floor, with a warning
    // (previously raised silently, unlike shard/parallelism/file-concurrency).
    let (low, low_log) = capture_warnings(|| ETLOptions::default().with_io_read_buffer(512));
    assert_eq!(low.read_buffer_bytes, 8 * 1024);
    assert!(
        low_log.contains("minimum that sustains streaming throughput"),
        "expected a floor-clamp warning, got:\n{low_log}"
    );

    // Oversized request: clamped down instead of attempting a usize::MAX
    // BufWriter allocation per file, with a warning.
    let (high, high_log) =
        capture_warnings(|| ETLOptions::default().with_io_write_buffer(usize::MAX));
    assert_eq!(high.write_buffer_bytes, 256 * 1024 * 1024);
    assert!(
        high_log.contains("bound per-file allocation"),
        "expected a ceiling-clamp warning, got:\n{high_log}"
    );

    // In-range request: passes through with no warning at all.
    let (ok, ok_log) =
        capture_warnings(|| ETLOptions::default().with_io_read_buffer(1024 * 1024));
    assert_eq!(ok.read_buffer_bytes, 1024 * 1024);
    assert!(
        ok_log.is_empty(),
        "an in-range IO buffer value must not warn, got:\n{ok_log}"
    );
}

#[test]
fn etl_options_forward_adaptive_mem_and_inflight_groups_to_runtime_cfgs() {
    let adaptive = AdaptiveMemCfg {
        soft_low_frac: 0.99,
        high_frac: 0.999,
        adapt_cooldown_ms: 50,
    };
    let opts = ETLOptions::default()
        .with_io_buffers(32 * 1024, 64 * 1024)
        .with_inflight_bytes(123 * 1024 * 1024)
        .with_inflight_groups(3)
        .with_adaptive_mem(adaptive.clone());

    let dedupe = DedupeCfg::from(&opts);
    assert_eq!(dedupe.mem.soft_low_frac, adaptive.soft_low_frac);
    assert_eq!(dedupe.mem.high_frac, adaptive.high_frac);
    assert_eq!(dedupe.mem.adapt_cooldown_ms, adaptive.adapt_cooldown_ms);
    assert_eq!(dedupe.read_buf_bytes, 32 * 1024);
    assert_eq!(dedupe.write_buf_bytes, 64 * 1024);
    assert_eq!(dedupe.inflight_bytes, 123 * 1024 * 1024);

    let bucketing = BucketingCfg::from(&opts);
    assert_eq!(bucketing.mem.soft_low_frac, adaptive.soft_low_frac);
    assert_eq!(bucketing.mem.high_frac, adaptive.high_frac);
    assert_eq!(bucketing.mem.adapt_cooldown_ms, adaptive.adapt_cooldown_ms);
    assert_eq!(bucketing.inflight_bytes, 123 * 1024 * 1024);
    assert_eq!(bucketing.inflight_groups, 3);
}

#[test]
fn inflight_groups_builder_clamps_zero_to_one() {
    let opts = ETLOptions::default().with_inflight_groups(0);
    assert_eq!(opts.inflight_groups, 1);
    assert_eq!(BucketingCfg::from(&opts).inflight_groups, 1);
}
