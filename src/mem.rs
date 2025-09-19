use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use sysinfo::{System, SystemExt};

/// Cached, low-overhead memory watcher.
/// - Refreshes at most every `REFRESH_EVERY`.
/// - Uses available/total RAM to decide when to throttle.
struct MemState {
    sys: System,
    last_check: Instant,
    last_frac: f64, // available / total (0.0..1.0)
}

static STATE: OnceLock<Mutex<MemState>> = OnceLock::new();
const REFRESH_EVERY: Duration = Duration::from_millis(500);

fn with_state<F, T>(f: F) -> T
where
    F: FnOnce(&mut MemState) -> T,
{
    let m = STATE.get_or_init(|| {
        let mut s = System::new();
        s.refresh_memory();
        Mutex::new(MemState {
            sys: s,
            last_check: Instant::now() - REFRESH_EVERY * 2,
            last_frac: 1.0,
        })
    });
    let mut guard = m.lock().unwrap();
    f(&mut guard)
}

/// Returns a recent estimate of available memory fraction (0.0..1.0).
pub fn available_memory_fraction() -> f64 {
    with_state(|st| {
        let now = Instant::now();
        if now.duration_since(st.last_check) >= REFRESH_EVERY {
            st.sys.refresh_memory();
            let total = st.sys.total_memory() as f64;
            let avail = st.sys.available_memory() as f64;
            st.last_frac = if total > 0.0 { (avail / total).clamp(0.0, 1.0) } else { 1.0 };
            st.last_check = now;
        }
        st.last_frac
    })
}

/// Returns true if the cached available-memory fraction is below `threshold` (e.g., 0.10 for 10%).
pub fn is_low_memory(threshold: f64) -> bool {
    available_memory_fraction() < threshold
}

/// Cooperative backoff: yields briefly if under the threshold.
/// Safe to call frequently — uses cached memory values internally.
pub fn maybe_throttle_low_memory(threshold: f64) {
    if is_low_memory(threshold) {
        std::thread::sleep(Duration::from_millis(25));
    }
}
