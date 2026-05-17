use crate::config::clamp_parallelism_threads;

/// Run `f` inside a scoped Rayon thread pool sized to `n` threads. If `n` is
/// `None` (or `Some(0)`), run on the global default pool. Positive values are
/// clamped through [`crate::config::max_parallelism_limit`]; if Rayon still
/// rejects the pool, RETL logs a warning and safely falls back to the global
/// pool instead of panicking.
///
/// Prefer this over `rayon::ThreadPoolBuilder::build_global()`, which mutates
/// process-wide state, only succeeds for the first caller, and prevents
/// different stages from picking different thread counts.
pub fn with_thread_pool<R, F>(n: Option<usize>, f: F) -> R
where
    F: FnOnce() -> R + Send,
    R: Send,
{
    match n {
        Some(k) if k > 0 => {
            let clamped = clamp_parallelism_threads(k, "with_thread_pool");
            match rayon::ThreadPoolBuilder::new().num_threads(clamped).build() {
                Ok(pool) => pool.install(f),
                Err(e) => {
                    tracing::warn!(
                        requested = k,
                        clamped,
                        error = %e,
                        "failed to build scoped Rayon thread pool; falling back to global pool"
                    );
                    f()
                }
            }
        }
        _ => f(),
    }
}
