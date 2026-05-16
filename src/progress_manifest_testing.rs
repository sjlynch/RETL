use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug)]
struct TestSaveFailure {
    id: u64,
    out_dir: PathBuf,
    skip_attempts: usize,
    remaining_failures: usize,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
static SAVE_FAILURES: std::sync::Mutex<Vec<TestSaveFailure>> = std::sync::Mutex::new(Vec::new());

pub(crate) struct TestSaveFailureGuard {
    id: u64,
}

impl Drop for TestSaveFailureGuard {
    fn drop(&mut self) {
        let mut failures = SAVE_FAILURES
            .lock()
            .expect("progress-manifest test failure mutex poisoned");
        failures.retain(|failure| failure.id != self.id);
    }
}

pub(crate) fn fail_saves_after_attempts_for_tests(
    out_dir: impl Into<PathBuf>,
    skip_attempts: usize,
    failures: usize,
) -> TestSaveFailureGuard {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    SAVE_FAILURES
        .lock()
        .expect("progress-manifest test failure mutex poisoned")
        .push(TestSaveFailure {
            id,
            out_dir: out_dir.into(),
            skip_attempts,
            remaining_failures: failures,
        });
    TestSaveFailureGuard { id }
}

pub(super) fn maybe_fail_save_for_tests(out_dir: &Path) -> Result<()> {
    let mut failures = SAVE_FAILURES
        .lock()
        .expect("progress-manifest test failure mutex poisoned");
    let Some(failure) = failures.iter_mut().find(|failure| {
        failure.out_dir == out_dir
            && (failure.skip_attempts > 0 || failure.remaining_failures > 0)
    }) else {
        return Ok(());
    };

    if failure.skip_attempts > 0 {
        failure.skip_attempts -= 1;
        return Ok(());
    }

    failure.remaining_failures -= 1;
    Err(anyhow::anyhow!(
        "injected progress manifest save failure for {}",
        out_dir.display()
    ))
}
