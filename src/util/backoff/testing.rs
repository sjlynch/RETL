use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::retry::WIN_ERR_SHARING_VIOLATION;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TestIoOp {
    Open,
    Create,
    CreateDir,
    CreateDirAll,
    ReadDir,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum TestIoPathMatch {
    Exact(PathBuf),
    FileName(String),
}

impl TestIoPathMatch {
    fn matches(&self, path: &Path) -> bool {
        match self {
            Self::Exact(expected) => expected == path,
            Self::FileName(expected) => path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name == expected),
        }
    }
}

#[derive(Clone, Debug)]
struct TestIoFailure {
    op: TestIoOp,
    path_match: TestIoPathMatch,
    remaining: usize,
    raw_os_error: i32,
}

static TEST_IO_FAILURES: Mutex<Vec<TestIoFailure>> = Mutex::new(Vec::new());

pub(crate) struct TestIoFailureGuard {
    op: TestIoOp,
    path_match: TestIoPathMatch,
}

impl Drop for TestIoFailureGuard {
    fn drop(&mut self) {
        let mut failures = TEST_IO_FAILURES
            .lock()
            .expect("test I/O failure mutex poisoned");
        failures.retain(|f| !(f.op == self.op && f.path_match == self.path_match));
    }
}

pub(crate) fn inject_retriable_io_errors_for_tests(
    op: TestIoOp,
    path: impl Into<PathBuf>,
    failures: usize,
) -> TestIoFailureGuard {
    inject_retriable_io_errors_for_match_tests(op, TestIoPathMatch::Exact(path.into()), failures)
}

pub(crate) fn inject_retriable_io_errors_for_file_name_tests(
    op: TestIoOp,
    file_name: impl Into<String>,
    failures: usize,
) -> TestIoFailureGuard {
    inject_retriable_io_errors_for_match_tests(
        op,
        TestIoPathMatch::FileName(file_name.into()),
        failures,
    )
}

fn inject_retriable_io_errors_for_match_tests(
    op: TestIoOp,
    path_match: TestIoPathMatch,
    failures: usize,
) -> TestIoFailureGuard {
    let guard = TestIoFailureGuard {
        op,
        path_match: path_match.clone(),
    };
    TEST_IO_FAILURES
        .lock()
        .expect("test I/O failure mutex poisoned")
        .push(TestIoFailure {
            op,
            path_match,
            remaining: failures,
            raw_os_error: WIN_ERR_SHARING_VIOLATION,
        });
    guard
}

pub(super) fn maybe_inject_retriable_io_error_for_tests(
    op: TestIoOp,
    path: &Path,
) -> io::Result<()> {
    let mut failures = TEST_IO_FAILURES
        .lock()
        .expect("test I/O failure mutex poisoned");
    if let Some(failure) = failures.iter_mut().find(|failure| {
        failure.op == op && failure.remaining > 0 && failure.path_match.matches(path)
    }) {
        failure.remaining -= 1;
        return Err(io::Error::from_raw_os_error(failure.raw_os_error));
    }
    Ok(())
}
