//! Windows-friendly retry/backoff helpers, split by policy and operation.
//!
//! This module keeps the `crate::util::*` surface stable while isolating the
//! retry classifier/budget (`retry`), filesystem wrappers (`fs_ops`), atomic
//! replace helper (`replace`), and unit-test failure injection (`testing`).

mod fs_ops;
mod replace;
mod retry;
#[cfg(test)]
mod testing;

pub use fs_ops::{
    create_dir_all_with_backoff, create_dir_all_with_default_backoff, create_new_with_backoff,
    create_new_with_default_backoff, create_with_backoff, create_with_default_backoff,
    open_with_backoff, open_with_default_backoff, read_dir_with_backoff,
    read_dir_with_default_backoff, remove_dir_all_with_backoff,
    remove_dir_all_with_default_backoff, remove_dir_all_with_short_backoff, remove_with_backoff,
    remove_with_default_backoff, remove_with_short_backoff,
};
pub(crate) use fs_ops::{create_dir_with_backoff, create_dir_with_default_backoff};
pub use replace::replace_file_atomic_backoff;
#[cfg(any(test, feature = "test-utils"))]
pub use retry::{cap_backoff_budget_for_test, TestBackoffBudgetGuard};
pub use retry::{DEFAULT_BACKOFF_DELAY_MS, DEFAULT_BACKOFF_TRIES, SHORT_BACKOFF_TRIES};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use testing::TestIoFailureGuard;
#[cfg(test)]
pub(crate) use testing::{
    inject_retriable_io_errors_for_file_name_tests, inject_retriable_io_errors_for_tests, TestIoOp,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn create_dir_all_with_backoff_retries_transient_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("nested").join("leaf");
        let _guard = inject_retriable_io_errors_for_tests(TestIoOp::CreateDirAll, &dir, 2);

        create_dir_all_with_backoff(&dir, 3, 0).expect("create dir with retry");

        assert!(
            dir.is_dir(),
            "directory should exist after retried create_dir_all"
        );
    }

    #[test]
    fn read_dir_with_backoff_retries_transient_errors() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let file = tmp.path().join("entry.txt");
        fs::write(&file, b"x").expect("seed file");
        let _guard = inject_retriable_io_errors_for_tests(TestIoOp::ReadDir, tmp.path(), 1);

        let entries = read_dir_with_backoff(tmp.path(), 2, 0).expect("read_dir with retry");
        let names: Vec<_> = entries
            .into_iter()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();

        assert_eq!(names, vec!["entry.txt"]);
    }
}
