
#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{inject_retriable_io_errors_for_tests, TestIoOp};

    #[test]
    fn ensure_work_dir_retries_transient_create_dir_all() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let work_dir = tmp.path().join("work");
        let _guard = inject_retriable_io_errors_for_tests(TestIoOp::CreateDirAll, &work_dir, 1);

        let etl = RedditETL::new().work_dir(&work_dir);
        let created = etl.ensure_work_dir().expect("ensure work dir");

        assert_eq!(created, work_dir);
        assert!(created.is_dir());
    }
}
