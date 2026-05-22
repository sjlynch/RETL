use super::CorpusLocalStatus;
use crate::util::open_with_default_backoff;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Read};
use std::path::Path;

pub(crate) fn inspect_local_file(
    path: &Path,
    expected_bytes: Option<u64>,
    expected_sha256: Option<&str>,
    verify_checksums: bool,
) -> CorpusLocalStatus {
    let meta = match fs::metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return CorpusLocalStatus::Missing,
        Err(e) => {
            return CorpusLocalStatus::Inaccessible {
                error: e.to_string(),
            }
        }
    };
    if !meta.is_file() {
        return CorpusLocalStatus::Inaccessible {
            error: "path exists but is not a file".to_string(),
        };
    }

    let actual_bytes = meta.len();
    let size_matches = expected_bytes.map(|expected| expected == actual_bytes);
    // A hash failure does not make a present, correctly-sized file inaccessible:
    // keep the size facts we already have and surface the hash error as a
    // warning (`sha256_error`) so `has_local_problem()` does not flag the file.
    let (sha256_matches, sha256_actual, sha256_error) = if verify_checksums {
        match expected_sha256 {
            Some(expected) => match sha256_file(path) {
                Ok(actual) => (
                    Some(actual.eq_ignore_ascii_case(expected)),
                    Some(actual),
                    None,
                ),
                Err(e) => (None, None, Some(format!("failed to compute sha256: {e}"))),
            },
            None => (None, None, None),
        }
    } else {
        (None, None, None)
    };

    CorpusLocalStatus::Present {
        actual_bytes,
        size_matches,
        sha256_matches,
        sha256_actual,
        sha256_error,
    }
}

fn sha256_file(path: &Path) -> io::Result<String> {
    // Route the open through the backoff wrapper so a transient Windows
    // sharing/AV hiccup retries instead of surfacing as a hash failure.
    let mut file = open_with_default_backoff(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 128 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{
        cap_backoff_budget_for_test, inject_retriable_io_errors_for_file_name_tests, TestIoOp,
    };

    // sha256("abc"), used as the manifest's expected checksum.
    const SHA256_ABC: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn hash_failure_keeps_a_present_correctly_sized_file_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corpus_hash_fail_inspect_test.bin");
        fs::write(&path, b"abc").unwrap();

        // Every open attempt fails with a transient (retriable) error; the
        // capped budget makes the failure surface immediately.
        let _cap = cap_backoff_budget_for_test(1, 0);
        let _inject = inject_retriable_io_errors_for_file_name_tests(
            TestIoOp::Open,
            "corpus_hash_fail_inspect_test.bin",
            1,
        );

        let status = inspect_local_file(&path, Some(3), Some(SHA256_ABC), true);

        match status {
            CorpusLocalStatus::Present {
                actual_bytes,
                size_matches,
                sha256_matches,
                sha256_actual,
                sha256_error,
            } => {
                assert_eq!(actual_bytes, 3);
                assert_eq!(size_matches, Some(true));
                assert_eq!(sha256_matches, None);
                assert_eq!(sha256_actual, None);
                assert!(
                    sha256_error.is_some(),
                    "hash error must be surfaced as a warning, got None"
                );
            }
            other => panic!("expected Present-with-warning, got {other:?}"),
        }
    }

    #[test]
    fn transient_open_error_during_hash_recovers_via_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corpus_hash_recover_inspect_test.bin");
        fs::write(&path, b"abc").unwrap();

        // A single transient open failure; the backoff retry then succeeds.
        let _inject = inject_retriable_io_errors_for_file_name_tests(
            TestIoOp::Open,
            "corpus_hash_recover_inspect_test.bin",
            1,
        );

        let status = inspect_local_file(&path, Some(3), Some(SHA256_ABC), true);

        match status {
            CorpusLocalStatus::Present {
                sha256_matches,
                sha256_error,
                ..
            } => {
                assert_eq!(sha256_matches, Some(true));
                assert!(sha256_error.is_none());
            }
            other => panic!("expected Present with a verified hash, got {other:?}"),
        }
    }
}
