use super::CorpusLocalStatus;
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
    let (sha256_matches, sha256_actual) = if verify_checksums {
        match expected_sha256 {
            Some(expected) => match sha256_file(path) {
                Ok(actual) => (Some(actual.eq_ignore_ascii_case(expected)), Some(actual)),
                Err(e) => {
                    return CorpusLocalStatus::Inaccessible {
                        error: format!("failed to compute sha256: {e}"),
                    }
                }
            },
            None => (None, None),
        }
    } else {
        (None, None)
    };

    CorpusLocalStatus::Present {
        actual_bytes,
        size_matches,
        sha256_matches,
        sha256_actual,
    }
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
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
