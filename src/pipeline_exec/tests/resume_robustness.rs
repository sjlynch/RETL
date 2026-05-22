use super::*;

fn entry(size: u64, lines: u64) -> MonthEntry {
    MonthEntry {
        size,
        lines,
        sha256: None,
    }
}

/// `surviving_resumed_parts` is the pre-seed filter for the resumed-`parts`
/// list: it must return only the published outputs still present on disk at
/// the size recorded in the manifest, dropping anything deleted or truncated
/// since load-time validation. Downstream parents/aggregate stages open these
/// paths directly, so a phantom path would fail far from the real cause.
#[test]
fn surviving_resumed_parts_keeps_only_present_files_at_recorded_size() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();

    // Present at the recorded size -> kept.
    fs::write(dir.join("part_RC_2006-01.jsonl"), b"hello\n").unwrap();
    // Present but a different size (truncated/grown) -> dropped.
    fs::write(dir.join("part_RC_2006-02.jsonl"), b"a much longer body\n").unwrap();
    // RC_2006-03 is absent on disk -> dropped.

    let mut months = std::collections::HashMap::new();
    for key in ["RC_2006-01", "RC_2006-02", "RC_2006-03"] {
        months.insert(key.to_string(), entry(6, 1));
    }

    let got = surviving_resumed_parts(&months, |key| dir.join(format!("part_{key}.jsonl")));

    assert_eq!(got, vec![dir.join("part_RC_2006-01.jsonl")]);
}

/// An empty manifest pre-seeds an empty parts list (no panic, no phantom).
#[test]
fn surviving_resumed_parts_is_empty_for_empty_manifest() {
    let tmp = tempfile::tempdir().unwrap();
    let months: std::collections::HashMap<String, MonthEntry> = std::collections::HashMap::new();
    let got =
        surviving_resumed_parts(&months, |key| tmp.path().join(format!("part_{key}.jsonl")));
    assert!(got.is_empty());
}
