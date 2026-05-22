use crate::common::cli::retl;
use crate::common::{make_corpus_basic, read_jsonl_values};
use predicates::str::contains;
use std::fs;

#[test]
fn parents_cli_resolves_direct_id_file_to_parent_payload_jsonl() {
    let base = make_corpus_basic();
    let ids = base.join("direct_parent_ids.txt");
    // All IDs are `t1_`/`t3_`-prefixed so no `--id-kind` is needed; mixing a
    // `t3_`-prefixed ID with `--id-kind comment` is now a hard error (see
    // `parents_cli_rejects_prefixed_id_contradicting_id_kind`).
    fs::write(&ids, "t1_c1\n\n# duplicate is ignored\nt1_c1\n").unwrap();
    let cache = base.join("parents_cache_direct_ids");
    let out = base.join("direct_parents.jsonl");
    let work = base.join("work_parents_direct_ids");

    retl()
        .args([
            "parents",
            "--ids-file",
            ids.to_str().unwrap(),
            "--parent-id",
            "t3_s1",
            "--cache",
            cache.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--parent-fields",
            "author,body,score,title,selftext",
            "--resume",
            "--no-progress",
        ])
        .assert()
        .success();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 2, "direct output should be de-duplicated");

    let comment = values
        .iter()
        .find(|v| v.get("kind").and_then(|v| v.as_str()) == Some("comment"))
        .expect("comment parent payload should be written");
    assert_eq!(comment.get("id").and_then(|v| v.as_str()), Some("c1"));
    assert_eq!(
        comment.get("author").and_then(|v| v.as_str()),
        Some("alice")
    );
    assert_eq!(
        comment.get("body").and_then(|v| v.as_str()),
        Some("I love Rust http://rust-lang.org")
    );
    assert_eq!(comment.get("score").and_then(|v| v.as_i64()), Some(2));

    let submission = values
        .iter()
        .find(|v| v.get("kind").and_then(|v| v.as_str()) == Some("submission"))
        .expect("submission parent payload should be written");
    assert_eq!(submission.get("id").and_then(|v| v.as_str()), Some("s1"));
    assert_eq!(
        submission.get("title").and_then(|v| v.as_str()),
        Some("Rust news")
    );
    assert_eq!(
        submission.get("author").and_then(|v| v.as_str()),
        Some("bob")
    );
}

#[test]
fn parents_cli_rejects_bare_direct_ids_without_kind() {
    let base = make_corpus_basic();

    retl()
        .args([
            "parents",
            "--parent-id",
            "c1",
            "--cache",
            base.join("cache_bare_missing_kind").to_str().unwrap(),
            "--out",
            base.join("parents_bare_missing_kind.jsonl")
                .to_str()
                .unwrap(),
            "--data-dir",
            base.to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .failure()
        .stderr(contains("requires --id-kind"));
}

#[test]
fn parents_cli_resolves_bare_ids_with_matching_id_kind() {
    let base = make_corpus_basic();
    let ids = base.join("bare_kind_ids.txt");
    // A bare ID needs `--id-kind`; an explicitly `t1_`-prefixed ID whose kind
    // agrees with `--id-kind comment` is accepted and de-duplicated against it.
    fs::write(&ids, "c1\n\n# duplicate is ignored\nt1_c1\n").unwrap();
    let cache = base.join("parents_cache_bare_kind");
    let out = base.join("bare_kind_parents.jsonl");
    let work = base.join("work_parents_bare_kind");

    retl()
        .args([
            "parents",
            "--ids-file",
            ids.to_str().unwrap(),
            "--id-kind",
            "comment",
            "--cache",
            cache.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--start",
            "2006-01",
            "--end",
            "2006-01",
            "--parent-fields",
            "author,body,score",
            "--resume",
            "--no-progress",
        ])
        .assert()
        .success();

    let values = read_jsonl_values(&out);
    assert_eq!(values.len(), 1, "bare + prefixed `c1` should de-duplicate");
    assert_eq!(
        values[0].get("kind").and_then(|v| v.as_str()),
        Some("comment")
    );
    assert_eq!(values[0].get("id").and_then(|v| v.as_str()), Some("c1"));
}

#[test]
fn parents_cli_rejects_prefixed_id_contradicting_id_kind() {
    let dir = tempfile::tempdir().unwrap();

    // `t1_` says comment, `--id-kind submission` says submission: two
    // contradictory inputs. The old behavior silently honored the prefix and
    // ignored `--id-kind`; it must now be a hard error naming both.
    retl()
        .args([
            "parents",
            "--parent-id",
            "t1_c1",
            "--id-kind",
            "submission",
            "--cache",
            dir.path().join("cache").to_str().unwrap(),
            "--out",
            dir.path().join("parents.jsonl").to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .failure()
        .stderr(contains("disagree"))
        .stderr(contains("t1_c1"))
        .stderr(contains("comment"))
        .stderr(contains("submission"));
}

#[test]
fn parents_cli_rejects_unsupported_direct_id_prefix() {
    let dir = tempfile::tempdir().unwrap();

    retl()
        .args([
            "parents",
            "--parent-id",
            "t2_user",
            "--cache",
            dir.path().join("cache").to_str().unwrap(),
            "--out",
            dir.path().join("parents.jsonl").to_str().unwrap(),
            "--no-progress",
        ])
        .assert()
        .failure()
        .stderr(contains("unsupported parent ID prefix"))
        .stderr(contains("t1_"))
        .stderr(contains("t3_"));
}
