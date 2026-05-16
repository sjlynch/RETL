use crate::common::parents::*;
use retl::{RedditETL, YearMonth};
use std::fs;

#[test]
fn resolve_parent_maps_sweeps_crash_leftover_under_staging() {
    let tmp = tempfile::tempdir().unwrap();
    let base = tmp.path().join("corpus");
    let work_dir = tmp.path().join("work");
    let cache_dir = tmp.path().join("parents_cache");
    let ym = YearMonth::new(2006, 1);

    write_comment_parent_corpus(&base, ym, &[("p1", "parent body")]);
    let spool = tmp.path().join("spool.jsonl");
    write_parent_ref_spool(&spool, "t1_p1");
    let ids = collect_parent_ids_for_test(&base, &work_dir, &spool);

    // Plant a `_staging/` leftover that looks like the previous run crashed
    // mid-flush. The format matches `unique_inprogress_path`:
    //     <basename>.retl-<pid>-<counter>-<nanos>.inprogress
    // Use a PID that is extremely unlikely to be live (u32::MAX) so
    // `process_is_running` returns false and the sweep removes it.
    let comments_out = cache_dir.join("comments");
    let staging = comments_out.join("_staging");
    fs::create_dir_all(&staging).unwrap();
    let leftover_name = format!("RC_{ym}.json.retl-{}-0-0.inprogress", u32::MAX);
    let leftover = staging.join(&leftover_name);
    fs::write(&leftover, b"partial flush from a dead run").unwrap();
    assert!(leftover.exists(), "test failed to plant the leftover");

    // Now run resolver. Sweep-on-entry should remove the planted leftover,
    // and the shard write should publish a fresh `RC_<ym>.json` via a
    // distinct PID/nonce-suffixed staged file.
    RedditETL::new()
        .base_dir(&base)
        .work_dir(&work_dir)
        .date_range(Some(ym), Some(ym))
        .progress(false)
        .resolve_parent_maps(&ids, &cache_dir, false)
        .unwrap();

    assert!(
        !leftover.exists(),
        "sweep-on-entry should have removed crash leftover {}",
        leftover.display()
    );

    let cache = read_comment_cache(&cache_dir, ym);
    assert_eq!(
        cache.get("p1").map(String::as_str),
        Some("parent body"),
        "subsequent run should publish the live shard after sweep"
    );

    // No stray `*.tmp` sibling at the legacy path either — that path is gone.
    let legacy_sibling = comments_out.join(format!("RC_{ym}.json.tmp"));
    assert!(
        !legacy_sibling.exists(),
        "legacy sibling temp `{}` should not be produced by the shard writer",
        legacy_sibling.display()
    );

    // After a successful publish, the staging dir contains no leftover
    // belonging to this run either.
    let live_leftovers: Vec<std::path::PathBuf> = fs::read_dir(&staging)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .map(|n| n.ends_with(".inprogress"))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        live_leftovers.is_empty(),
        "no staged leftovers should remain after a successful resolve, found {:?}",
        live_leftovers
    );
}
