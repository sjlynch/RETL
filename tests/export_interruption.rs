//! T11: an interrupted export must not leave a half-written `.zst` at the
//! final destination.
//!
//! Pre-#T1 (today): `export_partitioned` writes the encoder directly to the
//! destination path. If decoding the source aborts mid-stream (truncated tail,
//! checksum failure, etc.) the wrapper logs a warning and `stream_job` returns
//! `Ok(partial_records)`. `enc.finish()` then succeeds and a partial,
//! corrupt-by-omission `.zst` is left at the destination. This test FAILS
//! today; that failure is the load-bearing tripwire for the #T1 fix.
//!
//! Post-#T1 (target): writes go to a staging path (e.g. `*.zst.inprogress`)
//! and only get atomically renamed to the destination on full success. On a
//! decode error the destination must remain absent.
//!
//! Until #T1 lands this test is ignored to keep CI green; flip on the `ignore`
//! attribute (or remove it entirely) once the staging-rename fix is merged so
//! it acts as a regression guard going forward.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ExportFormat, RedditETL, Sources, YearMonth};
use std::fs;

#[test]
#[ignore = "Pre-#T1: export_partitioned writes directly to dest, so a \
            mid-file decode error leaves a partial .zst at destination. \
            Un-ignore once #T1's staging+atomic-rename fix lands."]
fn interrupted_export_leaves_no_half_written_zst_at_destination() {
    // Build a corpus, then drop a *truncated* RC monthly into it so the
    // streaming decoder fails mid-file during export.
    let base = make_corpus_basic();
    let truncated = base.join("comments").join("RC_2006-04.zst");
    make_truncated_zst(&truncated, /*records=*/ 500, /*truncate_by=*/ 256);

    let out_dir = base.join("export_interrupted");

    // Run the export. We expect the call itself to return Ok (the streaming
    // reader is documented to warn-and-skip), but no `.zst` should land at the
    // destination because the export of THAT month was interrupted.
    let res = RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 4)), Some(YearMonth::new(2006, 4)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .allow_pseudo_users()
        .export_partitioned(&out_dir, ExportFormat::Zst);
    assert!(res.is_ok(), "export_partitioned should not bubble decode errors: {:?}", res.err());

    // ---- The load-bearing assertion ---------------------------------------
    // No `.zst` at the FINAL destination directory tree.
    let comments_dir = out_dir.join("comments");
    let stray: Vec<_> = if comments_dir.exists() {
        fs::read_dir(&comments_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.ends_with(".zst"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect()
    } else {
        Vec::new()
    };
    assert!(
        stray.is_empty(),
        "interrupted export must not leave a `.zst` at the destination, \
         found: {:?}",
        stray
    );

    // Allow (but do not require) a `.zst.inprogress` somewhere under the
    // export root — that's the post-#T1 staging artifact.
    fn has_inprogress(dir: &std::path::Path) -> bool {
        if !dir.exists() {
            return false;
        }
        for ent in fs::read_dir(dir).unwrap().flatten() {
            let p = ent.path();
            if p.is_dir() {
                if has_inprogress(&p) {
                    return true;
                }
            } else if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.ends_with(".zst.inprogress") {
                    return true;
                }
            }
        }
        false
    }
    let _ = has_inprogress(&out_dir); // documented-but-not-required artifact
}
