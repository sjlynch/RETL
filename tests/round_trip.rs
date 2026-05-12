//! T11: scan -> export -> re-scan round-trip equality.
//!
//! For Zst exports the output layout matches the input corpus
//! (`comments/RC_YYYY-MM.zst`, `submissions/RS_YYYY-MM.zst`), so we can rescan
//! the export with the same `RedditETL` and demand identical counts/IDs.
//!
//! For Jsonl exports we read the `.jsonl` files directly because the corpus
//! discovery layer only looks for `.zst`.
//!
//! With `whitelist_fields` the round-trip is a structural subset: every
//! exported record's whitelisted keys must equal the original's value for the
//! same key, and no non-whitelisted keys may appear in the export.

#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{ExportFormat, RedditETL, Sources, YearMonth};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// All "programming" records from the basic corpus, keyed by id.
/// Includes pseudo-users so c3 (`[deleted]`) is present.
fn original_records_by_id(base: &std::path::Path) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for sub in ["comments", "submissions"] {
        let dir = base.join(sub);
        if !dir.exists() {
            continue;
        }
        for ent in std::fs::read_dir(&dir).unwrap().flatten() {
            let p = ent.path();
            if p.extension().and_then(|s| s.to_str()) != Some("zst") {
                continue;
            }
            for line in decompress_zst_lines(&p) {
                let v: Value = serde_json::from_str(&line).unwrap();
                if v.get("subreddit").and_then(|s| s.as_str()) != Some("programming") {
                    continue;
                }
                let id = v
                    .get("id")
                    .and_then(|s| s.as_str())
                    .expect("record without id")
                    .to_string();
                out.insert(id, v);
            }
        }
    }
    out
}

#[test]
fn round_trip_zst_preserves_counts_and_ids() {
    let base = make_corpus_basic();
    let out_dir = base.join("rt_zst");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .export_partitioned(&out_dir, ExportFormat::Zst)
        .unwrap();

    // Layout matches a real corpus, so we can rescan the export.
    let rc = out_dir.join("comments").join("RC_2006-01.zst");
    let rs = out_dir.join("submissions").join("RS_2006-01.zst");
    assert!(rc.exists(), "exported RC zst missing at {}", rc.display());
    assert!(rs.exists(), "exported RS zst missing at {}", rs.display());

    let counts = RedditETL::new()
        .base_dir(&out_dir)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .count_by_month()
        .unwrap();

    assert_eq!(
        counts.get(&YearMonth::new(2006, 1)).copied(),
        Some(5),
        "rescan of Zst export should see all 5 'programming' records (3 RC + 2 RS)"
    );

    // Per-record equality on id and a couple of key fields.
    let originals = original_records_by_id(&base);
    let exported = original_records_by_id(&out_dir);

    let original_ids: BTreeSet<_> = originals.keys().cloned().collect();
    let exported_ids: BTreeSet<_> = exported.keys().cloned().collect();
    assert_eq!(
        original_ids, exported_ids,
        "Zst round-trip must preserve the full set of record ids"
    );

    for (id, orig) in &originals {
        let got = exported.get(id).expect("exported record missing");
        for k in ["author", "subreddit", "created_utc"] {
            assert_eq!(
                got.get(k),
                orig.get(k),
                "field `{}` mismatch for id={}",
                k,
                id
            );
        }
    }
}

#[test]
fn round_trip_jsonl_preserves_counts_and_ids() {
    let base = make_corpus_basic();
    let out_dir = base.join("rt_jsonl");

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .export_partitioned(&out_dir, ExportFormat::Jsonl)
        .unwrap();

    let rc = out_dir.join("comments").join("RC_2006-01.jsonl");
    let rs = out_dir.join("submissions").join("RS_2006-01.jsonl");
    assert!(rc.exists(), "exported RC jsonl missing at {}", rc.display());
    assert!(rs.exists(), "exported RS jsonl missing at {}", rs.display());

    // Rescan-equivalent: count + collect from the .jsonl files directly,
    // because the corpus discovery only matches `.zst`.
    let mut exported: BTreeMap<String, Value> = BTreeMap::new();
    for p in [&rc, &rs] {
        for v in read_jsonl_values(p) {
            let id = v
                .get("id")
                .and_then(|s| s.as_str())
                .expect("record without id")
                .to_string();
            exported.insert(id, v);
        }
    }
    assert_eq!(
        exported.len(),
        5,
        "Jsonl export should contain all 5 'programming' records"
    );

    let originals = original_records_by_id(&base);
    assert_eq!(
        originals.keys().cloned().collect::<BTreeSet<_>>(),
        exported.keys().cloned().collect::<BTreeSet<_>>(),
        "Jsonl round-trip must preserve the full set of record ids"
    );

    for (id, orig) in &originals {
        let got = exported.get(id).unwrap();
        for k in ["author", "subreddit", "created_utc"] {
            assert_eq!(
                got.get(k),
                orig.get(k),
                "field `{}` mismatch for id={}",
                k,
                id
            );
        }
    }
}

#[test]
fn round_trip_zst_with_whitelist_is_structural_subset() {
    let base = make_corpus_basic();
    let out_dir = base.join("rt_zst_whitelist");

    let whitelist = ["id", "author", "subreddit", "created_utc"];

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .include_pseudo_users()
        .whitelist_fields(whitelist)
        .export_partitioned(&out_dir, ExportFormat::Zst)
        .unwrap();

    let originals = original_records_by_id(&base);
    let exported = original_records_by_id(&out_dir);

    assert_eq!(
        originals.keys().cloned().collect::<BTreeSet<_>>(),
        exported.keys().cloned().collect::<BTreeSet<_>>(),
        "whitelist round-trip must still preserve the full set of record ids"
    );

    let allowed: BTreeSet<&str> = whitelist.iter().copied().collect();
    for (id, got) in &exported {
        let obj = got
            .as_object()
            .unwrap_or_else(|| panic!("exported record for id={} is not an object", id));

        // 1) No non-whitelisted keys may leak into the export.
        for k in obj.keys() {
            assert!(
                allowed.contains(k.as_str()),
                "exported record id={} contains non-whitelisted key `{}`",
                id,
                k
            );
        }

        // 2) Every whitelisted key present in the export must equal the
        //    original record's value (structural-subset equality).
        let orig = originals
            .get(id)
            .unwrap_or_else(|| panic!("original record for id={} missing", id));
        for k in &whitelist {
            if let Some(v) = obj.get(*k) {
                assert_eq!(
                    Some(v),
                    orig.get(*k),
                    "whitelisted field `{}` mismatch for id={}",
                    k,
                    id
                );
            }
        }
    }
}
