//! Behaviors of `PartitionWriters` not covered by the basic lambdas test:
//!
//! - finalize without any writes still produces empty `<stem>_part_NNNN.ndjson`
//!   files at the destination (one per partition) and removes the staging
//!   `.inprogress` files.
//! - finalize promotes outputs in stable, sorted order by part index.
//! - flush_all can be called repeatedly without breaking the writer.
//! - parts=0 is clamped to 1 (per the doc comment in
//!   `PartitionWriters::new(... parts.max(1) ...)`).
//! - The shard hash routes the same key to the same partition on every call
//!   (already tested) AND distributes a large key universe to every
//!   partition with reasonable balance (sanity check that the hash isn't
//!   collapsing keys to a single shard).

use retl::PartitionWriters;
use std::collections::HashMap;
use std::fs;

#[test]
fn finalize_without_writes_emits_empty_per_partition_files() {
    let dir = tempfile::tempdir().unwrap();
    let pw = PartitionWriters::new(dir.path(), "empty", 4, 64 * 1024).unwrap();
    let parts = pw.finalize().unwrap();
    assert_eq!(parts.len(), 4);
    for p in &parts {
        assert!(p.exists(), "expected empty part file at {}", p.display());
        let meta = fs::metadata(p).unwrap();
        assert_eq!(meta.len(), 0, "no writes => zero bytes; got {} for {}", meta.len(), p.display());
    }

    // Staging dir should be free of .inprogress leftovers.
    let staging = dir.path().join("_staging");
    let leftovers: Vec<_> = fs::read_dir(&staging)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with(".inprogress"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "expected no .inprogress in staging after finalize, got: {:?}",
        leftovers.iter().map(|e| e.path()).collect::<Vec<_>>()
    );
}

#[test]
fn parts_zero_is_clamped_to_one() {
    let dir = tempfile::tempdir().unwrap();
    let pw = PartitionWriters::new(dir.path(), "zero", 0, 64 * 1024).unwrap();
    // Writes still succeed (the lone partition picks up everything).
    pw.write_with("alice", |w| {
        w.write_all(b"{\"u\":\"alice\"}\n")?;
        Ok(())
    })
    .unwrap();
    pw.write_with("bob", |w| {
        w.write_all(b"{\"u\":\"bob\"}\n")?;
        Ok(())
    })
    .unwrap();
    let parts = pw.finalize().unwrap();
    assert_eq!(parts.len(), 1, "parts=0 must clamp to 1 partition");
    let body = fs::read_to_string(&parts[0]).unwrap();
    assert!(body.contains("alice"));
    assert!(body.contains("bob"));
}

#[test]
fn flush_all_is_idempotent_and_safe_between_writes() {
    let dir = tempfile::tempdir().unwrap();
    let pw = PartitionWriters::new(dir.path(), "flushy", 2, 64 * 1024).unwrap();

    pw.write_with("k1", |w| { w.write_all(b"line-1\n")?; Ok(()) }).unwrap();
    pw.flush_all().unwrap();
    pw.flush_all().unwrap();

    pw.write_with("k2", |w| { w.write_all(b"line-2\n")?; Ok(()) }).unwrap();
    pw.flush_all().unwrap();

    let parts = pw.finalize().unwrap();
    let total: u64 = parts.iter().map(|p| fs::metadata(p).unwrap().len()).sum();
    assert!(total >= "line-1\nline-2\n".len() as u64);
    let combined: String = parts
        .iter()
        .map(|p| fs::read_to_string(p).unwrap())
        .collect::<Vec<_>>()
        .join("");
    assert!(combined.contains("line-1"));
    assert!(combined.contains("line-2"));
}

#[test]
fn finalize_returns_paths_in_partition_index_order() {
    let dir = tempfile::tempdir().unwrap();
    let pw = PartitionWriters::new(dir.path(), "ordered", 8, 64 * 1024).unwrap();
    let parts = pw.finalize().unwrap();
    assert_eq!(parts.len(), 8);
    for (i, p) in parts.iter().enumerate() {
        let want = format!("ordered_part_{:04}.ndjson", i);
        assert_eq!(
            p.file_name().unwrap().to_string_lossy(),
            want,
            "partition {} has unexpected name {}",
            i,
            p.display()
        );
    }
}

#[test]
fn shard_hash_distributes_large_key_universe_across_partitions() {
    // Sanity-check the hash isn't degenerate: 2000 distinct keys over 8
    // partitions should populate every partition.
    let dir = tempfile::tempdir().unwrap();
    let pw = PartitionWriters::new(dir.path(), "spread", 8, 64 * 1024).unwrap();
    for i in 0..2000 {
        let k = format!("user_{:04}", i);
        pw.write_with(&k, |w| {
            writeln!(w, "{{\"u\":\"{}\"}}", k)?;
            Ok(())
        })
        .unwrap();
    }
    let parts = pw.finalize().unwrap();
    let mut count_per_part: HashMap<usize, usize> = HashMap::new();
    for (i, p) in parts.iter().enumerate() {
        let n = fs::read_to_string(p)
            .unwrap()
            .lines()
            .filter(|s| !s.is_empty())
            .count();
        count_per_part.insert(i, n);
    }
    let total: usize = count_per_part.values().sum();
    assert_eq!(total, 2000, "every record must land somewhere");
    // Every partition received at least one key (probabilistically near-certain
    // for 2000 keys over 8 buckets with ahash::RandomState seeded).
    for (i, n) in &count_per_part {
        assert!(*n > 0, "partition {} got 0 keys, distribution looks degenerate: {:?}", i, count_per_part);
    }
}

#[test]
fn concurrent_writes_via_rayon_scope_land_on_correct_partitions() {
    // Locks in the `&self` concurrency contract: a single PartitionWriters can be
    // shared across threads and each thread's writes must land in the partition
    // its key hashes to (no lost or interleaved bytes within a single line).
    let dir = tempfile::tempdir().unwrap();
    let pw = PartitionWriters::new(dir.path(), "rayon", 8, 64 * 1024).unwrap();
    let pw_ref = &pw;

    const PER_THREAD: usize = 250;
    const THREADS: usize = 8;

    rayon::scope(|s| {
        for t in 0..THREADS {
            s.spawn(move |_| {
                for i in 0..PER_THREAD {
                    let user = format!("user_t{}_n{:04}", t, i);
                    // Each line is self-contained (one write_all under the partition lock).
                    pw_ref
                        .write_with(&user, |w| {
                            let line = format!("{{\"u\":\"{}\"}}\n", user);
                            w.write_all(line.as_bytes())?;
                            Ok(())
                        })
                        .unwrap();
                }
            });
        }
    });

    let parts = pw.finalize().unwrap();
    assert_eq!(parts.len(), 8);

    let mut total = 0usize;
    let mut seen_users: HashMap<String, usize> = HashMap::new();
    for (i, p) in parts.iter().enumerate() {
        let body = fs::read_to_string(p).unwrap();
        for line in body.lines().filter(|s| !s.is_empty()) {
            // Each line must be intact JSON (no torn writes from concurrent threads
            // hashing to the same partition).
            let v: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("torn line in partition {}: {:?} ({})", i, line, e));
            let user = v.get("u").and_then(|x| x.as_str()).unwrap().to_string();
            // Same user must always land in the same partition.
            if let Some(prev) = seen_users.insert(user.clone(), i) {
                assert_eq!(prev, i, "user {} split across partitions {} and {}", user, prev, i);
            }
            total += 1;
        }
    }
    assert_eq!(total, THREADS * PER_THREAD, "every write must land somewhere exactly once");
}
