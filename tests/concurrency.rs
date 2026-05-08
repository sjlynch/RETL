//! Concurrency stress tests:
//! - `ShardedWriter`: 16 OS threads write concurrently; dedup output must equal the
//!   expected unique-key set.
//! - `for_each_file_limited`: drive at meaningful concurrency (>1) and verify the
//!   callback fires once per file, in arbitrary order, with no lost work.

#[path = "common/mod.rs"]
mod common;

use common::*;

use retl::{
    for_each_file_limited, FileJob, FileKind, ShardedWriter, YearMonth,
};

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

#[test]
fn sharded_writer_concurrent_writes_dedup_to_expected_set() {
    let dir = tempfile::tempdir().unwrap();
    let writer = ShardedWriter::create(dir.path(), "stress", 16).unwrap();

    // 16 threads × 2,500 keys, with overlap (modulo) so dedup actually has work.
    let n_threads = 16usize;
    let keys_per_thread = 2_500usize;
    let distinct_universe = 5_000usize; // 8000 thread-thread overlaps after modulo

    std::thread::scope(|s| {
        for t in 0..n_threads {
            let writer = &writer;
            s.spawn(move || {
                for i in 0..keys_per_thread {
                    let k = (t * keys_per_thread + i) % distinct_universe;
                    writer.write(&format!("user_{:05}", k)).unwrap();
                }
            });
        }
    });

    // Dedup on disk. Concatenated output should equal the expected unique set.
    let dedup_paths = writer.dedup("stress").unwrap();

    let mut got: BTreeSet<String> = BTreeSet::new();
    for p in &dedup_paths {
        for line in BufReader::new(File::open(p).unwrap()).lines().flatten() {
            if !line.is_empty() {
                assert!(got.insert(line.clone()), "duplicate across shards: {}", line);
            }
        }
    }
    assert_eq!(got.len(), distinct_universe);

    // Spot-check a few expected keys.
    assert!(got.contains("user_00000"));
    assert!(got.contains(&format!("user_{:05}", distinct_universe - 1)));

    // And no surprise extras.
    let expected: BTreeSet<String> =
        (0..distinct_universe).map(|k| format!("user_{:05}", k)).collect();
    assert_eq!(got, expected);
}

#[test]
fn for_each_file_limited_runs_callback_once_per_file_at_concurrency() {
    // Build a multi-month corpus to get many distinct files (FileJob entries).
    let months: Vec<YearMonth> = (1..=12)
        .map(|m| YearMonth::new(2006, m as u8))
        .collect();
    let base = make_corpus_multi_month(&months);

    // Synthesize FileJobs directly — we don't need the planner here.
    let mut jobs: Vec<FileJob> = Vec::new();
    for ym in &months {
        let label = format!("{:04}-{:02}", ym.year, ym.month);
        jobs.push(FileJob {
            kind: FileKind::Comment,
            ym: *ym,
            path: base.join("comments").join(format!("RC_{}.zst", label)),
        });
        jobs.push(FileJob {
            kind: FileKind::Submission,
            ym: *ym,
            path: base.join("submissions").join(format!("RS_{}.zst", label)),
        });
    }
    assert_eq!(jobs.len(), 24);

    let calls = AtomicUsize::new(0);
    let seen: Mutex<Vec<String>> = Mutex::new(Vec::new());

    for_each_file_limited(&jobs, 8, |job| {
        calls.fetch_add(1, Ordering::Relaxed);
        seen.lock().unwrap().push(job.path.display().to_string());
        Ok(())
    })
    .unwrap();

    assert_eq!(calls.load(Ordering::Relaxed), 24);
    let mut paths = seen.into_inner().unwrap();
    paths.sort();
    paths.dedup();
    assert_eq!(paths.len(), 24, "every job seen exactly once");
}

#[test]
fn for_each_file_limited_propagates_errors() {
    let months: Vec<YearMonth> = (1..=4).map(|m| YearMonth::new(2006, m as u8)).collect();
    let base = make_corpus_multi_month(&months);
    let mut jobs: Vec<FileJob> = Vec::new();
    for ym in &months {
        let label = format!("{:04}-{:02}", ym.year, ym.month);
        jobs.push(FileJob {
            kind: FileKind::Comment,
            ym: *ym,
            path: base.join("comments").join(format!("RC_{}.zst", label)),
        });
    }

    let res = for_each_file_limited(&jobs, 4, |_job| {
        Err(anyhow::anyhow!("boom"))
    });
    assert!(res.is_err(), "errors from callback must propagate");
}

#[test]
fn for_each_file_limited_with_limit_one_is_serial() {
    // limit <= 1 should take the serial path with no rayon involvement.
    let months: Vec<YearMonth> = (1..=3).map(|m| YearMonth::new(2006, m as u8)).collect();
    let base = make_corpus_multi_month(&months);
    let mut jobs: Vec<FileJob> = Vec::new();
    for ym in &months {
        let label = format!("{:04}-{:02}", ym.year, ym.month);
        jobs.push(FileJob {
            kind: FileKind::Comment,
            ym: *ym,
            path: base.join("comments").join(format!("RC_{}.zst", label)),
        });
    }

    let order = Mutex::new(Vec::<usize>::new());
    for_each_file_limited(&jobs, 1, |job| {
        order.lock().unwrap().push(job.ym.month as usize);
        Ok(())
    })
    .unwrap();

    // Serial path preserves input order.
    assert_eq!(order.into_inner().unwrap(), vec![1, 2, 3]);
}
