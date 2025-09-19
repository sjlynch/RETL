#[path = "common/mod.rs"]
mod common;

use common::read_lines;
use retl::{RedditETL, Sources, YearMonth, PartitionWriters};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

/// Demonstrates lambda/callback support in the core partition sink:
/// - We create a PartitionWriters with 4 partitions.
/// - We emit arbitrary NDJSON lines via `write_with(user, |w| { ... })`.
/// - We assert that:
///     * All output files exist and contain the lines we wrote
///     * A given user’s lines always land in the SAME partition
#[test]
fn partition_writers_lambda_routing_and_output() {
    let tmp = tempfile::tempdir().unwrap();
    let parts_dir = tmp.path().join("parts");
    fs::create_dir_all(&parts_dir).unwrap();

    let mut pw = PartitionWriters::new(&parts_dir, "lambda", 4, 128 * 1024).unwrap();

    // Helper to write a single NDJSON line for a given user
    let mut write_line = |user: &str, payload: i64| {
        pw.write_with(user, |w| {
            let line = format!("{{\"user\":\"{}\",\"payload\":{}}}\n", user, payload);
            w.write_all(line.as_bytes())?;
            Ok(())
        })
    };

    // Emit some lines; "alice" appears twice.
    write_line("alice", 1).unwrap();
    write_line("alice", 2).unwrap();
    write_line("bob", 99).unwrap();
    write_line("charlie", 42).unwrap();

    let part_paths: Vec<PathBuf> = pw.finalize().unwrap();
    assert_eq!(part_paths.len(), 4, "expected 4 final partitions");

    // Read back all lines and verify routing invariants.
    let mut total_lines = 0usize;
    let mut user_to_part: HashMap<String, usize> = HashMap::new();

    for (i, p) in part_paths.iter().enumerate() {
        assert!(
            p.file_name().unwrap().to_string_lossy().ends_with(".ndjson"),
            "final part must end with .ndjson: {}",
            p.display()
        );

        let lines = read_lines(p);
        for line in lines {
            total_lines += 1;
            let v: serde_json::Value = serde_json::from_str(&line).unwrap();
            let user = v.get("user").and_then(|x| x.as_str()).unwrap_or("").to_string();
            assert!(!user.is_empty(), "every emitted line should have a user");

            if let Some(prev_idx) = user_to_part.get(&user).copied() {
                assert_eq!(
                    prev_idx, i,
                    "user {} must be routed to a stable partition index (expected {}, got {})",
                    user, prev_idx, i
                );
            } else {
                user_to_part.insert(user, i);
            }
        }
    }

    assert_eq!(total_lines, 4, "expected exactly 4 NDJSON lines across partitions");
    assert!(user_to_part.contains_key("alice"));
    assert!(user_to_part.contains_key("bob"));
    assert!(user_to_part.contains_key("charlie"));
}

/// NEW: Demonstrates the JS-like callback for usernames:
/// - Use `scan().for_each_username(|name| { ... })` to collect authors in a closure.
/// - We exclude common bots so only human authors remain in the tiny test set.
#[test]
fn usernames_lambda_callback() {
    let base = common::make_corpus_basic();
    let mut authors: Vec<String> = Vec::new();

    RedditETL::new()
        .base_dir(&base)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .scan()
        .subreddit("programming")
        .exclude_common_bots()
        .for_each_username(|name| authors.push(name.to_string()))
        .unwrap();

    authors.sort();
    assert_eq!(authors, vec!["alice", "bob", "charlie"]);
}
