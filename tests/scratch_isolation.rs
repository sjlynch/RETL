#[path = "common/mod.rs"]
mod common;

use common::{make_corpus_basic, read_lines};
use retl::{RedditETL, ShardedKVWriter, ShardedWriter, Sources, YearMonth};
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

fn username_scan(base: &Path, work_dir: &Path, author: &str) -> Vec<String> {
    let mut names: Vec<String> = RedditETL::new()
        .base_dir(base)
        .work_dir(work_dir)
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .shard_count(4)
        .scan()
        .subreddit("programming")
        .author(author)
        .usernames()
        .unwrap()
        .collect();
    names.sort();
    names
}

fn read_text_set(paths: &[std::path::PathBuf]) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for p in paths {
        for line in BufReader::new(File::open(p).unwrap()).lines().flatten() {
            if !line.is_empty() {
                set.insert(line);
            }
        }
    }
    set
}

fn read_tsv_map(paths: &[std::path::PathBuf]) -> HashMap<String, i64> {
    let mut map = HashMap::new();
    for p in paths {
        for line in BufReader::new(File::open(p).unwrap()).lines().flatten() {
            if let Some((k, v)) = line.split_once('\t') {
                map.insert(k.to_string(), v.parse().unwrap());
            }
        }
    }
    map
}

#[test]
fn sharded_username_writers_with_same_prefix_use_isolated_roots() {
    let dir = tempfile::tempdir().unwrap();

    let writer_a = ShardedWriter::create(dir.path(), "usernames_q", 4).unwrap();
    writer_a.write("alice").unwrap();
    let root_a = writer_a.scratch_root().to_path_buf();

    let writer_b = ShardedWriter::create(dir.path(), "usernames_q", 4).unwrap();
    writer_b.write("bob").unwrap();
    let root_b = writer_b.scratch_root().to_path_buf();

    assert_ne!(
        root_a, root_b,
        "same-prefix runs must not share scratch roots"
    );

    let files_a = writer_a.dedup("usernames_q").unwrap();
    let files_b = writer_b.dedup("usernames_q").unwrap();

    assert_eq!(
        read_text_set(&files_a),
        BTreeSet::from(["alice".to_string()])
    );
    assert_eq!(read_text_set(&files_b), BTreeSet::from(["bob".to_string()]));
}

#[test]
fn held_username_stream_survives_later_scan_in_same_work_dir() {
    let base = make_corpus_basic();
    let work = tempfile::tempdir().unwrap();

    let first = RedditETL::new()
        .base_dir(&base)
        .work_dir(work.path())
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .shard_count(4)
        .scan()
        .subreddit("programming")
        .authors(["alice", "bob", "charlie"])
        .usernames()
        .unwrap();

    let second = username_scan(&base, work.path(), "bob");
    assert_eq!(second, vec!["bob"]);

    let mut first_names: Vec<String> = first.collect();
    first_names.sort();
    assert_eq!(first_names, vec!["alice", "bob", "charlie"]);

    let leftovers = fs::read_dir(work.path()).unwrap().count();
    assert_eq!(leftovers, 0, "consumed username streams should remove scratch roots");
}

#[test]
fn concurrent_username_scans_share_work_dir_without_interference() {
    let base = Arc::new(make_corpus_basic());
    let work = Arc::new(tempfile::tempdir().unwrap().keep());

    let base_a = Arc::clone(&base);
    let work_a = Arc::clone(&work);
    let a = std::thread::spawn(move || username_scan(base_a.as_path(), work_a.as_path(), "alice"));

    let base_b = Arc::clone(&base);
    let work_b = Arc::clone(&work);
    let b = std::thread::spawn(move || username_scan(base_b.as_path(), work_b.as_path(), "bob"));

    assert_eq!(a.join().unwrap(), vec!["alice"]);
    assert_eq!(b.join().unwrap(), vec!["bob"]);
}

#[test]
fn sharded_kv_writers_with_same_prefix_use_isolated_roots() {
    let dir = tempfile::tempdir().unwrap();

    let kv_a = ShardedKVWriter::create(dir.path(), "author_counts", 4).unwrap();
    kv_a.write_kv("alice", 1).unwrap();
    let root_a = kv_a.scratch_root().to_path_buf();

    let kv_b = ShardedKVWriter::create(dir.path(), "author_counts", 4).unwrap();
    kv_b.write_kv("bob", 2).unwrap();
    let root_b = kv_b.scratch_root().to_path_buf();

    assert_ne!(
        root_a, root_b,
        "same-prefix KV runs must not share scratch roots"
    );

    let files_a = kv_a.reduce_sum("author_counts").unwrap();
    let files_b = kv_b.reduce_sum("author_counts").unwrap();

    assert_eq!(
        read_tsv_map(&files_a),
        HashMap::from([("alice".to_string(), 1)])
    );
    assert_eq!(
        read_tsv_map(&files_b),
        HashMap::from([("bob".to_string(), 2)])
    );

    let first_a = ShardedKVWriter::create(dir.path(), "first_seen", 4).unwrap();
    first_a.write_kv("alice", 20).unwrap();
    first_a.write_kv("alice", 10).unwrap();

    let first_b = ShardedKVWriter::create(dir.path(), "first_seen", 4).unwrap();
    first_b.write_kv("bob", 30).unwrap();
    first_b.write_kv("bob", 25).unwrap();

    let min_a = first_a.reduce_min("first_seen").unwrap();
    let min_b = first_b.reduce_min("first_seen").unwrap();

    assert_eq!(
        read_tsv_map(&min_a),
        HashMap::from([("alice".to_string(), 10)])
    );
    assert_eq!(
        read_tsv_map(&min_b),
        HashMap::from([("bob".to_string(), 25)])
    );
}

#[test]
fn concurrent_author_and_first_seen_counts_share_work_dir() {
    let base = Arc::new(make_corpus_basic());
    let work = Arc::new(tempfile::tempdir().unwrap().keep());
    let out_dir = Arc::new(tempfile::tempdir().unwrap().keep());

    let base_a = Arc::clone(&base);
    let work_a = Arc::clone(&work);
    let out_a = Arc::clone(&out_dir).join("alice_counts.tsv");
    let author_a = std::thread::spawn(move || {
        RedditETL::new()
            .base_dir(base_a.as_path())
            .work_dir(work_a.as_path())
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .shard_count(4)
            .scan()
            .subreddit("programming")
            .author("alice")
            .author_counts_to_tsv(&out_a)
            .unwrap();
        out_a
    });

    let base_b = Arc::clone(&base);
    let work_b = Arc::clone(&work);
    let out_b = Arc::clone(&out_dir).join("bob_counts.tsv");
    let author_b = std::thread::spawn(move || {
        RedditETL::new()
            .base_dir(base_b.as_path())
            .work_dir(work_b.as_path())
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .shard_count(4)
            .scan()
            .subreddit("programming")
            .author("bob")
            .author_counts_to_tsv(&out_b)
            .unwrap();
        out_b
    });

    let base_c = Arc::clone(&base);
    let work_c = Arc::clone(&work);
    let out_c = Arc::clone(&out_dir).join("alice_first.tsv");
    let first_c = std::thread::spawn(move || {
        RedditETL::new()
            .base_dir(base_c.as_path())
            .work_dir(work_c.as_path())
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .shard_count(4)
            .scan()
            .subreddit("programming")
            .author("alice")
            .build_first_seen_index_to_tsv(&out_c)
            .unwrap();
        out_c
    });

    let base_d = Arc::clone(&base);
    let work_d = Arc::clone(&work);
    let out_d = Arc::clone(&out_dir).join("bob_first.tsv");
    let first_d = std::thread::spawn(move || {
        RedditETL::new()
            .base_dir(base_d.as_path())
            .work_dir(work_d.as_path())
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
            .progress(false)
            .shard_count(4)
            .scan()
            .subreddit("programming")
            .author("bob")
            .build_first_seen_index_to_tsv(&out_d)
            .unwrap();
        out_d
    });

    assert_eq!(read_lines(&author_a.join().unwrap()), vec!["alice\t1"]);
    assert_eq!(read_lines(&author_b.join().unwrap()), vec!["bob\t1"]);
    assert_eq!(
        read_lines(&first_c.join().unwrap()),
        vec!["alice\t1136074600"]
    );
    assert_eq!(
        read_lines(&first_d.join().unwrap()),
        vec!["bob\t1136073600"]
    );
}
