#[path = "common/mod.rs"]
mod common;

use common::*;
use retl::{AggregatePartialReadPolicy, Aggregator, RedditETL};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

/// A tiny aggregator: counts how many JSON objects were ingested overall.
/// Demonstrates implementing `Aggregator` and running `aggregate_jsonls_parallel`.
#[derive(Default, Serialize, Deserialize)]
struct RecCount {
    count: u64,
}
impl Aggregator for RecCount {
    fn ingest(&mut self, _record: &Value) {
        self.count += 1;
    }
    fn merge(&mut self, other: Self) {
        self.count += other.count;
    }
}

#[derive(Default, Serialize, Deserialize)]
struct ThreadCountAgg {
    max_threads: usize,
}
impl Aggregator for ThreadCountAgg {
    fn ingest(&mut self, _record: &Value) {
        self.max_threads = self.max_threads.max(rayon::current_num_threads());
    }
    fn merge(&mut self, other: Self) {
        self.max_threads = self.max_threads.max(other.max_threads);
    }
}

#[derive(Default, Serialize, Deserialize)]
struct BlockingRecCount {
    count: u64,
}

impl Aggregator for BlockingRecCount {
    fn ingest(&mut self, record: &Value) {
        self.count += 1;
        if record.get("pause").and_then(Value::as_bool) == Some(true) {
            if let Some(pause) = current_aggregate_pause() {
                pause.mark_entered_and_wait();
            }
        }
    }

    fn merge(&mut self, other: Self) {
        self.count += other.count;
    }
}

/// An aggregator whose `merge` always panics. Models a non-associative or
/// otherwise buggy user `Aggregator` whose merge unwinds rather than returning
/// `Err`. `ingest` is well-behaved so per-input shards still build and the
/// panic only fires during the merge phase.
#[derive(Default, Serialize, Deserialize)]
struct PanicOnMergeAgg {
    count: u64,
}
impl Aggregator for PanicOnMergeAgg {
    fn ingest(&mut self, _record: &Value) {
        self.count += 1;
    }
    fn merge(&mut self, _other: Self) {
        panic!("PanicOnMergeAgg::merge intentionally panics");
    }
}

struct AggregatePause {
    entered: Mutex<bool>,
    entered_cv: Condvar,
    released: Mutex<bool>,
    released_cv: Condvar,
}

impl AggregatePause {
    fn new() -> Self {
        Self {
            entered: Mutex::new(false),
            entered_cv: Condvar::new(),
            released: Mutex::new(false),
            released_cv: Condvar::new(),
        }
    }

    fn mark_entered_and_wait(&self) {
        {
            let mut entered = self.entered.lock().unwrap();
            *entered = true;
            self.entered_cv.notify_all();
        }

        let released = self.released.lock().unwrap();
        drop(
            self.released_cv
                .wait_while(released, |released| !*released)
                .unwrap(),
        );
    }

    fn wait_entered(&self, timeout: Duration) -> bool {
        let entered = self.entered.lock().unwrap();
        let (entered, _) = self
            .entered_cv
            .wait_timeout_while(entered, timeout, |entered| !*entered)
            .unwrap();
        *entered
    }

    fn release(&self) {
        let mut released = self.released.lock().unwrap();
        *released = true;
        self.released_cv.notify_all();
    }
}

fn aggregate_pause_slot() -> &'static Mutex<Option<Arc<AggregatePause>>> {
    static SLOT: OnceLock<Mutex<Option<Arc<AggregatePause>>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

fn current_aggregate_pause() -> Option<Arc<AggregatePause>> {
    aggregate_pause_slot().lock().unwrap().clone()
}

struct AggregatePauseGuard {
    pause: Arc<AggregatePause>,
}

impl AggregatePauseGuard {
    fn install(pause: Arc<AggregatePause>) -> Self {
        *aggregate_pause_slot().lock().unwrap() = Some(pause.clone());
        Self { pause }
    }
}

impl Drop for AggregatePauseGuard {
    fn drop(&mut self) {
        self.pause.release();
        *aggregate_pause_slot().lock().unwrap() = None;
    }
}

fn aggregate_shard_paths_recursive(dir: &Path) -> Vec<PathBuf> {
    if !dir.exists() {
        return Vec::new();
    }

    let mut shards = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            shards.extend(aggregate_shard_paths_recursive(&path));
            continue;
        }
        let is_aggregate_shard = path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.starts_with("agg_") && name.ends_with(".json"))
            .unwrap_or(false);
        if is_aggregate_shard {
            shards.push(path);
        }
    }
    shards
}

/// Per-run `run_*` shard directories directly under `shards_dir`. After a
/// successful aggregate these are cleaned up; a non-empty result means a
/// run leaked its scratch directory.
fn shard_run_dirs(shards_dir: &Path) -> Vec<PathBuf> {
    if !shards_dir.exists() {
        return Vec::new();
    }
    fs::read_dir(shards_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.starts_with("run_"))
                    .unwrap_or(false)
        })
        .collect()
}

fn wait_for_aggregate_shards(dir: &Path, timeout: Duration) -> Vec<PathBuf> {
    let start = Instant::now();
    loop {
        let shards = aggregate_shard_paths_recursive(dir);
        if !shards.is_empty() || start.elapsed() >= timeout {
            return shards;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Build inputs by spooling per-month parts first, then aggregate those JSONL files.
/// We spool for authors ["alice","bob","charlie","AutoModerator"].
/// The sample month has 5 rows total, but `[deleted]` is **not** in that allow-list,
/// so the spooled inputs contain **4** rows (2 submissions + 2 comments).
#[test]
fn aggregate_over_jsonl_inputs() {
    let base = make_corpus_basic();

    // First, produce small JSONL inputs via spooling.
    let spool_dir = base.join("spool_for_agg");
    let authors = vec![
        "alice".to_string(),
        "bob".to_string(),
        "charlie".to_string(),
        "AutoModerator".to_string(),
    ];

    let (parts, n) = RedditETL::new()
        .base_dir(&base)
        .progress(false)
        .scan()
        .subreddit("programming")
        .authors(&authors)
        .extract_spool_monthly(&spool_dir)
        .unwrap();

    // Expect 4 records (everything except the [deleted] comment).
    assert_eq!(n, 4);

    // Now aggregate those inputs.
    let shards_dir = base.join("agg_shards");
    let final_out = base.join("agg_final.json");
    RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel::<RecCount>(parts, &shards_dir, &final_out, false)
        .unwrap();

    // Validate the final count matches the number of spooled rows.
    let f = File::open(&final_out).unwrap();
    let r = BufReader::new(f);
    let agg: RecCount = serde_json::from_reader(r).unwrap();
    assert_eq!(agg.count, 4);
}

#[test]
fn aggregate_same_basename_inputs_get_distinct_shards() {
    let tmp = tempfile::tempdir().unwrap();
    let run_a = tmp.path().join("run_a");
    let run_b = tmp.path().join("run_b");
    fs::create_dir_all(&run_a).unwrap();
    fs::create_dir_all(&run_b).unwrap();
    let a = run_a.join("part_RC_2006-01.jsonl");
    let b = run_b.join("part_RC_2006-01.jsonl");
    fs::write(&a, "{\"id\":\"a1\"}\n{\"id\":\"a2\"}\n").unwrap();
    fs::write(&b, "{\"id\":\"b1\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (agg, report) = RedditETL::new()
        .progress(false)
        .parallelism(2)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![a, b], &shards_dir)
        .unwrap();

    // Two same-basename inputs from different directories must land on
    // distinct shard paths. Had they collided, one shard would have
    // overwritten the other and the merged count could not have reached 3.
    assert_eq!(agg.count, 3);
    assert_eq!(report.merged_shards, 2);
    assert_eq!(report.problem_count(), 0);

    // The per-run shard directory is scratch and is removed after a
    // successful merge — no `run_*` directory or shard file may survive.
    assert!(
        shard_run_dirs(&shards_dir).is_empty(),
        "per-run shard directory leaked after a successful aggregate"
    );
    assert_eq!(aggregate_shard_paths_recursive(&shards_dir).len(), 0);
}

#[test]
fn aggregate_removes_per_run_shard_dir_after_successful_merge() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input.jsonl");
    fs::write(&input, "{\"id\":\"a\"}\n{\"id\":\"b\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    // Repeated runs against a shared shards_dir must not accumulate
    // `run_*` scratch directories of shard JSON.
    for _ in 0..3 {
        let (agg, report) = RedditETL::new()
            .progress(false)
            .aggregate_jsonls_parallel_collect::<RecCount>(vec![input.clone()], &shards_dir)
            .unwrap();
        assert_eq!(agg.count, 2);
        assert_eq!(report.problem_count(), 0);
    }

    assert!(
        shard_run_dirs(&shards_dir).is_empty(),
        "aggregate leaked per-run shard directories across repeated runs"
    );
    assert_eq!(aggregate_shard_paths_recursive(&shards_dir).len(), 0);
}

#[test]
fn aggregate_panicking_merge_does_not_leak_run_scratch() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.jsonl");
    let b = tmp.path().join("b.jsonl");
    fs::write(&a, "{\"id\":\"a\"}\n").unwrap();
    fs::write(&b, "{\"id\":\"b\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    // A panicking `Aggregator::merge` unwinds past the success-only cleanup
    // check. The per-run `run_*` scratch directory must still be reclaimed by
    // the `ScratchGuard` drop during unwind — otherwise every panicking run
    // silently accretes shard JSON under `shards_dir`.
    let shards_for_run = shards_dir.clone();
    let inputs = vec![a.clone(), b.clone()];
    let result = std::panic::catch_unwind(move || {
        RedditETL::new()
            .progress(false)
            .parallelism(2)
            .aggregate_jsonls_parallel_collect::<PanicOnMergeAgg>(inputs, &shards_for_run)
    });
    assert!(
        result.is_err(),
        "a panicking Aggregator::merge should unwind, not return"
    );

    assert!(
        shard_run_dirs(&shards_dir).is_empty(),
        "panicking merge leaked per-run scratch: {:?}",
        shard_run_dirs(&shards_dir)
    );
    assert_eq!(
        aggregate_shard_paths_recursive(&shards_dir).len(),
        0,
        "panicking merge leaked shard JSON"
    );
}

#[test]
fn aggregate_malformed_json_line_counts_shard_error() {
    let tmp = tempfile::tempdir().unwrap();
    let bad = tmp.path().join("bad.jsonl");
    fs::write(&bad, "{\"id\":\"ok\"}\nnot-json\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (agg, report) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![bad.clone()], &shards_dir)
        .unwrap();

    assert_eq!(agg.count, 0, "malformed shard should be dropped");
    assert_eq!(report.merged_shards, 0);
    assert_eq!(report.fatal_count(), 1);
    assert_eq!(report.fatal_inputs[0].input, bad);
    assert_eq!(aggregate_shard_paths_recursive(&shards_dir).len(), 0);
}

#[test]
fn aggregate_ignores_stale_shards_between_runs() {
    let tmp = tempfile::tempdir().unwrap();
    let a = tmp.path().join("a.jsonl");
    let b = tmp.path().join("b.jsonl");
    fs::write(&a, "{\"id\":\"a\"}\n").unwrap();
    fs::write(&b, "{\"id\":\"b\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (first, report) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![a.clone(), b], &shards_dir)
        .unwrap();
    assert_eq!(first.count, 2);
    assert_eq!(report.merged_shards, 2);
    assert_eq!(report.problem_count(), 0);

    let (second, report) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![a], &shards_dir)
        .unwrap();
    assert_eq!(
        second.count, 1,
        "stale shard from first run leaked into second"
    );
    assert_eq!(report.merged_shards, 1);
    assert_eq!(report.problem_count(), 0);
}

#[test]
fn aggregate_concurrent_same_shards_dir_different_outputs_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let a_fast = tmp.path().join("a_fast.jsonl");
    let a_slow = tmp.path().join("a_slow.jsonl");
    let b_input = tmp.path().join("b.jsonl");
    fs::write(&a_fast, "{\"id\":\"a1\"}\n{\"id\":\"a2\"}\n").unwrap();
    fs::write(&a_slow, "{\"id\":\"a3\",\"pause\":true}\n").unwrap();
    fs::write(&b_input, "{\"id\":\"b1\"}\n{\"id\":\"b2\"}\n").unwrap();
    let shards_dir = tmp.path().join("agg_shards");
    let final_a = tmp.path().join("agg_a.json");
    let final_b = tmp.path().join("agg_b.json");

    let pause = Arc::new(AggregatePause::new());
    let _pause_guard = AggregatePauseGuard::install(pause.clone());

    let shards_for_a = shards_dir.clone();
    let out_a = final_a.clone();
    let handle_a = thread::spawn(move || {
        RedditETL::new()
            .progress(false)
            .parallelism(2)
            .aggregate_jsonls_parallel::<BlockingRecCount>(
                vec![a_fast, a_slow],
                &shards_for_a,
                &out_a,
                false,
            )
    });

    if !pause.wait_entered(Duration::from_secs(5)) {
        pause.release();
        let _ = handle_a.join();
        panic!("first aggregate did not reach the synchronization point");
    }

    let first_shards = wait_for_aggregate_shards(&shards_dir, Duration::from_secs(5));
    if first_shards.is_empty() {
        pause.release();
        let _ = handle_a.join();
        panic!("first aggregate did not publish a live shard before the second run");
    }

    let shards_for_b = shards_dir.clone();
    let out_b = final_b.clone();
    let handle_b = thread::spawn(move || {
        RedditETL::new()
            .progress(false)
            .parallelism(1)
            .aggregate_jsonls_parallel::<BlockingRecCount>(
                vec![b_input],
                &shards_for_b,
                &out_b,
                false,
            )
    });

    let report_b = match handle_b.join().expect("second aggregate thread panicked") {
        Ok(report) => report,
        Err(e) => {
            pause.release();
            let _ = handle_a.join();
            panic!("second aggregate failed: {e:#}");
        }
    };
    assert_eq!(report_b.merged_shards, 1);
    assert_eq!(report_b.problem_count(), 0);

    pause.release();
    let report_a = handle_a
        .join()
        .expect("first aggregate thread panicked")
        .expect("first aggregate failed after second run shared shards_dir");
    assert_eq!(report_a.merged_shards, 2);
    assert_eq!(report_a.problem_count(), 0);

    let agg_a: BlockingRecCount =
        serde_json::from_str(&fs::read_to_string(&final_a).unwrap()).unwrap();
    let agg_b: BlockingRecCount =
        serde_json::from_str(&fs::read_to_string(&final_b).unwrap()).unwrap();
    assert_eq!(agg_a.count, 3);
    assert_eq!(agg_b.count, 2);
}

#[test]
fn aggregate_partial_read_is_reported_and_skipped_by_default() {
    let tmp = tempfile::tempdir().unwrap();
    let partial = tmp.path().join("partial.jsonl");
    fs::write(&partial, b"{\"id\":\"ok\"}\n{\"id\":\"").unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&partial)
        .unwrap()
        .write_all(&[0xff, b'\n'])
        .unwrap();
    let shards_dir = tmp.path().join("agg_shards");

    let (agg, report) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect::<RecCount>(vec![partial.clone()], &shards_dir)
        .unwrap();

    assert_eq!(agg.count, 0, "strict partial-read policy drops the input");
    assert_eq!(report.merged_shards, 0);
    assert_eq!(report.partial_count(), 1);
    assert_eq!(report.partial_inputs[0].input, partial);
}

#[test]
fn aggregate_partial_read_can_be_merged_by_explicit_policy() {
    let tmp = tempfile::tempdir().unwrap();
    let partial = tmp.path().join("partial.jsonl");
    fs::write(&partial, b"{\"id\":\"ok\"}\n{\"id\":\"").unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(&partial)
        .unwrap()
        .write_all(&[0xff, b'\n'])
        .unwrap();

    let (agg, report) = RedditETL::new()
        .progress(false)
        .aggregate_jsonls_parallel_collect_with_policy::<RecCount, _>(
            vec![partial.clone()],
            &tmp.path().join("agg_shards"),
            RecCount::default,
            AggregatePartialReadPolicy::MergePartial,
        )
        .unwrap();

    assert_eq!(
        agg.count, 1,
        "explicit tolerant policy merges records read before the error"
    );
    assert_eq!(report.merged_shards, 1);
    assert_eq!(report.partial_count(), 1);
    assert_eq!(report.partial_inputs[0].input, partial);
}

#[test]
fn aggregate_honors_parallelism_one() {
    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("input.jsonl");
    fs::write(&input, "{\"id\":\"a\"}\n{\"id\":\"b\"}\n").unwrap();

    let (agg, report) = RedditETL::new()
        .progress(false)
        .parallelism(1)
        .aggregate_jsonls_parallel_collect::<ThreadCountAgg>(
            vec![input],
            &tmp.path().join("shards"),
        )
        .unwrap();

    assert_eq!(report.merged_shards, 1);
    assert_eq!(report.problem_count(), 0);
    assert_eq!(agg.max_threads, 1);
}
