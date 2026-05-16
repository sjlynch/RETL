#![allow(dead_code)]

use retl::{ParentIds, ParentMaps, ParentPayload, RedditETL, YearMonth};
use serde_json::json;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Output;

use super::write_zst_lines;

pub fn make_parent_pipeline_dirs(base: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    let work_dir = base.join("work");
    let spool_dir = work_dir.join("spool");
    let spool_with_parents_dir = work_dir.join("spool_with_parents");
    let parents_cache_dir = work_dir.join("parents_cache");
    let lib_tmp = work_dir.join("lib_tmp");

    for dir in [
        &spool_dir,
        &spool_with_parents_dir,
        &parents_cache_dir,
        &lib_tmp,
    ] {
        fs::create_dir_all(dir).unwrap();
    }

    (
        work_dir,
        spool_dir,
        spool_with_parents_dir,
        parents_cache_dir,
        lib_tmp,
    )
}

pub fn empty_parent_maps() -> ParentMaps {
    ParentMaps {
        comments: HashMap::new(),
        submissions: HashMap::new(),
        comment_shards: Some(HashMap::new()),
        submission_shards: Some(HashMap::new()),
        payload_spec: Default::default(),
    }
}

pub fn write_comment_parent_corpus(base: &Path, ym: YearMonth, parents: &[(&str, &str)]) {
    let lines: Vec<String> = parents
        .iter()
        .map(|(id, body)| {
            json!({
                "author": "parent_author",
                "body": body,
                "created_utc": 1136073600,
                "id": id,
                "score": 1,
                "subreddit": "programming"
            })
            .to_string()
        })
        .collect();
    write_zst_lines(
        &base.join("comments").join(format!("RC_{}.zst", ym)),
        &lines,
    );
}

pub fn write_parent_ref_spool(path: &Path, parent_id: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(
        path,
        format!(
            "{{\"id\":\"child\",\"body\":\"child\",\"parent_id\":\"{}\",\"link_id\":\"t3_s\",\"created_utc\":1136073600}}\n",
            parent_id
        ),
    )
    .unwrap();
}

pub fn collect_parent_ids_for_test(base: &Path, work_dir: &Path, spool: &Path) -> ParentIds {
    RedditETL::new()
        .base_dir(base)
        .work_dir(work_dir)
        .progress(false)
        .collect_parent_ids_from_jsonls(vec![spool.to_path_buf()])
        .unwrap()
}

pub fn read_comment_cache(cache_dir: &Path, ym: YearMonth) -> HashMap<String, String> {
    let path = cache_dir.join("comments").join(format!("RC_{}.json", ym));
    let file = File::open(path).unwrap();
    serde_json::from_reader(BufReader::new(file)).unwrap()
}

pub fn read_submission_cache(cache_dir: &Path, ym: YearMonth) -> HashMap<String, (String, String)> {
    let path = cache_dir
        .join("submissions")
        .join(format!("RS_{}.json", ym));
    let file = File::open(path).unwrap();
    serde_json::from_reader(BufReader::new(file)).unwrap()
}

pub fn read_comment_payload_cache(
    cache_dir: &Path,
    ym: YearMonth,
) -> HashMap<String, ParentPayload> {
    let path = cache_dir.join("comments").join(format!("RC_{}.json", ym));
    let file = File::open(path).unwrap();
    serde_json::from_reader(BufReader::new(file)).unwrap()
}

pub fn count_jsonl_lines(paths: &[PathBuf]) -> usize {
    paths
        .iter()
        .map(|p| {
            let f = File::open(p).unwrap();
            let r = BufReader::new(f);
            r.lines()
                .map(|line| line.unwrap())
                .filter(|s| !s.trim().is_empty())
                .count()
        })
        .sum()
}

pub fn make_cross_month_parent_corpus() -> PathBuf {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.keep();

    write_zst_lines(
        &base.join("submissions").join("RS_2005-12.zst"),
        &[json!({
            "author": "parent_author",
            "created_utc": 1133395200,
            "domain": "example.com",
            "id": "s_dec",
            "score": 10,
            "selftext": "parent selftext",
            "title": "December parent",
            "subreddit": "programming"
        })
        .to_string()],
    );

    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[json!({
            "author": "child_author",
            "body": "child body",
            "created_utc": 1136073600,
            "id": "c_jan",
            "link_id": "t3_s_dec",
            "parent_id": "t3_s_dec",
            "score": 1,
            "subreddit": "programming"
        })
        .to_string()],
    );

    base
}

pub fn write_cross_month_spool(base: &Path) -> PathBuf {
    let spool = base.join("spool_cross_month");
    fs::create_dir_all(&spool).unwrap();
    fs::write(
        spool.join("part_RC_2006-01.jsonl"),
        format!(
            "{}\n",
            json!({
                "author": "child_author",
                "body": "child body",
                "created_utc": 1136073600,
                "id": "c_jan",
                "link_id": "t3_s_dec",
                "parent_id": "t3_s_dec",
                "score": 1,
                "subreddit": "programming"
            })
        ),
    )
    .unwrap();
    spool
}

pub fn command_output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

pub fn run_parents_for_warning_case(
    base: &Path,
    spool: &Path,
    window_months: &str,
    suffix: &str,
) -> Output {
    let cache = base.join(format!("parents_cache_{suffix}"));
    let out = base.join(format!("parents_out_{suffix}"));
    let work = base.join(format!("work_parents_{suffix}"));

    super::cli::retl()
        .env("RUST_LOG", "warn")
        .args([
            "parents",
            "--spool",
            spool.to_str().unwrap(),
            "--cache",
            cache.to_str().unwrap(),
            "--out",
            out.to_str().unwrap(),
            "--data-dir",
            base.to_str().unwrap(),
            "--work-dir",
            work.to_str().unwrap(),
            "--window-months",
            window_months,
            "--no-progress",
        ])
        .output()
        .expect("parents command should run")
}
