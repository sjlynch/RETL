use super::*;

pub(super) fn write_zst_lines(path: &Path, lines: &[String]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = File::create(path).unwrap();
    let mut enc = zstd::stream::write::Encoder::new(file, 3).unwrap();
    for line in lines {
        writeln!(&mut enc, "{line}").unwrap();
    }
    enc.finish().unwrap();
}

pub(super) fn make_one_comment_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    let line = json!({
        "id":"c1",
        "author":"alice",
        "subreddit":"programming",
        "created_utc":1136073600_i64,
        "score":1,
        "body":"hello",
    })
    .to_string();
    write_zst_lines(&base.join("comments").join("RC_2006-01.zst"), &[line]);
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

pub(super) fn one_month_scan(base: &Path) -> ScanPlan {
    RedditETL::new()
        .base_dir(base)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 1)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
}

pub(super) fn manifest_json(out_dir: &Path) -> Value {
    serde_json::from_slice(&fs::read(out_dir.join("_progress.json")).unwrap()).unwrap()
}

pub(super) fn comment_line(id: &str, author: &str, created_utc: i64) -> String {
    json!({
        "id": id,
        "author": author,
        "subreddit": "programming",
        "created_utc": created_utc,
        "score": 1,
        "body": "hello",
        "parent_id": "t3_s1",
        "link_id": "t3_s1",
    })
    .to_string()
}

pub(super) fn make_two_month_comment_corpus() -> PathBuf {
    let base = tempfile::tempdir().unwrap().keep();
    write_zst_lines(
        &base.join("comments").join("RC_2006-01.zst"),
        &[comment_line("jan_real", "jan_real", 1_136_073_600)],
    );
    write_zst_lines(
        &base.join("comments").join("RC_2006-02.zst"),
        &[comment_line("feb_real", "feb_real", 1_138_752_000)],
    );
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

/// Build a corpus of `months` consecutive monthly comment files starting at
/// `RC_2006-01.zst`, one `programming` record each. Used by the manifest-
/// commit abort tests, which need many months so a save failure injected
/// early can be shown to *stop* further publishing rather than letting every
/// remaining month land.
pub(super) fn make_n_month_comment_corpus(months: u8) -> PathBuf {
    assert!((1..=12).contains(&months), "helper covers a single 2006 year");
    let base = tempfile::tempdir().unwrap().keep();
    for i in 0..months {
        let ym = YearMonth::new(2006, i + 1);
        let created = 1_136_073_600 + i64::from(i) * 2_419_200;
        write_zst_lines(
            &base.join("comments").join(format!("RC_{ym}.zst")),
            &[comment_line(&format!("c{i}"), "alice", created)],
        );
    }
    fs::create_dir_all(base.join("submissions")).unwrap();
    base
}

pub(super) fn two_month_scan(base: &Path, work_dir: &Path) -> ScanPlan {
    RedditETL::new()
        .base_dir(base)
        .work_dir(work_dir)
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .progress(false)
        .resume(true)
        .scan()
        .subreddit("programming")
}

pub(super) fn seed_scan_checkpoint_part(
    plan: &ScanPlan,
    key: &str,
    contents: &str,
    lines: u64,
) -> PathBuf {
    let files = plan_pipeline_files(&plan.etl, Some(&plan.query)).unwrap();
    let fingerprint = build_resume_fingerprint(
        &plan.etl,
        &plan.query,
        ANALYTICS_CHECKPOINT_OPERATION,
        plan.limit,
        &files,
    )
    .unwrap();
    let work_dir = plan.etl.ensure_work_dir().unwrap();
    let checkpoint_dir = scan_checkpoint_dir(&work_dir, &fingerprint);
    fs::create_dir_all(&checkpoint_dir).unwrap();
    let part = checkpoint_part_path(&checkpoint_dir, key);
    fs::write(&part, contents).unwrap();
    let mut months = std::collections::HashMap::new();
    months.insert(
        key.to_string(),
        MonthEntry {
            size: fs::metadata(&part).unwrap().len(),
            lines,
            sha256: None,
        },
    );
    crate::progress_manifest::save(&checkpoint_dir, &months, Some(&fingerprint)).unwrap();
    checkpoint_dir
}

pub(super) fn read_tsv_i64(path: &Path) -> std::collections::HashMap<String, i64> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('\t')?;
            Some((key.to_string(), value.parse::<i64>().unwrap()))
        })
        .collect()
}

pub(super) fn spool_fingerprint_for(plan: ScanPlan) -> String {
    let plan = plan.build().unwrap();
    let files = plan_pipeline_files(&plan.etl, Some(&plan.query)).unwrap();
    build_resume_fingerprint(&plan.etl, &plan.query, "spool", plan.limit, &files).unwrap()
}
