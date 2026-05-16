
pub(crate) fn run_quickstart(args: QuickstartArgs) -> Result<()> {
    let data_dir = prepare_quickstart_sample_corpus(&args.out_dir)?;
    let work_dir = args.out_dir.join("etl_work");
    retl::create_dir_all_with_default_backoff(&work_dir)
        .with_context(|| format!("creating quickstart work dir {}", work_dir.display()))?;

    let base = RedditETL::new()
        .base_dir(&data_dir)
        .work_dir(&work_dir)
        .parallelism(1)
        .file_concurrency(1)
        .progress(false)
        .sources(Sources::Both);

    let counts = base
        .clone()
        .scan()
        .subreddit("programming")
        .count_by_month()?;

    let mut authors = Vec::new();
    base.scan()
        .subreddit("programming")
        .for_each_username(|u| authors.push(u.to_string()))?;
    authors.sort();

    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    writeln!(
        w,
        "Prepared sample corpus from benches/data/sample.jsonl under {}",
        data_dir.display()
    )?;
    writeln!(w, "Feature demo: subreddit=programming, source=RC+RS")?;
    for (ym, n) in counts {
        writeln!(w, "{ym}\t{n} records")?;
    }
    writeln!(
        w,
        "Found {} unique authors: {}",
        authors.len(),
        authors.join(", ")
    )?;
    writeln!(
        w,
        "Next: retl sample --data-dir {} --source both --subreddit programming --limit 3",
        data_dir.display()
    )?;
    w.flush()?;
    Ok(())
}

fn prepare_quickstart_sample_corpus(out_dir: &Path) -> Result<PathBuf> {
    let data_dir = out_dir.join("data");
    let comments_dir = data_dir.join("comments");
    let submissions_dir = data_dir.join("submissions");
    retl::create_dir_all_with_default_backoff(&comments_dir)
        .with_context(|| format!("creating comments dir {}", comments_dir.display()))?;
    retl::create_dir_all_with_default_backoff(&submissions_dir)
        .with_context(|| format!("creating submissions dir {}", submissions_dir.display()))?;

    let mut comments = Vec::new();
    let mut submissions = Vec::new();
    for line in QUICKSTART_SAMPLE_JSONL
        .lines()
        .filter(|line| !line.trim().is_empty())
    {
        let value: Value = serde_json::from_str(line).context("parse sample JSONL line")?;
        if value.get("body").is_some() {
            comments.push(line.to_owned());
        } else if value.get("title").is_some() {
            submissions.push(line.to_owned());
        }
    }

    write_quickstart_zst_lines(&comments_dir.join("RC_2020-01.zst"), &comments)?;
    write_quickstart_zst_lines(&submissions_dir.join("RS_2020-01.zst"), &submissions)?;

    Ok(data_dir)
}

fn write_quickstart_zst_lines(path: &Path, lines: &[String]) -> Result<()> {
    let file = retl::create_with_default_backoff(path)
        .with_context(|| format!("creating quickstart sample {}", path.display()))?;
    let mut enc =
        zstd::stream::write::Encoder::new(file, QUICKSTART_ZST_LEVEL).context("zstd encoder")?;
    enc.include_checksum(true).context("enable zstd checksum")?;
    for line in lines {
        enc.write_all(line.as_bytes())?;
        enc.write_all(b"\n")?;
    }
    enc.finish().context("finish zstd frame")?;
    Ok(())
}
