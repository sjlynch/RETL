
fn dedup_one(input: &Path, output: &Path) -> Result<usize> {
    let f =
        crate::util::open_with_default_backoff(input).with_context(|| format!("open {}", input.display()))?;
    let mut r = BufReader::new(f);
    let mut set: AHashSet<String> = AHashSet::with_capacity(64_000);

    let mut buf = String::with_capacity(16 * 1024);
    loop {
        let n = read_line_capped(&mut r, &mut buf, DEFAULT_MAX_LINE_BYTES, input)
            .with_context(|| format!("read parent-id shard {}", input.display()))?;
        if n == 0 {
            break;
        }
        if !buf.is_empty() {
            set.insert(buf.clone());
        }
    }

    let unique_count = set.len();
    let out = crate::util::create_with_default_backoff(output)
        .with_context(|| format!("create {}", output.display()))?;
    let mut w = BufWriter::new(out);
    for s in set {
        w.write_all(s.as_bytes())?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(unique_count)
}
