
pub(crate) fn run_corpus(args: CorpusArgs) -> Result<()> {
    match args.command {
        CorpusCommand::Plan(plan) => run_corpus_plan(plan),
        CorpusCommand::Manifest(manifest) => run_corpus_manifest(manifest),
    }
}

fn load_corpus_manifest(path: Option<&Path>) -> Result<CorpusManifest> {
    match path {
        Some(path) => {
            let file = retl::open_with_default_backoff(path)
                .with_context(|| format!("opening corpus manifest {}", path.display()))?;
            let reader = BufReader::new(file);
            CorpusManifest::from_reader(reader)
                .with_context(|| format!("parsing corpus manifest {}", path.display()))
        }
        None => CorpusManifest::builtin().with_context(|| "parsing built-in corpus manifest"),
    }
}

fn write_text_or_stdout<T, F>(out: &Path, body: F) -> Result<T>
where
    F: FnOnce(&mut dyn Write) -> Result<T>,
{
    if out == Path::new("-") {
        let stdout = io::stdout();
        let mut w = BufWriter::new(stdout.lock());
        let result = body(&mut w)?;
        w.flush()?;
        Ok(result)
    } else {
        write_text_atomic(out, CLI_TEXT_WRITE_BUF_BYTES, body)
    }
}
