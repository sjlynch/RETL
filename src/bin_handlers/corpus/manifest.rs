
fn run_corpus_manifest(args: CorpusManifestArgs) -> Result<()> {
    write_text_or_stdout(&args.out, |w| {
        w.write_all(CorpusManifest::builtin_json().as_bytes())?;
        Ok(())
    })
}
