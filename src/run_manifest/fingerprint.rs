
fn manifest_fingerprint(
    operation: &str,
    command: Option<&str>,
    api_operation: Option<&str>,
    query: &Value,
    options: &Value,
    corpus: &CorpusSnapshot,
    inputs: &[FileIdentity],
    output: &OutputSnapshot,
    counts: &BTreeMap<String, u64>,
    resume: &ResumeSnapshot,
) -> Result<String> {
    let output_for_hash = json!({
        "kind": output.kind.as_str(),
        "format": output.format.as_str(),
    });
    let payload = json!({
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "operation": operation,
        "command": command,
        "api_operation": api_operation,
        "query": query,
        "options": options,
        "corpus": corpus,
        "inputs": inputs,
        "output": output_for_hash,
        "counts": counts,
        "resume": resume,
    });
    let bytes = serde_json::to_vec(&payload).context("serialize run manifest fingerprint input")?;
    Ok(stable_fnv1a_hex(&bytes))
}
