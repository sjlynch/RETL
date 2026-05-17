
fn resolver_fingerprint_path(final_dest: &Path) -> PathBuf {
    let mut sidecar = final_dest.as_os_str().to_os_string();
    sidecar.push(RESOLVER_SIDECAR_SUFFIX);
    PathBuf::from(sidecar)
}

fn resolver_kind_label(kind: FileKind) -> &'static str {
    match kind {
        FileKind::Comment => "comments",
        FileKind::Submission => "submissions",
    }
}

fn build_resolver_fingerprint(
    job: &FileJob,
    parent_ids: &ParentIdsFingerprint,
    resolution_range: &AttachResolutionRange,
    payload_spec: &ParentPayloadSpec,
) -> ResolverFingerprint {
    ResolverFingerprint {
        version: RESOLVER_FINGERPRINT_VERSION,
        resolver_format_version: RESOLVER_FORMAT_VERSION,
        source: ResolverSourceFingerprint {
            kind: resolver_kind_label(job.kind).to_string(),
            month: job.ym.to_string(),
            file: attach_file_identity(&job.path),
        },
        resolution_range: resolution_range.clone(),
        parent_ids: parent_ids.clone(),
        payload: parent_payload_fingerprint(payload_spec),
    }
}

fn resolver_fingerprint_matches(sidecar_path: &Path, expected: &ResolverFingerprint) -> bool {
    match crate::util::open_with_default_backoff(sidecar_path) {
        Ok(f) => match serde_json::from_reader::<_, ResolverFingerprint>(BufReader::new(f)) {
            Ok(actual) if &actual == expected => true,
            Ok(_) => {
                tracing::debug!(path=%sidecar_path.display(), "resume: parent resolver fingerprint changed, rebuilding");
                false
            }
            Err(e) => {
                tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: parent resolver fingerprint sidecar is unreadable, rebuilding");
                false
            }
        },
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path=%sidecar_path.display(), "resume: parent resolver fingerprint sidecar missing, rebuilding");
            false
        }
        Err(e) => {
            tracing::warn!(path=%sidecar_path.display(), error=%e, "resume: parent resolver fingerprint sidecar cannot be opened, rebuilding");
            false
        }
    }
}

fn write_resolver_fingerprint_atomic(
    sidecar_path: &Path,
    fingerprint: &ResolverFingerprint,
    write_buf_bytes: usize,
) -> Result<()> {
    write_at_path_atomic(sidecar_path, write_buf_bytes, |w| {
        serde_json::to_writer_pretty(&mut *w, fingerprint)?;
        w.write_all(b"\n")?;
        Ok(())
    })
    .with_context(|| {
        format!(
            "write resolver fingerprint sidecar {}",
            sidecar_path.display()
        )
    })
}
