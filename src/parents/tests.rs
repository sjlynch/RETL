
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::RedditETL;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    fn dummy_file_identity(tag: &str) -> AttachFileIdentity {
        AttachFileIdentity {
            path: tag.to_string(),
            exists: true,
            len: Some(tag.len() as u64),
            modified_unix_secs: Some(1),
            modified_nanos: Some(2),
        }
    }

    fn dummy_payload_fingerprint() -> ParentPayloadFingerprint {
        ParentPayloadFingerprint {
            payload_format_version: STRUCTURED_PARENT_PAYLOAD_FORMAT_VERSION,
            full_record: false,
            fields: vec!["body".to_string()],
        }
    }

    fn dummy_parent_id_set(kind: &str) -> ParentIdSetFingerprint {
        ParentIdSetFingerprint {
            kind: kind.to_string(),
            storage: "memory".to_string(),
            ids: 1,
            digest: format!("{kind}-digest"),
            shard_count: 0,
            backing_shards: Vec::new(),
        }
    }

    fn dummy_parent_ids_fingerprint() -> ParentIdsFingerprint {
        ParentIdsFingerprint {
            t1: dummy_parent_id_set("t1"),
            t3: dummy_parent_id_set("t3"),
        }
    }

    fn dummy_resolver_fingerprint(tag: &str) -> ResolverFingerprint {
        ResolverFingerprint {
            version: RESOLVER_FINGERPRINT_VERSION,
            resolver_format_version: RESOLVER_FORMAT_VERSION,
            source: ResolverSourceFingerprint {
                kind: "comments".to_string(),
                month: "2020-01".to_string(),
                file: dummy_file_identity(tag),
            },
            resolution_range: AttachResolutionRange {
                start: None,
                end: None,
            },
            parent_ids: dummy_parent_ids_fingerprint(),
            payload: dummy_payload_fingerprint(),
        }
    }

    fn dummy_attach_fingerprint(tag: &str) -> AttachFingerprint {
        AttachFingerprint {
            version: ATTACH_FINGERPRINT_VERSION,
            attach_format_version: ATTACH_FORMAT_VERSION,
            input: dummy_file_identity(tag),
            resolution_range: AttachResolutionRange {
                start: None,
                end: None,
            },
            parent_cache: AttachParentCacheFingerprint {
                payload: dummy_payload_fingerprint(),
                comment_shards: AttachShardSetFingerprint {
                    index_present: false,
                    shards: 0,
                    digest: format!("comments-{tag}"),
                },
                submission_shards: AttachShardSetFingerprint {
                    index_present: false,
                    shards: 0,
                    digest: format!("submissions-{tag}"),
                },
                eager_comments: AttachMapDigest {
                    entries: 0,
                    digest: format!("comments-map-{tag}"),
                },
                eager_submissions: AttachMapDigest {
                    entries: 0,
                    digest: format!("submissions-map-{tag}"),
                },
            },
        }
    }

    fn inprogress_entries(staging: &Path) -> Vec<PathBuf> {
        if !staging.exists() {
            return Vec::new();
        }
        let mut paths: Vec<PathBuf> = std::fs::read_dir(staging)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|s| s.to_str())
                    .map(|name| name.ends_with(INPROGRESS_EXT))
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();
        paths
    }

    fn fixed_inprogress_path(path: &Path) -> PathBuf {
        let mut staged = path.as_os_str().to_os_string();
        staged.push(INPROGRESS_EXT);
        PathBuf::from(staged)
    }

    fn release_waiters(release: &Arc<(Mutex<bool>, Condvar)>) {
        let (lock, cvar) = &**release;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }

    #[test]
    fn resolver_fingerprint_publish_failure_cleans_only_current_staged_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let staging = ensure_staging_dir(root).expect("staging dir");
        let unrelated = staging.join("other-run.retl-999999-0.inprogress");
        std::fs::write(&unrelated, b"other run").expect("write unrelated staged file");

        let sidecar_path = root.join("RC_2020-01.json.parents-resolve.json");
        std::fs::create_dir(&sidecar_path).expect("create publish-blocking directory");

        // Renaming onto an existing directory returns ERROR_ACCESS_DENIED (5)
        // on Windows, which is in the retry list (covers AV / sharing-violation
        // hiccups in production). Without a cap, this test waits out the full
        // 16-try * 50 ms linear backoff TWICE (rename, then copy fallback) —
        // ~14 s. Cap to 1 try / 0 ms delay so the failure surfaces in
        // microseconds; the invariant ("publish failure cleans only this
        // run's staged file") is independent of how long the failure took.
        let _backoff_cap = crate::util::cap_backoff_budget_for_test(1, 0);

        let result = write_resolver_fingerprint_atomic(
            &sidecar_path,
            &dummy_resolver_fingerprint("resolver-failure"),
            1024,
        );
        assert!(
            result.is_err(),
            "publishing over a directory should fail and trigger cleanup"
        );

        assert!(
            unrelated.exists(),
            "cleanup must not remove another run's staged file"
        );
        assert_eq!(
            inprogress_entries(&staging),
            vec![unrelated],
            "only this run's unique staged sidecar should be removed"
        );
        assert!(
            !fixed_inprogress_path(&sidecar_path).exists(),
            "resolver sidecars must not stage next to the final path"
        );
    }

    #[test]
    #[serial_test::serial]
    fn concurrent_attach_fingerprint_writes_use_distinct_staged_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let staging = ensure_staging_dir(root).expect("staging dir");
        let sidecar_path = root.join("part_RC_2020-01.jsonl.parents-attach.json");
        let sidecar_name = sidecar_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap()
            .to_string();

        let observed = Arc::new((Mutex::new(Vec::<PathBuf>::new()), Condvar::new()));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let observed_for_hook = Arc::clone(&observed);
        let release_for_hook = Arc::clone(&release);
        let staging_for_hook = staging.clone();
        let sidecar_name_for_hook = sidecar_name.clone();
        let _guard = crate::atomic_write::set_stage_path_observer_for_tests(Arc::new(
            move |staged: &Path| {
                let matches_sidecar = staged.parent() == Some(staging_for_hook.as_path())
                    && staged
                        .file_name()
                        .and_then(|s| s.to_str())
                        .map(|name| {
                            name.starts_with(&sidecar_name_for_hook)
                                && name.ends_with(INPROGRESS_EXT)
                        })
                        .unwrap_or(false);
                if !matches_sidecar {
                    return;
                }

                let (observed_lock, observed_cvar) = &*observed_for_hook;
                observed_lock.lock().unwrap().push(staged.to_path_buf());
                observed_cvar.notify_all();

                let (release_lock, release_cvar) = &*release_for_hook;
                let mut released = release_lock.lock().unwrap();
                while !*released {
                    released = release_cvar.wait(released).unwrap();
                }
            },
        ));

        let mut handles = Vec::new();
        for tag in ["first", "second"] {
            let staging = staging.clone();
            let sidecar_path = sidecar_path.clone();
            handles.push(thread::spawn(move || {
                write_attach_fingerprint_atomic(
                    &staging,
                    &sidecar_path,
                    &dummy_attach_fingerprint(tag),
                    1024,
                )
            }));
        }

        let snapshot = {
            let (lock, cvar) = &*observed;
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut observed_paths = lock.lock().unwrap();
            while observed_paths.len() < 2 {
                let now = Instant::now();
                if now >= deadline {
                    release_waiters(&release);
                    panic!(
                        "timed out waiting for two live sidecar staged paths; saw {:?}",
                        *observed_paths
                    );
                }
                let timeout = deadline.saturating_duration_since(now);
                let (guard, wait) = cvar.wait_timeout(observed_paths, timeout).unwrap();
                observed_paths = guard;
                if wait.timed_out() && observed_paths.len() < 2 {
                    release_waiters(&release);
                    panic!(
                        "timed out waiting for two live sidecar staged paths; saw {:?}",
                        *observed_paths
                    );
                }
            }
            observed_paths.clone()
        };
        let live_entries = inprogress_entries(&staging);
        let fixed_path_exists = fixed_inprogress_path(&sidecar_path).exists();

        release_waiters(&release);
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        assert_eq!(snapshot.len(), 2);
        assert_ne!(
            snapshot[0], snapshot[1],
            "concurrent sidecar writers must not share a fixed staged path"
        );
        assert_eq!(
            live_entries.len(),
            2,
            "both concurrent sidecar staging files should be visible while writers are live"
        );
        assert!(
            live_entries.iter().all(|path| snapshot.contains(path)),
            "live staged entries should be the observed unique sidecar paths"
        );
        assert!(
            !fixed_path_exists,
            "attach sidecars must not use the legacy fixed .inprogress sibling path"
        );
        assert!(
            inprogress_entries(&staging).is_empty(),
            "successful sidecar writes clean up staged files"
        );
    }

    #[test]
    fn attach_parents_with_no_file_name_input_returns_err() {
        let etl = RedditETL::new();
        let out_dir = tempfile::tempdir().expect("create out dir");
        let parents = ParentMaps {
            comments: HashMap::new(),
            submissions: HashMap::new(),
            comment_shards: None,
            submission_shards: None,
            payload_spec: Default::default(),
        };

        // `..` has no `file_name()` per `Path::file_name` semantics. The function
        // should surface a clean `Err` (from whichever guard fires first) rather
        // than panicking inside the rayon worker.
        let inputs = vec![PathBuf::from("..")];
        let result =
            etl.attach_parents_jsonls_parallel_with_stats(inputs, out_dir.path(), &parents, false);
        assert!(
            result.is_err(),
            "expected Err for input path with no file_name(), got Ok"
        );
    }
}
