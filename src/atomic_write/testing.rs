use std::path::Path;
use std::sync::Arc;

pub(super) type StagePathObserver = Arc<dyn Fn(&Path) + Send + Sync + 'static>;

static STAGE_PATH_OBSERVER: std::sync::Mutex<Option<StagePathObserver>> =
    std::sync::Mutex::new(None);

pub(crate) struct StagePathObserverGuard {
    previous: Option<StagePathObserver>,
}

impl Drop for StagePathObserverGuard {
    fn drop(&mut self) {
        let mut slot = STAGE_PATH_OBSERVER
            .lock()
            .expect("stage path observer mutex poisoned");
        *slot = self.previous.take();
    }
}

pub(crate) fn set_stage_path_observer_for_tests(
    observer: StagePathObserver,
) -> StagePathObserverGuard {
    let mut slot = STAGE_PATH_OBSERVER
        .lock()
        .expect("stage path observer mutex poisoned");
    StagePathObserverGuard {
        previous: slot.replace(observer),
    }
}

pub(super) fn notify_stage_path_for_tests(staged: &Path) {
    let observer = STAGE_PATH_OBSERVER
        .lock()
        .expect("stage path observer mutex poisoned")
        .clone();
    if let Some(observer) = observer {
        observer(staged);
    }
}

mod tests {
    use super::super::{
        ensure_staging_dir, sweep_stale_atomic_replace_tmp, sweep_stale_inprogress,
        write_jsonl_atomic, INPROGRESS_EXT,
    };
    use crate::util::ATOMIC_REPLACE_TMP_EXT;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Kills the spawned helper process when the test ends, pass or fail.
    struct ChildGuard(Child);

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Spawn a real, long-lived process so the sweep sees a genuine live PID.
    /// Used to model a PID that the OS recycled onto an unrelated process
    /// after the original RETL writer crashed.
    fn spawn_live_process() -> ChildGuard {
        let mut cmd = if cfg!(windows) {
            let mut c = Command::new("cmd");
            c.args(["/c", "ping", "-n", "60", "127.0.0.1"]);
            c
        } else {
            let mut c = Command::new("sleep");
            c.arg("60");
            c
        };
        cmd.stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn live helper process");
        // Give the OS a beat to register the process so the sweep's `sysinfo`
        // refresh reliably reports it as live.
        thread::sleep(Duration::from_millis(300));
        ChildGuard(child)
    }

    fn now_nanos() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    /// Write a staged `.inprogress` file with the RETL-owned suffix
    /// `<dest>.retl-<pid>-<nonce>-<nanos>.inprogress`.
    fn write_staged(staging: &Path, dest: &str, pid: u32, nonce: u64, nanos: u128) -> PathBuf {
        let path = staging.join(format!(
            "{dest}.retl-{pid}-{nonce}-{nanos}{INPROGRESS_EXT}"
        ));
        fs::write(&path, b"partial output").unwrap();
        path
    }

    /// Write an orphaned copy+rename fallback sibling with the RETL-owned
    /// suffix `<dest>.retl-<pid>-<nonce>-<nanos>.atomic-replace-tmp` directly
    /// in `dir` — next to where a published output would live.
    fn write_atomic_replace_tmp(
        dir: &Path,
        dest: &str,
        pid: u32,
        nonce: u64,
        nanos: u128,
    ) -> PathBuf {
        let path = dir.join(format!(
            "{dest}.retl-{pid}-{nonce}-{nanos}{ATOMIC_REPLACE_TMP_EXT}"
        ));
        fs::write(&path, b"orphaned fallback copy").unwrap();
        path
    }

    fn inprogress_entries(staging: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = fs::read_dir(staging)
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

    #[test]
    fn concurrent_writes_use_distinct_live_staging_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let staging = ensure_staging_dir(root).unwrap();
        let final_dest = root.join("out.jsonl");

        let ready = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for body in ["one\n", "two\n"] {
            let ready = Arc::clone(&ready);
            let release = Arc::clone(&release);
            let staging = staging.clone();
            let final_dest = final_dest.clone();
            handles.push(thread::spawn(move || {
                write_jsonl_atomic(&staging, &final_dest, 1024, |w| {
                    ready.wait();
                    release.wait();
                    w.write_all(body.as_bytes())?;
                    Ok(())
                })
            }));
        }

        ready.wait();
        assert_eq!(
            inprogress_entries(&staging).len(),
            2,
            "concurrent writers must not share a fixed staged path"
        );
        assert_eq!(
            sweep_stale_inprogress(root, true).unwrap(),
            0,
            "sweeping must not delete live staged files"
        );
        assert_eq!(
            inprogress_entries(&staging).len(),
            2,
            "live staged files survived sweep"
        );

        release.wait();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let published = fs::read_to_string(&final_dest).unwrap();
        assert!(matches!(published.as_str(), "one\n" | "two\n"));
        assert!(
            inprogress_entries(&staging).is_empty(),
            "successful writes clean up staged files"
        );
    }

    /// Acceptance: a stale `.inprogress` older than the age threshold is swept
    /// even when its embedded PID currently belongs to an unrelated live
    /// process (a recycled PID). A recent leftover owned by that same live PID
    /// is still left in place — it may be an active writer.
    #[test]
    fn stale_inprogress_with_recycled_pid_is_swept() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let staging = ensure_staging_dir(root).unwrap();

        // A genuinely live process. Its PID stands in for a recycled PID: the
        // original RETL writer crashed long ago and this unrelated process now
        // happens to hold the same number.
        let child = spawn_live_process();
        let recycled_pid = child.0.id();
        assert_ne!(
            recycled_pid,
            std::process::id(),
            "helper PID must differ from the test process"
        );

        // Stale leftover: embedded timestamp at the Unix epoch — decades old,
        // far beyond the recycle grace window.
        let stale = write_staged(&staging, "stale.jsonl", recycled_pid, 0, 0);
        // Fresh leftover owned by the same live PID — plausibly an active
        // writer, so it must survive the sweep.
        let fresh = write_staged(&staging, "fresh.jsonl", recycled_pid, 1, now_nanos());

        let swept = sweep_stale_inprogress(root, true).unwrap();
        assert_eq!(swept, 1, "only the stale recycled-PID file is swept");
        assert!(
            !stale.exists(),
            "stale .inprogress with a recycled PID was reclaimed"
        );
        assert!(
            fresh.exists(),
            "recent .inprogress owned by a live PID left in place"
        );
    }

    /// An old staged file owned by *this* still-running process must never be
    /// swept: a long-running RETL run legitimately holds an old live staged
    /// file, and sweeping it would delete its own active output.
    #[test]
    fn old_inprogress_owned_by_self_is_never_swept() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let staging = ensure_staging_dir(root).unwrap();

        let own = write_staged(&staging, "own.jsonl", std::process::id(), 0, 0);

        let swept = sweep_stale_inprogress(root, true).unwrap();
        assert_eq!(
            swept, 0,
            "the sweep never reclaims a staged file owned by the live self PID"
        );
        assert!(own.exists(), "self-owned .inprogress survived the sweep");
    }

    /// The age gate only ever *keeps* files; it never blocks the sweep of a
    /// leftover whose PID is no longer live, even one with a recent timestamp.
    #[test]
    fn recent_inprogress_owned_by_dead_pid_is_swept() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let staging = ensure_staging_dir(root).unwrap();

        // A PID that is not live: huge, and not a multiple-of-4 Windows PID.
        let dead_pid = u32::MAX - 1;
        let leftover = write_staged(&staging, "dead.jsonl", dead_pid, 0, now_nanos());

        let swept = sweep_stale_inprogress(root, true).unwrap();
        assert_eq!(swept, 1, "a dead-PID leftover is swept regardless of age");
        assert!(!leftover.exists(), "dead-PID .inprogress was reclaimed");
    }

    /// Acceptance: an orphaned `.atomic-replace-tmp` copy+rename fallback file
    /// left in an output directory by a dead PID is reclaimed by a later run's
    /// sweep — without disturbing the real published outputs beside it. These
    /// files live next to `dest`, not under `_staging`, so before this fix no
    /// sweep ever reached them.
    #[test]
    fn orphaned_atomic_replace_tmp_with_dead_pid_is_reclaimed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        // A real published output the sweep must never touch.
        let published = root.join("part_RC_2024-01.jsonl");
        fs::write(&published, b"{\"id\":\"abc\"}\n").unwrap();

        // A PID that is not live: huge, and not a multiple-of-4 Windows PID.
        let dead_pid = u32::MAX - 1;
        let leftover =
            write_atomic_replace_tmp(root, "part_RC_2024-01.jsonl", dead_pid, 0, now_nanos());

        let swept = sweep_stale_inprogress(root, true).unwrap();
        assert_eq!(
            swept, 1,
            "the orphaned .atomic-replace-tmp leftover is swept"
        );
        assert!(
            !leftover.exists(),
            "dead-PID .atomic-replace-tmp fallback file was reclaimed"
        );
        assert!(
            published.exists(),
            "the published output beside it must be left in place"
        );
    }

    /// A `.atomic-replace-tmp` leftover owned by *this* live process is never
    /// swept — it may belong to a copy+rename fallback in progress right now.
    #[test]
    fn atomic_replace_tmp_owned_by_self_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        let own =
            write_atomic_replace_tmp(root, "out.jsonl", std::process::id(), 0, now_nanos());

        let swept = sweep_stale_inprogress(root, true).unwrap();
        assert_eq!(swept, 0, "a self-owned fallback leftover is never swept");
        assert!(
            own.exists(),
            "self-owned .atomic-replace-tmp survived the sweep"
        );
    }

    /// `sweep_stale_atomic_replace_tmp` reclaims leftovers from a nested output
    /// directory (e.g. a partitioned export's `comments/` dir) that the
    /// `out_root`-level scan in `sweep_stale_inprogress` does not reach.
    #[test]
    fn sweep_atomic_replace_tmp_reclaims_nested_dir_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("comments");
        fs::create_dir_all(&nested).unwrap();

        let dead_pid = u32::MAX - 1;
        let leftover =
            write_atomic_replace_tmp(&nested, "RC_2024-01.jsonl", dead_pid, 0, now_nanos());

        let swept = sweep_stale_atomic_replace_tmp(&nested, true).unwrap();
        assert_eq!(swept, 1, "the nested-dir fallback leftover is swept");
        assert!(
            !leftover.exists(),
            "nested .atomic-replace-tmp was reclaimed"
        );
    }
}
