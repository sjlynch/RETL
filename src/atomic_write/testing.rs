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
        ensure_staging_dir, sweep_stale_inprogress, write_jsonl_atomic, INPROGRESS_EXT,
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Barrier};
    use std::thread;

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
}
