
fn stable_path(path: &Path) -> PathBuf {
    if path.exists() {
        fs::canonicalize(path).unwrap_or_else(|_| absolutize(path))
    } else if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let parent = if parent.exists() {
            fs::canonicalize(parent).unwrap_or_else(|_| absolutize(parent))
        } else {
            absolutize(parent)
        };
        match path.file_name() {
            Some(name) => parent.join(name),
            None => parent,
        }
    } else {
        absolutize(path)
    }
}

fn absolutize(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}
