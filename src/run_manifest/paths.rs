
pub fn manifest_path_for_file(path: &Path) -> PathBuf {
    let mut sidecar = OsString::from(path.as_os_str());
    sidecar.push(FILE_MANIFEST_SUFFIX);
    PathBuf::from(sidecar)
}

pub fn manifest_path_for_directory(dir: &Path) -> PathBuf {
    dir.join(DIR_MANIFEST_NAME)
}
