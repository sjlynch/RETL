#![allow(dead_code)]

use assert_cmd::Command;
use std::fs;
use std::io::Write;
use std::path::Path;

pub fn retl() -> Command {
    Command::cargo_bin("retl").expect("retl binary should be built")
}

pub fn write_jsonl(path: &Path, rows: &[serde_json::Value]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut f = fs::File::create(path).unwrap();
    for row in rows {
        writeln!(f, "{}", row).unwrap();
    }
}

pub fn read_text_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect()
}
