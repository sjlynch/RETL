use super::backoff::open_with_default_backoff;
use crate::ndjson::{read_line_capped, DEFAULT_MAX_LINE_BYTES};
use crate::query::{normalize_str, QueryBuildError};

/// Returns a normalized (lowercase) default list of bot/service authors to exclude.
/// This is a conservative set focused on high-volume/systemic accounts.
/// Feel free to extend as needed; try_merge_extra_exclusions() will add env/file entries.
pub fn default_bot_authors() -> Vec<String> {
    // Hand-curated defaults (normalized to lowercase)
    let defaults = [
        "automoderator",
        "imguralbumbot",
        "autowikibot",
        "remindmebot",
        "totesmessenger",
        "tweet_poster",
        "video_link_bot",
        "gifvbot",
        "helper-bot",
        "github-actions[bot]",
        "slackbot",
        "discordbot",
    ];
    let mut v: Vec<String> = defaults.iter().map(|s| normalize_str(s)).collect();
    v.sort();
    v.dedup();
    v
}

/// Merge extra exclusions from env/file into the provided vector (in-place).
/// - ETL_EXCLUDE_AUTHORS: comma/semicolon/space separated names
/// - ETL_EXCLUDE_AUTHORS_FILE: path to newline-separated file of names
///
/// All entries are normalized (lowercase), then the list is sort+dedup. If
/// `ETL_EXCLUDE_AUTHORS_FILE` is set to a non-blank path, failure to open or
/// fully read it is fatal because the file is part of the query semantics.
/// Per-line reads are bounded by [`DEFAULT_MAX_LINE_BYTES`] so an adversarial
/// exclusions file cannot OOM the process.
pub(crate) fn try_merge_extra_exclusions(
    target: &mut Vec<String>,
) -> std::result::Result<(), QueryBuildError> {
    use std::io::BufReader;

    let mut extras = Vec::new();

    if let Ok(s) = std::env::var("ETL_EXCLUDE_AUTHORS") {
        for raw in s.split(|c: char| c == ',' || c == ';' || c.is_whitespace()) {
            let n = normalize_str(raw);
            if !n.is_empty() {
                extras.push(n);
            }
        }
    }

    if let Some(path_os) = std::env::var_os("ETL_EXCLUDE_AUTHORS_FILE") {
        let path = std::path::PathBuf::from(path_os);
        if !path.to_string_lossy().trim().is_empty() {
            let f = open_with_default_backoff(&path).map_err(|e| {
                QueryBuildError::new(format!(
                    "ETL_EXCLUDE_AUTHORS_FILE {} cannot be opened: {e}",
                    path.display()
                ))
            })?;
            let mut r = BufReader::new(f);
            let mut line = String::with_capacity(1024);
            let mut line_no: usize = 0;
            loop {
                line_no += 1;
                match read_line_capped(&mut r, &mut line, DEFAULT_MAX_LINE_BYTES, &path) {
                    Ok(0) => break,
                    Ok(_) => {
                        let n = normalize_str(&line);
                        if !n.is_empty() {
                            extras.push(n);
                        }
                    }
                    Err(e) => {
                        return Err(QueryBuildError::new(format!(
                            "ETL_EXCLUDE_AUTHORS_FILE {} could not be read at line {line_no}: {e}",
                            path.display()
                        )));
                    }
                }
            }
        }
    }

    target.extend(extras);

    // normalize + sort + dedup
    for s in target.iter_mut() {
        *s = normalize_str(s);
    }
    target.sort();
    target.dedup();
    Ok(())
}
