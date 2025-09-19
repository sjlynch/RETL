// ============================================================================
// Combined usage examples for reddit_etl.
// Everything is commented out so this file is documentation-only.
// Copy/paste blocks into your own small binaries as needed.
// ============================================================================

// --------------------------------------------------------------------------
// Export back to corpus-style monthly files (RC/RS), with JSONL or ZST output.
// Preserves exact bytes if no whitelist is set.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth, ExportFormat};
use std::path::Path;

fn main() -> Result<()> {
    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2014, 1)), Some(YearMonth::new(2014, 3)))
        .parallelism(12)
        .scan()
        // .subreddits(["programming", "rust"]) // optional; omit for ALL subreddits
        // .whitelist_fields(["author","created_utc","subreddit","id"]) // optional
        .export_partitioned(Path::new("out_corpus_jsonl"), ExportFormat::Jsonl)?; // RC_/RS_ .jsonl

    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2014, 1)), Some(YearMonth::new(2014, 3)))
        .scan()
        .subreddit("programming")
        .export_partitioned(Path::new("out_corpus_zst"), ExportFormat::Zst)?;   // RC_/RS_ .zst

    Ok(())
}
*/

// --------------------------------------------------------------------------
// Unique usernames in a single subreddit (comments + submissions) over months.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};

fn main() -> Result<()> {
    let mut stream = RedditETL::new()
        .base_dir("../reddit")
        .subreddit("programming")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2006, 1)), Some(YearMonth::new(2006, 2)))
        .parallelism(8)
        .usernames()?;

    while let Some(u) = stream.next() {
        println!("{u}");
    }
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Unique usernames across MULTIPLE subreddits (union), with query mode.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::fs::File;
use std::io::{BufWriter, Write};

fn main() -> Result<()> {
    let mut names = RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2015, 1)), Some(YearMonth::new(2016, 12)))
        .parallelism(12)
        .scan()
        .subreddits(["science", "askscience"])
        .usernames()?;

    let out = File::create("usernames_science_union.txt")?;
    let mut w = BufWriter::new(out);
    for name in &mut names {
        writeln!(w, "{name}")?;
    }
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Unique usernames per subreddit (one file per sub).
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::fs::{self, File};
use std::io::{BufWriter, Write};

fn main() -> Result<()> {
    let subs = ["programming", "rust", "cpp"];
    fs::create_dir_all("out_usernames")?;

    for s in subs {
        let mut it = RedditETL::new()
            .base_dir("../reddit")
            .sources(Sources::Both)
            .date_range(Some(YearMonth::new(2019, 1)), Some(YearMonth::new(2019, 12)))
            .scan()
            .subreddit(s)
            .usernames()?;

        let f = File::create(format!("out_usernames/{s}.txt"))?;
        let mut w = BufWriter::new(f);
        for u in &mut it {
            writeln!(w, "{u}")?;
        }
    }
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Intersection of usernames appearing in both subreddits.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::collections::HashSet;

fn collect(sub: &str) -> Result<HashSet<String>> {
    let mut set = HashSet::new();
    let mut it = RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2020, 1)), Some(YearMonth::new(2020, 12)))
        .scan()
        .subreddit(sub)
        .usernames()?;
    for u in &mut it {
        set.insert(u);
    }
    Ok(set)
}

fn main() -> Result<()> {
    let a = collect("science")?;
    let b = collect("askscience")?;
    let intersection: HashSet<_> = a.intersection(&b).cloned().collect();

    for u in intersection {
        println!("{u}");
    }
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Extract all posts/comments for a given list of authors in certain subreddits.
// Sorting/lowercasing is handled internally.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    let authors = vec!["Alice_123", "bob", "Charlie"]; // case-insensitive

    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2018, 1)), Some(YearMonth::new(2019, 6)))
        .parallelism(12)
        .scan()
        .subreddits(["technology", "gadgets"])
        .authors_in(authors)
        .extract_to_jsonl(Path::new("users_tech_2018_2019.jsonl"))?;

    Ok(())
}
*/

// --------------------------------------------------------------------------
// Extract a single user's activity without regex (convenience .author()).
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2017, 1)), Some(YearMonth::new(2018, 12)))
        .scan()
        .subreddits(["programming", "rust"])
        .author("alice_123")
        .extract_to_jsonl(Path::new("alice_123_prog_rust_2017_2018.jsonl"))?;
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Chained flow: find authors, then pull all their posts/comments.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::collections::HashSet;
use std::path::Path;

fn main() -> Result<()> {
    // Phase 1: discover authors (ANY subreddits by omitting .subreddits())
    let mut authors = HashSet::<String>::new();
    let mut it = RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2017, 1)), Some(YearMonth::new(2018, 12)))
        .scan()
        .usernames()?; // ALL subreddits

    for u in &mut it { authors.insert(u.to_lowercase()); }

    // Phase 2: collect content for these authors in focused subs
    let authors_vec: Vec<_> = authors.into_iter().collect();

    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2017, 1)), Some(YearMonth::new(2018, 12)))
        .parallelism(16)
        .scan()
        .subreddits(["bitcoin", "cryptocurrency"])
        .authors_in(authors_vec)
        .extract_to_jsonl(Path::new("crypto_authors_2017_2018.jsonl"))?;

    Ok(())
}
*/

// --------------------------------------------------------------------------
// Whitelist fields for comments (compact schema).
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 3)))
        .scan()
        .subreddit("askscience")
        .whitelist_fields([
            "author","body","created_utc","subreddit",
            "parent_id","link_id","id","score",
        ])
        .extract_to_jsonl(Path::new("askscience_comments_q1_2016_minimal.jsonl"))?;
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Whitelist fields for submissions (domain/title analytics).
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2015, 1)), Some(YearMonth::new(2015, 12)))
        .scan()
        .subreddit("technology")
        .whitelist_fields(["id","author","created_utc","title","selftext",
                           "url","domain","score","subreddit"])
        .extract_to_jsonl(Path::new("technology_submissions_2015_minimal.jsonl"))?;
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Count by month (keywords + URL presence + min score).
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};

fn main() -> Result<()> {
    let counts = RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Both)
        .date_range(Some(YearMonth::new(2016, 1)), Some(YearMonth::new(2016, 12)))
        .scan()
        .subreddit("worldnews")
        .keywords_any(["election","vote","ballot"])
        .contains_url(true)
        .min_score(10)
        .count_by_month()?;

    for (ym, n) in counts {
        println!("{ym}\t{n}");
    }
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Filter submissions by domain list.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};
use std::path::Path;

fn main() -> Result<()> {
    RedditETL::new()
        .base_dir("../reddit")
        .sources(Sources::Submissions)
        .date_range(Some(YearMonth::new(2018, 1)), Some(YearMonth::new(2018, 6)))
        .scan()
        .subreddit("worldnews")
        .domains_in(["bbc.co.uk", "nytimes.com", "theguardian.com"])
        .extract_to_jsonl(Path::new("worldnews_submissions_h1_2018_selected_domains.jsonl"))?;
    Ok(())
}
*/

// --------------------------------------------------------------------------
// Tuning work directory, shard count, and rayon parallelism.
// --------------------------------------------------------------------------
/*
use anyhow::Result;
use reddit_etl::{RedditETL, Sources, YearMonth};

fn main() -> Result<()> {
    let mut usernames = RedditETL::new()
        .base_dir("/mnt/big_drive/reddit")
        .work_dir("/mnt/fast_nvme/tmp/reddit_etl")
        .shard_count(512)
        .parallelism(24)
        .subreddit("dataisbeautiful")
        .sources(Sources::Comments)
        .date_range(Some(YearMonth::new(2021, 1)), Some(YearMonth::new(2021, 12)))
        .usernames()?;

    while let Some(u) = usernames.next() {
        println!("{u}");
    }
    Ok(())
}
*/

// --------------------------------------------------------------------------
// NEW: Pipeline example
// 1) Build a list of all usernames that participated in a specific subreddit.
// 2) Export every found user's content into a single JSON file, grouped by
//    user, then by subreddit, with separate posts and comments.
// 3) For each comment, include the parent post/comment text.
// Notes:
//  - This example streams the dataset directly to resolve parent contents.
//  - It is designed as an illustrative sample; adapt for your scale.
// --------------------------------------------------------------------------
/*
use anyhow::{Result, Context};
use reddit_etl::{RedditETL, Sources, YearMonth};
use serde_json::{Value, json, Map};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use regex::Regex;
use walkdir::WalkDir;
use zstd::stream::read::Decoder;

// Simple line reader for .zst JSONL (mirrors the library config).
fn for_each_line_zst(path: &Path, mut on_line: impl FnMut(&str) -> Result<()>) -> Result<()> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut dec = Decoder::new(file)?;
    dec.window_log_max(31)?;
    let mut r = BufReader::new(dec);
    let mut buf = String::with_capacity(16 * 1024);
    loop {
        buf.clear();
        let n = r.read_line(&mut buf)?;
        if n == 0 { break; }
        if buf.ends_with('\n') { buf.pop(); if buf.ends_with('\r') { buf.pop(); } }
        on_line(&buf)?;
    }
    Ok(())
}

// Discover monthly files (RC/RS) under a directory.
fn discover_months(dir: &Path, kind: &str) -> Vec<PathBuf> {
    // kind: "RC" or "RS"
    let re = Regex::new(&format!(r"^{}_(\d{{4}})-(\d{{2}})\.zst$", kind)).unwrap();
    let mut files = Vec::new();
    if dir.exists() {
        for entry in WalkDir::new(dir).min_depth(1).max_depth(1) {
            if let Ok(ent) = entry {
                if let Some(name) = ent.file_name().to_str() {
                    if re.is_match(name) {
                        files.push(ent.path().to_path_buf());
                    }
                }
            }
        }
    }
    files.sort();
    files
}

#[derive(Default)]
struct UserSubredditAgg {
    posts: Vec<Value>,
    comments: Vec<Value>,
}

fn main() -> Result<()> {
    let base_dir = Path::new("../reddit");
    let target_sub = "programming";
    let start = YearMonth::new(2006, 1);
    let end   = YearMonth::new(2006, 3);

    // -------------------- Step 1: collect usernames in a subreddit --------------------
    let mut authors = Vec::<String>::new();
    {
        let mut it = RedditETL::new()
            .base_dir(base_dir)
            .sources(Sources::Both)
            .date_range(Some(start), Some(end))
            .scan()
            .subreddit(target_sub)
            .usernames()?;

        for u in &mut it { authors.push(u.to_lowercase()); }
        authors.sort();
        authors.dedup();
    }

    // -------------------- Step 2: extract all content for those users ----------------
    let temp = Path::new("tmp_programming_authors.jsonl");
    {
        RedditETL::new()
            .base_dir(base_dir)
            .sources(Sources::Both)
            .date_range(Some(start), Some(end))
            .scan()
            .subreddit(target_sub)
            .authors_in(authors.clone())
            .extract_to_jsonl(temp)?;
    }

    // -------------------- Step 3: read extracted content; collect parent ids ---------
    let mut parent_t1: HashSet<String> = HashSet::new(); // comment parents
    let mut parent_t3: HashSet<String> = HashSet::new(); // submission parents
    let mut records: Vec<Value> = Vec::new();

    {
        let f = File::open(temp)?;
        let r = BufReader::new(f);
        for line in r.lines() {
            let line = line?;
            if line.is_empty() { continue; }
            let v: Value = serde_json::from_str(&line)?;
            // Collect parent references
            if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                if let Some(rest) = parent_id.strip_prefix("t1_") { parent_t1.insert(rest.to_string()); }
                if let Some(rest) = parent_id.strip_prefix("t3_") { parent_t3.insert(rest.to_string()); }
            }
            // Also collect the submission for each comment (link_id)
            if let Some(link_id) = v.get("link_id").and_then(|x| x.as_str()) {
                if let Some(rest) = link_id.strip_prefix("t3_") { parent_t3.insert(rest.to_string()); }
            }
            records.push(v);
        }
    }

    // -------------------- Step 4: resolve parents by scanning monthly corpora --------
    let comments_dir = base_dir.join("comments");
    let submissions_dir = base_dir.join("submissions");
    let rc_files = discover_months(&comments_dir, "RC");
    let rs_files = discover_months(&submissions_dir, "RS");

    // Parent maps: "comment_id" -> body, and "submission_id" -> {title/selftext}
    let mut parent_comment_text: HashMap<String, String> = HashMap::new();
    let mut parent_submission_text: HashMap<String, (String, String)> = HashMap::new();

    // Comments (RC)
    for rc in rc_files {
        for_each_line_zst(&rc, |line| {
            let v: Value = match serde_json::from_str(line) { Ok(val) => val, Err(_) => return Ok(()) };
            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                if parent_t1.contains(id) {
                    if let Some(body) = v.get("body").and_then(|x| x.as_str()) {
                        parent_comment_text.insert(id.to_string(), body.to_string());
                    }
                }
            }
            Ok(())
        })?;
    }

    // Submissions (RS)
    for rs in rs_files {
        for_each_line_zst(&rs, |line| {
            let v: Value = match serde_json::from_str(line) { Ok(val) => val, Err(_) => return Ok(()) };
            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                if parent_t3.contains(id) {
                    let title = v.get("title").and_then(|x| x.as_str()).unwrap_or_default().to_string();
                    let selftext = v.get("selftext").and_then(|x| x.as_str()).unwrap_or_default().to_string();
                    parent_submission_text.insert(id.to_string(), (title, selftext));
                }
            }
            Ok(())
        })?;
    }

    // -------------------- Step 5: aggregate per user -> subreddit --------------------
    // Structure:
    // {
    //   "userA": {
    //     "programming": {
    //       "posts":    [ ... ],
    //       "comments": [ ... with "parent" {...} ]
    //     },
    //     "rust": { ... }
    //   },
    //   "userB": { ... }
    // }
    let mut agg: HashMap<String, HashMap<String, UserSubredditAgg>> = HashMap::new();

    for v in records {
        let author = v.get("author").and_then(|x| x.as_str()).unwrap_or("").to_lowercase();
        let subreddit = v.get("subreddit").and_then(|x| x.as_str()).unwrap_or("").to_lowercase();
        if author.is_empty() || subreddit.is_empty() { continue; }

        let user_entry = agg.entry(author).or_default();
        let sub_entry = user_entry.entry(subreddit).or_default();

        let is_comment = v.get("body").is_some() && v.get("link_id").is_some();
        if is_comment {
            // Attach parent payload
            let mut obj = v.clone(); // keep full record
            let mut parent_obj = Map::new();

            if let Some(parent_id) = v.get("parent_id").and_then(|x| x.as_str()) {
                if let Some(rest) = parent_id.strip_prefix("t1_") {
                    if let Some(text) = parent_comment_text.get(rest) {
                        parent_obj.insert("kind".into(), Value::String("comment".into()));
                        parent_obj.insert("id".into(), Value::String(rest.to_string()));
                        parent_obj.insert("body".into(), Value::String(text.clone()));
                    }
                } else if let Some(rest) = parent_id.strip_prefix("t3_") {
                    if let Some((title, selftext)) = parent_submission_text.get(rest) {
                        parent_obj.insert("kind".into(), Value::String("submission".into()));
                        parent_obj.insert("id".into(), Value::String(rest.to_string()));
                        parent_obj.insert("title".into(), Value::String(title.clone()));
                        parent_obj.insert("selftext".into(), Value::String(selftext.clone()));
                    }
                }
            }
            obj.as_object_mut().unwrap().insert("parent".into(), Value::Object(parent_obj));
            sub_entry.comments.push(obj);
        } else {
            sub_entry.posts.push(v.clone());
        }
    }

    // -------------------- Step 6: write a single aggregated JSON file ---------------
    let out = File::create("programming_users_agg.json")?;
    let mut w = BufWriter::new(out);
    serde_json::to_writer_pretty(&mut w, &agg)?;
    w.flush()?;

    Ok(())
}
*/
