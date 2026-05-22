use super::*;
use crate::progress_manifest::testing::fail_saves_after_attempts_for_tests;
use crate::{KeyExtractor, RedditETL, ScanPlan, Sources, YearMonth};
use serde_json::{json, Value};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

mod checkpoint_reuse;
mod fingerprint;
mod fixtures;
mod manifest_commit;
mod partitioned_zero_record;
mod resume_robustness;
