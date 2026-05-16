//! Resumable `extract_spool_monthly`: a second run reads the sidecar
//! `_progress.json` next to the spool outputs and skips months that the prior
//! run already published.

#[path = "common/mod.rs"]
mod common;

#[path = "spool_resume/cleanup.rs"]
mod cleanup;
#[path = "spool_resume/corruption_and_concurrency.rs"]
mod corruption_and_concurrency;
#[path = "spool_resume/fingerprint.rs"]
mod fingerprint;
#[path = "spool_resume/manifest.rs"]
mod manifest;
