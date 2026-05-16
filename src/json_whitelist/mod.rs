//! Streaming whitelist tokenizer.
//!
//! The slow path in [`crate::streaming::stream_job`] (whitelist branch) used to
//! `serde_json::from_str` every line into a `Value`, copy the requested fields
//! into a fresh `Map`, then re-serialize. For whitelisted exports — a common
//! production config — this dominated CPU.
//!
//! [`WhitelistTokenizer`] walks the raw byte buffer once at the top level only.
//! For each top-level pair it scans the key, checks membership against a small
//! `HashSet`, and if matched copies the original `key:raw_value` byte slice
//! verbatim into a reusable output buffer. Unknown keys are skipped by tracking
//! brace/bracket nesting and string-escape state — no allocation, no `Value`
//! round-trip.
//!
//! Reddit records are flat top-level objects (with arrays/objects only nested
//! inside specific known fields), and the whitelist itself is top-level keys,
//! so a top-level walk is exactly what's needed.
//!
//! On any structural surprise the tokenizer returns [`TokenizerError::Malformed`]
//! and the caller falls back to the slow path so we never regress correctness.

mod skip;
mod timestamps;
mod tokenizer;

pub use tokenizer::WhitelistTokenizer;

#[derive(Debug)]
pub enum TokenizerError {
    /// The input is not a syntactically well-formed flat top-level object that
    /// the tokenizer knows how to walk. The caller should fall back to the
    /// `serde_json::Value` slow path.
    Malformed,
}

#[cfg(test)]
mod tests;
