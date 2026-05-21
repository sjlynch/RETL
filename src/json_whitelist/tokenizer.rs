use ahash::AHashMap;

use super::skip::{scan_string_body, skip_value, skip_ws};
use super::timestamps::emit_timestamp_rewritten_value;
use super::TokenizerError;

/// Small, reusable tokenizer that emits a compact JSON object containing only
/// whitelisted top-level keys, copying their raw value bytes from the input.
///
/// Construct once per pipeline (the field set is hashed up front) and reuse the
/// same instance — and the same output `String` buffer — across every line.
pub struct WhitelistTokenizer {
    /// Canonical (escape-decoded) whitelisted key bytes → the positions in the
    /// caller's field list that requested that key. Doubles as the membership
    /// set: a top-level key is whitelisted iff it has an entry here. Every
    /// entry holds at least one index (`new` always pushes), and the first
    /// index serves as a stable per-key slot for duplicate-key detection.
    key_indices: AHashMap<Vec<u8>, Vec<usize>>,
}

impl WhitelistTokenizer {
    pub fn new<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut key_indices: AHashMap<Vec<u8>, Vec<usize>> = AHashMap::new();
        for (idx, field) in fields.into_iter().enumerate() {
            let key = field.as_ref().as_bytes().to_vec();
            key_indices.entry(key).or_default().push(idx);
        }
        Self { key_indices }
    }

    /// Walk `line` and append a compact JSON object containing the whitelisted
    /// fields to `out`. `out` is cleared on entry. On error `out` is left empty
    /// and the caller should use the slow path.
    pub fn tokenize_into(&self, line: &str, out: &mut String) -> Result<(), TokenizerError> {
        self.tokenize_with(line, out, None, |_key, raw, out| {
            // SAFETY: bytes are a sub-slice of the original `&str`, which is
            // valid UTF-8. `from_utf8_unchecked` avoids re-validating.
            out.push_str(unsafe { std::str::from_utf8_unchecked(raw) });
        })
    }

    /// Like [`tokenize_into`], and also records the requested-field indices
    /// whose top-level keys were present in this line. `matched_indices` is
    /// cleared on entry and contains indices into the field list used to build
    /// this tokenizer on success.
    pub(crate) fn tokenize_into_with_matches(
        &self,
        line: &str,
        out: &mut String,
        matched_indices: &mut Vec<usize>,
    ) -> Result<(), TokenizerError> {
        self.tokenize_with(line, out, Some(matched_indices), |_key, raw, out| {
            // SAFETY: bytes are a sub-slice of the original `&str`, which is
            // valid UTF-8. `from_utf8_unchecked` avoids re-validating.
            out.push_str(unsafe { std::str::from_utf8_unchecked(raw) });
        })
    }

    /// Like [`tokenize_into`], but additionally rewrites integer values of the
    /// three timestamp keys (`created_utc`, `retrieved_on`, `edited`) into
    /// quoted RFC3339 strings as it copies them to `out`. This fuses the
    /// whitelist projection and the byte-level human-timestamp rewrite into a
    /// single pass over the raw JSON line, replacing the two-pass
    /// `tokenize_into` followed by `rewrite_human_timestamps_bytes` chain in
    /// `streaming::stream_job` when both transforms are enabled.
    ///
    /// Behavior matches the two-pass composition byte-for-byte at the JSON
    /// value level: non-integer values for the timestamp keys (null, bool,
    /// floats, strings) are emitted verbatim, integers outside the
    /// `OffsetDateTime` range are emitted verbatim, and non-timestamp keys are
    /// always emitted verbatim. On any structural surprise the method returns
    /// [`TokenizerError::Malformed`] and the caller falls back to the slow
    /// `serde_json::Value` path.
    pub fn tokenize_and_rewrite_timestamps_into(
        &self,
        line: &str,
        out: &mut String,
    ) -> Result<(), TokenizerError> {
        self.tokenize_with(line, out, None, emit_timestamp_rewritten_value)
    }

    /// Like [`tokenize_and_rewrite_timestamps_into`], and also records the
    /// requested-field indices whose top-level keys were present in this line.
    pub(crate) fn tokenize_and_rewrite_timestamps_into_with_matches(
        &self,
        line: &str,
        out: &mut String,
        matched_indices: &mut Vec<usize>,
    ) -> Result<(), TokenizerError> {
        self.tokenize_with(
            line,
            out,
            Some(matched_indices),
            emit_timestamp_rewritten_value,
        )
    }

    /// Shared parser body for the two public methods. Walks `line` once,
    /// emitting a compact JSON object of whitelisted top-level fields to
    /// `out`. For each matched pair the key bytes (verbatim from the input)
    /// and `:` are written, then `emit_value` is invoked with the canonical
    /// (escape-decoded) key bytes and the raw value bytes, and decides what
    /// to write for the value.
    ///
    /// A line that repeats a whitelisted top-level key is rejected with
    /// [`TokenizerError::Malformed`]. The slow `serde_json::Value` fallback
    /// parses into a `serde_json::Map`, which collapses duplicate keys
    /// last-wins (`{"id":"a","id":"b"}` → `{"id":"b"}`); copying both pairs
    /// verbatim here would emit a duplicate-keyed object and diverge from the
    /// slow path for the same input. Deferring to the fallback keeps the two
    /// paths byte-identical. Duplicate *non*-whitelisted keys are harmless —
    /// they are skipped on both paths — so they do not trigger a fallback.
    #[inline]
    fn tokenize_with<F>(
        &self,
        line: &str,
        out: &mut String,
        mut matched_indices: Option<&mut Vec<usize>>,
        emit_value: F,
    ) -> Result<(), TokenizerError>
    where
        F: Fn(&[u8], &[u8], &mut String),
    {
        out.clear();
        if let Some(indices) = matched_indices.as_mut() {
            indices.clear();
        }
        macro_rules! malformed {
            () => {{
                out.clear();
                if let Some(indices) = matched_indices.as_mut() {
                    indices.clear();
                }
                return Err(TokenizerError::Malformed);
            }};
        }

        let bytes = line.as_bytes();
        let mut i = skip_ws(bytes, 0);
        if i >= bytes.len() || bytes[i] != b'{' {
            malformed!();
        }
        i += 1;
        out.push('{');

        // Independent counters: `first_pair` tracks the input walk (must
        // accept exactly one ',' between consecutive pairs), while
        // `emitted_any` tracks the output (whether to prepend a ',' before
        // the next emitted pair). They diverge whenever an input pair is
        // skipped because its key isn't in the whitelist.
        let mut first_pair = true;
        let mut emitted_any = false;
        // Bitmask of whitelisted keys already emitted on this line, indexed by
        // each key's stable slot (see the duplicate-key handling below). Used
        // to reject lines with a repeated whitelisted top-level key so they
        // fall back to the slow path instead of emitting a duplicate-keyed
        // object the slow path would never produce.
        let mut seen_keys: u128 = 0;
        loop {
            i = skip_ws(bytes, i);
            if i >= bytes.len() {
                malformed!();
            }
            if bytes[i] == b'}' {
                i += 1;
                break;
            }
            if !first_pair {
                if bytes[i] != b',' {
                    malformed!();
                }
                i += 1;
                i = skip_ws(bytes, i);
            }
            first_pair = false;

            // Key string.
            if i >= bytes.len() || bytes[i] != b'"' {
                malformed!();
            }
            let key_quoted_start = i;
            let key_content_start = i + 1;
            let (key_content_end, key_has_escape) = match scan_string_body(bytes, i + 1) {
                Some(v) => v,
                None => {
                    malformed!();
                }
            };
            let key_quoted_end = key_content_end + 1; // includes closing quote
            i = key_quoted_end;

            // ':' separator.
            i = skip_ws(bytes, i);
            if i >= bytes.len() || bytes[i] != b':' {
                malformed!();
            }
            i += 1;
            i = skip_ws(bytes, i);

            // Value (any JSON value).
            let value_start = i;
            let value_end = match skip_value(bytes, i) {
                Some(e) => e,
                None => {
                    malformed!();
                }
            };
            i = value_end;

            // Membership test. Plain ASCII keys (the common case) hit the fast
            // raw-byte path. If the JSON key contains a backslash escape the
            // key bytes don't equal their decoded form, so we ask serde_json
            // for the canonical decoding before checking the whitelist. The
            // decoded bytes are retained so `emit_value` can also see the
            // canonical key (needed for the timestamp-rewrite special case).
            // The lookup returns the requested-field indices directly, so it
            // serves as both the membership test and the matched-index source.
            let decoded_key: Option<Vec<u8>>;
            let field_indices: Option<&Vec<usize>> = if key_has_escape {
                let raw = &bytes[key_quoted_start..key_quoted_end];
                match serde_json::from_slice::<String>(raw) {
                    Ok(decoded) => {
                        let found = self.key_indices.get(decoded.as_bytes());
                        decoded_key = Some(decoded.into_bytes());
                        found
                    }
                    Err(_) => {
                        malformed!();
                    }
                }
            } else {
                decoded_key = None;
                self.key_indices
                    .get(&bytes[key_content_start..key_content_end])
            };

            if let Some(field_indices) = field_indices {
                // Reject a repeated whitelisted top-level key: the slow path
                // would collapse it last-wins, so emitting both occurrences
                // here would diverge (see `tokenize_with`'s doc comment).
                // `field_indices` is non-empty by construction, and its first
                // entry is a stable per-key slot — each field-list position
                // belongs to exactly one key, so distinct keys get distinct
                // slots. Slots past bit 127 (only reachable with an
                // implausibly large whitelist) are clamped onto the top bit,
                // which can only ever cause an extra, still-correct slow-path
                // fallback — never a silent divergence.
                let bit = 1u128 << field_indices[0].min(127);
                if seen_keys & bit != 0 {
                    malformed!();
                }
                seen_keys |= bit;

                if emitted_any {
                    out.push(',');
                }
                emitted_any = true;
                // SAFETY: bytes are a sub-slice of the original `&str`, which is
                // valid UTF-8. `from_utf8_unchecked` avoids re-validating.
                out.push_str(unsafe {
                    std::str::from_utf8_unchecked(&bytes[key_quoted_start..key_quoted_end])
                });
                out.push(':');

                let canonical_key: &[u8] = match &decoded_key {
                    Some(d) => d.as_slice(),
                    None => &bytes[key_content_start..key_content_end],
                };

                if let Some(indices_out) = matched_indices.as_mut() {
                    indices_out.extend(field_indices.iter().copied());
                }

                emit_value(canonical_key, &bytes[value_start..value_end], out);
            }
        }

        // Trailing whitespace (none in compact serde output) is tolerated, but
        // any non-ws content after the closing brace means the line wasn't a
        // single top-level object.
        let tail = skip_ws(bytes, i);
        if tail != bytes.len() {
            malformed!();
        }

        out.push('}');
        Ok(())
    }
}
