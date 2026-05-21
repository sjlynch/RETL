
#[derive(Debug)]
struct WhitelistMatchState {
    fast_seen: u64,
    slow_seen: u64,
    fast_field_seen: Vec<bool>,
    slow_field_seen: Vec<bool>,
    reported: bool,
}

impl WhitelistMatchState {
    fn new(field_count: usize) -> Self {
        Self {
            fast_seen: 0,
            slow_seen: 0,
            fast_field_seen: vec![false; field_count],
            slow_field_seen: vec![false; field_count],
            reported: false,
        }
    }
}

/// Shared per-export whitelist sanity checker. It tracks the requested field
/// names and reports once if any individual field never appears in accepted
/// records. Fast-path and slow-path observations are kept separate: fast-path
/// presence decides the warning/error, while slow-path-only matches are called
/// out explicitly so tokenizer fallback lines cannot hide a real typo.
#[derive(Debug)]
pub(crate) struct WhitelistMatchTracker {
    strict: bool,
    field_names: Vec<String>,
    state: std::sync::Mutex<WhitelistMatchState>,
}

impl WhitelistMatchTracker {
    pub(crate) fn new<I, S>(strict: bool, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let field_names: Vec<String> = fields.into_iter().map(Into::into).collect();
        let state = WhitelistMatchState::new(field_names.len());
        Self {
            strict,
            field_names,
            state: std::sync::Mutex::new(state),
        }
    }

    pub(crate) fn observe(&self, emission: WhitelistEmission<'_>) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("whitelist validation state lock poisoned"))?;

        if emission.used_slow_path {
            state.slow_seen += 1;
            mark_fields_seen(&mut state.slow_field_seen, emission.matched_fields);
        } else {
            state.fast_seen += 1;
            mark_fields_seen(&mut state.fast_field_seen, emission.matched_fields);
        }
        Ok(())
    }

    /// Decide the per-field verdict for the whole job. In non-strict mode a
    /// missing field only logs a `warn!`; in strict mode it returns `Err`.
    ///
    /// This is necessarily **post-hoc** — it can only run once every record has
    /// been observed, which by then means each month's output is already
    /// published and (when resuming) its `_progress.json` entry committed. A
    /// caller that publishes per-month outputs must therefore undo those side
    /// effects on a strict `Err` (see
    /// `pipeline_exec::finalize_whitelist_strict`); otherwise a resumed run
    /// would skip the months and never re-trigger this check.
    pub(crate) fn finalize(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow!("whitelist validation state lock poisoned"))?;
        self.report_missing_fields(&mut state)
    }

    fn report_missing_fields(&self, state: &mut WhitelistMatchState) -> Result<()> {
        if state.reported || state.fast_seen == 0 || self.field_names.is_empty() {
            return Ok(());
        }

        let missing: Vec<usize> = state
            .fast_field_seen
            .iter()
            .enumerate()
            .filter_map(|(idx, seen)| (!*seen).then_some(idx))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }

        state.reported = true;
        let slow_only: Vec<usize> = missing
            .iter()
            .copied()
            .filter(|idx| state.slow_field_seen.get(*idx).copied().unwrap_or(false))
            .collect();
        let missing_names = join_field_names(&self.field_names, missing.iter().copied());
        let all_missing = missing.len() == self.field_names.len();
        let mut msg = if all_missing {
            if state.fast_seen >= WHITELIST_ZERO_MATCH_SAMPLE {
                format!(
                    "--whitelist matched zero fields on the first {} records; fields never matched: {}; {}",
                    WHITELIST_ZERO_MATCH_SAMPLE, missing_names, WHITELIST_ZERO_MATCH_HINT
                )
            } else {
                format!(
                    "--whitelist matched zero fields on all {} records; fields never matched: {}; {}",
                    state.fast_seen, missing_names, WHITELIST_ZERO_MATCH_HINT
                )
            }
        } else {
            let observed_names = join_field_names(
                &self.field_names,
                state
                    .fast_field_seen
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, seen)| (*seen).then_some(idx)),
            );
            format!(
                "--whitelist fields never matched any fast-path records: {}; observed fields: {}; {}",
                missing_names, observed_names, WHITELIST_ZERO_MATCH_HINT
            )
        };
        if !slow_only.is_empty() {
            msg.push_str(&format!(
                " Fields matched only on slow-path emissions and were excluded from this check: {}.",
                join_field_names(&self.field_names, slow_only.into_iter())
            ));
        }
        if self.strict {
            return Err(anyhow!(msg));
        }
        tracing::warn!("{}", msg);
        Ok(())
    }
}

fn mark_fields_seen(seen: &mut [bool], matched_fields: &[usize]) {
    for idx in matched_fields {
        if let Some(slot) = seen.get_mut(*idx) {
            *slot = true;
        }
    }
}

fn join_field_names(fields: &[String], indices: impl Iterator<Item = usize>) -> String {
    let mut names: Vec<&str> = indices
        .filter_map(|idx| fields.get(idx).map(String::as_str))
        .collect();
    names.sort_unstable();
    names.dedup();
    names.join(", ")
}

/// Append `buf` followed by a newline to `writer` and bump the running record count.
#[inline]
fn write_and_count<W: Write + ?Sized>(
    writer: &mut W,
    buf: &[u8],
    written: &mut u64,
) -> io::Result<()> {
    writer.write_all(buf)?;
    writer.write_all(b"\n")?;
    *written += 1;
    Ok(())
}
