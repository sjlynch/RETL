# `src/paths/` orientation

Owns the on-disk `RC_YYYY-MM.zst` / `RS_YYYY-MM.zst` naming convention and the planner that turns `(sources, comments_dir, submissions_dir, start, end)` into a deterministic list of `FileJob` for the rest of the pipeline.

- `types.rs` defines `FileKind` (Comment/Submission), `FileJob` (kind + `YearMonth` + path), `Discovered` (per-source `BTreeMap<YearMonth, PathBuf>`), and the error/status types `PlanningError`, `SourceStatus`, `MissingMonthDiagnostic`.
- `discover.rs::discover_all` / `discover_all_checked` / `discover_sources_checked` walk a directory one level deep, regex-match the canonical filenames, and skip-with-warning invalid months (e.g. `RC_2024-00.zst`).
- `plan.rs::plan_files` clamps the requested range to each source's discovered min/max and emits one `FileJob` per existing month; `plan_files_checked` upgrades silent emptiness to `PlanningError::{NoSourceFiles, DateRangeNoFiles}`.
- `diagnostics.rs` formats `PlanningError`, computes `missing_month_diagnostics`, and `log_missing_month_warnings` emits user-facing `tracing::warn!` (only when a start or end was supplied — default scans stay quiet).
- Filename regex and `expected_pattern` strings are part of the public CLI surface; tests assert their wording.
