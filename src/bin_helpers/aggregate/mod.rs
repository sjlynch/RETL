// CLI-only aggregator types and helpers used by `retl aggregate`. Split
// across submodules by concern:
// - `spec.rs`:    CLI-parsing wrappers (`RecCount`, `GroupBySpec`,
//                 `MetricKind`, `MetricSpec`).
// - `numeric.rs`: exact-arithmetic core (`MetricNumber`, `NumericSum`,
//                 `MetricSortValue`, plus the numeric parsing/comparison
//                 helpers).
// - `format.rs`:  pure number-to-string formatting; does not depend on the
//                 other aggregate submodules.
// - `group.rs`:   the grouped `Aggregator` implementation
//                 (`MetricState`, `GroupMetricAgg`) and the per-key value
//                 extractors.
//
// These files are textually concatenated by `bin_helpers/mod.rs` via
// `include!`, so the imports below are shared with the rest of the
// bin_helpers module (build.rs, etl.rs, etc.).

use anyhow::{Context, Result};
use retl::{
    Aggregator, ConfigBuildError, JsonPointerPredicate, NumericComparison, PartialReadReporter,
    RedditETL, Sources, YearMonth,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

use crate::bin_args::CommonOpts;

include!("spec.rs");
include!("numeric.rs");
include!("format.rs");
include!("group.rs");

// -----------------------------------------------------------------------------
// Builder helpers.
// -----------------------------------------------------------------------------
