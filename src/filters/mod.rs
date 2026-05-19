//! Filtering helpers and query logic that work on MinimalRecord fast-path fields,
//! plus optional full-parse checks and record-level date/timestamp bounds.

mod bounds;
mod ci;
mod full;
mod minimal;
mod targets;
mod text;
mod url;

use crate::zstd_jsonl::MinimalRecord;

pub use self::bounds::{bounds_tuple, within_bounds, ym_from_epoch, DateBounds};
pub use self::full::matches_full;
pub use self::minimal::{matches_minimal, matches_subreddit_basic};
pub use self::targets::resolve_target_subs_from;

#[inline]
fn any_text_field_matches(min: &MinimalRecord, mut pred: impl FnMut(&str) -> bool) -> bool {
    min.body.as_deref().map_or(false, &mut pred)
        || min.selftext.as_deref().map_or(false, &mut pred)
        || min.title.as_deref().map_or(false, pred)
}
