
/// Data source toggle (comments, submissions, both).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sources {
    Comments,
    Submissions,
    Both,
}
