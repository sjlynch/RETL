//! User-facing run provenance manifests.
//!
//! These sidecars are separate from RETL's resumable `_progress.json`: they are
//! stable, machine-readable descriptions of a completed user-facing output.

include!("types.rs");
include!("paths.rs");
include!("write.rs");
include!("identity.rs");
include!("upstream.rs");
include!("identity_tail.rs");
include!("fingerprint.rs");
include!("path_stability.rs");
include!("write_tail.rs");
include!("options.rs");
