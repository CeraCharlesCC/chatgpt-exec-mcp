//! In-memory recent activity with a bounded, temporary compressed history.
//!
//! The dashboard's periodic snapshots touch only the recent queue. Full
//! snapshots and older pages read zstd chunks on demand. Archive writes run
//! on one background worker; file I/O never holds the activity mutex.

mod archive;
mod events;
mod history;

pub(crate) use events::{ActivityContext, ActivityEmissionContext};
pub use events::{
    ActivityEvent, ActivityLifecycle, ActivityLifecycleType, ActivityMessage,
    ActivityMessageStatus, ActivityPhase,
};
pub use history::{ActivityHub, ActivityPage};
