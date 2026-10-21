//! Shared fixtures for the independent agent-pool test suites.

use std::time::Duration;

use crate::config::AgentNameAllocation;

use super::{AgentPoolStore, DEFAULT_TTL_MS, PoolAction, PoolSendArgs, PoolWaitArgs};

pub(super) fn store() -> (tempfile::TempDir, AgentPoolStore) {
    store_with_allocation(AgentNameAllocation::default())
}

pub(super) fn store_with_allocation(
    allocation: AgentNameAllocation,
) -> (tempfile::TempDir, AgentPoolStore) {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let store = AgentPoolStore::open_with_name_allocation(
        &directory.path().join("agent-pool.sqlite3"),
        Duration::from_millis(DEFAULT_TTL_MS as u64),
        ["alice", "bob", "carol", "dave"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        allocation,
    )
    .unwrap();
    (directory, store)
}

pub(super) fn send(pool: &str, target: &str, body: &str) -> PoolSendArgs {
    PoolSendArgs {
        pool: pool.into(),
        target: Some(target.into()),
        message: Some(body.into()),
        in_reply_to: None,
        action: None,
    }
}

pub(super) fn leave(pool: &str) -> PoolSendArgs {
    PoolSendArgs {
        pool: pool.into(),
        target: None,
        message: None,
        in_reply_to: None,
        action: Some(PoolAction::Leave),
    }
}

pub(super) fn waiting(pool: &str, seconds: u64) -> PoolWaitArgs {
    PoolWaitArgs {
        pool: pool.into(),
        wait_seconds: seconds,
    }
}

pub(super) async fn poll_pending<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}
