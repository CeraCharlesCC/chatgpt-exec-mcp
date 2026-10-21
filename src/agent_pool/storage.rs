//! Atomic pool persistence: membership, message fanout, inbox offers and ACKs.
//! This component never awaits or owns notification subscriptions.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rmcp::ErrorData as McpError;
use rusqlite::Connection;

use crate::config::AgentNameAllocation;

mod admin;
mod membership;
mod messages;
mod schema;

use membership::cleanup;

use super::{ADMIN_AGENT, MAX_NAME_CHARS, PoolMembersResult, now_ms, storage_error};

const MAX_PENDING: i64 = 10_000;
const MAX_BATCH_COUNT: usize = 32;
const MAX_BATCH_BYTES: usize = 128 * 1024;
const MAX_MESSAGE_BYTES: usize = 65_536;
const MAX_ADMIN_MESSAGES: i64 = 1_000;
// Keep routine reads free of global cleanup. The detached maintenance loop
// works in small transactions and releases its connection when the store dies.
const EXPIRY_BATCH_SIZE: i64 = 128;
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(5);

pub(super) struct PoolStorage {
    connection: Arc<Mutex<Connection>>,
    lease_ttl_ms: i64,
    agent_names: Arc<[String]>,
    name_allocation: AgentNameAllocation,
    // Last successfully assigned dictionary index for each account/pool.
    // Memberships are durable, but the rotation itself resets on core restart.
    name_cursors: Mutex<HashMap<(String, String), usize>>,
}

pub(super) struct MembersState {
    pub(super) result: PoolMembersResult,
    pub(super) membership_revision: i64,
    pub(super) next_expiry_ms: Option<i64>,
    pub(super) pending: bool,
}

/// Coordination intent recorded with a successful database operation. No
/// caller needs a second membership query after a transaction commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MutationEffect {
    /// Only the returned data changed; waiting peers need no notification.
    None,
    /// Wake observers to recheck queues (also preserves legacy no-op wakes).
    Wake,
    /// A member was added or an active member was removed from this pool.
    MembershipChanged,
}

pub(super) struct Mutation<T> {
    pub(super) value: T,
    pub(super) effect: MutationEffect,
}

impl<T> Mutation<T> {
    pub(super) fn new(value: T, effect: MutationEffect) -> Self {
        Self { value, effect }
    }
}

struct MemberMessage<'a> {
    principal: &'a str,
    pool: &'a str,
    sender: &'a str,
    sender_member_id: Option<i64>,
    target: &'a str,
    body: &'a str,
    in_reply_to: Option<&'a str>,
    created_ms: i64,
}

impl PoolStorage {
    pub(super) fn open(
        path: &Path,
        lease_ttl: Duration,
        agent_names: Vec<String>,
        name_allocation: AgentNameAllocation,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !agent_names.is_empty(),
            "agent name dictionary must not be empty"
        );
        let mut unique_names = std::collections::HashSet::new();
        for name in &agent_names {
            anyhow::ensure!(
                !name.is_empty()
                    && name.chars().count() <= MAX_NAME_CHARS
                    && name.trim() == name
                    && !name.chars().any(char::is_control)
                    && !matches!(name.as_str(), "global" | ADMIN_AGENT),
                "agent name dictionary contains an invalid or reserved name"
            );
            anyhow::ensure!(
                unique_names.insert(name.clone()),
                "agent name dictionary contains duplicates"
            );
        }
        let lease_ttl_ms = i64::try_from(lease_ttl.as_millis())
            .map_err(|_| anyhow::anyhow!("agent pool membership TTL is too large"))?;
        anyhow::ensure!(
            lease_ttl_ms > 0,
            "agent pool membership TTL must be positive"
        );
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
            anyhow::ensure!(
                std::fs::metadata(parent)?.permissions().mode() & 0o077 == 0,
                "agent pool database parent directory must be private to its owner"
            );
        }
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        anyhow::ensure!(
            file.metadata()?.permissions().mode() & 0o077 == 0,
            "agent pool database permissions must be restricted to its owner"
        );

        let mut connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        schema::initialize(&mut connection)?;
        let connection = Arc::new(Mutex::new(connection));
        let maintenance_connection = Arc::downgrade(&connection);
        if let Err(error) = std::thread::Builder::new()
            .name("pool-sqlite-maintenance".to_owned())
            .spawn(move || {
                loop {
                    std::thread::sleep(MAINTENANCE_INTERVAL);
                    let Some(connection) = maintenance_connection.upgrade() else {
                        break;
                    };
                    let Ok(mut connection) = connection.lock() else {
                        break;
                    };
                    let Ok(transaction) = connection.transaction() else {
                        continue;
                    };
                    // A failed tick rolls back and retries at the next tick;
                    // no open transaction or lock survives the sleep interval.
                    if cleanup(&transaction, now_ms()).is_ok() {
                        let _ = transaction.commit();
                    }
                }
            })
        {
            // If the OS refuses another thread, writes still retire bounded
            // expiries and pool_members resolves its own expiry revisions.
            eprintln!("agent pool periodic maintenance unavailable: {error}");
        }
        Ok(Self {
            connection,
            lease_ttl_ms,
            agent_names: agent_names.into(),
            name_allocation,
            name_cursors: Mutex::new(HashMap::new()),
        })
    }

    pub(super) fn connection(&self) -> Result<MutexGuard<'_, Connection>, McpError> {
        self.connection.lock().map_err(|_| storage_error())
    }
}

#[cfg(test)]
mod tests;
