//! Durable account-scoped cooperative agent pools.
//!
//! Membership is bound to `_meta["openai/session"]` on the first `pool_send`.
//! Agent names are allocated automatically from the configured dictionary.
//! Ordinary tool activity refreshes the membership lease and piggybacks queued
//! peer messages and offered receipts. A subsequent non-overlapping tool call
//! retires previous offers. This is implicit retirement, not proof that a
//! client received or read a response; interrupted responses may be lost.

use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rmcp::ErrorData as McpError;

use crate::config::AgentNameAllocation;

#[cfg(test)]
mod comm_tests;
mod coordinator;
mod protocol;
mod storage;
#[cfg(test)]
mod test_support;

pub use coordinator::SessionTurn;
use coordinator::{ChangeNotifier, PoolCoordinator};
pub use protocol::{
    AdminMember, AdminMessage, AdminMessagesPage, AgentIdentity, Communications, DeliveryReceipt,
    MessageRecord, PeerMessage, PoolAction, PoolMembersArgs, PoolMembersResult, PoolSendArgs,
    PoolSendResult, PoolWaitArgs, PoolWaitOutcome, PoolWaitReason, PoolWaitResult, ReceiptRecord,
    ReceiptStatus,
};
use storage::{MutationEffect, PoolStorage};

const DEFAULT_TTL_MS: i64 = 86_400_000;
const MAX_NAME_CHARS: usize = 128;
const ADMIN_AGENT: &str = "admin";

/// Durable pool facade. Transactions and asynchronous coordination have
/// separate owners; successful mutations notify waiters only after commit.
pub struct AgentPoolStore {
    storage: Arc<PoolStorage>,
    coordinator: PoolCoordinator,
}

/// Run a complete storage operation on a blocking worker. SQLite connections
/// and their mutex guards must never be held on Tokio's async worker threads.
async fn blocking_storage<T: Send + 'static>(
    storage: Arc<PoolStorage>,
    operation: impl FnOnce(&PoolStorage) -> Result<T, McpError> + Send + 'static,
) -> Result<T, McpError> {
    tokio::task::spawn_blocking(move || operation(&storage))
        .await
        .map_err(|_| storage_error())?
}

/// Run notifications only after storage has committed. Call this on the
/// blocking worker for async mutations so cancellation of an awaiting caller
/// cannot hide a committed membership or delivery change.
fn notify_after_commit(
    notifier: &ChangeNotifier,
    principal: &str,
    pool: Option<&str>,
    effect: MutationEffect,
) {
    match effect {
        MutationEffect::Wake => notifier.committed_change(),
        MutationEffect::MembershipChanged => notifier.committed_membership_change(
            principal,
            pool.expect("membership change must identify its pool"),
        ),
    }
}

impl AgentPoolStore {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        Self::open_with_ttl(path, Duration::from_millis(DEFAULT_TTL_MS as u64))
    }

    pub fn open_with_ttl(path: &Path, lease_ttl: Duration) -> anyhow::Result<Self> {
        Self::open_with_names(
            path,
            lease_ttl,
            crate::config::default_agent_name_dictionary(),
        )
    }

    pub fn open_with_names(
        path: &Path,
        lease_ttl: Duration,
        agent_names: Vec<String>,
    ) -> anyhow::Result<Self> {
        Self::open_with_name_allocation(
            path,
            lease_ttl,
            agent_names,
            AgentNameAllocation::default(),
        )
    }

    pub fn open_with_name_allocation(
        path: &Path,
        lease_ttl: Duration,
        agent_names: Vec<String>,
        allocation: AgentNameAllocation,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            storage: Arc::new(PoolStorage::open(path, lease_ttl, agent_names, allocation)?),
            coordinator: PoolCoordinator::new(),
        })
    }

    /// Start a call, retiring prior offers only outside an overlapping group.
    pub async fn start_tool(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Option<SessionTurn>, McpError> {
        self.coordinator
            .start_tool(Arc::clone(&self.storage), principal, session)
            .await
    }

    /// Collect communications for any call, including an older overlapping wait.
    pub async fn finish_tool_turn(
        &self,
        principal: &str,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Result<Communications, McpError> {
        self.coordinator
            .finish_tool_turn(
                Arc::clone(&self.storage),
                principal,
                session,
                turn,
                None,
                true,
            )
            .await
    }

    pub fn identities_for_session(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<AgentIdentity>, McpError> {
        self.storage.identities_for_session(principal, session)
    }

    /// Async server call sites use this form rather than locking SQLite on a
    /// Tokio worker. The synchronous facade remains for setup and test users.
    pub async fn identities_for_session_async(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<AgentIdentity>, McpError> {
        let principal = principal.to_owned();
        let session = session.map(str::to_owned);
        blocking_storage(Arc::clone(&self.storage), move |storage| {
            storage.identities_for_session(&principal, session.as_deref())
        })
        .await
    }

    pub fn admin_members(&self, principal: &str) -> Result<Vec<AdminMember>, McpError> {
        self.storage.admin_members(principal)
    }

    pub async fn admin_members_async(&self, principal: &str) -> Result<Vec<AdminMember>, McpError> {
        let principal = principal.to_owned();
        blocking_storage(Arc::clone(&self.storage), move |storage| {
            storage.admin_members(&principal)
        })
        .await
    }

    pub fn admin_messages(
        &self,
        principal: &str,
        limit: usize,
    ) -> Result<Vec<AdminMessage>, McpError> {
        Ok(self.admin_messages_page(principal, limit, None)?.messages)
    }

    pub fn admin_messages_page(
        &self,
        principal: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<AdminMessagesPage, McpError> {
        self.storage.admin_messages_page(principal, limit, cursor)
    }

    pub async fn admin_messages_page_async(
        &self,
        principal: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<AdminMessagesPage, McpError> {
        let principal = principal.to_owned();
        let cursor = cursor.map(str::to_owned);
        blocking_storage(Arc::clone(&self.storage), move |storage| {
            storage.admin_messages_page(&principal, limit, cursor.as_deref())
        })
        .await
    }

    /// Keep acknowledged admin messages visible until the store is reopened.
    pub fn admin_ack(&self, principal: &str, message_id: &str) -> Result<bool, McpError> {
        self.storage.admin_ack(principal, message_id)
    }

    pub async fn admin_ack_async(
        &self,
        principal: &str,
        message_id: &str,
    ) -> Result<bool, McpError> {
        let principal = principal.to_owned();
        let message_id = message_id.to_owned();
        blocking_storage(Arc::clone(&self.storage), move |storage| {
            storage.admin_ack(&principal, &message_id)
        })
        .await
    }

    /// Remove all admin inbox messages for this account, including unread ones.
    pub fn admin_clear_messages(&self, principal: &str) -> Result<usize, McpError> {
        self.storage.admin_clear_messages(principal)
    }

    pub async fn admin_clear_messages_async(&self, principal: &str) -> Result<usize, McpError> {
        let principal = principal.to_owned();
        blocking_storage(Arc::clone(&self.storage), move |storage| {
            storage.admin_clear_messages(&principal)
        })
        .await
    }

    pub fn admin_send(
        &self,
        principal: &str,
        pool: &str,
        target: &str,
        message: &str,
    ) -> Result<PoolSendResult, McpError> {
        let mutation = self.storage.admin_send(principal, pool, target, message)?;
        notify_after_commit(
            &self.coordinator.notification_sender(),
            principal,
            Some(pool),
            mutation.effect,
        );
        Ok(mutation.value)
    }

    pub async fn admin_send_async(
        &self,
        principal: &str,
        pool: &str,
        target: &str,
        message: &str,
    ) -> Result<PoolSendResult, McpError> {
        let principal = principal.to_owned();
        let pool = pool.to_owned();
        let target = target.to_owned();
        let message = message.to_owned();
        let notifications = self.coordinator.notification_sender();
        let result = blocking_storage(Arc::clone(&self.storage), move |storage| {
            let mutation = storage.admin_send(&principal, &pool, &target, &message)?;
            notify_after_commit(&notifications, &principal, Some(&pool), mutation.effect);
            Ok(mutation.value)
        })
        .await?;
        Ok(result)
    }

    pub fn admin_terminate(
        &self,
        principal: &str,
        pool: &str,
        agent: &str,
    ) -> Result<bool, McpError> {
        let mutation = self.storage.admin_terminate(principal, pool, agent)?;
        notify_after_commit(
            &self.coordinator.notification_sender(),
            principal,
            Some(pool),
            mutation.effect,
        );
        Ok(mutation.value)
    }

    pub async fn admin_terminate_async(
        &self,
        principal: &str,
        pool: &str,
        agent: &str,
    ) -> Result<bool, McpError> {
        let principal = principal.to_owned();
        let pool = pool.to_owned();
        let agent = agent.to_owned();
        let notifications = self.coordinator.notification_sender();
        let result = blocking_storage(Arc::clone(&self.storage), move |storage| {
            let mutation = storage.admin_terminate(&principal, &pool, &agent)?;
            notify_after_commit(&notifications, &principal, Some(&pool), mutation.effect);
            Ok(mutation.value)
        })
        .await?;
        Ok(result)
    }

    pub fn members(
        &self,
        principal: &str,
        args: PoolMembersArgs,
        session: Option<&str>,
    ) -> Result<PoolMembersResult, McpError> {
        Ok(self
            .storage
            .members_state(principal, &args, session)?
            .result)
    }

    pub async fn members_async(
        &self,
        principal: &str,
        args: PoolMembersArgs,
        session: Option<&str>,
    ) -> Result<PoolMembersResult, McpError> {
        let principal = principal.to_owned();
        let session = session.map(str::to_owned);
        blocking_storage(Arc::clone(&self.storage), move |storage| {
            Ok(storage
                .members_state(&principal, &args, session.as_deref())?
                .result)
        })
        .await
    }

    pub async fn wait(
        &self,
        principal: &str,
        args: PoolWaitArgs,
        session: Option<&str>,
    ) -> Result<PoolWaitOutcome, McpError> {
        // This observer starts before any SQLite await: a fast join+leave
        // cannot disappear before the first off-thread revision snapshot.
        let membership = self.coordinator.observe_membership(principal, &args.pool);
        let turn = self.start_tool(principal, session).await?;
        self.coordinator
            .wait(
                Arc::clone(&self.storage),
                principal,
                args,
                session,
                turn.as_ref(),
                membership,
            )
            .await
    }

    pub fn send(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        let pool = args.pool.clone();
        let mutation = self.storage.send_with_effects(principal, args, session)?;
        notify_after_commit(
            &self.coordinator.notification_sender(),
            principal,
            Some(&pool),
            mutation.effect,
        );
        Ok(mutation.value)
    }

    pub async fn send_async(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        let principal = principal.to_owned();
        let session = session.map(str::to_owned);
        let pool = args.pool.clone();
        let notifications = self.coordinator.notification_sender();
        let result = blocking_storage(Arc::clone(&self.storage), move |storage| {
            let mutation = storage.send_with_effects(&principal, args, session.as_deref())?;
            notify_after_commit(&notifications, &principal, Some(&pool), mutation.effect);
            Ok(mutation.value)
        })
        .await?;
        Ok(result)
    }
}

fn validate_name(value: &str, field: &str) -> Result<(), McpError> {
    if value.is_empty()
        || value.chars().count() > MAX_NAME_CHARS
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(invalid(Cow::Owned(format!(
            "{field} must contain 1 to 128 characters with no control characters or surrounding whitespace"
        ))));
    }
    Ok(())
}

fn validate_session(value: &str) -> Result<(), McpError> {
    if value.is_empty() || value.len() > 512 || value.chars().any(char::is_control) {
        return Err(invalid(
            "openai/session must contain 1 to 512 bytes with no control characters",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<Cow<'static, str>>) -> McpError {
    McpError::invalid_params(message, None)
}

fn storage_error() -> McpError {
    McpError::internal_error("agent pool storage operation failed", None)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests;
