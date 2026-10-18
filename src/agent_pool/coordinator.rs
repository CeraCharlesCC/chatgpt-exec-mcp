//! Per-session overlapping calls and observable-change/lease-expiry waiting.
//! No storage or session lock is held across a wait.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use rmcp::ErrorData as McpError;
use tokio::sync::watch;

use super::storage::PoolStorage;
use super::{
    Communications, PoolMembersArgs, PoolWaitArgs, PoolWaitReason, PoolWaitResult, invalid, now_ms,
    storage_error, validate_name, validate_session,
};

type SessionKey = (String, String);
type SessionCalls = Arc<Mutex<usize>>;

/// Keeps an overlapping group of calls alive through response preparation.
/// Calls in the same group may re-offer messages; they never retire one
/// another's offers. A subsequent non-overlapping call retires the group.
pub struct SessionTurn {
    calls: SessionCalls,
    principal: String,
    session: String,
}

impl Drop for SessionTurn {
    fn drop(&mut self) {
        if let Ok(mut active) = self.calls.lock() {
            *active -= 1;
        }
    }
}

pub(super) struct PoolCoordinator {
    sessions: Mutex<HashMap<SessionKey, Weak<Mutex<usize>>>>,
    changes: watch::Sender<()>,
}

impl PoolCoordinator {
    pub(super) fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            changes: watch::channel(()).0,
        }
    }

    pub(super) fn committed_change(&self) {
        self.changes.send_replace(());
    }

    pub(super) async fn start_tool(
        &self,
        storage: &PoolStorage,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Option<SessionTurn>, McpError> {
        validate_name(principal, "principal")?;
        let Some(session) = session else {
            return Ok(None);
        };
        validate_session(session)?;
        let calls = {
            let mut sessions = self.sessions.lock().map_err(|_| storage_error())?;
            sessions.retain(|_, calls| calls.strong_count() > 0);
            let key = (principal.to_owned(), session.to_owned());
            if let Some(calls) = sessions.get(&key).and_then(Weak::upgrade) {
                calls
            } else {
                let calls = Arc::new(Mutex::new(0));
                sessions.insert(key, Arc::downgrade(&calls));
                calls
            }
        };
        let mut active = calls.lock().map_err(|_| storage_error())?;
        if *active == 0 {
            storage.begin_tool_state(principal, Some(session))?;
        } else {
            storage.refresh_tool_state(principal, Some(session))?;
        }
        *active += 1;
        drop(active);
        Ok(Some(SessionTurn {
            calls,
            principal: principal.into(),
            session: session.into(),
        }))
    }

    pub(super) async fn finish_tool_turn(
        &self,
        storage: &PoolStorage,
        principal: &str,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
        pool: Option<&str>,
    ) -> Result<Communications, McpError> {
        let (Some(session), Some(turn)) = (session, turn) else {
            return Ok(Communications::default());
        };
        if turn.principal != principal || turn.session != session {
            return Err(invalid("tool turn belongs to another account or session"));
        }
        let comms = storage.collect_tool_comms(principal, Some(session), pool)?;
        // Collecting a direct message may enqueue its sender's offered receipt.
        if !comms.messages.is_empty() {
            self.committed_change();
        }
        Ok(comms)
    }

    pub(super) async fn wait(
        &self,
        storage: &PoolStorage,
        principal: &str,
        args: PoolWaitArgs,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Result<PoolWaitResult, McpError> {
        let session =
            session.ok_or_else(|| invalid("pool_wait requires openai/session metadata"))?;
        validate_session(session)?;
        if !(5..=45).contains(&args.wait_seconds) {
            return Err(invalid("wait_seconds must be between 5 and 45"));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(args.wait_seconds);
        // Subscribe before observing state: commits between a read and await
        // cannot be missed. Re-check the selected pool after every wakeup.
        let mut changes = self.changes.subscribe();
        let members = PoolMembersArgs { pool: args.pool };
        let mut state = storage.members_state(principal, &members, Some(session))?;
        let initial_revision = state.membership_revision;
        loop {
            let mut membership_changed = state.membership_revision != initial_revision;
            if state.pending || membership_changed || tokio::time::Instant::now() >= deadline {
                let comms = self
                    .finish_tool_turn(storage, principal, Some(session), turn, Some(&members.pool))
                    .await?;
                // Collection may observe expiry or overlap a membership commit.
                // Include changes through the final observed state as well.
                state = storage.members_state(principal, &members, Some(session))?;
                membership_changed |= state.membership_revision != initial_revision;
                let reason = if !comms.messages.is_empty() {
                    PoolWaitReason::Message
                } else if !comms.delivery_receipts.is_empty() {
                    PoolWaitReason::Receipt
                } else if membership_changed {
                    PoolWaitReason::MembershipChanged
                } else if tokio::time::Instant::now() >= deadline {
                    PoolWaitReason::Timeout
                } else {
                    // Expiry or an explicit leave can remove pending rows.
                    state = storage.members_state(principal, &members, Some(session))?;
                    continue;
                };
                return Ok(PoolWaitResult {
                    reason,
                    messages: comms.messages,
                    membership_changed,
                    delivery_receipts: comms.delivery_receipts,
                });
            }
            let wake_at = state.next_expiry_ms.map_or(deadline, |expires| {
                deadline.min(
                    tokio::time::Instant::now()
                        + Duration::from_millis((expires - now_ms()).max(0) as u64),
                )
            });
            tokio::select! {
                _ = changes.changed() => {},
                _ = tokio::time::sleep_until(wake_at) => {},
            }
            state = storage.members_state(principal, &members, Some(session))?;
        }
    }
}
