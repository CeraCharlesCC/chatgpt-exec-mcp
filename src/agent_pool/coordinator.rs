//! Tool-turn ordering and observable-change/lease-expiry waiting.
//! The only persistence dependency is a short synchronous transaction at each
//! transition; no storage lock is held across a wait.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use rmcp::ErrorData as McpError;
use tokio::sync::{Mutex as AsyncMutex, watch};

use super::storage::PoolStorage;
use super::{
    PeerMessage, PoolMembersArgs, PoolMembersResult, invalid, now_ms, storage_error, validate_name,
    validate_session,
};

#[derive(Default)]
struct SessionOrder {
    next_sequence: u64,
    latest_started: u64,
}

type SessionKey = (String, String);
type SessionOrderHandle = Arc<AsyncMutex<SessionOrder>>;
type SessionOrderWeak = Weak<AsyncMutex<SessionOrder>>;

pub struct SessionTurn {
    sequence: u64,
    order: SessionOrderHandle,
}

pub(super) struct PoolCoordinator {
    session_orders: Mutex<HashMap<SessionKey, SessionOrderWeak>>,
    changes: watch::Sender<()>,
}

impl PoolCoordinator {
    pub(super) fn new() -> Self {
        Self {
            session_orders: Mutex::new(HashMap::new()),
            changes: watch::channel(()).0,
        }
    }

    /// Called by the facade only after a mutating storage transaction commits.
    pub(super) fn committed_change(&self) {
        self.changes.send_replace(());
    }

    /// Start a correlated tool turn. Only the short inbox transition is
    /// serialized; the actual tool operation runs without holding this lock.
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
        let order = {
            let mut orders = self.session_orders.lock().map_err(|_| storage_error())?;
            orders.retain(|_, order| order.strong_count() > 0);
            let key = (principal.to_owned(), session.to_owned());
            if let Some(order) = orders.get(&key).and_then(Weak::upgrade) {
                order
            } else {
                let order = Arc::new(AsyncMutex::new(SessionOrder::default()));
                orders.insert(key, Arc::downgrade(&order));
                order
            }
        };
        let mut state = order.lock().await;
        state.next_sequence = state
            .next_sequence
            .checked_add(1)
            .ok_or_else(storage_error)?;
        let sequence = state.next_sequence;
        state.latest_started = sequence;
        storage.begin_tool_state(principal, Some(session))?;
        drop(state);
        Ok(Some(SessionTurn { sequence, order }))
    }

    /// Finish a correlated tool turn. A call that was superseded by a later
    /// concurrently-started call does not offer inbox rows; the latest turn
    /// will offer them instead.
    pub(super) async fn finish_tool_turn(
        &self,
        storage: &PoolStorage,
        principal: &str,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Result<Vec<PeerMessage>, McpError> {
        let (Some(session), Some(turn)) = (session, turn) else {
            return Ok(Vec::new());
        };
        let state = turn.order.lock().await;
        if turn.sequence != state.latest_started {
            return Ok(Vec::new());
        }
        storage.collect_tool_messages(principal, Some(session))
    }

    pub(super) async fn wait_members(
        &self,
        storage: &PoolStorage,
        principal: &str,
        args: PoolMembersArgs,
        session: Option<&str>,
    ) -> Result<PoolMembersResult, McpError> {
        let Some(seconds) = args.wait_seconds else {
            return Ok(storage.members_state(principal, &args, session)?.result);
        };
        if !(5..=45).contains(&seconds) {
            return Err(invalid("wait_seconds must be between 5 and 45"));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
        // Subscribe before checking the database. A commit between the check
        // and changed().await remains visible, including to multiple waiters.
        let mut changes = self.changes.subscribe();
        let mut state = storage.members_state(principal, &args, session)?;
        let initial_members = state.member_ids.clone();
        loop {
            if state.pending
                || state.member_ids != initial_members
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(state.result);
            }
            let wake_at = state.next_expiry_ms.map_or(deadline, |expires| {
                deadline.min(
                    tokio::time::Instant::now()
                        + Duration::from_millis((expires - now_ms()).max(0) as u64),
                )
            });
            // No database or session-order lock is held while awaiting.
            tokio::select! {
                _ = changes.changed() => {},
                _ = tokio::time::sleep_until(wake_at) => {},
            }
            // Observe state only: internal wakeups never ACK or offer messages.
            state = storage.members_state(principal, &args, session)?;
        }
    }
}
