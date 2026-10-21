//! Per-session overlapping calls and observable-change/lease-expiry waiting.
//! No storage or session lock is held across a wait.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use rmcp::ErrorData as McpError;
use tokio::sync::watch;

use super::storage::{MembersState, PoolStorage};
use super::{
    Communications, PoolMembersArgs, PoolWaitArgs, PoolWaitReason, PoolWaitResult,
    blocking_storage, invalid, now_ms, storage_error, validate_name, validate_session,
};

type SessionKey = (String, String);
type PoolKey = (String, String);

/// The database remains authoritative for membership revisions. These weak,
/// short-lived counters only bridge changes committed while a newly polled
/// wait is still awaiting its first off-thread SQLite snapshot.
#[derive(Clone)]
pub(super) struct ChangeNotifier {
    changes: watch::Sender<()>,
    membership_observers: Arc<Mutex<HashMap<PoolKey, Weak<AtomicUsize>>>>,
}

impl ChangeNotifier {
    fn new() -> Self {
        Self {
            changes: watch::channel(()).0,
            membership_observers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(super) fn committed_change(&self) {
        self.changes.send_replace(());
    }

    pub(super) fn committed_membership_change(&self, principal: &str, pool: &str) {
        let observers = self
            .membership_observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(counter) = observers
            .get(&(principal.to_owned(), pool.to_owned()))
            .and_then(Weak::upgrade)
        {
            counter.fetch_add(1, Ordering::AcqRel);
        }
        drop(observers);
        self.committed_change();
    }

    fn observe_membership(&self, principal: &str, pool: &str) -> (Arc<AtomicUsize>, usize) {
        let mut observers = self
            .membership_observers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // No permanent second registry: unused pool observers are discarded.
        observers.retain(|_, counter| counter.strong_count() > 0);
        let key = (principal.to_owned(), pool.to_owned());
        let counter = if let Some(counter) = observers.get(&key).and_then(Weak::upgrade) {
            counter
        } else {
            let counter = Arc::new(AtomicUsize::new(0));
            observers.insert(key, Arc::downgrade(&counter));
            counter
        };
        let epoch = counter.load(Ordering::Acquire);
        (counter, epoch)
    }

    pub(super) fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }
}

#[derive(Default)]
struct SessionCalls {
    // Dropping a turn must never wait on a blocking SQLite operation. A gate
    // serializes starts on blocking workers; releases are atomic and immediate.
    active: AtomicUsize,
    start_gate: Mutex<()>,
}

/// Keeps an overlapping group of calls alive through response preparation.
/// Calls in the same group may re-offer messages; they never retire one
/// another's offers. A subsequent non-overlapping call retires the group.
pub struct SessionTurn {
    calls: Arc<SessionCalls>,
    principal: String,
    session: String,
}

impl Drop for SessionTurn {
    fn drop(&mut self) {
        self.calls.active.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(super) struct PoolCoordinator {
    sessions: Mutex<HashMap<SessionKey, Weak<SessionCalls>>>,
    notifications: ChangeNotifier,
}

impl PoolCoordinator {
    pub(super) fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            notifications: ChangeNotifier::new(),
        }
    }

    pub(super) fn observe_membership(
        &self,
        principal: &str,
        pool: &str,
    ) -> (Arc<AtomicUsize>, usize) {
        self.notifications.observe_membership(principal, pool)
    }

    pub(super) fn notification_sender(&self) -> ChangeNotifier {
        self.notifications.clone()
    }

    pub(super) async fn start_tool(
        &self,
        storage: Arc<PoolStorage>,
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
                let calls = Arc::new(SessionCalls::default());
                sessions.insert(key, Arc::downgrade(&calls));
                calls
            }
        };
        let principal = principal.to_owned();
        let session = session.to_owned();
        blocking_storage(storage, move |storage| {
            let _gate = calls.start_gate.lock().map_err(|_| storage_error())?;
            if calls.active.load(Ordering::Acquire) == 0 {
                storage.begin_tool_state(&principal, Some(&session))?;
            } else {
                storage.refresh_tool_state(&principal, Some(&session))?;
            }
            calls.active.fetch_add(1, Ordering::AcqRel);
            drop(_gate);
            Ok(Some(SessionTurn {
                calls,
                principal,
                session,
            }))
        })
        .await
    }

    pub(super) async fn finish_tool_turn(
        &self,
        storage: Arc<PoolStorage>,
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
        let principal = principal.to_owned();
        let session = session.to_owned();
        let pool = pool.map(str::to_owned);
        let notifications = self.notification_sender();
        let comms = blocking_storage(storage, move |storage| {
            let comms = storage.collect_tool_comms(&principal, Some(&session), pool.as_deref())?;
            // A queued offered receipt must wake waiters even if the caller
            // drops the awaiting future after this transaction commits.
            if !comms.messages.is_empty() {
                notifications.committed_change();
            }
            Ok(comms)
        })
        .await?;
        Ok(comms)
    }

    pub(super) async fn wait(
        &self,
        storage: Arc<PoolStorage>,
        principal: &str,
        args: PoolWaitArgs,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
        membership_observer: (Arc<AtomicUsize>, usize),
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
        let mut changes = self.notifications.subscribe();
        let members = PoolMembersArgs { pool: args.pool };
        let mut state = read_members_state(&storage, principal, &members, session).await?;
        let initial_revision = state.membership_revision;
        let (membership_epoch, initial_epoch) = membership_observer;
        loop {
            let mut membership_changed = state.membership_revision != initial_revision
                || membership_epoch.load(Ordering::Acquire) != initial_epoch;
            if state.pending || membership_changed || tokio::time::Instant::now() >= deadline {
                let comms = self
                    .finish_tool_turn(
                        Arc::clone(&storage),
                        principal,
                        Some(session),
                        turn,
                        Some(&members.pool),
                    )
                    .await?;
                // Collection may observe expiry or overlap a membership commit.
                // Include changes through the final observed state as well.
                state = read_members_state(&storage, principal, &members, session).await?;
                membership_changed |= state.membership_revision != initial_revision
                    || membership_epoch.load(Ordering::Acquire) != initial_epoch;
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
                    state = read_members_state(&storage, principal, &members, session).await?;
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
            state = read_members_state(&storage, principal, &members, session).await?;
        }
    }
}

async fn read_members_state(
    storage: &Arc<PoolStorage>,
    principal: &str,
    members: &PoolMembersArgs,
    session: &str,
) -> Result<MembersState, McpError> {
    let principal = principal.to_owned();
    let members = members.clone();
    let session = session.to_owned();
    blocking_storage(Arc::clone(storage), move |storage| {
        storage.members_state(&principal, &members, Some(&session))
    })
    .await
}
