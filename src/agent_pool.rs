//! Durable account-scoped cooperative agent pools.
//!
//! Membership is bound to `_meta["openai/session"]` on the first `pool_send`.
//! Agent names are allocated automatically from the configured dictionary.
//! Ordinary tool activity refreshes the membership lease and piggybacks queued
//! peer messages. A message offered on one tool call is acknowledged by the
//! same session's next tool call, giving at-least-once delivery without poll or
//! acknowledgement tools.

use std::borrow::Cow;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rmcp::ErrorData as McpError;
use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

mod coordinator;
mod storage;

use coordinator::PoolCoordinator;
pub use coordinator::SessionTurn;
use storage::PoolStorage;

const DEFAULT_TTL_MS: i64 = 86_400_000;
const MAX_NAME_CHARS: usize = 128;
const ADMIN_AGENT: &str = "admin";

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersArgs {
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
    /// Wait timeout in seconds; omit for an immediate snapshot.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_wait",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(
        with = "u64",
        range(min = 5, max = 45),
        skip_serializing_if = "Option::is_none"
    )]
    pub wait_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolSendArgs {
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
    /// Member name, or global for other current members.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "String", length(min = 1, max = MAX_NAME_CHARS), skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// UTF-8 text, at most 65,536 bytes.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(
        with = "String",
        length(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub message: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(
        with = "String",
        length(min = 1, max = MAX_NAME_CHARS),
        skip_serializing_if = "Option::is_none"
    )]
    pub in_reply_to: Option<String>,
    /// Omit to send; leave removes this membership (pool and action only).
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_action",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PoolAction", skip_serializing_if = "Option::is_none")]
    pub action: Option<PoolAction>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "rmcp::schemars")]
pub enum PoolAction {
    Leave,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PeerMessage {
    pub message_id: String,
    pub pool: String,
    pub from: String,
    pub to: String,
    /// Original send target.
    pub target: String,
    pub sent_at_ms: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub in_reply_to: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AgentIdentity {
    pub pool: String,
    pub agent: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminMember {
    pub pool: String,
    pub agent: String,
    pub session: String,
    pub last_seen_ms: i64,
    pub expires_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminMessage {
    pub message_id: String,
    pub pool: String,
    pub from: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    pub created_ms: i64,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PoolMembersResult {
    pub agents: Vec<String>,
    /// This session's member name.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub self_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<PeerMessage>", default)]
    pub peer_messages: Option<Vec<PeerMessage>>,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PoolSendResult {
    /// Name assigned on joining.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub assigned_agent: Option<String>,
    /// Present when a delivery is queued.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub message_id: Option<String>,
    /// Queued recipients.
    pub recipients: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<PeerMessage>", default)]
    pub peer_messages: Option<Vec<PeerMessage>>,
}

fn deserialize_optional_non_null_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?.map_or_else(
        || {
            Err(serde::de::Error::custom(
                "null is not allowed; omit the field instead",
            ))
        },
        |value| Ok(Some(value)),
    )
}

fn deserialize_optional_non_null_wait<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<u64>::deserialize(deserializer)?
        .ok_or_else(|| serde::de::Error::custom("null is not allowed; omit the field instead"))
        .map(Some)
}

fn deserialize_optional_non_null_action<'de, D>(
    deserializer: D,
) -> Result<Option<PoolAction>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<PoolAction>::deserialize(deserializer)?.map_or_else(
        || {
            Err(serde::de::Error::custom(
                "null is not allowed; omit the field instead",
            ))
        },
        |value| Ok(Some(value)),
    )
}

/// Durable pool facade. Transactions and asynchronous coordination have
/// separate owners; successful mutations notify waiters only after commit.
pub struct AgentPoolStore {
    storage: PoolStorage,
    coordinator: PoolCoordinator,
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
        Ok(Self {
            storage: PoolStorage::open(path, lease_ttl, agent_names)?,
            coordinator: PoolCoordinator::new(),
        })
    }

    /// Start a correlated tool turn and ACK the previous committed offer.
    pub async fn start_tool(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Option<SessionTurn>, McpError> {
        self.coordinator
            .start_tool(&self.storage, principal, session)
            .await
    }

    /// Only the latest concurrently started turn may offer inbox messages.
    pub async fn finish_tool_turn(
        &self,
        principal: &str,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Result<Vec<PeerMessage>, McpError> {
        self.coordinator
            .finish_tool_turn(&self.storage, principal, session, turn)
            .await
    }

    pub fn identities_for_session(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<AgentIdentity>, McpError> {
        self.storage.identities_for_session(principal, session)
    }

    pub fn admin_members(&self, principal: &str) -> Result<Vec<AdminMember>, McpError> {
        self.storage.admin_members(principal)
    }

    pub fn admin_messages(
        &self,
        principal: &str,
        limit: usize,
    ) -> Result<Vec<AdminMessage>, McpError> {
        self.storage.admin_messages(principal, limit)
    }

    pub fn admin_send(
        &self,
        principal: &str,
        pool: &str,
        target: &str,
        message: &str,
        in_reply_to: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        let result = self
            .storage
            .admin_send(principal, pool, target, message, in_reply_to)?;
        self.coordinator.committed_change();
        Ok(result)
    }

    pub fn admin_terminate(
        &self,
        principal: &str,
        pool: &str,
        agent: &str,
    ) -> Result<bool, McpError> {
        let result = self.storage.admin_terminate(principal, pool, agent)?;
        self.coordinator.committed_change();
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

    pub async fn wait_members(
        &self,
        principal: &str,
        args: PoolMembersArgs,
        session: Option<&str>,
    ) -> Result<PoolMembersResult, McpError> {
        self.coordinator
            .wait_members(&self.storage, principal, args, session)
            .await
    }

    pub fn send(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        let result = self.storage.send(principal, args, session)?;
        self.coordinator.committed_change();
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
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    fn store() -> (tempfile::TempDir, AgentPoolStore) {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = AgentPoolStore::open_with_names(
            &directory.path().join("agent-pool.sqlite3"),
            Duration::from_millis(DEFAULT_TTL_MS as u64),
            ["alice", "bob", "carol", "dave"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        )
        .unwrap();
        (directory, store)
    }

    fn send(pool: &str, target: &str, body: &str) -> PoolSendArgs {
        PoolSendArgs {
            pool: pool.into(),
            target: Some(target.into()),
            message: Some(body.into()),
            in_reply_to: None,
            action: None,
        }
    }

    fn leave(pool: &str) -> PoolSendArgs {
        PoolSendArgs {
            pool: pool.into(),
            target: None,
            message: None,
            in_reply_to: None,
            action: Some(PoolAction::Leave),
        }
    }

    fn waiting(pool: &str, seconds: u64) -> PoolMembersArgs {
        PoolMembersArgs {
            pool: pool.into(),
            wait_seconds: Some(seconds),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wait_bounds_immediate_snapshot_and_timeout() {
        let (_directory, store) = store();
        for seconds in [0, 4, 46, u64::MAX] {
            assert!(
                store
                    .wait_members("owner", waiting("p", seconds), None)
                    .await
                    .is_err()
            );
        }
        for value in [json!(null), json!(-1), json!(10.5), json!("35")] {
            assert!(
                serde_json::from_value::<PoolMembersArgs>(
                    json!({"pool":"p", "wait_seconds":value})
                )
                .is_err()
            );
        }
        let before = tokio::time::Instant::now();
        let args = serde_json::from_value(json!({"pool":"p"})).unwrap();
        assert!(
            store
                .wait_members("owner", args, None)
                .await
                .unwrap()
                .agents
                .is_empty()
        );
        assert_eq!(tokio::time::Instant::now(), before);
        for seconds in [5, 10, 45] {
            let before = tokio::time::Instant::now();
            assert!(
                store
                    .wait_members("owner", waiting("p", seconds), None)
                    .await
                    .unwrap()
                    .agents
                    .is_empty()
            );
            assert_eq!(
                tokio::time::Instant::now() - before,
                Duration::from_secs(seconds)
            );
        }
    }

    #[tokio::test]
    async fn wait_wakes_all_observers_on_join_leave_and_name_reuse() {
        let (_directory, store) = store();
        let store = Arc::new(store);
        let observe = || {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .wait_members("owner", waiting("p", 35), None)
                    .await
                    .unwrap()
            })
        };
        let first = observe();
        let second = observe();
        tokio::task::yield_now().await;
        // Unrelated pool/account changes must not finish the wait.
        store
            .send("owner", send("other", "global", "join"), Some("other"))
            .unwrap();
        store
            .send("other-owner", send("p", "global", "join"), Some("other"))
            .unwrap();
        tokio::task::yield_now().await;
        assert!(!first.is_finished() && !second.is_finished());
        store
            .send("owner", send("p", "global", "join"), Some("a"))
            .unwrap();
        for task in [first, second] {
            let result = tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(result.agents, ["alice"]);
            assert!(result.self_agent.is_none());
        }
        let reused = observe();
        tokio::task::yield_now().await;
        // The same visible name is a new member, even if leave/join coalesce.
        store.send("owner", leave("p"), Some("a")).unwrap();
        store
            .send("owner", send("p", "global", "rejoin"), Some("a"))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), reused)
                .await
                .unwrap()
                .unwrap()
                .agents,
            ["alice"]
        );
        let left = observe();
        tokio::task::yield_now().await;
        store.admin_terminate("owner", "p", "alice").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), left)
                .await
                .unwrap()
                .unwrap()
                .agents
                .is_empty()
        );
    }

    #[tokio::test]
    async fn wait_messages_across_pools_are_offered_once_and_cancellation_keeps_inbox() {
        let (_directory, store) = store();
        let store = Arc::new(store);
        store
            .send("owner", send("p", "global", "join"), Some("a"))
            .unwrap();
        store
            .send("owner", send("q", "global", "join"), Some("a"))
            .unwrap();
        let turn = store.start_tool("owner", Some("a")).await.unwrap();
        let waiter = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .wait_members("owner", waiting("p", 35), Some("a"))
                    .await
                    .unwrap()
            })
        };
        tokio::task::yield_now().await;
        store
            .admin_send("owner", "q", "alice", "cross-pool", None)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        let peers = store
            .finish_tool_turn("owner", Some("a"), turn.as_ref())
            .await
            .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].message, "cross-pool");
        assert_eq!(peers[0].pool, "q");
        let turn = store.start_tool("owner", Some("a")).await.unwrap();
        assert!(
            store
                .finish_tool_turn("owner", Some("a"), turn.as_ref())
                .await
                .unwrap()
                .is_empty()
        );

        let cancelled = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .wait_members("owner", waiting("p", 35), Some("a"))
                    .await
            })
        };
        tokio::task::yield_now().await;
        store
            .admin_send("owner", "p", "alice", "retain", None)
            .unwrap();
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        let turn = store.start_tool("owner", Some("a")).await.unwrap();
        // Already pending messages return immediately without an internal offer.
        tokio::time::timeout(
            Duration::from_secs(1),
            store.wait_members("owner", waiting("p", 35), Some("a")),
        )
        .await
        .unwrap()
        .unwrap();
        let peers = store
            .finish_tool_turn("owner", Some("a"), turn.as_ref())
            .await
            .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].message, "retain");
        assert_eq!(
            store
                .members(
                    "owner",
                    PoolMembersArgs {
                        pool: "p".into(),
                        wait_seconds: None
                    },
                    Some("a")
                )
                .unwrap()
                .self_agent
                .as_deref(),
            Some("alice")
        );
    }

    #[tokio::test]
    async fn wait_observes_expiry_without_a_notification() {
        let (_directory, store) = store();
        store
            .send("owner", send("p", "global", "join"), Some("a"))
            .unwrap();
        store
            .storage
            .connection()
            .unwrap()
            .execute("UPDATE members SET expires=?1", [now_ms() + 100])
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            store.wait_members("owner", waiting("p", 35), None),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(result.agents.is_empty());
    }

    #[test]
    fn broadcast_identifies_each_recipient_and_original_target() {
        let (_directory, store) = store();
        for session in ["a", "b", "c"] {
            store
                .send("owner", send("p", "global", "join"), Some(session))
                .unwrap();
            store
                .storage
                .begin_tool_state("owner", Some(session))
                .unwrap();
        }
        for session in ["a", "b", "c"] {
            store
                .storage
                .collect_tool_messages("owner", Some(session))
                .unwrap();
            store
                .storage
                .begin_tool_state("owner", Some(session))
                .unwrap();
        }
        let before = now_ms();
        let sent = store
            .send("owner", send("p", "global", "broadcast"), Some("a"))
            .unwrap();
        for (session, recipient) in [("b", "bob"), ("c", "carol")] {
            let peers = store
                .storage
                .collect_tool_messages("owner", Some(session))
                .unwrap();
            assert_eq!(peers.len(), 1);
            assert_eq!(Some(&peers[0].message_id), sent.message_id.as_ref());
            assert_eq!(peers[0].to, recipient);
            assert_eq!(peers[0].target, "global");
            assert!((before..=now_ms()).contains(&peers[0].sent_at_ms));
        }
    }

    #[test]
    fn admin_global_broadcast_includes_all_active_members() {
        let (_directory, store) = store();
        let owner = "account-a";
        for session in ["a", "b"] {
            store
                .send(owner, send("project", "global", "join"), Some(session))
                .unwrap();
        }

        // Clear Bob's join announcement from Alice before the admin broadcast.
        store.storage.begin_tool_state(owner, Some("a")).unwrap();
        store
            .storage
            .collect_tool_messages(owner, Some("a"))
            .unwrap();
        store.storage.begin_tool_state(owner, Some("a")).unwrap();

        let sent = store
            .admin_send(owner, "project", "global", "notice", None)
            .unwrap();
        assert_eq!(sent.recipients, ["alice", "bob"]);
        assert!(sent.message_id.is_some());

        for (session, recipient) in [("a", "alice"), ("b", "bob")] {
            store
                .storage
                .begin_tool_state(owner, Some(session))
                .unwrap();
            let peers = store
                .storage
                .collect_tool_messages(owner, Some(session))
                .unwrap();
            assert_eq!(peers.len(), 1);
            assert_eq!(Some(&peers[0].message_id), sent.message_id.as_ref());
            assert_eq!(peers[0].from, ADMIN_AGENT);
            assert_eq!(peers[0].to, recipient);
            assert_eq!(peers[0].target, "global");
            assert_eq!(peers[0].message, "notice");
        }
    }

    #[test]
    fn implicit_join_fanout_acknowledgement_and_sender_checks() {
        let (_directory, store) = store();
        let owner = "account-a";
        let first = store
            .send(owner, send("project", "global", "hello"), Some("a"))
            .unwrap();
        assert_eq!(first.assigned_agent.as_deref(), Some("alice"));
        assert!(first.recipients.is_empty());
        assert!(first.message_id.is_none());

        let empty_again = store
            .send(owner, send("project", "global", "still alone"), Some("a"))
            .unwrap();
        assert!(empty_again.assigned_agent.is_none());
        assert!(empty_again.recipients.is_empty());
        assert!(empty_again.message_id.is_none());

        let bob_join = store
            .send(owner, send("project", "global", "joined"), Some("b"))
            .unwrap();
        assert_eq!(bob_join.assigned_agent.as_deref(), Some("bob"));
        assert!(bob_join.message_id.is_some());
        let broadcast = store
            .send(owner, send("project", "global", "team"), Some("a"))
            .unwrap();
        assert!(broadcast.assigned_agent.is_none());
        assert_eq!(broadcast.recipients, ["bob"]);
        assert!(broadcast.message_id.is_some());

        store.storage.begin_tool_state(owner, Some("b")).unwrap();
        let offered = store
            .storage
            .collect_tool_messages(owner, Some("b"))
            .unwrap();
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].from, "alice");
        assert_eq!(offered[0].message, "team");
        store.storage.begin_tool_state(owner, Some("b")).unwrap();
        assert!(
            store
                .storage
                .collect_tool_messages(owner, Some("b"))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn automatic_names_exhaust_leave_reuse_and_failed_join_rolls_back() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = AgentPoolStore::open_with_names(
            &directory.path().join("agent-pool.sqlite3"),
            Duration::from_millis(DEFAULT_TTL_MS as u64),
            ["one", "two"].into_iter().map(str::to_owned).collect(),
        )
        .unwrap();

        assert_eq!(
            store
                .send("owner", send("p", "global", "join"), Some("a"))
                .unwrap()
                .assigned_agent
                .as_deref(),
            Some("one")
        );
        assert_eq!(
            store
                .send("owner", send("p", "global", "join"), Some("b"))
                .unwrap()
                .assigned_agent
                .as_deref(),
            Some("two")
        );
        assert!(
            store
                .send("owner", send("p", "global", "join"), Some("c"))
                .unwrap_err()
                .message
                .contains("dictionary is exhausted")
        );

        store.send("owner", leave("p"), Some("a")).unwrap();
        assert_eq!(
            store
                .send("owner", send("p", "global", "join"), Some("c"))
                .unwrap()
                .assigned_agent
                .as_deref(),
            Some("one")
        );

        // A failed first targeted send must not consume an allocated name.
        assert!(
            store
                .send("owner", send("q", "nobody", "fail"), Some("x"))
                .is_err()
        );
        assert_eq!(
            store
                .send("owner", send("q", "global", "join"), Some("y"))
                .unwrap()
                .assigned_agent
                .as_deref(),
            Some("one")
        );
    }

    #[test]
    fn leave_is_pool_scoped_for_a_session() {
        let (_directory, store) = store();
        store
            .send("owner", send("left", "global", "join"), Some("session"))
            .unwrap();
        store
            .send("owner", send("right", "global", "join"), Some("session"))
            .unwrap();

        store.send("owner", leave("left"), Some("session")).unwrap();
        let left = store
            .members(
                "owner",
                PoolMembersArgs {
                    pool: "left".into(),
                    wait_seconds: None,
                },
                Some("session"),
            )
            .unwrap();
        assert!(left.agents.is_empty());
        assert!(left.self_agent.is_none());

        let right = store
            .members(
                "owner",
                PoolMembersArgs {
                    pool: "right".into(),
                    wait_seconds: None,
                },
                Some("session"),
            )
            .unwrap();
        assert_eq!(right.agents, ["alice"]);
        assert_eq!(right.self_agent.as_deref(), Some("alice"));

        // Repeated leave is intentionally idempotent.
        store.send("owner", leave("left"), Some("session")).unwrap();
    }

    #[tokio::test]
    async fn concurrent_turns_do_not_block_tools_or_duplicate_offers() {
        let (_directory, store) = store();
        let owner = "account-a";
        store
            .send(owner, send("project", "global", "join"), Some("a"))
            .unwrap();
        store
            .send(owner, send("project", "global", "join"), Some("b"))
            .unwrap();

        // Consume and acknowledge Bob's join announcement first.
        store.storage.begin_tool_state(owner, Some("a")).unwrap();
        assert_eq!(
            store
                .storage
                .collect_tool_messages(owner, Some("a"))
                .unwrap()
                .len(),
            1
        );
        store.storage.begin_tool_state(owner, Some("a")).unwrap();

        store
            .send(owner, send("project", "alice", "parallel"), Some("b"))
            .unwrap();

        // Both turns can start before either finishes. Only the latest-started
        // turn is allowed to offer the inbox row.
        let first = store.start_tool(owner, Some("a")).await.unwrap().unwrap();
        let second = store.start_tool(owner, Some("a")).await.unwrap().unwrap();
        assert!(
            store
                .finish_tool_turn(owner, Some("a"), Some(&first))
                .await
                .unwrap()
                .is_empty()
        );
        let offered = store
            .finish_tool_turn(owner, Some("a"), Some(&second))
            .await
            .unwrap();
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].message, "parallel");

        let third = store.start_tool(owner, Some("a")).await.unwrap().unwrap();
        assert!(
            store
                .finish_tool_turn(owner, Some("a"), Some(&third))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn cancelled_latest_turn_keeps_messages_for_the_next_turn() {
        let (_directory, store) = store();
        store
            .send("owner", send("p", "global", "join"), Some("a"))
            .unwrap();
        let first = store.start_tool("owner", Some("a")).await.unwrap();
        let cancelled = store.start_tool("owner", Some("a")).await.unwrap();
        store
            .admin_send("owner", "p", "alice", "retain after cancellation", None)
            .unwrap();
        drop(cancelled);
        assert!(
            store
                .finish_tool_turn("owner", Some("a"), first.as_ref())
                .await
                .unwrap()
                .is_empty()
        );
        let next = store.start_tool("owner", Some("a")).await.unwrap();
        let messages = store
            .finish_tool_turn("owner", Some("a"), next.as_ref())
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message, "retain after cancellation");
        let acknowledged = store.start_tool("owner", Some("a")).await.unwrap();
        assert!(
            store
                .finish_tool_turn("owner", Some("a"), acknowledged.as_ref())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn same_session_in_different_accounts_has_independent_turns_and_acks() {
        let (_directory, store) = store();
        for principal in ["one", "two"] {
            store
                .send(principal, send("p", "global", "join"), Some("same-session"))
                .unwrap();
            store
                .admin_send(principal, "p", "alice", principal, None)
                .unwrap();
        }
        let first = store.start_tool("one", Some("same-session")).await.unwrap();
        let second = store.start_tool("two", Some("same-session")).await.unwrap();
        for (principal, turn) in [("one", &first), ("two", &second)] {
            let offered = store
                .finish_tool_turn(principal, Some("same-session"), turn.as_ref())
                .await
                .unwrap();
            assert_eq!(offered.len(), 1);
            assert_eq!(offered[0].message, principal);
        }
        let acknowledged = store.start_tool("one", Some("same-session")).await.unwrap();
        assert!(
            store
                .finish_tool_turn("one", Some("same-session"), acknowledged.as_ref())
                .await
                .unwrap()
                .is_empty()
        );
        // Account one's next call cannot ACK account two's durable offer.
        let retained = store
            .finish_tool_turn("two", Some("same-session"), second.as_ref())
            .await
            .unwrap();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].message, "two");
    }

    #[tokio::test]
    async fn failed_join_send_is_invisible_to_membership_waiters() {
        use std::future::Future;
        use std::task::Poll;

        let (_directory, store) = store();
        let mut waiter = Box::pin(store.wait_members("owner", waiting("p", 35), None));
        std::future::poll_fn(|context| {
            assert!(waiter.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(
            store
                .send("owner", send("p", "missing", "rollback"), Some("failed"))
                .is_err()
        );
        assert!(
            store
                .members("owner", waiting("p", 35), None)
                .unwrap()
                .agents
                .is_empty()
        );
        std::future::poll_fn(|context| {
            assert!(waiter.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let joined = store
            .send("owner", send("p", "global", "commit"), Some("joined"))
            .unwrap();
        assert_eq!(joined.assigned_agent.as_deref(), Some("alice"));
        let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed.agents, ["alice"]);
    }

    #[test]
    fn duplicate_names_restart_durability_and_expiry_cleanup() {
        let (directory, store) = store();
        let path = directory.path().join("agent-pool.sqlite3");
        store
            .send("owner", send("p", "global", "join"), Some("a"))
            .unwrap();
        store
            .send("owner", send("p", "global", "join"), Some("b"))
            .unwrap();
        store
            .send("owner", send("p", "bob", "durable"), Some("a"))
            .unwrap();
        store.storage.begin_tool_state("owner", Some("b")).unwrap();
        assert_eq!(
            store
                .storage
                .collect_tool_messages("owner", Some("b"))
                .unwrap()[0]
                .message,
            "durable"
        );
        // Restart before Bob's next tool call implicitly acknowledges the offer.
        drop(store);

        let restarted = AgentPoolStore::open_with_names(
            &path,
            Duration::from_millis(DEFAULT_TTL_MS as u64),
            ["alice", "bob", "carol", "dave"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        )
        .unwrap();
        restarted
            .storage
            .begin_tool_state("owner", Some("b"))
            .unwrap();
        assert_eq!(
            restarted
                .storage
                .collect_tool_messages("owner", Some("b"))
                .unwrap()[0]
                .message,
            "durable"
        );
        restarted
            .storage
            .begin_tool_state("owner", Some("b"))
            .unwrap();
        assert!(
            restarted
                .storage
                .collect_tool_messages("owner", Some("b"))
                .unwrap()
                .is_empty()
        );
        {
            let connection = restarted.storage.connection().unwrap();
            connection
                .execute(
                    "UPDATE members SET expires=?1 WHERE agent='bob'",
                    [now_ms() - 1],
                )
                .unwrap();
        }
        assert_eq!(
            restarted
                .members(
                    "owner",
                    PoolMembersArgs {
                        pool: "p".into(),
                        wait_seconds: None
                    },
                    Some("a")
                )
                .unwrap()
                .agents,
            ["alice"]
        );
    }

    #[test]
    fn hidden_admin_address_supports_bidirectional_messages_and_termination() {
        let (_directory, store) = store();
        let owner = "account-a";

        let to_admin = store
            .send(
                owner,
                send("project", ADMIN_AGENT, "need human input"),
                Some("alice-session"),
            )
            .unwrap();
        assert_eq!(to_admin.recipients, [ADMIN_AGENT]);
        assert_eq!(to_admin.assigned_agent.as_deref(), Some("alice"));

        let inbox = store.admin_messages(owner, 10).unwrap();
        assert_eq!(inbox.len(), 1);
        assert_eq!(inbox[0].from, "alice");
        assert_eq!(inbox[0].message, "need human input");

        let from_admin = store
            .admin_send(owner, "project", "alice", "continue", None)
            .unwrap();
        assert_eq!(from_admin.recipients, ["alice"]);

        store
            .storage
            .begin_tool_state(owner, Some("alice-session"))
            .unwrap();
        let offered = store
            .storage
            .collect_tool_messages(owner, Some("alice-session"))
            .unwrap();
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].from, ADMIN_AGENT);
        assert_eq!(offered[0].message, "continue");

        let identities = store
            .identities_for_session(owner, Some("alice-session"))
            .unwrap();
        assert_eq!(identities.len(), 1);
        assert_eq!(identities[0].agent, "alice");

        assert!(store.admin_terminate(owner, "project", "alice").unwrap());
        assert!(store.admin_members(owner).unwrap().is_empty());
        assert!(
            store
                .members(
                    owner,
                    PoolMembersArgs {
                        pool: "project".into(),
                        wait_seconds: None,
                    },
                    Some("alice-session"),
                )
                .unwrap()
                .agents
                .is_empty()
        );
    }

    #[test]
    fn database_rejects_symlinks_and_permissive_files() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let path = directory.path().join("pool.sqlite3");
        std::fs::write(&path, []).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(AgentPoolStore::open(&path).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = directory.path().join("pool-link.sqlite3");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(AgentPoolStore::open(&link).is_err());
    }
}
