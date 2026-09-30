//! Durable account-scoped cooperative agent pools.
//!
//! Membership is bound to `_meta["openai/session"]` on the first `pool_send`.
//! Ordinary tool activity refreshes the membership lease and piggybacks queued
//! peer messages. A message offered on one tool call is acknowledged by the
//! same session's next tool call, giving at-least-once delivery without poll or
//! acknowledgement tools.

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rmcp::ErrorData as McpError;
use rmcp::model::ErrorCode;
use rmcp::schemars::JsonSchema;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

const DEFAULT_TTL_MS: i64 = 86_400_000;
const MAX_PENDING: i64 = 10_000;
const MAX_BATCH_COUNT: usize = 32;
const MAX_BATCH_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersArgs {
    pub pool: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolSendArgs {
    /// Pool name within this account.
    pub pool: String,
    /// Member name, or global to send to all other active members.
    pub agent: String,
    /// Message text, up to 64 KiB.
    pub message: String,
    #[serde(default)]
    pub in_reply_to: Option<String>,
    #[serde(default)]
    /// Required on the first send in a pool. Later sends infer the bound sender.
    pub from_agent: Option<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(crate = "rmcp::schemars")]
pub struct PeerMessage {
    pub message_id: String,
    pub pool: String,
    pub from: String,
    pub to: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersResult {
    pub pool: String,
    pub agents: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<PeerMessage>", default)]
    pub peer_messages: Option<Vec<PeerMessage>>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolSendResult {
    pub message_id: String,
    pub recipients: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<PeerMessage>", default)]
    pub peer_messages: Option<Vec<PeerMessage>>,
}

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

pub struct AgentPoolStore {
    connection: Mutex<Connection>,
    session_orders: Mutex<HashMap<SessionKey, SessionOrderWeak>>,
    lease_ttl_ms: i64,
}

impl AgentPoolStore {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        Self::open_with_ttl(path, Duration::from_millis(DEFAULT_TTL_MS as u64))
    }

    pub fn open_with_ttl(path: &Path, lease_ttl: Duration) -> anyhow::Result<Self> {
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

        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS members (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               principal TEXT NOT NULL,
               pool TEXT NOT NULL,
               agent TEXT NOT NULL,
               session TEXT NOT NULL,
               expires INTEGER NOT NULL,
               last_seen INTEGER NOT NULL,
               generation TEXT NOT NULL,
               ack_cursor INTEGER NOT NULL DEFAULT 0,
               offered_cursor INTEGER
             );
             CREATE UNIQUE INDEX IF NOT EXISTS member_name
               ON members(principal,pool,agent);
             CREATE UNIQUE INDEX IF NOT EXISTS member_session
               ON members(principal,pool,session);
             CREATE INDEX IF NOT EXISTS member_session_lookup
               ON members(principal,session,expires);
             CREATE TABLE IF NOT EXISTS messages (
               id TEXT PRIMARY KEY,
               principal TEXT NOT NULL,
               pool TEXT NOT NULL,
               sender TEXT NOT NULL,
               target TEXT NOT NULL,
               body TEXT NOT NULL,
               in_reply_to TEXT,
               created INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS inbox (
               cursor INTEGER PRIMARY KEY AUTOINCREMENT,
               member_id INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
               message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
               UNIQUE(member_id,message_id)
             );
             CREATE INDEX IF NOT EXISTS inbox_member_cursor ON inbox(member_id,cursor);
             CREATE INDEX IF NOT EXISTS inbox_message_id ON inbox(message_id);",
        )?;
        // A process restart cannot know whether an offered response reached its
        // caller. Keep the inbox rows and re-offer them rather than risking
        // at-most-once loss across a crash.
        connection.execute(
            "UPDATE members SET offered_cursor=NULL WHERE offered_cursor IS NOT NULL",
            [],
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
            session_orders: Mutex::new(HashMap::new()),
            lease_ttl_ms,
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, McpError> {
        self.connection.lock().map_err(|_| storage_error())
    }

    /// Start a correlated tool turn. Only the short inbox transition is
    /// serialized; the actual tool operation runs without holding this lock.
    pub async fn start_tool(
        &self,
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
        self.begin_tool_state(principal, Some(session))?;
        drop(state);
        Ok(Some(SessionTurn { sequence, order }))
    }

    /// Finish a correlated tool turn. A call that was superseded by a later
    /// concurrently-started call does not offer inbox rows; the latest turn
    /// will offer them instead.
    pub async fn finish_tool_turn(
        &self,
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
        self.collect_tool_messages(principal, Some(session))
    }

    /// A new tool call acknowledges the previous offer and refreshes all leases
    /// correlated with this session.
    fn begin_tool_state(&self, principal: &str, session: Option<&str>) -> Result<(), McpError> {
        validate_name(principal, "principal")?;
        let Some(session) = session else {
            return Ok(());
        };
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;

        let offered: Vec<(i64, i64)> = {
            let mut statement = transaction
                .prepare(
                    "SELECT id,offered_cursor FROM members
                     WHERE principal=?1 AND session=?2 AND offered_cursor IS NOT NULL",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(params![principal, session], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| storage_error())?
        };
        for (member_id, cursor) in offered {
            transaction
                .execute(
                    "DELETE FROM inbox WHERE member_id=?1 AND cursor<=?2",
                    params![member_id, cursor],
                )
                .map_err(|_| storage_error())?;
            transaction
                .execute(
                    "UPDATE members
                     SET ack_cursor=MAX(ack_cursor,?2),offered_cursor=NULL
                     WHERE id=?1",
                    params![member_id, cursor],
                )
                .map_err(|_| storage_error())?;
        }
        transaction
            .execute(
                "UPDATE members SET expires=?3,last_seen=?4
                 WHERE principal=?1 AND session=?2",
                params![principal, session, now + self.lease_ttl_ms, now],
            )
            .map_err(|_| storage_error())?;
        prune_messages(&transaction)?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(())
    }

    /// Collect a bounded batch for this session and persist the highest cursor
    /// offered per member. Rows are retained until the session's next tool call.
    fn collect_tool_messages(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<PeerMessage>, McpError> {
        validate_name(principal, "principal")?;
        let Some(session) = session else {
            return Ok(Vec::new());
        };
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;

        let rows: Vec<(i64, i64, PeerMessage)> = {
            let mut statement = transaction
                .prepare(
                    "SELECT i.cursor,m.id,msg.id,msg.pool,msg.sender,msg.target,msg.body,msg.in_reply_to
                     FROM inbox i
                     JOIN members m ON m.id=i.member_id
                     JOIN messages msg ON msg.id=i.message_id
                     WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3
                     ORDER BY i.cursor LIMIT ?4",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(
                    params![principal, session, now, MAX_BATCH_COUNT as i64],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            PeerMessage {
                                message_id: row.get(2)?,
                                pool: row.get(3)?,
                                from: row.get(4)?,
                                to: row.get(5)?,
                                message: row.get(6)?,
                                in_reply_to: row.get(7)?,
                            },
                        ))
                    },
                )
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| storage_error())?
        };

        let mut messages = Vec::new();
        let mut offered = HashMap::<i64, i64>::new();
        let mut bytes = 0usize;
        for (cursor, member_id, message) in rows {
            let size = message.message.len()
                + message.message_id.len()
                + message.pool.len()
                + message.from.len()
                + message.to.len()
                + message.in_reply_to.as_ref().map_or(0, String::len)
                + 64;
            if !messages.is_empty() && bytes.saturating_add(size) > MAX_BATCH_BYTES {
                break;
            }
            bytes = bytes.saturating_add(size);
            offered
                .entry(member_id)
                .and_modify(|highest| *highest = (*highest).max(cursor))
                .or_insert(cursor);
            messages.push(message);
        }
        for (member_id, cursor) in offered {
            transaction
                .execute(
                    "UPDATE members SET offered_cursor=CASE
                       WHEN offered_cursor IS NULL OR offered_cursor<?2 THEN ?2
                       ELSE offered_cursor END
                     WHERE id=?1",
                    params![member_id, cursor],
                )
                .map_err(|_| storage_error())?;
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok(messages)
    }

    pub fn members(
        &self,
        principal: &str,
        args: PoolMembersArgs,
    ) -> Result<PoolMembersResult, McpError> {
        validate_name(principal, "principal")?;
        validate_name(&args.pool, "pool")?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let agents = {
            let mut statement = transaction
                .prepare(
                    "SELECT agent FROM members
                     WHERE principal=?1 AND pool=?2 AND expires>?3 ORDER BY agent",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(params![principal, args.pool, now], |row| row.get(0))
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<String>>>()
                .map_err(|_| storage_error())?
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolMembersResult {
            pool: args.pool,
            agents,
            peer_messages: None,
        })
    }

    pub fn send(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(principal, "principal")?;
        validate_name(&args.pool, "pool")?;
        validate_name(&args.agent, "agent")?;
        if args.message.is_empty() || args.message.len() > 65_536 {
            return Err(invalid("message must contain 1 to 65536 bytes"));
        }
        if let Some(reply) = &args.in_reply_to {
            validate_name(reply, "in_reply_to")?;
        }
        if let Some(from_agent) = &args.from_agent {
            validate_name(from_agent, "from_agent")?;
            if from_agent == "global" {
                return Err(invalid("global is reserved as the broadcast target"));
            }
        }
        let session = session.ok_or_else(|| {
            invalid("pool_send requires openai/session metadata to bind the sender")
        })?;
        validate_session(session)?;

        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;

        let bound: Option<(i64, String)> = transaction
            .query_row(
                "SELECT id,agent FROM members
                 WHERE principal=?1 AND pool=?2 AND session=?3",
                params![principal, args.pool, session],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| storage_error())?;

        let (sender_id, sender) = match bound {
            Some((member_id, agent)) => {
                if args
                    .from_agent
                    .as_ref()
                    .is_some_and(|requested| requested != &agent)
                {
                    return Err(invalid("from_agent cannot override the correlated sender"));
                }
                (member_id, agent)
            }
            None => {
                let agent = args
                    .from_agent
                    .as_deref()
                    .ok_or_else(|| invalid("first pool_send in a pool requires from_agent"))?;
                if transaction
                    .query_row(
                        "SELECT 1 FROM members
                         WHERE principal=?1 AND pool=?2 AND agent=?3",
                        params![principal, args.pool, agent],
                        |_| Ok(()),
                    )
                    .optional()
                    .map_err(|_| storage_error())?
                    .is_some()
                {
                    return Err(agent_conflict());
                }
                transaction
                    .execute(
                        "INSERT INTO members(principal,pool,agent,session,expires,last_seen,generation)
                         VALUES(?1,?2,?3,?4,?5,?6,?7)",
                        params![
                            principal,
                            args.pool,
                            agent,
                            session,
                            now + self.lease_ttl_ms,
                            now,
                            Uuid::new_v4().to_string()
                        ],
                    )
                    .map_err(|_| agent_conflict())?;
                (transaction.last_insert_rowid(), agent.to_owned())
            }
        };

        transaction
            .execute(
                "UPDATE members SET expires=?2,last_seen=?3 WHERE id=?1",
                params![sender_id, now + self.lease_ttl_ms, now],
            )
            .map_err(|_| storage_error())?;

        let targets: Vec<(i64, String)> = {
            let mut statement = if args.agent == "global" {
                transaction
                    .prepare(
                        "SELECT id,agent FROM members
                         WHERE principal=?1 AND pool=?2 AND id<>?3 AND expires>?4
                         ORDER BY agent",
                    )
                    .map_err(|_| storage_error())?
            } else {
                transaction
                    .prepare(
                        "SELECT id,agent FROM members
                         WHERE principal=?1 AND pool=?2 AND agent=?3 AND expires>?4
                         ORDER BY agent",
                    )
                    .map_err(|_| storage_error())?
            };
            if args.agent == "global" {
                statement
                    .query_map(params![principal, args.pool, sender_id, now], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|_| storage_error())?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(|_| storage_error())?
            } else {
                statement
                    .query_map(params![principal, args.pool, args.agent, now], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|_| storage_error())?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(|_| storage_error())?
            }
        };
        if args.agent != "global" && targets.is_empty() {
            return Err(invalid("target agent is not an active pool member"));
        }

        let pending: i64 = transaction
            .query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get(0))
            .map_err(|_| storage_error())?;
        if pending + targets.len() as i64 > MAX_PENDING {
            return Err(invalid("agent pool inbox is full; retry later"));
        }

        let message_id = format!("msg_{}", Uuid::new_v4());
        let recipients = targets
            .iter()
            .map(|(_, agent)| agent.clone())
            .collect::<Vec<_>>();
        if !targets.is_empty() {
            transaction
                .execute(
                    "INSERT INTO messages(id,principal,pool,sender,target,body,in_reply_to,created)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        message_id,
                        principal,
                        args.pool,
                        sender,
                        args.agent,
                        args.message,
                        args.in_reply_to,
                        now
                    ],
                )
                .map_err(|_| storage_error())?;
            for (member_id, _) in &targets {
                transaction
                    .execute(
                        "INSERT INTO inbox(member_id,message_id) VALUES(?1,?2)",
                        params![member_id, message_id],
                    )
                    .map_err(|_| storage_error())?;
            }
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolSendResult {
            message_id,
            recipients,
            peer_messages: None,
        })
    }
}

fn cleanup(connection: &Connection, now: i64) -> Result<(), McpError> {
    connection
        .execute("DELETE FROM members WHERE expires<=?1", [now])
        .map_err(|_| storage_error())?;
    prune_messages(connection)
}

fn prune_messages(connection: &Connection) -> Result<(), McpError> {
    connection
        .execute(
            "DELETE FROM messages WHERE NOT EXISTS (
               SELECT 1 FROM inbox WHERE inbox.message_id=messages.id
             )",
            [],
        )
        .map_err(|_| storage_error())?;
    Ok(())
}

fn validate_name(value: &str, field: &str) -> Result<(), McpError> {
    if value.is_empty()
        || value.chars().count() > 128
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

fn agent_conflict() -> McpError {
    McpError::new(
        ErrorCode(-32000),
        "Agent name is already active for another session in this pool",
        Some(json!({"reason":"agent_conflict"})),
    )
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

    fn store() -> (tempfile::TempDir, AgentPoolStore) {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = AgentPoolStore::open(&directory.path().join("agent-pool.sqlite3")).unwrap();
        (directory, store)
    }

    fn send(pool: &str, target: &str, from: Option<&str>, body: &str) -> PoolSendArgs {
        PoolSendArgs {
            pool: pool.into(),
            agent: target.into(),
            message: body.into(),
            in_reply_to: None,
            from_agent: from.map(str::to_owned),
        }
    }

    #[test]
    fn implicit_join_fanout_acknowledgement_and_sender_checks() {
        let (_directory, store) = store();
        let owner = "account-a";
        let first = store
            .send(
                owner,
                send("project", "global", Some("alice"), "hello"),
                Some("a"),
            )
            .unwrap();
        assert!(first.recipients.is_empty());
        store
            .send(
                owner,
                send("project", "global", Some("bob"), "joined"),
                Some("b"),
            )
            .unwrap();
        let broadcast = store
            .send(owner, send("project", "global", None, "team"), Some("a"))
            .unwrap();
        assert_eq!(broadcast.recipients, ["bob"]);
        assert!(
            store
                .send(
                    owner,
                    send("project", "alice", Some("bob"), "forged"),
                    Some("a")
                )
                .is_err()
        );

        store.begin_tool_state(owner, Some("b")).unwrap();
        let offered = store.collect_tool_messages(owner, Some("b")).unwrap();
        assert_eq!(offered.len(), 1);
        assert_eq!(offered[0].from, "alice");
        assert_eq!(offered[0].message, "team");
        store.begin_tool_state(owner, Some("b")).unwrap();
        assert!(
            store
                .collect_tool_messages(owner, Some("b"))
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn concurrent_turns_do_not_block_tools_or_duplicate_offers() {
        let (_directory, store) = store();
        let owner = "account-a";
        store
            .send(
                owner,
                send("project", "global", Some("alice"), "join"),
                Some("a"),
            )
            .unwrap();
        store
            .send(
                owner,
                send("project", "global", Some("bob"), "join"),
                Some("b"),
            )
            .unwrap();

        // Consume and acknowledge Bob's join announcement first.
        store.begin_tool_state(owner, Some("a")).unwrap();
        assert_eq!(
            store.collect_tool_messages(owner, Some("a")).unwrap().len(),
            1
        );
        store.begin_tool_state(owner, Some("a")).unwrap();

        store
            .send(owner, send("project", "alice", None, "parallel"), Some("b"))
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

    #[test]
    fn duplicate_names_restart_durability_and_expiry_cleanup() {
        let (directory, store) = store();
        let path = directory.path().join("agent-pool.sqlite3");
        store
            .send(
                "owner",
                send("p", "global", Some("alice"), "join"),
                Some("a"),
            )
            .unwrap();
        assert!(
            store
                .send(
                    "owner",
                    send("p", "global", Some("alice"), "join"),
                    Some("other")
                )
                .is_err()
        );
        store
            .send("owner", send("p", "global", Some("bob"), "join"), Some("b"))
            .unwrap();
        store
            .send("owner", send("p", "bob", None, "durable"), Some("a"))
            .unwrap();
        store.begin_tool_state("owner", Some("b")).unwrap();
        assert_eq!(
            store.collect_tool_messages("owner", Some("b")).unwrap()[0].message,
            "durable"
        );
        // Restart before Bob's next tool call implicitly acknowledges the offer.
        drop(store);

        let restarted = AgentPoolStore::open(&path).unwrap();
        restarted.begin_tool_state("owner", Some("b")).unwrap();
        assert_eq!(
            restarted.collect_tool_messages("owner", Some("b")).unwrap()[0].message,
            "durable"
        );
        restarted.begin_tool_state("owner", Some("b")).unwrap();
        assert!(
            restarted
                .collect_tool_messages("owner", Some("b"))
                .unwrap()
                .is_empty()
        );
        {
            let connection = restarted.connection().unwrap();
            connection
                .execute(
                    "UPDATE members SET expires=?1 WHERE agent='bob'",
                    [now_ms() - 1],
                )
                .unwrap();
        }
        assert_eq!(
            restarted
                .members("owner", PoolMembersArgs { pool: "p".into() })
                .unwrap()
                .agents,
            ["alice"]
        );
    }

    #[test]
    fn pruning_has_message_lookup_index() {
        let (_directory, store) = store();
        let connection = store.connection().unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='inbox_message_id'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
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
