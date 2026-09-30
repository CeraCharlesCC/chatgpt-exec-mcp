//! Durable account-scoped cooperative agent pools.
//!
//! Membership is bound to `_meta["openai/session"]` on the first `pool_send`.
//! Agent names are allocated automatically from the configured dictionary.
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
const MAX_NAME_CHARS: usize = 128;
const MAX_MESSAGE_BYTES: usize = 65_536;
const ADMIN_AGENT: &str = "admin";
const MAX_ADMIN_MESSAGES: i64 = 1_000;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersArgs {
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars", transform = pool_send_schema)]
pub struct PoolSendArgs {
    /// Pool name within this account.
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
    /// Active member name, or global for all other active members.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "String", length(min = 1, max = MAX_NAME_CHARS), skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Message text, 1 to 65,536 UTF-8 bytes.
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
    /// Omit for a normal send. Set to exit to remove this session from the pool.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_operation",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PoolExitOperation", skip_serializing_if = "Option::is_none")]
    pub operation: Option<PoolExitOperation>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "rmcp::schemars")]
pub enum PoolExitOperation {
    Exit,
}

fn pool_send_schema(schema: &mut rmcp::schemars::Schema) {
    schema.insert(
        "oneOf".to_owned(),
        json!([
            {
                "required": ["target", "message"],
                "not": {"required": ["operation"]}
            },
            {
                "required": ["operation"],
                "not": {
                    "anyOf": [
                        {"required": ["target"]},
                        {"required": ["message"]},
                        {"required": ["in_reply_to"]}
                    ]
                }
            }
        ]),
    );
}

#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
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
    /// This session's allocated name in the pool, when joined.
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
    /// Name allocated on this session's first send to the pool.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub assigned_agent: Option<String>,
    /// Message id when at least one delivery was queued; omitted otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub message_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
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

fn deserialize_optional_non_null_operation<'de, D>(
    deserializer: D,
) -> Result<Option<PoolExitOperation>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<PoolExitOperation>::deserialize(deserializer)?.map_or_else(
        || {
            Err(serde::de::Error::custom(
                "null is not allowed; omit the field instead",
            ))
        },
        |value| Ok(Some(value)),
    )
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
    agent_names: Arc<[String]>,
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
             CREATE INDEX IF NOT EXISTS inbox_message_id ON inbox(message_id);
             CREATE TABLE IF NOT EXISTS admin_messages (
               id TEXT PRIMARY KEY,
               principal TEXT NOT NULL,
               pool TEXT NOT NULL,
               sender TEXT NOT NULL,
               body TEXT NOT NULL,
               in_reply_to TEXT,
               created INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS admin_messages_principal_created
               ON admin_messages(principal,created);",
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
            agent_names: agent_names.into(),
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

    pub fn identities_for_session(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<AgentIdentity>, McpError> {
        validate_name(principal, "principal")?;
        let Some(session) = session else {
            return Ok(Vec::new());
        };
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let identities = {
            let mut statement = transaction
                .prepare(
                    "SELECT pool,agent FROM members
                     WHERE principal=?1 AND session=?2 AND expires>?3
                     ORDER BY pool,agent",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(params![principal, session, now], |row| {
                    Ok(AgentIdentity {
                        pool: row.get(0)?,
                        agent: row.get(1)?,
                    })
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|_| storage_error())?
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(identities)
    }

    pub fn admin_members(&self, principal: &str) -> Result<Vec<AdminMember>, McpError> {
        validate_name(principal, "principal")?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let members = {
            let mut statement = transaction
                .prepare(
                    "SELECT pool,agent,session,last_seen,expires FROM members
                     WHERE principal=?1 AND expires>?2 ORDER BY pool,agent",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(params![principal, now], |row| {
                    Ok(AdminMember {
                        pool: row.get(0)?,
                        agent: row.get(1)?,
                        session: row.get(2)?,
                        last_seen_ms: row.get(3)?,
                        expires_ms: row.get(4)?,
                    })
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|_| storage_error())?
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(members)
    }

    pub fn admin_messages(
        &self,
        principal: &str,
        limit: usize,
    ) -> Result<Vec<AdminMessage>, McpError> {
        validate_name(principal, "principal")?;
        let connection = self.connection()?;
        let mut statement = connection
            .prepare(
                "SELECT id,pool,sender,body,in_reply_to,created FROM admin_messages
                 WHERE principal=?1 ORDER BY created DESC,id DESC LIMIT ?2",
            )
            .map_err(|_| storage_error())?;
        statement
            .query_map(params![principal, limit.min(500) as i64], |row| {
                Ok(AdminMessage {
                    message_id: row.get(0)?,
                    pool: row.get(1)?,
                    from: row.get(2)?,
                    message: row.get(3)?,
                    in_reply_to: row.get(4)?,
                    created_ms: row.get(5)?,
                })
            })
            .map_err(|_| storage_error())?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|_| storage_error())
    }

    pub fn admin_send(
        &self,
        principal: &str,
        pool: &str,
        target: &str,
        message: &str,
        in_reply_to: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(principal, "principal")?;
        validate_name(pool, "pool")?;
        validate_name(target, "target")?;
        if target == ADMIN_AGENT {
            return Err(invalid("admin cannot target itself"));
        }
        if message.is_empty() || message.len() > MAX_MESSAGE_BYTES {
            return Err(invalid("message must contain 1 to 65536 bytes"));
        }
        if let Some(reply) = in_reply_to {
            validate_name(reply, "in_reply_to")?;
        }
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let targets: Vec<(i64, String)> = {
            let mut statement = if target == "global" {
                transaction
                    .prepare(
                        "SELECT id,agent FROM members
                         WHERE principal=?1 AND pool=?2 AND expires>?3 ORDER BY agent",
                    )
                    .map_err(|_| storage_error())?
            } else {
                transaction
                    .prepare(
                        "SELECT id,agent FROM members
                         WHERE principal=?1 AND pool=?2 AND agent=?3 AND expires>?4 ORDER BY agent",
                    )
                    .map_err(|_| storage_error())?
            };
            if target == "global" {
                statement
                    .query_map(params![principal, pool, now], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|_| storage_error())?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(|_| storage_error())?
            } else {
                statement
                    .query_map(params![principal, pool, target, now], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|_| storage_error())?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(|_| storage_error())?
            }
        };
        if target != "global" && targets.is_empty() {
            return Err(invalid("target agent is not an active pool member"));
        }
        let pending: i64 = transaction
            .query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get(0))
            .map_err(|_| storage_error())?;
        if pending + targets.len() as i64 > MAX_PENDING {
            return Err(invalid("agent pool inbox is full; retry later"));
        }
        let recipients = targets
            .iter()
            .map(|(_, agent)| agent.clone())
            .collect::<Vec<_>>();
        let message_id = if targets.is_empty() {
            None
        } else {
            let message_id = format!("msg_{}", Uuid::new_v4());
            transaction
                .execute(
                    "INSERT INTO messages(id,principal,pool,sender,target,body,in_reply_to,created)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        message_id,
                        principal,
                        pool,
                        ADMIN_AGENT,
                        target,
                        message,
                        in_reply_to,
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
            Some(message_id)
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolSendResult {
            assigned_agent: None,
            message_id,
            recipients,
            peer_messages: None,
        })
    }

    pub fn admin_terminate(
        &self,
        principal: &str,
        pool: &str,
        agent: &str,
    ) -> Result<bool, McpError> {
        validate_name(principal, "principal")?;
        validate_name(pool, "pool")?;
        validate_name(agent, "agent")?;
        if agent == ADMIN_AGENT {
            return Err(invalid("agent name is reserved"));
        }
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let deleted = transaction
            .execute(
                "DELETE FROM members WHERE principal=?1 AND pool=?2 AND agent=?3",
                params![principal, pool, agent],
            )
            .map_err(|_| storage_error())?;
        prune_messages(&transaction)?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(deleted > 0)
    }

    pub fn members(
        &self,
        principal: &str,
        args: PoolMembersArgs,
        session: Option<&str>,
    ) -> Result<PoolMembersResult, McpError> {
        validate_name(principal, "principal")?;
        validate_name(&args.pool, "pool")?;
        if let Some(session) = session {
            validate_session(session)?;
        }
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
        let self_agent = match session {
            Some(session) => transaction
                .query_row(
                    "SELECT agent FROM members
                     WHERE principal=?1 AND pool=?2 AND session=?3 AND expires>?4",
                    params![principal, args.pool, session, now],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|_| storage_error())?,
            None => None,
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolMembersResult {
            agents,
            self_agent,
            peer_messages: None,
        })
    }

    pub fn send(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(&args.pool, "pool")?;
        match args.operation {
            Some(PoolExitOperation::Exit) => {
                if args.target.is_some() || args.message.is_some() || args.in_reply_to.is_some() {
                    return Err(invalid("operation=exit accepts only pool and operation"));
                }
                self.exit_pool(principal, &args.pool, session)
            }
            None => {
                let target = args
                    .target
                    .ok_or_else(|| invalid("pool_send requires target for send"))?;
                let message = args
                    .message
                    .ok_or_else(|| invalid("pool_send requires message for send"))?;
                self.send_message(
                    principal,
                    &args.pool,
                    target,
                    message,
                    args.in_reply_to,
                    session,
                )
            }
        }
    }

    fn exit_pool(
        &self,
        principal: &str,
        pool: &str,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(principal, "principal")?;
        let session = session.ok_or_else(|| {
            invalid("pool_send requires openai/session metadata to identify the sender")
        })?;
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        transaction
            .execute(
                "DELETE FROM members WHERE principal=?1 AND pool=?2 AND session=?3",
                params![principal, pool, session],
            )
            .map_err(|_| storage_error())?;
        prune_messages(&transaction)?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolSendResult::default())
    }

    fn send_message(
        &self,
        principal: &str,
        pool: &str,
        target: String,
        message: String,
        in_reply_to: Option<String>,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(principal, "principal")?;
        validate_name(&target, "target")?;
        if message.is_empty() || message.len() > MAX_MESSAGE_BYTES {
            return Err(invalid("message must contain 1 to 65536 bytes"));
        }
        if let Some(reply) = &in_reply_to {
            validate_name(reply, "in_reply_to")?;
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
                params![principal, pool, session],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| storage_error())?;

        let (sender_id, sender, assigned_agent) = match bound {
            Some((member_id, agent)) => (member_id, agent, None),
            None => {
                let agent = self.allocate_agent_name(&transaction, principal, pool)?;
                transaction
                    .execute(
                        "INSERT INTO members(principal,pool,agent,session,expires,last_seen,generation)
                         VALUES(?1,?2,?3,?4,?5,?6,?7)",
                        params![
                            principal,
                            pool,
                            &agent,
                            session,
                            now + self.lease_ttl_ms,
                            now,
                            Uuid::new_v4().to_string()
                        ],
                    )
                    .map_err(|_| agent_conflict())?;
                (transaction.last_insert_rowid(), agent.clone(), Some(agent))
            }
        };

        transaction
            .execute(
                "UPDATE members SET expires=?2,last_seen=?3 WHERE id=?1",
                params![sender_id, now + self.lease_ttl_ms, now],
            )
            .map_err(|_| storage_error())?;

        if target == ADMIN_AGENT {
            let message_id = format!("msg_{}", Uuid::new_v4());
            transaction
                .execute(
                    "INSERT INTO admin_messages(id,principal,pool,sender,body,in_reply_to,created)
                     VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        message_id,
                        principal,
                        pool,
                        sender,
                        message,
                        in_reply_to,
                        now
                    ],
                )
                .map_err(|_| storage_error())?;
            transaction
                .execute(
                    "DELETE FROM admin_messages WHERE principal=?1 AND rowid NOT IN (
                       SELECT rowid FROM admin_messages WHERE principal=?1
                       ORDER BY created DESC,rowid DESC LIMIT ?2
                     )",
                    params![principal, MAX_ADMIN_MESSAGES],
                )
                .map_err(|_| storage_error())?;
            transaction.commit().map_err(|_| storage_error())?;
            return Ok(PoolSendResult {
                assigned_agent,
                message_id: Some(message_id),
                recipients: vec![ADMIN_AGENT.to_owned()],
                peer_messages: None,
            });
        }

        let targets: Vec<(i64, String)> = {
            let mut statement = if target == "global" {
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
            if target == "global" {
                statement
                    .query_map(params![principal, pool, sender_id, now], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|_| storage_error())?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(|_| storage_error())?
            } else {
                statement
                    .query_map(params![principal, pool, target, now], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })
                    .map_err(|_| storage_error())?
                    .collect::<rusqlite::Result<_>>()
                    .map_err(|_| storage_error())?
            }
        };
        if target != "global" && targets.is_empty() {
            return Err(invalid("target agent is not an active pool member"));
        }

        let pending: i64 = transaction
            .query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get(0))
            .map_err(|_| storage_error())?;
        if pending + targets.len() as i64 > MAX_PENDING {
            return Err(invalid("agent pool inbox is full; retry later"));
        }

        let recipients = targets
            .iter()
            .map(|(_, agent)| agent.clone())
            .collect::<Vec<_>>();
        let message_id = if targets.is_empty() {
            None
        } else {
            let message_id = format!("msg_{}", Uuid::new_v4());
            transaction
                .execute(
                    "INSERT INTO messages(id,principal,pool,sender,target,body,in_reply_to,created)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        message_id,
                        principal,
                        pool,
                        sender,
                        target,
                        message,
                        in_reply_to,
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
            Some(message_id)
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolSendResult {
            assigned_agent,
            message_id,
            recipients,
            peer_messages: None,
        })
    }

    fn allocate_agent_name(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        principal: &str,
        pool: &str,
    ) -> Result<String, McpError> {
        for candidate in self.agent_names.iter() {
            let occupied = transaction
                .query_row(
                    "SELECT 1 FROM members WHERE principal=?1 AND pool=?2 AND agent=?3",
                    params![principal, pool, candidate],
                    |_| Ok(()),
                )
                .optional()
                .map_err(|_| storage_error())?
                .is_some();
            if !occupied {
                return Ok(candidate.clone());
            }
        }
        Err(invalid("agent name dictionary is exhausted for this pool"))
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
            operation: None,
        }
    }

    fn exit(pool: &str) -> PoolSendArgs {
        PoolSendArgs {
            pool: pool.into(),
            target: None,
            message: None,
            in_reply_to: None,
            operation: Some(PoolExitOperation::Exit),
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

    #[test]
    fn automatic_names_exhaust_exit_reuse_and_failed_join_rolls_back() {
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

        store.send("owner", exit("p"), Some("a")).unwrap();
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
    fn exit_is_pool_scoped_for_a_session() {
        let (_directory, store) = store();
        store
            .send("owner", send("left", "global", "join"), Some("session"))
            .unwrap();
        store
            .send("owner", send("right", "global", "join"), Some("session"))
            .unwrap();

        store.send("owner", exit("left"), Some("session")).unwrap();
        let left = store
            .members(
                "owner",
                PoolMembersArgs {
                    pool: "left".into(),
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
                },
                Some("session"),
            )
            .unwrap();
        assert_eq!(right.agents, ["alice"]);
        assert_eq!(right.self_agent.as_deref(), Some("alice"));

        // Repeated exit is intentionally idempotent.
        store.send("owner", exit("left"), Some("session")).unwrap();
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
        store.begin_tool_state(owner, Some("a")).unwrap();
        assert_eq!(
            store.collect_tool_messages(owner, Some("a")).unwrap().len(),
            1
        );
        store.begin_tool_state(owner, Some("a")).unwrap();

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
        store.begin_tool_state("owner", Some("b")).unwrap();
        assert_eq!(
            store.collect_tool_messages("owner", Some("b")).unwrap()[0].message,
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
                .members("owner", PoolMembersArgs { pool: "p".into() }, Some("a"))
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
            .begin_tool_state(owner, Some("alice-session"))
            .unwrap();
        let offered = store
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
