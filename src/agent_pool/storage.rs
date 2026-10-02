//! Atomic pool persistence: membership, message fanout, inbox offers and ACKs.
//! This component never awaits or owns notification subscriptions.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rmcp::ErrorData as McpError;
use rmcp::model::ErrorCode;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;
use uuid::Uuid;

use super::{
    ADMIN_AGENT, AdminMember, AdminMessage, AgentIdentity, MAX_NAME_CHARS, PeerMessage, PoolAction,
    PoolMembersArgs, PoolMembersResult, PoolSendArgs, PoolSendResult, invalid, now_ms,
    storage_error, validate_name, validate_session,
};

const MAX_PENDING: i64 = 10_000;
const MAX_BATCH_COUNT: usize = 32;
const MAX_BATCH_BYTES: usize = 128 * 1024;
const MAX_MESSAGE_BYTES: usize = 65_536;
const MAX_ADMIN_MESSAGES: i64 = 1_000;

pub(super) struct PoolStorage {
    connection: Mutex<Connection>,
    lease_ttl_ms: i64,
    agent_names: Arc<[String]>,
}

pub(super) struct MembersState {
    pub(super) result: PoolMembersResult,
    pub(super) member_ids: Vec<i64>,
    pub(super) next_expiry_ms: Option<i64>,
    pub(super) pending: bool,
}

struct MemberMessage<'a> {
    principal: &'a str,
    pool: &'a str,
    sender: &'a str,
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
            lease_ttl_ms,
            agent_names: agent_names.into(),
        })
    }

    pub(super) fn connection(&self) -> Result<MutexGuard<'_, Connection>, McpError> {
        self.connection.lock().map_err(|_| storage_error())
    }

    /// A new tool call acknowledges the previous offer and refreshes all leases
    /// correlated with this session.
    pub(super) fn begin_tool_state(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<(), McpError> {
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
    pub(super) fn collect_tool_messages(
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
                    "SELECT i.cursor,m.id,msg.id,msg.pool,msg.sender,m.agent,msg.body,msg.in_reply_to,msg.target,msg.created
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
                                target: row.get(8)?,
                                sent_at_ms: row.get(9)?,
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
                + message.target.len()
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

    pub(super) fn identities_for_session(
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

    pub(super) fn admin_members(&self, principal: &str) -> Result<Vec<AdminMember>, McpError> {
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

    pub(super) fn admin_messages(
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

    pub(super) fn admin_send(
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
        let targets =
            Self::resolve_member_targets(&transaction, principal, pool, target, None, now)?;
        let (message_id, recipients) = Self::queue_member_message(
            &transaction,
            MemberMessage {
                principal,
                pool,
                sender: ADMIN_AGENT,
                target,
                body: message,
                in_reply_to,
                created_ms: now,
            },
            &targets,
        )?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolSendResult {
            assigned_agent: None,
            message_id,
            recipients,
            peer_messages: None,
        })
    }

    fn resolve_member_targets(
        transaction: &rusqlite::Transaction<'_>,
        principal: &str,
        pool: &str,
        target: &str,
        exclude_global_member_id: Option<i64>,
        now: i64,
    ) -> Result<Vec<(i64, String)>, McpError> {
        let targets: Vec<(i64, String)> = if target == "global" {
            let mut statement = transaction
                .prepare(
                    "SELECT id,agent FROM members
                     WHERE principal=?1 AND pool=?2
                       AND (?3 IS NULL OR id<>?3) AND expires>?4
                     ORDER BY agent",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(
                    params![principal, pool, exclude_global_member_id, now],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| storage_error())?
        } else {
            let mut statement = transaction
                .prepare(
                    "SELECT id,agent FROM members
                     WHERE principal=?1 AND pool=?2 AND agent=?3 AND expires>?4
                     ORDER BY agent",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(params![principal, pool, target, now], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| storage_error())?
        };
        if target != "global" && targets.is_empty() {
            return Err(invalid("target agent is not an active pool member"));
        }
        Ok(targets)
    }

    fn queue_member_message(
        transaction: &rusqlite::Transaction<'_>,
        message: MemberMessage<'_>,
        targets: &[(i64, String)],
    ) -> Result<(Option<String>, Vec<String>), McpError> {
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
                        message.principal,
                        message.pool,
                        message.sender,
                        message.target,
                        message.body,
                        message.in_reply_to,
                        message.created_ms
                    ],
                )
                .map_err(|_| storage_error())?;
            for (member_id, _) in targets {
                transaction
                    .execute(
                        "INSERT INTO inbox(member_id,message_id) VALUES(?1,?2)",
                        params![member_id, message_id],
                    )
                    .map_err(|_| storage_error())?;
            }
            Some(message_id)
        };
        Ok((message_id, recipients))
    }

    pub(super) fn admin_terminate(
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

    pub(super) fn members_state(
        &self,
        principal: &str,
        args: &PoolMembersArgs,
        session: Option<&str>,
    ) -> Result<MembersState, McpError> {
        validate_name(principal, "principal")?;
        validate_name(&args.pool, "pool")?;
        if let Some(session) = session {
            validate_session(session)?;
        }
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let members = {
            let mut statement = transaction
                .prepare(
                    "SELECT id,agent,expires FROM members
                     WHERE principal=?1 AND pool=?2 AND expires>?3 ORDER BY agent",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(params![principal, args.pool, now], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<_>>>()
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
        let pending = match session {
            Some(session) => transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM inbox i JOIN members m ON m.id=i.member_id
                 WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3)",
                    params![principal, session, now],
                    |row| row.get(0),
                )
                .map_err(|_| storage_error())?,
            None => false,
        };
        transaction.commit().map_err(|_| storage_error())?;
        Ok(MembersState {
            result: PoolMembersResult {
                agents: members.iter().map(|(_, agent, _)| agent.clone()).collect(),
                self_agent,
                peer_messages: None,
            },
            member_ids: members.iter().map(|(id, _, _)| *id).collect(),
            next_expiry_ms: members.iter().map(|(_, _, expires)| *expires).min(),
            pending,
        })
    }

    pub(super) fn send(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(&args.pool, "pool")?;
        match args.action {
            Some(PoolAction::Leave) => {
                if args.target.is_some() || args.message.is_some() || args.in_reply_to.is_some() {
                    return Err(invalid("action=leave accepts only pool and action"));
                }
                self.leave_pool(principal, &args.pool, session)
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

    fn leave_pool(
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

        let targets = Self::resolve_member_targets(
            &transaction,
            principal,
            pool,
            &target,
            Some(sender_id),
            now,
        )?;
        let (message_id, recipients) = Self::queue_member_message(
            &transaction,
            MemberMessage {
                principal,
                pool,
                sender: &sender,
                target: &target,
                body: &message,
                in_reply_to: in_reply_to.as_deref(),
                created_ms: now,
            },
            &targets,
        )?;
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

fn agent_conflict() -> McpError {
    McpError::new(
        ErrorCode(-32000),
        "Agent name is already active for another session in this pool",
        Some(json!({"reason":"agent_conflict"})),
    )
}
