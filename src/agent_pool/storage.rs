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

use crate::config::AgentNameAllocation;

use super::{
    ADMIN_AGENT, AdminMember, AdminMessage, AdminMessagesPage, AgentIdentity, Communications,
    DeliveryReceipt, MAX_NAME_CHARS, PeerMessage, PoolAction, PoolMembersArgs, PoolMembersResult,
    PoolSendArgs, PoolSendResult, ReceiptStatus, invalid, now_ms, storage_error, validate_name,
    validate_session,
};

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
        let had_orphan_trigger: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master
              WHERE type='trigger' AND name='inbox_reclaim_message')",
            [],
            |row| row.get(0),
        )?;
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
               offered_cursor INTEGER
             );
             CREATE UNIQUE INDEX IF NOT EXISTS member_name
               ON members(principal,pool,agent);
             CREATE UNIQUE INDEX IF NOT EXISTS member_session
               ON members(principal,pool,session);
             CREATE INDEX IF NOT EXISTS member_session_lookup
               ON members(principal,session,expires);
             CREATE INDEX IF NOT EXISTS member_expiry ON members(expires);
             CREATE INDEX IF NOT EXISTS member_pool_expiry
               ON members(principal,pool,expires);
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
               created INTEGER NOT NULL,
               acknowledged INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS admin_messages_principal_created
               ON admin_messages(principal,created);",
        )?;
        let transaction = connection.transaction()?;
        // A message is durable until its last recipient retires the inbox row.
        // This trigger also marks completion of the one-time orphan cleanup
        // below, so its creation must roll back if any startup migration fails.
        transaction.execute_batch(
            "CREATE TRIGGER IF NOT EXISTS inbox_reclaim_message AFTER DELETE ON inbox BEGIN
               DELETE FROM messages WHERE id=OLD.message_id AND NOT EXISTS (
                 SELECT 1 FROM inbox WHERE message_id=OLD.message_id
               );
             END;",
        )?;
        // Historical generation values were never read; ACKs retire inbox
        // rows using offered_cursor, without reading the old ack_cursor. Use
        // SQLite's native DROP COLUMN so IDs, AUTOINCREMENT, foreign keys,
        // indexes and triggers stay intact. Column checks also accept earlier
        // partial migrations. Unknown schema dependencies fail and roll back
        // this entire transaction, including both drops and restart resets.
        for column in ["generation", "ack_cursor"] {
            let exists: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('members') WHERE name=?1)",
                [column],
                |row| row.get(0),
            )?;
            if exists {
                transaction
                    .execute_batch(&format!("ALTER TABLE members DROP COLUMN {column}"))
                    .map_err(|error| {
                        anyhow::anyhow!("unable to migrate obsolete members.{column}: {error}")
                    })?;
            }
        }
        let has_acknowledged: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('admin_messages')
             WHERE name='acknowledged')",
            [],
            |row| row.get(0),
        )?;
        if !has_acknowledged {
            transaction.execute_batch(
                "ALTER TABLE admin_messages ADD COLUMN acknowledged INTEGER NOT NULL DEFAULT 0",
            )?;
        }
        // Additive migration: legacy queued messages have no trustworthy sender
        // membership identity. Leave it NULL rather than notifying a reused name.
        for (table, column, definition) in [
            ("members", "receipt_offered_cursor", "INTEGER"),
            (
                "messages",
                "sender_member_id",
                "INTEGER REFERENCES members(id) ON DELETE SET NULL",
            ),
            ("inbox", "offered_at", "INTEGER"),
            (
                "admin_messages",
                "sender_member_id",
                "INTEGER REFERENCES members(id) ON DELETE SET NULL",
            ),
            ("admin_messages", "offered_at", "INTEGER"),
        ] {
            let exists: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?1) WHERE name=?2)",
                params![table, column],
                |row| row.get(0),
            )?;
            if !exists {
                transaction.execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN {column} {definition}"
                ))?;
            }
        }
        transaction.execute_batch(
            "CREATE TABLE IF NOT EXISTS delivery_receipts (
               cursor INTEGER PRIMARY KEY AUTOINCREMENT,
               member_id INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
               message_id TEXT NOT NULL,
               pool TEXT NOT NULL,
               recipient TEXT NOT NULL,
               recipient_member_id INTEGER NOT NULL,
               offered_at INTEGER NOT NULL,
               UNIQUE(member_id,message_id,recipient_member_id)
             );
             CREATE INDEX IF NOT EXISTS receipt_member_cursor
               ON delivery_receipts(member_id,cursor);
             CREATE TABLE IF NOT EXISTS pool_revisions (
               principal TEXT NOT NULL, pool TEXT NOT NULL, revision INTEGER NOT NULL,
               PRIMARY KEY(principal,pool)
             );
             CREATE TRIGGER IF NOT EXISTS member_join_revision AFTER INSERT ON members BEGIN
               INSERT INTO pool_revisions(principal,pool,revision) VALUES(NEW.principal,NEW.pool,1)
               ON CONFLICT(principal,pool) DO UPDATE SET revision=revision+1;
             END;
             CREATE TRIGGER IF NOT EXISTS member_leave_revision AFTER DELETE ON members BEGIN
               INSERT INTO pool_revisions(principal,pool,revision) VALUES(OLD.principal,OLD.pool,1)
               ON CONFLICT(principal,pool) DO UPDATE SET revision=revision+1;
             END;",
        )?;
        transaction.execute("UPDATE members SET receipt_offered_cursor=NULL", [])?;
        // ACK only marks an admin message during this run; discard it on restart.
        transaction.execute("DELETE FROM admin_messages WHERE acknowledged=1", [])?;
        // A process restart cannot know whether an offered response reached its
        // caller. Keep the inbox rows and re-offer them rather than risking
        // at-most-once loss across a crash.
        transaction.execute(
            "UPDATE members SET offered_cursor=NULL WHERE offered_cursor IS NOT NULL",
            [],
        )?;
        // One-time migration cleanup for orphan rows left by an older binary.
        // Subsequent inbox changes reclaim messages incrementally via trigger,
        // so reopening an already migrated DB must not rescan every message.
        if !had_orphan_trigger {
            prune_messages(&transaction)
                .map_err(|_| anyhow::anyhow!("unable to prune orphan messages"))?;
        }
        transaction.commit()?;
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

    /// A non-overlapping tool call retires previous offers and refreshes leases
    /// correlated with this session.
    pub(super) fn begin_tool_state(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<(), McpError> {
        self.tool_state(principal, session, true)
    }

    pub(super) fn refresh_tool_state(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<(), McpError> {
        self.tool_state(principal, session, false)
    }

    fn tool_state(
        &self,
        principal: &str,
        session: Option<&str>,
        retire: bool,
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

        if retire {
            let offered: Vec<(i64, i64)> = {
                let mut statement = transaction
                    .prepare(
                        "SELECT id,offered_cursor FROM members
                     WHERE principal=?1 AND session=?2 AND offered_cursor IS NOT NULL
                       AND expires>?3",
                    )
                    .map_err(|_| storage_error())?;
                statement
                    .query_map(params![principal, session, now], |row| {
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
                        "UPDATE members SET offered_cursor=NULL WHERE id=?1",
                        [member_id],
                    )
                    .map_err(|_| storage_error())?;
            }
            transaction
                .execute(
                    "DELETE FROM delivery_receipts WHERE EXISTS (
                   SELECT 1 FROM members m WHERE m.id=delivery_receipts.member_id
                   AND m.principal=?1 AND m.session=?2
                   AND delivery_receipts.cursor<=m.receipt_offered_cursor)",
                    params![principal, session],
                )
                .map_err(|_| storage_error())?;
            transaction.execute(
                "UPDATE members SET receipt_offered_cursor=NULL WHERE principal=?1 AND session=?2",
                params![principal, session],
            ).map_err(|_| storage_error())?;
        }
        transaction
            .execute(
                "UPDATE members SET expires=?3,last_seen=?4
                 WHERE principal=?1 AND session=?2 AND expires>?4",
                params![principal, session, now + self.lease_ttl_ms, now],
            )
            .map_err(|_| storage_error())?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(())
    }

    /// Collect a bounded batch for this session and persist the highest cursor
    /// offered per member. Rows are retained until the session's next tool call.
    #[cfg(test)]
    pub(super) fn collect_tool_messages(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<PeerMessage>, McpError> {
        Ok(self.collect_tool_comms(principal, session, None)?.messages)
    }

    pub(super) fn collect_tool_comms(
        &self,
        principal: &str,
        session: Option<&str>,
        pool: Option<&str>,
    ) -> Result<Communications, McpError> {
        validate_name(principal, "principal")?;
        let Some(session) = session else {
            return Ok(Communications::default());
        };
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;

        let rows: Vec<(i64, i64, PeerMessage)> = {
            let mut statement = transaction
                .prepare(
                    "SELECT i.cursor,m.id,msg.id,msg.pool,msg.sender,m.agent,msg.body,msg.in_reply_to,msg.target,msg.created
                     FROM inbox i
                     JOIN members m ON m.id=i.member_id
                     JOIN messages msg ON msg.id=i.message_id
                     WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3
                        AND (?4 IS NULL OR msg.pool=?4)
                      ORDER BY i.cursor LIMIT ?5",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(
                    params![principal, session, now, pool, MAX_BATCH_COUNT as i64],
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
            // A receipt describes a first offer, never a read or acceptance.
            // Capture the original sender membership at enqueue time.
            transaction
                .execute(
                    "INSERT OR IGNORE INTO delivery_receipts
                   (member_id,message_id,pool,recipient,recipient_member_id,offered_at)
                 SELECT sender.id,msg.id,msg.pool,?3,?4,?5
                 FROM inbox i JOIN messages msg ON msg.id=i.message_id
                 JOIN members sender ON sender.id=msg.sender_member_id
                 WHERE i.cursor=?1 AND i.member_id=?4 AND i.offered_at IS NULL
                   AND msg.target<>'global' AND sender.expires>?5
                   AND sender.principal=?2",
                    params![cursor, principal, message.to, member_id, now],
                )
                .map_err(|_| storage_error())?;
            transaction
                .execute(
                    "UPDATE inbox SET offered_at=COALESCE(offered_at,?2) WHERE cursor=?1",
                    params![cursor, now],
                )
                .map_err(|_| storage_error())?;
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
        let receipts = {
            let mut statement = transaction
                .prepare(
                    "SELECT r.cursor,r.member_id,r.message_id,r.pool,r.recipient,r.offered_at
                 FROM delivery_receipts r JOIN members m ON m.id=r.member_id
                 WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3
                   AND (?4 IS NULL OR r.pool=?4)
                 ORDER BY r.cursor LIMIT ?5",
                )
                .map_err(|_| storage_error())?;
            statement
                .query_map(
                    params![principal, session, now, pool, MAX_BATCH_COUNT as i64],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            DeliveryReceipt {
                                message_id: row.get(2)?,
                                pool: row.get(3)?,
                                to: row.get(4)?,
                                status: ReceiptStatus::Offered,
                                offered_at_ms: row.get(5)?,
                            },
                        ))
                    },
                )
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|_| storage_error())?
        };
        for (cursor, member_id, _) in &receipts {
            transaction.execute(
                "UPDATE members SET receipt_offered_cursor=MAX(COALESCE(receipt_offered_cursor,0),?2)
                 WHERE id=?1", params![member_id, cursor],
            ).map_err(|_| storage_error())?;
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok(Communications {
            messages,
            delivery_receipts: receipts
                .into_iter()
                .map(|(_, _, receipt)| receipt)
                .collect(),
        })
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

    pub(super) fn admin_messages_page(
        &self,
        principal: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(AdminMessagesPage, bool), McpError> {
        validate_name(principal, "principal")?;
        // Cursor is the (timestamp, ID) of the last displayed message. IDs
        // break ties for messages created within the same millisecond.
        let after = cursor
            .map(|value| {
                let (created, id) = value
                    .split_once(':')
                    .ok_or_else(|| invalid("invalid admin message cursor"))?;
                let created = created
                    .parse::<i64>()
                    .map_err(|_| invalid("invalid admin message cursor"))?;
                Uuid::parse_str(
                    id.strip_prefix("msg_")
                        .ok_or_else(|| invalid("invalid admin message cursor"))?,
                )
                .map_err(|_| invalid("invalid admin message cursor"))?;
                Ok::<_, McpError>((created, id))
            })
            .transpose()?;
        let limit = limit.min(500);
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        let mut messages = {
            let mut statement = transaction.prepare(
                "SELECT id,pool,sender,body,in_reply_to,created,acknowledged FROM admin_messages
                 WHERE principal=?1 AND (?2 IS NULL OR created<?2 OR (created=?2 AND id<?3))
                 ORDER BY created DESC,id DESC LIMIT ?4",
            ).map_err(|_| storage_error())?;
            statement
                .query_map(
                    params![
                        principal,
                        after.as_ref().map(|a| a.0),
                        after.as_ref().map(|a| a.1),
                        (limit + 1) as i64
                    ],
                    |row| {
                        Ok(AdminMessage {
                            message_id: row.get(0)?,
                            pool: row.get(1)?,
                            from: row.get(2)?,
                            message: row.get(3)?,
                            in_reply_to: row.get(4)?,
                            created_ms: row.get(5)?,
                            acknowledged: row.get(6)?,
                        })
                    },
                )
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|_| storage_error())?
        };
        let has_more = messages.len() > limit;
        messages.truncate(limit);
        let next_cursor = if has_more {
            messages
                .last()
                .map(|last| format!("{}:{}", last.created_ms, last.message_id))
        } else {
            None
        };
        let mut changed = false;
        for message in &messages {
            changed |= transaction
                .execute(
                    "INSERT OR IGNORE INTO delivery_receipts
                   (member_id,message_id,pool,recipient,recipient_member_id,offered_at)
                 SELECT sender.id,msg.id,msg.pool,'admin',0,?3
                 FROM admin_messages msg JOIN members sender ON sender.id=msg.sender_member_id
                 WHERE msg.id=?1 AND msg.principal=?2 AND msg.offered_at IS NULL
                   AND sender.principal=?2 AND sender.expires>?3",
                    params![message.message_id, principal, now],
                )
                .map_err(|_| storage_error())?
                != 0;
            transaction
                .execute(
                    "UPDATE admin_messages SET offered_at=COALESCE(offered_at,?2) WHERE id=?1",
                    params![message.message_id, now],
                )
                .map_err(|_| storage_error())?;
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok((
            AdminMessagesPage {
                messages,
                next_cursor,
            },
            changed,
        ))
    }

    pub(super) fn admin_ack(&self, principal: &str, message_id: &str) -> Result<bool, McpError> {
        validate_name(principal, "principal")?;
        validate_name(message_id, "message_id")?;
        let connection = self.connection()?;
        let updated = connection
            .execute(
                "UPDATE admin_messages SET acknowledged=1 WHERE principal=?1 AND id=?2",
                params![principal, message_id],
            )
            .map_err(|_| storage_error())?;
        Ok(updated != 0)
    }

    pub(super) fn admin_clear_messages(&self, principal: &str) -> Result<usize, McpError> {
        validate_name(principal, "principal")?;
        self.connection()?
            .execute("DELETE FROM admin_messages WHERE principal=?1", [principal])
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
                sender_member_id: None,
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
            delivery_receipts: None,
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
        ensure_pending_capacity(transaction, targets.len(), message.created_ms)?;
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
                    "INSERT INTO messages(id,principal,pool,sender,target,body,in_reply_to,created,sender_member_id)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                    params![
                        message_id,
                        message.principal,
                        message.pool,
                        message.sender,
                        message.target,
                        message.body,
                        message.in_reply_to,
                        message.created_ms,
                        message.sender_member_id
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
                "DELETE FROM members WHERE principal=?1 AND pool=?2 AND agent=?3 AND expires>?4",
                params![principal, pool, agent, now],
            )
            .map_err(|_| storage_error())?;
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
        // Waiting callers must observe expiry as a membership revision, even
        // when no unrelated tool call commits or broadcasts a notification.
        // This is pool-local and bounded; ordinary reads never prune messages.
        expire_pool_members_batch(&transaction, principal, &args.pool, now)?;
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
                 WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3 AND m.pool=?4)
                     OR EXISTS(SELECT 1 FROM delivery_receipts r JOIN members m ON m.id=r.member_id
                       WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3 AND r.pool=?4)",
                    params![principal, session, now, args.pool],
                    |row| row.get(0),
                )
                .map_err(|_| storage_error())?,
            None => false,
        };
        let membership_revision = transaction.query_row(
            "SELECT COALESCE((SELECT revision FROM pool_revisions WHERE principal=?1 AND pool=?2),0)",
            params![principal, args.pool], |row| row.get(0),
        ).map_err(|_| storage_error())?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(MembersState {
            result: PoolMembersResult {
                agents: members.iter().map(|(_, agent, _)| agent.clone()).collect(),
                self_agent,
                peer_messages: None,
                delivery_receipts: None,
            },
            membership_revision,
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
        // An expiry batch might be filled with unrelated pools. Never revive
        // a stale session or leave its unique constraint blocking a new join.
        transaction
            .execute(
                "DELETE FROM members WHERE principal=?1 AND pool=?2 AND session=?3
                   AND expires<=?4",
                params![principal, pool, session, now],
            )
            .map_err(|_| storage_error())?;

        let bound: Option<(i64, String)> = transaction
            .query_row(
                "SELECT id,agent FROM members
                 WHERE principal=?1 AND pool=?2 AND session=?3 AND expires>?4",
                params![principal, pool, session, now],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|_| storage_error())?;

        let cursor_key = (principal.to_owned(), pool.to_owned());
        // Advance rotation only after a successful first send commits.
        let mut name_cursors =
            if bound.is_none() && self.name_allocation == AgentNameAllocation::RoundRobin {
                Some(self.name_cursors.lock().map_err(|_| storage_error())?)
            } else {
                None
            };
        let (sender_id, sender, assigned_agent, allocated_index) = match bound {
            Some((member_id, agent)) => (member_id, agent, None, None),
            None => {
                let last_index = name_cursors
                    .as_ref()
                    .and_then(|cursors| cursors.get(&cursor_key).copied());
                let (index, agent) =
                    self.allocate_agent_name(&transaction, principal, pool, last_index, now)?;
                transaction
                    .execute(
                        "INSERT INTO members(principal,pool,agent,session,expires,last_seen)
                         VALUES(?1,?2,?3,?4,?5,?6)",
                        params![
                            principal,
                            pool,
                            &agent,
                            session,
                            now + self.lease_ttl_ms,
                            now
                        ],
                    )
                    .map_err(|_| agent_conflict())?;
                (
                    transaction.last_insert_rowid(),
                    agent.clone(),
                    Some(agent),
                    Some(index),
                )
            }
        };

        transaction
            .execute(
                "UPDATE members SET expires=?2,last_seen=?3 WHERE id=?1",
                params![sender_id, now + self.lease_ttl_ms, now],
            )
            .map_err(|_| storage_error())?;

        if target == ADMIN_AGENT {
            ensure_pending_capacity(&transaction, 1, now)?;
            let message_id = format!("msg_{}", Uuid::new_v4());
            transaction
                .execute(
                    "INSERT INTO admin_messages(id,principal,pool,sender,body,in_reply_to,created,sender_member_id)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        message_id,
                        principal,
                        pool,
                        sender,
                        message,
                        in_reply_to,
                        now,
                        sender_id
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
            if let (Some(cursors), Some(index)) = (name_cursors.as_mut(), allocated_index) {
                cursors.insert(cursor_key, index);
            }
            return Ok(PoolSendResult {
                assigned_agent,
                message_id: Some(message_id),
                recipients: vec![ADMIN_AGENT.to_owned()],
                peer_messages: None,
                delivery_receipts: None,
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
                sender_member_id: Some(sender_id),
                target: &target,
                body: &message,
                in_reply_to: in_reply_to.as_deref(),
                created_ms: now,
            },
            &targets,
        )?;
        transaction.commit().map_err(|_| storage_error())?;
        if let (Some(cursors), Some(index)) = (name_cursors.as_mut(), allocated_index) {
            cursors.insert(cursor_key, index);
        }
        Ok(PoolSendResult {
            assigned_agent,
            message_id,
            recipients,
            peer_messages: None,
            delivery_receipts: None,
        })
    }

    fn allocate_agent_name(
        &self,
        transaction: &rusqlite::Transaction<'_>,
        principal: &str,
        pool: &str,
        last_index: Option<usize>,
        now: i64,
    ) -> Result<(usize, String), McpError> {
        let start = last_index.map_or(0, |index| (index + 1) % self.agent_names.len());
        for offset in 0..self.agent_names.len() {
            let index = (start + offset) % self.agent_names.len();
            let candidate = &self.agent_names[index];
            let occupied: Option<(i64, i64)> = transaction
                .query_row(
                    "SELECT id,expires FROM members WHERE principal=?1 AND pool=?2 AND agent=?3",
                    params![principal, pool, candidate],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|_| storage_error())?;
            match occupied {
                None => return Ok((index, candidate.clone())),
                Some((member_id, expires)) if expires <= now => {
                    transaction
                        .execute("DELETE FROM members WHERE id=?1", [member_id])
                        .map_err(|_| storage_error())?;
                    return Ok((index, candidate.clone()));
                }
                Some(_) => {}
            }
        }
        Err(invalid("agent name dictionary is exhausted for this pool"))
    }
}

fn cleanup(connection: &Connection, now: i64) -> Result<(), McpError> {
    expire_members_batch(connection, now)?;
    Ok(())
}

fn expire_members_batch(connection: &Connection, now: i64) -> Result<usize, McpError> {
    // The expiry index permits one bounded pass per maintenance tick or write.
    // Membership revision triggers and cascading inbox/message cleanup occur
    // atomically as the expired rows are removed.
    connection
        .execute(
            "DELETE FROM members WHERE id IN (
               SELECT id FROM members WHERE expires<=?1
               ORDER BY expires,id LIMIT ?2
             )",
            params![now, EXPIRY_BATCH_SIZE],
        )
        .map_err(|_| storage_error())
}

fn expire_pool_members_batch(
    connection: &Connection,
    principal: &str,
    pool: &str,
    now: i64,
) -> Result<(), McpError> {
    let has_expired: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM members
             WHERE principal=?1 AND pool=?2 AND expires<=?3)",
            params![principal, pool, now],
            |row| row.get(0),
        )
        .map_err(|_| storage_error())?;
    if has_expired {
        connection
            .execute(
                "DELETE FROM members WHERE id IN (
                   SELECT id FROM members WHERE principal=?1 AND pool=?2 AND expires<=?3
                   ORDER BY expires,id LIMIT ?4
                 )",
                params![principal, pool, now, EXPIRY_BATCH_SIZE],
            )
            .map_err(|_| storage_error())?;
    }
    Ok(())
}

fn ensure_pending_capacity(
    connection: &Connection,
    additional: usize,
    now: i64,
) -> Result<(), McpError> {
    loop {
        let pending: i64 = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM inbox) + (SELECT COUNT(*) FROM delivery_receipts)
                     + (SELECT COUNT(*) FROM admin_messages WHERE acknowledged=0)",
                [],
                |row| row.get(0),
            )
            .map_err(|_| storage_error())?;
        if pending.saturating_add(additional as i64) <= MAX_PENDING {
            return Ok(());
        }
        // Routine writes do a single bounded expiry batch. Only when stale
        // inbox rows would incorrectly reject a valid send do we keep draining
        // batches until capacity returns or no expired memberships remain.
        if expire_members_batch(connection, now)? == 0 {
            return Err(invalid("agent pool inbox is full; retry later"));
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn reopen(directory: &tempfile::TempDir) -> anyhow::Result<PoolStorage> {
        PoolStorage::open(
            &directory.path().join("pool.sqlite3"),
            Duration::from_secs(60),
            vec!["alice".into(), "bob".into()],
            AgentNameAllocation::FirstAvailable,
        )
    }

    fn has_column(connection: &Connection, column: &str) -> bool {
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('members') WHERE name=?1)",
                [column],
                |row| row.get(0),
            )
            .unwrap()
    }

    fn add_obsolete_columns(connection: &Connection) {
        for (column, definition) in [
            ("generation", "TEXT NOT NULL DEFAULT 'legacy'"),
            ("ack_cursor", "INTEGER NOT NULL DEFAULT 0"),
        ] {
            if !has_column(connection, column) {
                connection
                    .execute_batch(&format!(
                        "ALTER TABLE members ADD COLUMN {column} {definition}"
                    ))
                    .unwrap();
            }
        }
    }

    #[test]
    fn obsolete_column_migration_preserves_oldest_database_and_reoffers() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let path = directory.path().join("pool.sqlite3");
        let connection = Connection::open(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        // The initial v0.2.1 layout predates sender IDs, receipts, revisions,
        // acknowledged admin messages, and the incremental orphan trigger.
        connection
            .execute_batch(
                "PRAGMA foreign_keys=ON;
             CREATE TABLE members (
               id INTEGER PRIMARY KEY AUTOINCREMENT, principal TEXT NOT NULL,
               pool TEXT NOT NULL, agent TEXT NOT NULL, session TEXT NOT NULL,
               expires INTEGER NOT NULL, last_seen INTEGER NOT NULL,
               generation TEXT NOT NULL, ack_cursor INTEGER NOT NULL DEFAULT 0,
               offered_cursor INTEGER);
             CREATE TABLE messages (
               id TEXT PRIMARY KEY, principal TEXT NOT NULL, pool TEXT NOT NULL,
               sender TEXT NOT NULL, target TEXT NOT NULL, body TEXT NOT NULL,
               in_reply_to TEXT, created INTEGER NOT NULL);
             CREATE TABLE inbox (
               cursor INTEGER PRIMARY KEY AUTOINCREMENT,
               member_id INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
               message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
               UNIQUE(member_id,message_id));
             CREATE TABLE admin_messages (
               id TEXT PRIMARY KEY, principal TEXT NOT NULL, pool TEXT NOT NULL,
               sender TEXT NOT NULL, body TEXT NOT NULL, in_reply_to TEXT,
               created INTEGER NOT NULL);
             INSERT INTO members VALUES
               (42,'owner','work','alice','alice',9223372036854775807,10,'gen',99,100);
             INSERT INTO messages VALUES
               ('queued','owner','work','bob','alice','retained',NULL,10);
             INSERT INTO inbox VALUES(100,42,'queued');
             UPDATE sqlite_sequence SET seq=900 WHERE name='members';",
            )
            .unwrap();
        drop(connection);

        let storage = reopen(&directory).unwrap();
        {
            let connection = storage.connection().unwrap();
            assert!(!has_column(&connection, "generation"));
            assert!(!has_column(&connection, "ack_cursor"));
            let member: (i64, String, Option<i64>) = connection
                .query_row("SELECT id,session,offered_cursor FROM members", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap();
            assert_eq!(member, (42, "alice".into(), None));
            assert_eq!(
                connection
                    .query_row("PRAGMA foreign_keys", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                1
            );
            assert!(
                !connection
                    .prepare("PRAGMA foreign_key_check")
                    .unwrap()
                    .exists([])
                    .unwrap()
            );
        }
        // A restart clears the offer, never the queued row: beginning a tool
        // before re-offering must not mistake the legacy ACK watermark for ACK.
        storage.begin_tool_state("owner", Some("alice")).unwrap();
        let messages = storage
            .collect_tool_messages("owner", Some("alice"))
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].message_id, "queued");
        storage.begin_tool_state("owner", Some("alice")).unwrap();
        {
            let connection = storage.connection().unwrap();
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM inbox", [], |row| row.get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                connection
                    .query_row("SELECT COUNT(*) FROM messages", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        assert!(insert_member(&storage, "work", "bob", now_ms() + 60_000) > 900);
        drop(storage);
        assert!(reopen(&directory).is_ok(), "migration must be idempotent");
    }

    #[test]
    fn obsolete_column_migration_preserves_receipts_revisions_and_foreign_keys() {
        let (directory, storage) = storage();
        let alice = insert_member(&storage, "work", "alice", now_ms() + 60_000);
        let bob = insert_member(&storage, "work", "bob", now_ms() + 60_000);
        {
            let connection = storage.connection().unwrap();
            add_obsolete_columns(&connection);
            connection.execute(
                "INSERT INTO messages(id,principal,pool,sender,target,body,created,sender_member_id)
                 VALUES('queued','owner','work','alice','bob','retained',10,?1)", [alice]
            ).unwrap();
            connection.execute("INSERT INTO inbox(cursor,member_id,message_id,offered_at) VALUES(200,?1,'queued',20)", [bob]).unwrap();
            connection.execute(
                "INSERT INTO delivery_receipts(cursor,member_id,message_id,pool,recipient,recipient_member_id,offered_at)
                 VALUES(300,?1,'queued','work','bob',?2,20)", params![alice,bob]
            ).unwrap();
            connection.execute("UPDATE members SET offered_cursor=200,receipt_offered_cursor=300,ack_cursor=199", []).unwrap();
            connection
                .execute("UPDATE pool_revisions SET revision=99", [])
                .unwrap();
        }
        drop(storage);
        let storage = reopen(&directory).unwrap();
        {
            let connection = storage.connection().unwrap();
            assert!(!has_column(&connection, "generation"));
            assert!(!has_column(&connection, "ack_cursor"));
            assert_eq!(
                connection
                    .query_row("SELECT revision FROM pool_revisions", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                99
            );
            assert_eq!(connection.query_row("SELECT COUNT(*) FROM members WHERE offered_cursor IS NULL AND receipt_offered_cursor IS NULL", [], |row| row.get::<_, i64>(0)).unwrap(), 2);
        }
        storage.begin_tool_state("owner", Some("alice")).unwrap();
        let receipt = storage
            .collect_tool_comms("owner", Some("alice"), None)
            .unwrap();
        assert_eq!(receipt.delivery_receipts.len(), 1);
        assert_eq!(receipt.delivery_receipts[0].message_id, "queued");
        assert_eq!(
            storage
                .collect_tool_messages("owner", Some("bob"))
                .unwrap()
                .len(),
            1
        );
        storage.begin_tool_state("owner", Some("alice")).unwrap();
        let connection = storage.connection().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM delivery_receipts", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        // Preserve sender SET NULL, inbox CASCADE, revision triggers and final
        // recipient reclamation without rebuilding any referenced table.
        connection
            .execute("DELETE FROM members WHERE id=?1", [alice])
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT sender_member_id FROM messages", [], |row| row
                    .get::<_, Option<i64>>(0))
                .unwrap(),
            None
        );
        connection
            .execute("DELETE FROM members WHERE id=?1", [bob])
            .unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM messages", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            connection
                .query_row("SELECT revision FROM pool_revisions", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            101
        );
        assert!(
            !connection
                .prepare("PRAGMA foreign_key_check")
                .unwrap()
                .exists([])
                .unwrap()
        );
    }

    #[test]
    fn obsolete_column_migration_rolls_back_when_custom_schema_depends_on_column() {
        let (directory, storage) = storage();
        let member = insert_member(&storage, "work", "alice", now_ms() + 60_000);
        {
            let connection = storage.connection().unwrap();
            add_obsolete_columns(&connection);
            connection
                .execute_batch(
                    "UPDATE members SET offered_cursor=123;
                 CREATE TRIGGER custom_ack AFTER UPDATE OF offered_cursor ON members BEGIN
                   SELECT NEW.ack_cursor;
                 END;",
                )
                .unwrap();
        }
        drop(storage);
        assert!(
            reopen(&directory).is_err(),
            "unknown schema consumers must fail closed"
        );
        let connection = Connection::open(directory.path().join("pool.sqlite3")).unwrap();
        assert!(
            has_column(&connection, "generation"),
            "earlier DROP must roll back"
        );
        assert!(has_column(&connection, "ack_cursor"));
        assert_eq!(
            connection
                .query_row(
                    "SELECT offered_cursor FROM members WHERE id=?1",
                    [member],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            123
        );
    }

    #[test]
    fn obsolete_column_migration_accepts_fresh_and_partially_migrated_schemas() {
        for missing_column in [None, Some("generation"), Some("ack_cursor")] {
            let (directory, storage) = storage();
            {
                let connection = storage.connection().unwrap();
                add_obsolete_columns(&connection);
                if let Some(column) = missing_column {
                    connection
                        .execute_batch(&format!("ALTER TABLE members DROP COLUMN {column}"))
                        .unwrap();
                }
            }
            drop(storage);
            let storage = reopen(&directory).unwrap();
            {
                let connection = storage.connection().unwrap();
                assert!(!has_column(&connection, "generation"));
                assert!(!has_column(&connection, "ack_cursor"));
            }
            // A second startup with neither obsolete column is also valid.
            drop(storage);
            assert!(reopen(&directory).is_ok());
        }
    }

    #[test]
    fn failed_obsolete_column_migration_does_not_skip_orphan_cleanup_on_retry() {
        let (directory, storage) = storage();
        {
            let connection = storage.connection().unwrap();
            add_obsolete_columns(&connection);
            // Older databases have neither incremental reclamation nor its
            // one-time orphan cleanup marker. A custom dependency can make the
            // column migration fail before that cleanup has been performed.
            connection
                .execute_batch(
                    "DROP TRIGGER inbox_reclaim_message;
                     INSERT INTO messages(id,principal,pool,sender,target,body,created)
                       VALUES('orphan','owner','work','alice','bob','retained',10);
                     CREATE TRIGGER custom_ack AFTER UPDATE OF offered_cursor ON members BEGIN
                       SELECT NEW.ack_cursor;
                     END;",
                )
                .unwrap();
        }
        drop(storage);

        assert!(reopen(&directory).is_err());
        let connection = Connection::open(directory.path().join("pool.sqlite3")).unwrap();
        let has_trigger: bool = connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master
                 WHERE type='trigger' AND name='inbox_reclaim_message')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !has_trigger,
            "failed migration must roll back the cleanup marker"
        );
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM messages", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        connection.execute_batch("DROP TRIGGER custom_ack").unwrap();
        drop(connection);

        let storage = reopen(&directory).unwrap();
        let connection = storage.connection().unwrap();
        assert_eq!(
            connection
                .query_row("SELECT COUNT(*) FROM messages", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0,
            "retry must still perform historical orphan cleanup"
        );
    }

    fn storage() -> (tempfile::TempDir, PoolStorage) {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let storage = PoolStorage::open(
            &directory.path().join("pool.sqlite3"),
            Duration::from_secs(60),
            vec!["alice".into(), "bob".into()],
            AgentNameAllocation::FirstAvailable,
        )
        .unwrap();
        (directory, storage)
    }

    fn insert_member(storage: &PoolStorage, pool: &str, session: &str, expires: i64) -> i64 {
        let connection = storage.connection().unwrap();
        connection
            .execute(
                "INSERT INTO members(principal,pool,agent,session,expires,last_seen)
                 VALUES('owner',?1,?2,?3,?4,?5)",
                params![pool, session, session, expires, now_ms()],
            )
            .unwrap();
        connection.last_insert_rowid()
    }

    #[test]
    fn unrelated_expired_members_are_not_deleted_by_read_lookups() {
        let (_directory, storage) = storage();
        let expired_id = insert_member(&storage, "other", "alice", now_ms() - 1);
        let selected_id = insert_member(&storage, "selected", "bob", now_ms() - 1);

        assert!(
            storage
                .identities_for_session("owner", Some("alice"))
                .unwrap()
                .is_empty()
        );
        assert!(storage.admin_members("owner").unwrap().is_empty());
        assert!(
            storage
                .admin_messages_page("owner", 10, None)
                .unwrap()
                .0
                .messages
                .is_empty()
        );
        assert!(
            storage
                .collect_tool_comms("owner", Some("alice"), None)
                .unwrap()
                .is_empty()
        );

        let before_revision: i64 = storage
            .connection()
            .unwrap()
            .query_row(
                "SELECT revision FROM pool_revisions WHERE principal='owner' AND pool='selected'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let state = storage
            .members_state(
                "owner",
                &PoolMembersArgs {
                    pool: "selected".into(),
                },
                None,
            )
            .unwrap();
        assert!(state.result.agents.is_empty());
        assert_eq!(state.membership_revision, before_revision + 1);

        let connection = storage.connection().unwrap();
        assert!(
            !connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM members WHERE id=?1)",
                    [selected_id],
                    |row| row.get::<_, bool>(0)
                )
                .unwrap()
        );
        assert!(
            connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM members WHERE id=?1)",
                    [expired_id],
                    |row| row.get::<_, bool>(0)
                )
                .unwrap()
        );
    }

    #[test]
    fn removing_last_inbox_recipient_immediately_reclaims_message() {
        let (_directory, storage) = storage();
        let alice = insert_member(&storage, "work", "alice", now_ms() + 60_000);
        let bob = insert_member(&storage, "work", "bob", now_ms() + 60_000);
        let connection = storage.connection().unwrap();
        connection.execute("INSERT INTO messages(id,principal,pool,sender,target,body,created) VALUES('message','owner','work','alice','global','hello',?1)", [now_ms()]).unwrap();
        for member_id in [alice, bob] {
            connection
                .execute(
                    "INSERT INTO inbox(member_id,message_id) VALUES(?1,'message')",
                    [member_id],
                )
                .unwrap();
        }
        connection
            .execute("DELETE FROM inbox WHERE member_id=?1", [alice])
            .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE id='message'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        connection
            .execute("DELETE FROM inbox WHERE member_id=?1", [bob])
            .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE id='message'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn expiration_cleanup_uses_index_without_scanning_members() {
        let (_directory, storage) = storage();
        let connection = storage.connection().unwrap();
        let plan: Vec<String> = connection
            .prepare("EXPLAIN QUERY PLAN SELECT id FROM members WHERE expires<=?1 ORDER BY expires LIMIT ?2")
            .unwrap()
            .query_map(params![now_ms(), 128], |row| row.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(
            plan.iter().any(|detail| detail.contains("member_expiry")),
            "expected expiration index, query plan: {plan:?}"
        );
        assert!(
            !plan.iter().any(|detail| detail.contains("SCAN members")),
            "unexpected full scan: {plan:?}"
        );
    }

    #[test]
    fn expiry_cleanup_is_bounded_and_advances_revision_across_batches() {
        let (_directory, storage) = storage();
        let now = now_ms();
        let total = EXPIRY_BATCH_SIZE + 5;
        for number in 0..total {
            insert_member(&storage, "backlog", &format!("stale-{number}"), now - 1);
        }
        let connection = storage.connection().unwrap();
        assert_eq!(
            expire_members_batch(&connection, now).unwrap() as i64,
            EXPIRY_BATCH_SIZE
        );
        let remaining: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM members WHERE pool='backlog'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 5);
        assert_eq!(expire_members_batch(&connection, now).unwrap(), 5);
        assert_eq!(expire_members_batch(&connection, now).unwrap(), 0);
        let revision: i64 = connection
            .query_row(
                "SELECT revision FROM pool_revisions WHERE principal='owner' AND pool='backlog'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revision, 2 * total);
    }

    #[test]
    fn expired_name_and_session_can_rejoin_behind_an_expiry_backlog() {
        let (_directory, storage) = storage();
        let now = now_ms();
        for number in 0..EXPIRY_BATCH_SIZE {
            insert_member(
                &storage,
                "backlog",
                &format!("stale-{number}"),
                now - 10_000,
            );
        }
        insert_member(&storage, "work", "alice", now - 1);
        let join = || PoolSendArgs {
            pool: "work".into(),
            target: Some("global".into()),
            message: Some("join".into()),
            in_reply_to: None,
            action: None,
        };
        // A new session needs to reclaim an expired dictionary name after the
        // bounded maintenance batch spent all its budget on unrelated pools.
        let first = storage.send("owner", join(), Some("fresh")).unwrap();
        assert_eq!(first.assigned_agent.as_deref(), Some("alice"));
        // A reused session must also evict an expired same-session row before
        // allocating. Otherwise the member_session unique key still blocks it.
        insert_member(&storage, "other", "bob", now - 1);
        for number in 0..EXPIRY_BATCH_SIZE {
            insert_member(
                &storage,
                "backlog2",
                &format!("stale-{number}"),
                now - 10_000,
            );
        }
        let second = storage
            .send(
                "owner",
                PoolSendArgs {
                    pool: "other".into(),
                    ..join()
                },
                Some("bob"),
            )
            .unwrap();
        assert_eq!(second.assigned_agent.as_deref(), Some("alice"));
        let connection = storage.connection().unwrap();
        let active: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM members WHERE session IN ('fresh','bob') AND expires>?1",
                [now],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(active, 2);
    }

    #[test]
    fn idle_housekeeping_reclaims_expired_members_without_read_traffic() {
        let (_directory, storage) = storage();
        let expired_id = insert_member(&storage, "idle", "alice", now_ms() - 1);
        {
            let connection = storage.connection().unwrap();
            connection
                .execute("INSERT INTO messages(id,principal,pool,sender,target,body,created) VALUES('idle-message','owner','idle','bob','alice','unread',?1)", [now_ms()])
                .unwrap();
            connection
                .execute(
                    "INSERT INTO inbox(member_id,message_id) VALUES(?1,'idle-message')",
                    [expired_id],
                )
                .unwrap();
        }
        // No storage calls occur during this interval: the worker is genuinely
        // periodic, not a deferred cleanup callback on the next read.
        std::thread::sleep(MAINTENANCE_INTERVAL + Duration::from_secs(1));
        let connection = storage.connection().unwrap();
        let remaining: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM members WHERE pool='idle'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0);
        let orphan_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE id='idle-message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(orphan_count, 0);
    }
}
