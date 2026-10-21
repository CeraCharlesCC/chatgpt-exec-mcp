//! Durable peer inbox offers, receipts, and common transactional fanout.

use super::membership::expire_members_batch;
use std::collections::HashMap;

use rmcp::ErrorData as McpError;
use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::agent_pool::{
    Communications, MessageRecord, ReceiptRecord, ReceiptStatus, invalid, now_ms, storage_error,
    validate_name, validate_session,
};

use super::{MAX_BATCH_BYTES, MAX_BATCH_COUNT, MAX_PENDING, MemberMessage, PoolStorage};

impl PoolStorage {
    /// Collect a bounded batch for this session and persist the highest cursor
    /// offered per member. Rows are retained until the session's next tool call.
    #[cfg(test)]
    pub(in crate::agent_pool) fn collect_tool_messages(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<Vec<MessageRecord>, McpError> {
        Ok(self.collect_tool_comms(principal, session, None)?.messages)
    }

    pub(in crate::agent_pool) fn collect_tool_comms(
        &self,
        principal: &str,
        session: Option<&str>,
        pool: Option<&str>,
    ) -> Result<Communications, McpError> {
        self.collect_tool_comms_if_ready(principal, session, pool, true)
    }

    /// A wait may have observed a message that an overlapping tool then retired.
    /// Leave receipts untouched when that stale observation cannot complete it.
    pub(in crate::agent_pool) fn collect_tool_comms_if_ready(
        &self,
        principal: &str,
        session: Option<&str>,
        pool: Option<&str>,
        receipt_only_ready: bool,
    ) -> Result<Communications, McpError> {
        validate_name(principal, "principal")?;
        let Some(session) = session else {
            return Ok(Communications::default());
        };
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;

        let rows: Vec<(i64, i64, MessageRecord)> = {
            let mut statement = transaction
                .prepare(
                    "SELECT i.cursor,m.id,msg.id,msg.pool,msg.sender,m.agent,msg.body,msg.target,msg.created
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
                            MessageRecord {
                                message_id: row.get(2)?,
                                pool: row.get(3)?,
                                from: row.get(4)?,
                                to: row.get(5)?,
                                message: row.get(6)?,
                                target: row.get(7)?,
                                sent_at_ms: row.get(8)?,
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
        if messages.is_empty() && !receipt_only_ready {
            transaction.commit().map_err(|_| storage_error())?;
            return Ok(Communications::default());
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
                            ReceiptRecord {
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

    pub(super) fn resolve_member_targets(
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

    pub(super) fn queue_member_message(
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
                    "INSERT INTO messages(id,principal,pool,sender,target,body,created,sender_member_id)
                     VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                    params![
                        message_id,
                        message.principal,
                        message.pool,
                        message.sender,
                        message.target,
                        message.body,
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
}

// Enforce the shared pending limit for agent and admin message fanout.
pub(super) fn ensure_pending_capacity(
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
