//! Account-scoped administrative membership and inbox operations.

use super::membership::cleanup;
use rmcp::ErrorData as McpError;
use rusqlite::params;
use uuid::Uuid;

use crate::agent_pool::{
    ADMIN_AGENT, AdminMember, AdminMessage, AdminMessagesPage, PoolSendResult, invalid, now_ms,
    storage_error, validate_name,
};

use super::{MAX_MESSAGE_BYTES, MemberMessage, Mutation, MutationEffect, PoolStorage};

impl PoolStorage {
    pub(in crate::agent_pool) fn admin_members(
        &self,
        principal: &str,
    ) -> Result<Vec<AdminMember>, McpError> {
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

    pub(in crate::agent_pool) fn admin_messages_page(
        &self,
        principal: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<AdminMessagesPage, McpError> {
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
            let mut statement = transaction
                .prepare(
                    "SELECT id,pool,sender,body,created,acknowledged FROM admin_messages
                 WHERE principal=?1 AND (?2 IS NULL OR created<?2 OR (created=?2 AND id<?3))
                 ORDER BY created DESC,id DESC LIMIT ?4",
                )
                .map_err(|_| storage_error())?;
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
                            created_ms: row.get(4)?,
                            acknowledged: row.get(5)?,
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
        for message in &messages {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO delivery_receipts
                   (member_id,message_id,pool,recipient,recipient_member_id,offered_at)
                 SELECT sender.id,msg.id,msg.pool,'admin',0,?3
                 FROM admin_messages msg JOIN members sender ON sender.id=msg.sender_member_id
                 WHERE msg.id=?1 AND msg.principal=?2 AND msg.offered_at IS NULL
                   AND sender.principal=?2 AND sender.expires>?3",
                    params![message.message_id, principal, now],
                )
                .map_err(|_| storage_error())?;
            transaction
                .execute(
                    "UPDATE admin_messages SET offered_at=COALESCE(offered_at,?2) WHERE id=?1",
                    params![message.message_id, now],
                )
                .map_err(|_| storage_error())?;
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok(AdminMessagesPage {
            messages,
            next_cursor,
        })
    }

    pub(in crate::agent_pool) fn admin_ack(
        &self,
        principal: &str,
        message_id: &str,
    ) -> Result<bool, McpError> {
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

    pub(in crate::agent_pool) fn admin_clear_messages(
        &self,
        principal: &str,
    ) -> Result<usize, McpError> {
        validate_name(principal, "principal")?;
        self.connection()?
            .execute("DELETE FROM admin_messages WHERE principal=?1", [principal])
            .map_err(|_| storage_error())
    }

    pub(in crate::agent_pool) fn admin_send(
        &self,
        principal: &str,
        pool: &str,
        target: &str,
        message: &str,
    ) -> Result<Mutation<PoolSendResult>, McpError> {
        validate_name(principal, "principal")?;
        validate_name(pool, "pool")?;
        validate_name(target, "target")?;
        if target == ADMIN_AGENT {
            return Err(invalid("admin cannot target itself"));
        }
        if message.is_empty() || message.len() > MAX_MESSAGE_BYTES {
            return Err(invalid("message must contain 1 to 65536 bytes"));
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
                created_ms: now,
            },
            &targets,
        )?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(Mutation::new(
            PoolSendResult {
                assigned_agent: None,
                message_id,
                recipients,
                messages: None,
                delivery_receipts: None,
            },
            MutationEffect::Wake,
        ))
    }

    pub(in crate::agent_pool) fn admin_terminate(
        &self,
        principal: &str,
        pool: &str,
        agent: &str,
    ) -> Result<Mutation<bool>, McpError> {
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
        Ok(Mutation::new(
            deleted > 0,
            if deleted > 0 {
                MutationEffect::MembershipChanged
            } else {
                MutationEffect::Wake
            },
        ))
    }
}
