//! Member lifecycle, active membership snapshots, joining, and name allocation.

use super::messages::ensure_pending_capacity;
use rmcp::ErrorData as McpError;
use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::agent_pool::{
    ADMIN_AGENT, AgentIdentity, PoolAction, PoolMembersArgs, PoolMembersResult, PoolSendArgs,
    PoolSendResult, invalid, now_ms, storage_error, validate_name, validate_session,
};
use crate::config::AgentNameAllocation;

use super::{
    EXPIRY_BATCH_SIZE, MAX_ADMIN_MESSAGES, MAX_MESSAGE_BYTES, MemberMessage, MembersState,
    Mutation, MutationEffect, PoolStorage,
};
use rmcp::model::ErrorCode;
use serde_json::json;

impl PoolStorage {
    /// A non-overlapping tool call retires previous offers and refreshes leases
    /// correlated with this session.
    pub(in crate::agent_pool) fn begin_tool_state(
        &self,
        principal: &str,
        session: Option<&str>,
    ) -> Result<(), McpError> {
        self.tool_state(principal, session, true)
    }

    pub(in crate::agent_pool) fn refresh_tool_state(
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

    pub(in crate::agent_pool) fn identities_for_session(
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

    pub(in crate::agent_pool) fn members_state(
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
                 WHERE m.principal=?1 AND m.session=?2 AND m.expires>?3 AND m.pool=?4)",
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
                messages: None,
                delivery_receipts: None,
            },
            membership_revision,
            next_expiry_ms: members.iter().map(|(_, _, expires)| *expires).min(),
            pending,
        })
    }

    pub(in crate::agent_pool) fn send_with_effects(
        &self,
        principal: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<Mutation<PoolSendResult>, McpError> {
        validate_name(&args.pool, "pool")?;
        match args.action {
            Some(PoolAction::Leave) => {
                if args.target.is_some() || args.message.is_some() {
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
                self.send_message(principal, &args.pool, target, message, session)
            }
        }
    }

    fn leave_pool(
        &self,
        principal: &str,
        pool: &str,
        session: Option<&str>,
    ) -> Result<Mutation<PoolSendResult>, McpError> {
        validate_name(principal, "principal")?;
        let session = session.ok_or_else(|| {
            invalid("pool_send requires openai/session metadata to identify the sender")
        })?;
        validate_session(session)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        // Observe the deleted row as part of this *same* transaction, rather
        // than reading membership in the facade and racing the actual leave.
        // Expired rows can also be deleted, but were never active observers.
        let left_active: Option<bool> = transaction
            .query_row(
                "DELETE FROM members WHERE principal=?1 AND pool=?2 AND session=?3
                 RETURNING expires>?4",
                params![principal, pool, session, now],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| storage_error())?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(Mutation::new(
            PoolSendResult::default(),
            if left_active.unwrap_or(false) {
                MutationEffect::MembershipChanged
            } else {
                MutationEffect::Wake
            },
        ))
    }

    fn send_message(
        &self,
        principal: &str,
        pool: &str,
        target: String,
        message: String,
        session: Option<&str>,
    ) -> Result<Mutation<PoolSendResult>, McpError> {
        validate_name(principal, "principal")?;
        validate_name(&target, "target")?;
        if message.is_empty() || message.len() > MAX_MESSAGE_BYTES {
            return Err(invalid("message must contain 1 to 65536 bytes"));
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
                    "INSERT INTO admin_messages(id,principal,pool,sender,body,created,sender_member_id)
                     VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        message_id,
                        principal,
                        pool,
                        sender,
                        message,
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
            let effect = if assigned_agent.is_some() {
                MutationEffect::MembershipChanged
            } else {
                MutationEffect::Wake
            };
            return Ok(Mutation::new(
                PoolSendResult {
                    assigned_agent,
                    message_id: Some(message_id),
                    recipients: vec![ADMIN_AGENT.to_owned()],
                    messages: None,
                    delivery_receipts: None,
                },
                effect,
            ));
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
                created_ms: now,
            },
            &targets,
        )?;
        transaction.commit().map_err(|_| storage_error())?;
        if let (Some(cursors), Some(index)) = (name_cursors.as_mut(), allocated_index) {
            cursors.insert(cursor_key, index);
        }
        let effect = if assigned_agent.is_some() {
            MutationEffect::MembershipChanged
        } else {
            MutationEffect::Wake
        };
        Ok(Mutation::new(
            PoolSendResult {
                assigned_agent,
                message_id,
                recipients,
                messages: None,
                delivery_receipts: None,
            },
            effect,
        ))
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

// Bounded maintenance shares the same member/revision semantics as joins.
pub(super) fn cleanup(connection: &Connection, now: i64) -> Result<(), McpError> {
    expire_members_batch(connection, now)?;
    Ok(())
}

pub(super) fn expire_members_batch(connection: &Connection, now: i64) -> Result<usize, McpError> {
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

fn agent_conflict() -> McpError {
    McpError::new(
        ErrorCode(-32000),
        "Agent name is already active for another session in this pool",
        Some(json!({"reason":"agent_conflict"})),
    )
}
