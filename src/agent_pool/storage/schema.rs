//! SQLite schema creation and crash-safe startup migrations.
//! The entire migration, orphan cleanup, and restart cursor reset commit atomically.

use rmcp::ErrorData as McpError;
use rusqlite::{Connection, params};

use crate::agent_pool::storage_error;

pub(super) fn initialize(connection: &mut Connection) -> anyhow::Result<()> {
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
    Ok(())
}

// Historical one-time migration fallback; routine inbox deletion uses a trigger.
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
