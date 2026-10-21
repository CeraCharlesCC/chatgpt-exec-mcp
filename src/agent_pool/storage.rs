//! Atomic pool persistence: membership, message fanout, inbox offers and ACKs.
//! This component never awaits or owns notification subscriptions.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use rmcp::ErrorData as McpError;
use rusqlite::{Connection, OptionalExtension, params};

use uuid::Uuid;

use crate::config::AgentNameAllocation;

mod admin;
mod membership;
mod messages;
mod schema;

use membership::cleanup;
#[cfg(test)]
use membership::expire_members_batch;

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

/// Coordination intent recorded with a successful database operation. No
/// caller needs a second membership query after a transaction commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum MutationEffect {
    /// Only the returned data changed; waiting peers need no notification.
    None,
    /// Wake observers to recheck queues (also preserves legacy no-op wakes).
    Wake,
    /// A member was added or an active member was removed from this pool.
    MembershipChanged,
}

pub(super) struct Mutation<T> {
    pub(super) value: T,
    pub(super) effect: MutationEffect,
}

impl<T> Mutation<T> {
    pub(super) fn new(value: T, effect: MutationEffect) -> Self {
        Self { value, effect }
    }
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
        schema::initialize(&mut connection)?;
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
    fn committed_send_effects_distinguish_joins_active_leaves_and_noops() {
        let (_directory, storage) = storage();
        let send = |target: &str| PoolSendArgs {
            pool: "work".into(),
            target: Some(target.into()),
            message: Some("hello".into()),
            in_reply_to: None,
            action: None,
        };
        let leave = || PoolSendArgs {
            pool: "work".into(),
            target: None,
            message: None,
            in_reply_to: None,
            action: Some(PoolAction::Leave),
        };

        let joined = storage
            .send_with_effects("owner", send("global"), Some("a"))
            .unwrap();
        assert!(joined.value.assigned_agent.is_some());
        assert_eq!(joined.effect, MutationEffect::MembershipChanged);

        let sent = storage
            .send_with_effects("owner", send("global"), Some("a"))
            .unwrap();
        assert!(sent.value.assigned_agent.is_none());
        assert_eq!(sent.effect, MutationEffect::Wake);

        // A rejected first send rolls back the inserted member and name claim.
        assert!(
            storage
                .send_with_effects("owner", send("missing"), Some("b"))
                .is_err()
        );
        assert_eq!(
            storage
                .members_state(
                    "owner",
                    &PoolMembersArgs {
                        pool: "work".into()
                    },
                    Some("b")
                )
                .unwrap()
                .result
                .self_agent,
            None
        );

        let left = storage
            .send_with_effects("owner", leave(), Some("a"))
            .unwrap();
        assert_eq!(left.effect, MutationEffect::MembershipChanged);
        let noop = storage
            .send_with_effects("owner", leave(), Some("a"))
            .unwrap();
        assert_eq!(noop.effect, MutationEffect::Wake);

        // Expired deletions retire stale rows but are not an active leave.
        insert_member(&storage, "work", "stale", now_ms() - 1);
        let expired = storage
            .send_with_effects("owner", leave(), Some("stale"))
            .unwrap();
        assert_eq!(expired.effect, MutationEffect::Wake);

        let admin = storage
            .admin_send("owner", "work", "global", "broadcast", None)
            .unwrap();
        assert_eq!(admin.effect, MutationEffect::Wake);
        assert!(admin.value.message_id.is_none());
        let terminate = storage.admin_terminate("owner", "work", "missing").unwrap();
        assert!(!terminate.value);
        assert_eq!(terminate.effect, MutationEffect::Wake);
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
                .value
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
        let first = storage
            .send_with_effects("owner", join(), Some("fresh"))
            .unwrap()
            .value;
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
            .send_with_effects(
                "owner",
                PoolSendArgs {
                    pool: "other".into(),
                    ..join()
                },
                Some("bob"),
            )
            .unwrap()
            .value;
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
