use super::tests::{leave, poll_pending, send, store, waiting};
use super::*;

async fn collect(store: &AgentPoolStore, session: &str) -> Communications {
    let turn = store.start_tool("owner", Some(session)).await.unwrap();
    store
        .finish_tool_turn("owner", Some(session), turn.as_ref())
        .await
        .unwrap()
}

fn join_pair(store: &AgentPoolStore) {
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    store
        .send("owner", send("p", "global", "join"), Some("b"))
        .unwrap();
}

#[tokio::test]
async fn offered_receipt_wakes_sender_without_a_reply_and_is_not_recursive() {
    let (_directory, store) = store();
    join_pair(&store);
    collect(&store, "a").await;
    let sent = store
        .send("owner", send("p", "bob", "do work"), Some("a"))
        .unwrap();
    let mut waiter = Box::pin(store.wait("owner", waiting("p", 35), Some("a")));
    poll_pending(waiter.as_mut()).await;
    let recipient = store.start_tool("owner", Some("b")).await.unwrap();
    let first = store
        .finish_tool_turn("owner", Some("b"), recipient.as_ref())
        .await
        .unwrap();
    let repeated = store
        .finish_tool_turn("owner", Some("b"), recipient.as_ref())
        .await
        .unwrap();
    assert_eq!(first.messages.len(), 1);
    assert_eq!(first.messages[0].message, "do work");
    assert_eq!(
        Some(&first.messages[0].message_id),
        sent.message_id.as_ref()
    );
    assert_eq!(first.messages[0].from, "alice");
    assert_eq!(first.messages[0].to, "bob");
    assert_eq!(first.messages, repeated.messages);
    let received = waiter.await.unwrap();
    assert_eq!(received.reason, PoolWaitReason::Receipt);
    assert!(!received.membership_changed && received.messages.is_empty());
    assert_eq!(received.delivery_receipts.len(), 1);
    let receipt = &received.delivery_receipts[0];
    assert_eq!(Some(&receipt.message_id), sent.message_id.as_ref());
    assert_eq!(receipt.status, ReceiptStatus::Offered);
    assert_eq!(receipt.to, "bob");
    assert!(receipt.offered_at_ms > 0);
    assert!(collect(&store, "a").await.is_empty());
    // Re-offering the original after its receipt was retired cannot create a
    // second receipt. Receiving a receipt cannot create a receipt of a receipt.
    store
        .finish_tool_turn("owner", Some("b"), recipient.as_ref())
        .await
        .unwrap();
    assert!(collect(&store, "a").await.is_empty());
    drop(recipient);
    assert!(collect(&store, "b").await.is_empty());
}

#[tokio::test]
async fn receipt_survives_original_message_pruning_and_restart() {
    let (directory, store) = store();
    join_pair(&store);
    collect(&store, "a").await;
    let sent = store
        .send("owner", send("p", "bob", "durable receipt"), Some("a"))
        .unwrap();
    collect(&store, "b").await;
    collect(&store, "b").await;
    let remaining: i64 = store
        .storage
        .connection()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM messages WHERE id=?1",
            [sent.message_id.as_ref().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
    drop(store);
    let store = AgentPoolStore::open_with_names(
        &directory.path().join("agent-pool.sqlite3"),
        Duration::from_secs(3600),
        vec!["alice".into(), "bob".into()],
    )
    .unwrap();
    let turn = store.start_tool("owner", Some("a")).await.unwrap();
    let offered = store
        .finish_tool_turn("owner", Some("a"), turn.as_ref())
        .await
        .unwrap();
    assert_eq!(offered.delivery_receipts.len(), 1);
    assert_eq!(
        Some(&offered.delivery_receipts[0].message_id),
        sent.message_id.as_ref()
    );
    drop(turn);
    drop(store);
    // An offered but not retired receipt is replayed after restart too.
    let store = AgentPoolStore::open(&directory.path().join("agent-pool.sqlite3")).unwrap();
    assert_eq!(
        collect(&store, "a").await.delivery_receipts,
        offered.delivery_receipts
    );
    assert!(collect(&store, "a").await.is_empty());
}

#[tokio::test]
async fn reused_sender_names_never_receive_the_previous_memberships_receipts() {
    for offer_before_leave in [false, true] {
        let (_directory, store) = store();
        join_pair(&store);
        collect(&store, "a").await;
        store
            .send("owner", send("p", "bob", "old sender"), Some("a"))
            .unwrap();
        if offer_before_leave {
            collect(&store, "b").await;
        }
        store.send("owner", leave("p"), Some("a")).unwrap();
        let new = store
            .send(
                "owner",
                send("p", "global", "new sender"),
                Some("replacement"),
            )
            .unwrap();
        assert_eq!(new.assigned_agent.as_deref(), Some("alice"));
        if !offer_before_leave {
            collect(&store, "b").await;
        }
        assert!(
            collect(&store, "replacement")
                .await
                .delivery_receipts
                .is_empty()
        );
    }
}

#[tokio::test]
async fn broadcast_has_no_receipt_and_self_send_can_return_both_message_and_receipt() {
    let (_directory, store) = store();
    join_pair(&store);
    collect(&store, "a").await;
    assert!(collect(&store, "b").await.delivery_receipts.is_empty());
    store
        .send("owner", send("p", "alice", "self"), Some("a"))
        .unwrap();
    let comms = collect(&store, "a").await;
    assert_eq!(comms.messages[0].message, "self");
    assert_eq!(comms.delivery_receipts[0].to, "alice");
    assert!(collect(&store, "a").await.is_empty());
}

#[tokio::test]
async fn message_reason_preserves_a_simultaneous_membership_change() {
    let (_directory, store) = store();
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    let mut wait = Box::pin(store.wait("owner", waiting("p", 35), Some("a")));
    poll_pending(wait.as_mut()).await;
    store
        .send("owner", send("p", "alice", "new member"), Some("b"))
        .unwrap();
    let result = wait.await.unwrap();
    assert_eq!(result.reason, PoolWaitReason::Message);
    assert!(result.membership_changed);
    assert_eq!(result.messages[0].message, "new member");
}

#[tokio::test]
async fn admin_snapshot_offer_not_manual_ack_generates_one_receipt() {
    let (_directory, store) = store();
    store
        .send("owner", send("p", "admin", "human input"), Some("a"))
        .unwrap();
    let mut wait = Box::pin(store.wait("owner", waiting("p", 35), Some("a")));
    poll_pending(wait.as_mut()).await;
    let inbox = store.admin_messages("owner", 10).unwrap();
    assert!(!inbox[0].acknowledged);
    let result = wait.await.unwrap();
    assert_eq!(result.reason, PoolWaitReason::Receipt);
    assert_eq!(result.delivery_receipts[0].to, "admin");
    collect(&store, "a").await;
    store.admin_messages("owner", 10).unwrap();
    store.admin_ack("owner", &inbox[0].message_id).unwrap();
    assert!(collect(&store, "a").await.is_empty());
}

#[tokio::test]
async fn legacy_pending_messages_migrate_without_guessing_sender_identity() {
    let (directory, store) = store();
    join_pair(&store);
    let sent = store
        .send("owner", send("p", "bob", "legacy peer"), Some("a"))
        .unwrap();
    store
        .send("owner", send("p", "admin", "legacy admin"), Some("b"))
        .unwrap();
    // Restore the schema that existed before communication receipts.
    store
        .storage
        .connection()
        .unwrap()
        .execute_batch(
            "DROP TABLE delivery_receipts;
         DROP TRIGGER member_join_revision;
         DROP TRIGGER member_leave_revision;
         DROP TABLE pool_revisions;
         ALTER TABLE messages DROP COLUMN sender_member_id;
         ALTER TABLE admin_messages DROP COLUMN sender_member_id;
         ALTER TABLE admin_messages DROP COLUMN offered_at;
         ALTER TABLE inbox DROP COLUMN offered_at;
         ALTER TABLE members DROP COLUMN receipt_offered_cursor;",
        )
        .unwrap();
    drop(store);
    let migrated = AgentPoolStore::open(&directory.path().join("agent-pool.sqlite3")).unwrap();
    let comms = collect(&migrated, "b").await;
    assert_eq!(comms.messages[0].message, "legacy peer");
    assert_eq!(
        Some(&comms.messages[0].message_id),
        sent.message_id.as_ref()
    );
    assert_eq!(
        migrated.admin_messages("owner", 10).unwrap()[0].message,
        "legacy admin"
    );
    assert!(collect(&migrated, "a").await.delivery_receipts.is_empty());
    assert!(collect(&migrated, "b").await.delivery_receipts.is_empty());
}

#[tokio::test]
async fn bounded_message_and_receipt_batches_drain_without_losing_the_tail() {
    let (_directory, store) = store();
    join_pair(&store);
    collect(&store, "a").await;
    let mut sent = Vec::new();
    for index in 0..40 {
        sent.push(
            store
                .send("owner", send("p", "bob", &index.to_string()), Some("a"))
                .unwrap()
                .message_id
                .unwrap(),
        );
    }
    assert_eq!(collect(&store, "b").await.messages.len(), 32);
    assert_eq!(collect(&store, "b").await.messages.len(), 8);
    let first = collect(&store, "a").await;
    let second = collect(&store, "a").await;
    assert_eq!(first.delivery_receipts.len(), 32);
    assert_eq!(second.delivery_receipts.len(), 8);
    let received = first
        .delivery_receipts
        .into_iter()
        .chain(second.delivery_receipts)
        .map(|r| r.message_id)
        .collect::<Vec<_>>();
    assert_eq!(sent, received);
    assert!(collect(&store, "a").await.is_empty());
}
