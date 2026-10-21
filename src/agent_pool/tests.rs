use super::test_support::{leave, poll_pending, send, store, store_with_allocation, waiting};
use super::*;
use serde_json::json;

#[tokio::test(flavor = "current_thread")]
async fn contended_pool_database_does_not_stall_tokio_timers() {
    let (_directory, store) = store();
    let storage = std::sync::Arc::new(store);
    let database = std::sync::Arc::clone(&storage);
    let (ready, locked) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let _connection = database.storage.connection().unwrap();
        ready.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(300));
    });
    locked.recv_timeout(Duration::from_secs(2)).unwrap();

    // A concurrent SQLite query may wait for the connection, but must
    // not prevent unrelated tasks on a single Tokio worker from running.
    let blocked = storage.start_tool("owner", Some("contended-session"));
    let result = tokio::time::timeout(Duration::from_millis(40), blocked).await;
    holder.join().unwrap();
    assert!(result.is_err(), "the database wait blocked the Tokio timer");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_storage_future_still_notifies_after_committed_send() {
    let (_directory, store) = store();
    let store = Arc::new(store);
    store
        .send("owner", send("p", "global", "joined"), Some("a"))
        .unwrap();
    store
        .send("owner", send("p", "global", "joined"), Some("b"))
        .unwrap();

    let mut changes = store.coordinator.notification_sender().subscribe();
    let database = Arc::clone(&store);
    let (ready, locked) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let _connection = database.storage.connection().unwrap();
        ready.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(200));
    });
    locked.recv_timeout(Duration::from_secs(2)).unwrap();

    // Dropping an awaiting HTTP handler does not abort an already queued
    // blocking SQLite operation. The notification belongs to its commit,
    // not to the awaiting task that may never resume.
    let mut pending = Box::pin(store.send_async(
        "owner",
        send("p", "bob", "detached committed send"),
        Some("a"),
    ));
    poll_pending(pending.as_mut()).await;
    drop(pending);
    holder.join().unwrap();

    tokio::time::timeout(Duration::from_secs(2), changes.changed())
        .await
        .expect("committed mutation did not notify waiters")
        .unwrap();
    let turn = store.start_tool("owner", Some("b")).await.unwrap();
    let comms = store
        .finish_tool_turn("owner", Some("b"), turn.as_ref())
        .await
        .unwrap();
    assert!(
        comms
            .messages
            .iter()
            .any(|message| message.message == "detached committed send")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn pending_wait_ignores_message_only_noop_and_unrelated_membership_changes() {
    let (_directory, store) = store();
    for session in ["a", "b"] {
        store
            .send("owner", send("selected", "global", "join"), Some(session))
            .unwrap();
    }
    // Drain Bob's join announcement and retire it before starting the wait.
    let turn = store.start_tool("owner", Some("a")).await.unwrap();
    store
        .finish_tool_turn("owner", Some("a"), turn.as_ref())
        .await
        .unwrap();
    drop(turn);

    let mut waiter = Box::pin(store.wait("owner", waiting("selected", 5), Some("a")));
    poll_pending(waiter.as_mut()).await;
    store
        .send("owner", send("other", "global", "join"), Some("other"))
        .unwrap();
    store
        .send(
            "another-account",
            send("selected", "global", "join"),
            Some("x"),
        )
        .unwrap();
    store
        .send("owner", leave("selected"), Some("not-a-member"))
        .unwrap();
    store
        .send(
            "owner",
            send("selected", "alice", "message only"),
            Some("b"),
        )
        .unwrap();

    let result = waiter.await.unwrap();
    assert_eq!(result.reason, PoolWaitReason::Message);
    assert!(!result.membership_changed);
    assert_eq!(result.messages[0].message, "message only");
}

#[tokio::test(start_paused = true)]
async fn wait_bounds_immediate_snapshot_and_timeout() {
    let (_directory, store) = store();
    for seconds in [0, 4, 46, u64::MAX] {
        assert!(
            store
                .wait("owner", waiting("p", seconds), Some("observer"))
                .await
                .is_err()
        );
    }
    for value in [
        json!(null),
        json!(-1),
        json!(4),
        json!(46),
        json!(10.5),
        json!("35"),
    ] {
        assert!(
            serde_json::from_value::<PoolWaitArgs>(json!({"pool":"p", "wait_seconds":value}))
                .is_err()
        );
    }
    assert!(
        serde_json::from_value::<PoolMembersArgs>(json!({"pool":"p", "wait_seconds":35})).is_err()
    );
    let args: PoolWaitArgs = serde_json::from_value(json!({"pool":"p"})).unwrap();
    assert_eq!(args.wait_seconds, 35);
    assert!(store.wait("owner", args, None).await.is_err());
    let before = tokio::time::Instant::now();
    let members = store
        .members("owner", PoolMembersArgs { pool: "p".into() }, None)
        .unwrap();
    assert!(members.agents.is_empty());
    assert_eq!(tokio::time::Instant::now(), before);
    for seconds in [5, 10, 45] {
        let before = tokio::time::Instant::now();
        let result = store
            .wait("owner", waiting("p", seconds), Some("observer"))
            .await
            .unwrap();
        assert_eq!(result.reason, PoolWaitReason::Timeout);
        assert!(result.messages.is_empty() && !result.membership_changed);
        assert_eq!(
            tokio::time::Instant::now() - before,
            Duration::from_secs(seconds)
        );
    }
}

#[tokio::test]
async fn wait_wakes_all_observers_and_detects_transient_memberships() {
    let (_directory, store) = store();
    let mut first = Box::pin(store.wait("owner", waiting("p", 35), Some("observer-1")));
    let mut second = Box::pin(store.wait("owner", waiting("p", 35), Some("observer-2")));
    poll_pending(first.as_mut()).await;
    poll_pending(second.as_mut()).await;
    store
        .send("owner", send("other", "global", "join"), Some("other"))
        .unwrap();
    store
        .send("other-owner", send("p", "global", "join"), Some("other"))
        .unwrap();
    poll_pending(first.as_mut()).await;
    poll_pending(second.as_mut()).await;
    // Even a join and leave coalesced into one wake is a membership change.
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    store.send("owner", leave("p"), Some("a")).unwrap();
    for result in [first.await.unwrap(), second.await.unwrap()] {
        assert_eq!(result.reason, PoolWaitReason::MembershipChanged);
        assert!(result.membership_changed && result.messages.is_empty());
    }
    let mut reused = Box::pin(store.wait("owner", waiting("p", 35), Some("observer")));
    poll_pending(reused.as_mut()).await;
    store
        .send("owner", send("p", "global", "rejoin"), Some("a"))
        .unwrap();
    assert!(reused.await.unwrap().membership_changed);
    let mut left = Box::pin(store.wait("owner", waiting("p", 35), Some("observer")));
    poll_pending(left.as_mut()).await;
    store.admin_terminate("owner", "p", "alice").unwrap();
    assert_eq!(
        left.await.unwrap().reason,
        PoolWaitReason::MembershipChanged
    );
}

#[tokio::test]
async fn wait_filters_pool_and_cancellation_keeps_unoffered_messages() {
    let (_directory, store) = store();
    for pool in ["p", "q"] {
        store
            .send("owner", send(pool, "global", "join"), Some("a"))
            .unwrap();
    }
    let mut waiter = Box::pin(store.wait("owner", waiting("p", 35), Some("a")));
    poll_pending(waiter.as_mut()).await;
    store
        .admin_send("owner", "q", "alice", "other-pool")
        .unwrap();
    poll_pending(waiter.as_mut()).await;
    store
        .admin_send("owner", "p", "alice", "selected-pool")
        .unwrap();
    let result = waiter.await.unwrap();
    assert_eq!(result.reason, PoolWaitReason::Message);
    assert!(!result.membership_changed);
    assert_eq!(result.messages.len(), 1);
    assert_eq!(result.messages[0].message, "selected-pool");
    let turn = store.start_tool("owner", Some("a")).await.unwrap();
    let comms = store
        .finish_tool_turn("owner", Some("a"), turn.as_ref())
        .await
        .unwrap();
    assert_eq!(comms.messages[0].message, "other-pool");
    drop(turn);
    let mut cancelled = Box::pin(store.wait("owner", waiting("p", 35), Some("a")));
    poll_pending(cancelled.as_mut()).await;
    drop(cancelled);
    store.admin_send("owner", "p", "alice", "retain").unwrap();
    let received = store
        .wait("owner", waiting("p", 35), Some("a"))
        .await
        .unwrap();
    assert_eq!(received.messages[0].message, "retain");
}

#[tokio::test]
async fn wait_observes_expiry_without_a_notification() {
    let (_directory, store) = store();
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    store
        .storage
        .connection()
        .unwrap()
        .execute("UPDATE members SET expires=?1", [now_ms() + 100])
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        store.wait("owner", waiting("p", 35), Some("observer")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.reason, PoolWaitReason::MembershipChanged);
    assert!(result.membership_changed && result.messages.is_empty());
}

#[test]
fn broadcast_identifies_each_recipient_and_original_target() {
    let (_directory, store) = store();
    for session in ["a", "b", "c"] {
        store
            .send("owner", send("p", "global", "join"), Some(session))
            .unwrap();
        store
            .storage
            .begin_tool_state("owner", Some(session))
            .unwrap();
    }
    for session in ["a", "b", "c"] {
        store
            .storage
            .collect_tool_messages("owner", Some(session))
            .unwrap();
        store
            .storage
            .begin_tool_state("owner", Some(session))
            .unwrap();
    }
    let before = now_ms();
    let sent = store
        .send("owner", send("p", "global", "broadcast"), Some("a"))
        .unwrap();
    for (session, recipient) in [("b", "bob"), ("c", "carol")] {
        let peers = store
            .storage
            .collect_tool_messages("owner", Some(session))
            .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(Some(&peers[0].message_id), sent.message_id.as_ref());
        assert_eq!(peers[0].to, recipient);
        assert_eq!(peers[0].target, "global");
        assert!((before..=now_ms()).contains(&peers[0].sent_at_ms));
    }
}

#[test]
fn admin_global_broadcast_includes_all_active_members() {
    let (_directory, store) = store();
    let owner = "account-a";
    for session in ["a", "b"] {
        store
            .send(owner, send("project", "global", "join"), Some(session))
            .unwrap();
    }

    // Clear Bob's join announcement from Alice before the admin broadcast.
    store.storage.begin_tool_state(owner, Some("a")).unwrap();
    store
        .storage
        .collect_tool_messages(owner, Some("a"))
        .unwrap();
    store.storage.begin_tool_state(owner, Some("a")).unwrap();

    let sent = store
        .admin_send(owner, "project", "global", "notice")
        .unwrap();
    assert_eq!(sent.recipients, ["alice", "bob"]);
    assert!(sent.message_id.is_some());

    for (session, recipient) in [("a", "alice"), ("b", "bob")] {
        store
            .storage
            .begin_tool_state(owner, Some(session))
            .unwrap();
        let peers = store
            .storage
            .collect_tool_messages(owner, Some(session))
            .unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(Some(&peers[0].message_id), sent.message_id.as_ref());
        assert_eq!(peers[0].from, ADMIN_AGENT);
        assert_eq!(peers[0].to, recipient);
        assert_eq!(peers[0].target, "global");
        assert_eq!(peers[0].message, "notice");
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

    store.storage.begin_tool_state(owner, Some("b")).unwrap();
    let offered = store
        .storage
        .collect_tool_messages(owner, Some("b"))
        .unwrap();
    assert_eq!(offered.len(), 1);
    assert_eq!(offered[0].from, "alice");
    assert_eq!(offered[0].message, "team");
    store.storage.begin_tool_state(owner, Some("b")).unwrap();
    assert!(
        store
            .storage
            .collect_tool_messages(owner, Some("b"))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn automatic_names_exhaust_leave_reuse_and_failed_join_rolls_back() {
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

    store.send("owner", leave("p"), Some("a")).unwrap();
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
fn default_name_allocation_reuses_first_available_gap() {
    let (_directory, store) = store();
    for (session, expected) in [("a", "alice"), ("b", "bob")] {
        assert_eq!(
            store
                .send("owner", send("p", "global", "join"), Some(session))
                .unwrap()
                .assigned_agent
                .as_deref(),
            Some(expected)
        );
    }
    store.send("owner", leave("p"), Some("a")).unwrap();
    assert_eq!(
        store
            .send("owner", send("p", "global", "join"), Some("c"))
            .unwrap()
            .assigned_agent
            .as_deref(),
        Some("alice")
    );
}

#[test]
fn name_rotation_skips_earlier_gaps_and_wraps() {
    let (_directory, store) = store_with_allocation(AgentNameAllocation::RoundRobin);
    let assigned = |session: &str| {
        store
            .send("owner", send("p", "global", "join"), Some(session))
            .unwrap()
            .assigned_agent
            .unwrap()
    };

    assert_eq!(assigned("a"), "alice");
    assert_eq!(assigned("b"), "bob");
    store.send("owner", leave("p"), Some("a")).unwrap();

    // Sending from a known session must neither rebind it nor move the cursor.
    assert!(
        store
            .send("owner", send("p", "global", "again"), Some("b"))
            .unwrap()
            .assigned_agent
            .is_none()
    );
    assert_eq!(assigned("c"), "carol"); // Skips free alice.
    assert_eq!(assigned("d"), "dave");
    assert_eq!(assigned("e"), "alice"); // Wraps around.
    store.send("owner", leave("p"), Some("b")).unwrap();
    assert_eq!(assigned("f"), "bob");
}

#[test]
fn name_rotation_is_scoped_to_principal_and_pool_including_admin_sends() {
    let (_directory, store) = store_with_allocation(AgentNameAllocation::RoundRobin);
    let assigned = |owner: &str, pool: &str, session: &str, target: &str| {
        store
            .send(owner, send(pool, target, "join"), Some(session))
            .unwrap()
            .assigned_agent
            .unwrap()
    };
    assert_eq!(assigned("owner", "p", "a", "global"), "alice");
    assert_eq!(assigned("owner", "q", "b", "admin"), "alice");
    assert_eq!(assigned("other", "p", "c", "global"), "alice");
    assert_eq!(assigned("owner", "q", "d", "global"), "bob");
    assert_eq!(assigned("owner", "p", "e", "global"), "bob");
    assert_eq!(assigned("other", "p", "f", "global"), "bob");
}

#[test]
fn failed_first_send_does_not_advance_name_rotation() {
    let (_directory, store) = store_with_allocation(AgentNameAllocation::RoundRobin);
    assert_eq!(
        store
            .send("owner", send("p", "global", "join"), Some("a"))
            .unwrap()
            .assigned_agent
            .as_deref(),
        Some("alice")
    );
    store.send("owner", leave("p"), Some("a")).unwrap();
    // This send would select bob, but the target is missing, so it rolls back.
    assert!(
        store
            .send("owner", send("p", "missing", "fail"), Some("bad"))
            .is_err()
    );
    assert_eq!(
        store
            .send("owner", send("p", "global", "join"), Some("good"))
            .unwrap()
            .assigned_agent
            .as_deref(),
        Some("bob")
    );
}

#[test]
fn restart_resets_rotation_without_rebinding_resumed_chat_sessions() {
    let (directory, store) = store_with_allocation(AgentNameAllocation::RoundRobin);
    let path = directory.path().join("agent-pool.sqlite3");
    for (session, name) in [("chat-a", "alice"), ("chat-b", "bob")] {
        assert_eq!(
            store
                .send("owner", send("p", "global", "join"), Some(session))
                .unwrap()
                .assigned_agent
                .as_deref(),
            Some(name)
        );
    }
    store.send("owner", leave("p"), Some("chat-a")).unwrap();
    assert_eq!(
        store
            .send("owner", send("p", "global", "join"), Some("chat-c"))
            .unwrap()
            .assigned_agent
            .as_deref(),
        Some("carol")
    );
    drop(store);

    let resumed = AgentPoolStore::open_with_name_allocation(
        &path,
        Duration::from_millis(DEFAULT_TTL_MS as u64),
        ["alice", "bob", "carol", "dave"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        AgentNameAllocation::RoundRobin,
    )
    .unwrap();
    for (session, name) in [("chat-b", "bob"), ("chat-c", "carol")] {
        // A resumed ChatGPT session keeps the SQLite membership, not a new name.
        assert_eq!(
            resumed
                .members("owner", PoolMembersArgs { pool: "p".into() }, Some(session),)
                .unwrap()
                .self_agent
                .as_deref(),
            Some(name)
        );
        assert!(
            resumed
                .send("owner", send("p", "global", "resume"), Some(session))
                .unwrap()
                .assigned_agent
                .is_none()
        );
    }
    // Memory resets on restart; the first free slot wins without rebinding
    // the two still-active sessions.
    assert_eq!(
        resumed
            .send("owner", send("p", "global", "join"), Some("chat-d"))
            .unwrap()
            .assigned_agent
            .as_deref(),
        Some("alice")
    );
    assert_eq!(
        resumed
            .send("owner", send("p", "global", "join"), Some("chat-e"))
            .unwrap()
            .assigned_agent
            .as_deref(),
        Some("dave")
    );
}

#[test]
fn leave_is_pool_scoped_for_a_session() {
    let (_directory, store) = store();
    store
        .send("owner", send("left", "global", "join"), Some("session"))
        .unwrap();
    store
        .send("owner", send("right", "global", "join"), Some("session"))
        .unwrap();

    store.send("owner", leave("left"), Some("session")).unwrap();
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

    // Repeated leave is intentionally idempotent.
    store.send("owner", leave("left"), Some("session")).unwrap();
}

#[tokio::test]
async fn an_older_wait_still_receives_after_a_newer_call_finishes() {
    let (_directory, store) = store();
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    let mut waiter = Box::pin(store.wait("owner", waiting("p", 35), Some("a")));
    poll_pending(waiter.as_mut()).await;
    let newer = store.start_tool("owner", Some("a")).await.unwrap();
    assert!(
        store
            .finish_tool_turn("owner", Some("a"), newer.as_ref())
            .await
            .unwrap()
            .is_empty()
    );
    drop(newer);
    store
        .admin_send("owner", "p", "alice", "arrives during wait")
        .unwrap();
    let received = waiter.await.unwrap();
    assert_eq!(received.reason, PoolWaitReason::Message);
    assert_eq!(received.messages[0].message, "arrives during wait");
}

#[tokio::test]
async fn overlapping_calls_reoffer_and_do_not_retire_each_others_batches() {
    let (_directory, store) = store();
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    store.admin_send("owner", "p", "alice", "parallel").unwrap();
    let first = store.start_tool("owner", Some("a")).await.unwrap();
    let offered = store
        .finish_tool_turn("owner", Some("a"), first.as_ref())
        .await
        .unwrap();
    let second = store.start_tool("owner", Some("a")).await.unwrap();
    let repeated = store
        .finish_tool_turn("owner", Some("a"), second.as_ref())
        .await
        .unwrap();
    assert_eq!(offered.messages, repeated.messages);
    assert_eq!(offered.messages.len(), 1);
    drop(first);
    drop(second);
    let next = store.start_tool("owner", Some("a")).await.unwrap();
    assert!(
        store
            .finish_tool_turn("owner", Some("a"), next.as_ref())
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn cancelling_a_newer_call_does_not_suppress_an_older_call() {
    let (_directory, store) = store();
    store
        .send("owner", send("p", "global", "join"), Some("a"))
        .unwrap();
    let first = store.start_tool("owner", Some("a")).await.unwrap();
    let cancelled = store.start_tool("owner", Some("a")).await.unwrap();
    store
        .admin_send("owner", "p", "alice", "retain after cancellation")
        .unwrap();
    drop(cancelled);
    let offered = store
        .finish_tool_turn("owner", Some("a"), first.as_ref())
        .await
        .unwrap();
    assert_eq!(offered.messages[0].message, "retain after cancellation");
}

#[tokio::test]
async fn same_session_in_different_accounts_has_independent_turns_and_acks() {
    let (_directory, store) = store();
    for principal in ["one", "two"] {
        store
            .send(principal, send("p", "global", "join"), Some("same-session"))
            .unwrap();
        store
            .admin_send(principal, "p", "alice", principal)
            .unwrap();
    }
    let first = store.start_tool("one", Some("same-session")).await.unwrap();
    let second = store.start_tool("two", Some("same-session")).await.unwrap();
    for (principal, turn) in [("one", &first), ("two", &second)] {
        let offered = store
            .finish_tool_turn(principal, Some("same-session"), turn.as_ref())
            .await
            .unwrap();
        assert_eq!(offered.messages.len(), 1);
        assert_eq!(offered.messages[0].message, principal);
    }
    drop(first);
    let acknowledged = store.start_tool("one", Some("same-session")).await.unwrap();
    assert!(
        store
            .finish_tool_turn("one", Some("same-session"), acknowledged.as_ref())
            .await
            .unwrap()
            .is_empty()
    );
    // Account one's next call cannot ACK account two's durable offer.
    let retained = store
        .finish_tool_turn("two", Some("same-session"), second.as_ref())
        .await
        .unwrap();
    assert_eq!(retained.messages.len(), 1);
    assert_eq!(retained.messages[0].message, "two");
}

#[tokio::test]
async fn failed_join_send_is_invisible_to_membership_waiters() {
    use std::future::Future;
    use std::task::Poll;

    let (_directory, store) = store();
    let mut waiter = Box::pin(store.wait("owner", waiting("p", 35), Some("observer")));
    std::future::poll_fn(|context| {
        assert!(waiter.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(
        store
            .send("owner", send("p", "missing", "rollback"), Some("failed"))
            .is_err()
    );
    assert!(
        store
            .members("owner", PoolMembersArgs { pool: "p".into() }, None)
            .unwrap()
            .agents
            .is_empty()
    );
    std::future::poll_fn(|context| {
        assert!(waiter.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    let joined = store
        .send("owner", send("p", "global", "commit"), Some("joined"))
        .unwrap();
    assert_eq!(joined.assigned_agent.as_deref(), Some("alice"));
    let observed = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observed.reason, PoolWaitReason::MembershipChanged);
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
    store.storage.begin_tool_state("owner", Some("b")).unwrap();
    assert_eq!(
        store
            .storage
            .collect_tool_messages("owner", Some("b"))
            .unwrap()[0]
            .message,
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
    restarted
        .storage
        .begin_tool_state("owner", Some("b"))
        .unwrap();
    assert_eq!(
        restarted
            .storage
            .collect_tool_messages("owner", Some("b"))
            .unwrap()[0]
            .message,
        "durable"
    );
    restarted
        .storage
        .begin_tool_state("owner", Some("b"))
        .unwrap();
    assert!(
        restarted
            .storage
            .collect_tool_messages("owner", Some("b"))
            .unwrap()
            .is_empty()
    );
    {
        let connection = restarted.storage.connection().unwrap();
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
        .admin_send(owner, "project", "alice", "continue")
        .unwrap();
    assert_eq!(from_admin.recipients, ["alice"]);

    store
        .storage
        .begin_tool_state(owner, Some("alice-session"))
        .unwrap();
    let offered = store
        .storage
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
                    pool: "project".into()
                },
                Some("alice-session"),
            )
            .unwrap()
            .agents
            .is_empty()
    );
}

#[test]
fn admin_messages_are_removed_only_on_restart_after_ack() {
    let (directory, store) = store();
    let path = directory.path().join("agent-pool.sqlite3");
    let acknowledged_id = store
        .send("owner", send("p", ADMIN_AGENT, "read"), Some("a"))
        .unwrap()
        .message_id
        .unwrap();
    assert!(store.admin_ack("owner", &acknowledged_id).unwrap());
    store
        .send("owner", send("p", ADMIN_AGENT, "unread"), Some("a"))
        .unwrap();
    store
        .send("other", send("p", ADMIN_AGENT, "other unread"), Some("a"))
        .unwrap();
    // Normal pool cleanup and repeated snapshots must retain ACKed rows.
    store.admin_members("owner").unwrap();
    for _ in 0..2 {
        let messages = store.admin_messages("owner", 10).unwrap();
        assert_eq!(messages.len(), 2);
        assert!(
            messages
                .iter()
                .find(|m| m.message_id == acknowledged_id)
                .unwrap()
                .acknowledged
        );
        assert!(
            !messages
                .iter()
                .find(|m| m.message == "unread")
                .unwrap()
                .acknowledged
        );
    }
    drop(store);

    let restarted = AgentPoolStore::open(&path).unwrap();
    let messages = restarted.admin_messages("owner", 10).unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message, "unread");
    assert!(!messages[0].acknowledged);
    let other = restarted.admin_messages("other", 10).unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].message, "other unread");
    assert!(!other[0].acknowledged);
}

#[test]
fn legacy_admin_messages_gain_ack_state_without_losing_messages() {
    let (directory, store) = store();
    let path = directory.path().join("agent-pool.sqlite3");
    let args = send("p", ADMIN_AGENT, "legacy message");
    let message_id = store
        .send("owner", args, Some("a"))
        .unwrap()
        .message_id
        .unwrap();
    // Recreate the schema used before admin ACKs existed.
    store
        .storage
        .connection()
        .unwrap()
        .execute_batch("ALTER TABLE admin_messages DROP COLUMN acknowledged")
        .unwrap();
    drop(store);

    let migrated = AgentPoolStore::open(&path).unwrap();
    let messages = migrated.admin_messages("owner", 10).unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, message_id);
    assert_eq!(messages[0].message, "legacy message");
    assert!(!messages[0].acknowledged);
    assert!(migrated.admin_ack("owner", &message_id).unwrap());
    drop(migrated);

    let restarted = AgentPoolStore::open(&path).unwrap();
    assert!(restarted.admin_messages("owner", 10).unwrap().is_empty());
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
