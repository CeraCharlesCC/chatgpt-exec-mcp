//! History, correlation, and archive worker regressions.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::*;

#[test]
fn disabled_sink_emits_no_history_and_allocates_no_archive() {
    let hub = ActivityHub::disabled();
    assert!(!hub.is_enabled());
    assert!(hub.archive_dir.is_none());
    assert!(hub.worker.get().is_none());

    for _ in 0..1_000 {
        assert_eq!(
            hub.emit(
                "tool.start",
                "info",
                Some("session"),
                vec![],
                json!({"large":"value"})
            ),
            0
        );
    }
    assert!(hub.snapshot(200).is_empty());
    assert!(hub.snapshot_all().unwrap().is_empty());
    for before in [None, Some(1), Some(u64::MAX)] {
        let page = hub.page(before, 200);
        assert!(page.events.is_empty());
        assert_eq!(page.next_before_id, None);
        assert!(!page.has_more);
    }
    assert_eq!(hub.clear(), 0);
    assert_eq!(hub.emit("after-clear", "info", None, vec![], json!({})), 0);
    assert!(hub.worker.get().is_none());
    let state = lock_state(&hub.shared);
    assert_eq!(state.next_id, 0);
    assert!(state.recent.is_empty());
    assert!(state.pending.is_empty());
    assert!(state.archived.is_empty());
}

#[test]
fn disabled_sink_skips_argument_conversion_and_enabled_history_is_unchanged() {
    struct UnexpectedStringConversion;
    impl From<UnexpectedStringConversion> for String {
        fn from(_: UnexpectedStringConversion) -> Self {
            panic!("disabled activity should never convert event labels")
        }
    }

    let disabled = ActivityHub::disabled();
    assert_eq!(
        disabled.emit(
            UnexpectedStringConversion,
            UnexpectedStringConversion,
            None,
            vec![],
            json!({})
        ),
        0
    );
    let enabled = ActivityHub::new(500);
    assert!(enabled.is_enabled());
    assert!(enabled.archive_dir.is_some());
    emit(&enabled, 500);
    await_archive(&enabled);
    assert_eq!(
        history_newest_first(&enabled),
        (1..=500).rev().collect::<Vec<_>>()
    );
    assert_eq!(
        enabled.emit("continued", "info", None, vec![], json!({})),
        501
    );
}

fn emit(hub: &ActivityHub, count: usize) {
    for n in 0..count {
        hub.emit("tool.start", "info", None, vec![], json!({"n":n}));
    }
}

fn await_archive(hub: &ActivityHub) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if lock_state(&hub.shared).pending.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "archive worker did not drain pending chunks"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn history_newest_first(hub: &ActivityHub) -> Vec<u64> {
    let mut ids = Vec::new();
    let mut before = None;
    loop {
        let page = hub.page(before, 200);
        assert!(page.events.len() <= 200);
        assert!(page.events.windows(2).all(|pair| pair[0].id < pair[1].id));
        ids.extend(page.events.iter().rev().map(|event| event.id));
        if !page.has_more {
            assert!(page.next_before_id.is_none());
            break;
        }
        let cursor = page.next_before_id.expect("older page cursor");
        assert_eq!(Some(cursor), page.events.first().map(|event| event.id));
        before = Some(cursor);
    }
    ids
}

#[test]
fn small_capacity_preserves_fixed_size_history() {
    let hub = ActivityHub::new(10);
    emit(&hub, 20);
    assert_eq!(
        hub.snapshot(100)
            .iter()
            .map(|event| event.id)
            .collect::<Vec<_>>(),
        (11..=20).collect::<Vec<_>>()
    );
    assert!(hub.archive_dir.is_none());
    assert_eq!(hub.clear(), 10);
    assert!(hub.snapshot(100).is_empty());
    assert_eq!(hub.emit("again", "info", None, vec![], json!({})), 21);
}

#[test]
fn spills_exactly_at_500_and_pages_across_ram_and_archive() {
    let hub = ActivityHub::new(500);
    emit(&hub, 499);
    assert_eq!(hub.snapshot(500).len(), 499);
    assert!(lock_state(&hub.shared).pending.is_empty());
    emit(&hub, 1);
    let snapshot = hub.snapshot(500);
    assert_eq!(snapshot.len(), 250);
    assert_eq!(snapshot[0].id, 251);
    assert_eq!(snapshot.last().unwrap().id, 500);

    // While compression is pending/in-flight, pages can still see those IDs.
    assert_eq!(
        history_newest_first(&hub),
        (1..=500).rev().collect::<Vec<_>>()
    );
    await_archive(&hub);
    let page = hub.page(None, 200);
    assert!(page.has_more);
    assert_eq!(page.next_before_id, Some(301));
    assert_eq!(page.events.first().unwrap().id, 301);
    assert_eq!(page.events.last().unwrap().id, 500);
    let page = hub.page(Some(301), 200);
    assert_eq!(page.events.first().unwrap().id, 101);
    assert_eq!(page.events.last().unwrap().id, 300);
    assert_eq!(
        history_newest_first(&hub),
        (1..=500).rev().collect::<Vec<_>>()
    );
}

#[test]
fn full_snapshot_combines_archived_pending_and_recent_events_once() {
    let hub = ActivityHub::new(500);
    emit(&hub, 500);
    await_archive(&hub);
    let hook = Arc::new(WorkerTestHook {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
        finished: Barrier::new(2),
    });
    *hub.shared.worker_hook.lock().unwrap() = Some(Arc::clone(&hook));
    emit(&hub, 250);
    hook.entered.wait();
    // One archived chunk, one pending chunk, and the recent queue must all
    // be visible while the compression worker is paused outside the mutex.
    let pending = hub.snapshot_all();
    hook.resume.wait();
    hook.finished.wait();
    *hub.shared.worker_hook.lock().unwrap() = None;
    await_archive(&hub);
    let pending = pending.unwrap();
    assert_eq!(
        pending.iter().map(|event| event.id).collect::<Vec<_>>(),
        (1..=750).collect::<Vec<_>>()
    );
    assert_eq!(
        serde_json::to_value(&pending).unwrap(),
        serde_json::to_value(hub.snapshot_all().unwrap()).unwrap()
    );
    assert_eq!(hub.clear(), 750);
    assert!(hub.snapshot_all().unwrap().is_empty());
}

#[test]
fn rotates_8_chunks_and_enforces_retention() {
    let hub = ActivityHub::new(500);
    // Allow the archive worker to finish between spills. Retention tests
    // must not conflate an intentionally bounded overload queue with disk rotation.
    for i in 1..=2_500 {
        hub.emit("tool.start", "info", None, vec![], json!({"i":i}));
        if i % 250 == 0 {
            await_archive(&hub);
        }
    }
    await_archive(&hub);
    let state = lock_state(&hub.shared);
    assert_eq!(state.archived.len(), 8);
    assert_eq!(state.archived.front().unwrap().first_id, 251);
    assert_eq!(state.archived.back().unwrap().last_id, 2_250);
    assert_eq!(state.recent.len(), 250);
    assert!(state.archive_bytes <= MAX_ARCHIVE_BYTES);
    drop(state);
    assert_eq!(
        history_newest_first(&hub),
        (251..=2_500).rev().collect::<Vec<_>>()
    );
    assert_eq!(
        hub.snapshot_all()
            .unwrap()
            .iter()
            .map(|event| event.id)
            .collect::<Vec<_>>(),
        (251..=2_500).collect::<Vec<_>>()
    );
    assert_eq!(hub.page(None, usize::MAX).events.len(), 200);
    assert_eq!(hub.page(None, 0).events.len(), 1);
}

#[test]
fn concurrent_emitters_generate_unique_ordered_ids() {
    let hub = Arc::new(ActivityHub::new(500));
    let threads: Vec<_> = (0..6)
        .map(|_| {
            let hub = Arc::clone(&hub);
            thread::spawn(move || {
                (0..200)
                    .map(|n| hub.emit("event", "info", None, vec![], json!({"n":n})))
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut returned = Vec::new();
    for task in threads {
        returned.extend(task.join().unwrap());
    }
    returned.sort_unstable();
    assert_eq!(returned, (1..=1_200).collect::<Vec<_>>());
    await_archive(&hub);
    let retained = history_newest_first(&hub);
    assert_eq!(retained, (1..=1_200).rev().collect::<Vec<_>>());
}

#[test]
fn unpaced_burst_sheds_oldest_history_without_holes() {
    let hub = ActivityHub::new(500);
    emit(&hub, 3_500);
    await_archive(&hub);
    let ids = history_newest_first(&hub);
    assert!(!ids.is_empty());
    assert_eq!(ids[0], 3_500);
    assert!(
        ids.windows(2).all(|pair| pair[0] == pair[1] + 1),
        "saturated compression must leave a contiguous newest suffix"
    );
    assert!(ids.len() <= MAX_RETAINED_EVENTS);
    let state = lock_state(&hub.shared);
    assert!(state.pending.len() <= MAX_PENDING_CHUNKS);
    assert!(state.archived.len() <= MAX_ARCHIVE_CHUNKS);
    assert!(state.archive_bytes <= MAX_ARCHIVE_BYTES);
    drop(state);
    assert_eq!(hub.clear(), ids.len());
}

#[test]
fn clear_invalidates_pending_and_inflight_chunks_without_reusing_ids() {
    let hub = ActivityHub::new(500);
    let hook = Arc::new(WorkerTestHook {
        entered: Barrier::new(2),
        resume: Barrier::new(2),
        finished: Barrier::new(2),
    });
    *hub.shared.worker_hook.lock().unwrap() = Some(Arc::clone(&hook));
    emit(&hub, 500);
    hook.entered.wait(); // worker has claimed the pending chunk
    let cleared = hub.clear();
    hook.resume.wait(); // let the obsolete compression finish
    hook.finished.wait(); // stale output is rejected and unlinked
    *hub.shared.worker_hook.lock().unwrap() = None;
    assert_eq!(cleared, 500);
    assert_eq!(
        fs::read_dir(hub.archive_dir.as_ref().unwrap().path())
            .unwrap()
            .count(),
        0
    );
    assert!(hub.snapshot(200).is_empty());
    assert!(hub.page(None, 200).events.is_empty());
    assert_eq!(
        hub.emit("after-clear", "info", None, vec![], json!({})),
        501
    );
    await_archive(&hub);
    assert_eq!(history_newest_first(&hub), vec![501]);
    assert_eq!(hub.clear(), 1);
    assert_eq!(hub.clear(), 0);
    assert_eq!(hub.emit("again", "info", None, vec![], json!({})), 502);
}

#[test]
fn disk_failure_drops_pending_chunks_and_keeps_emitting() {
    let hub = ActivityHub::new(500);
    emit(&hub, 500);
    await_archive(&hub);
    assert_eq!(lock_state(&hub.shared).archived.len(), 1);
    // Simulate a temp-directory deletion, permissions, or exhausted disk.
    // The only worker's writes now fail without crashing or blocking emit.
    let dir = hub.archive_dir.as_ref().unwrap().path().to_path_buf();
    fs::remove_dir_all(&dir).unwrap();
    emit(&hub, 4_000);
    await_archive(&hub);
    let state = lock_state(&hub.shared);
    assert!(state.pending.len() <= MAX_PENDING_CHUNKS);
    assert!(state.archived.is_empty());
    assert_eq!(state.archive_bytes, 0);
    drop(state);
    assert!(hub.snapshot(200).len() <= 200);
    assert!(hub.page(None, 200).events.len() <= 200);
    let ids = history_newest_first(&hub);
    assert!(ids.windows(2).all(|pair| pair[0] == pair[1] + 1));
    assert_eq!(
        hub.emit("after-disk-failure", "info", None, vec![], json!({})),
        4_501
    );
}

#[test]
fn temp_files_are_removed_on_clear_and_drop() {
    let hub = ActivityHub::new(500);
    let dir = hub.archive_dir.as_ref().unwrap().path().to_path_buf();
    emit(&hub, 500);
    await_archive(&hub);
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
    assert_eq!(hub.clear(), 500);
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 0);
    emit(&hub, 500);
    await_archive(&hub);
    assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
    drop(hub);
    assert!(!dir.exists());
}

#[test]
fn corrupted_archive_does_not_break_snapshot_or_page() {
    let hub = ActivityHub::new(500);
    emit(&hub, 500);
    await_archive(&hub);
    let file = lock_state(&hub.shared).archived[0].file.path.clone();
    fs::write(&file, b"not a zstd frame").unwrap();
    assert!(
        hub.snapshot_all().is_err(),
        "a full export must not silently omit a corrupt archive"
    );
    let snapshot = hub.snapshot(200);
    assert_eq!(snapshot.first().unwrap().id, 301);
    assert_eq!(snapshot.last().unwrap().id, 500);
    let page = hub.page(Some(251), 200);
    assert!(page.events.is_empty());
    assert!(!page.has_more);
}

#[test]
fn disk_budget_discards_oldest_archives() {
    let hub = ActivityHub::new(500);
    emit(&hub, 500);
    await_archive(&hub);
    let mut state = lock_state(&hub.shared);
    assert_eq!(state.archived.len(), 1);
    let bytes = state.archived[0].file.bytes;
    state.archive_bytes = MAX_ARCHIVE_BYTES + 1;
    let evicted = state.trim_archives();
    assert_eq!(evicted.len(), 1);
    assert!(state.archived.is_empty());
    assert!(state.archive_bytes <= MAX_ARCHIVE_BYTES);
    drop(state);
    drop(evicted);
    assert!(bytes > 0);
}

#[test]
fn typed_message_lifecycle_preserves_legacy_fields_and_is_self_contained() {
    let hub = ActivityHub::new(3);
    let args: PoolSendArgs = serde_json::from_value(json!({
        "pool":"team", "target":"Bob", "message":"hello", "in_reply_to":"m0"
    }))
    .unwrap();
    let message_context = ActivityMessage::from_pool_send_args(&args).unwrap();
    let start = hub.emit(
        "tool.start",
        "info",
        Some("session"),
        vec![],
        json!({"tool":"pool_send", "arguments":args}),
    );
    let begin = hub.snapshot(1).pop().unwrap();
    assert_eq!(begin.schema_version, 1);
    assert_eq!(begin.detail["arguments"]["message"], "hello");
    assert_eq!(
        begin.lifecycle.as_ref().unwrap().event_type,
        ActivityLifecycleType::MessageSend
    );
    assert_eq!(
        begin
            .lifecycle
            .as_ref()
            .unwrap()
            .message
            .as_ref()
            .unwrap()
            .status,
        Some(ActivityMessageStatus::Sending)
    );

    // Evict the start: finish must still contain its own complete message.
    for _ in 0..3 {
        hub.emit("noise", "info", None, vec![], json!({}));
    }
    let finish = hub.emit_with_context(
        "tool.finish",
        "info",
        Some("session"),
        vec![],
        json!({"tool":"pool_send", "start_event_id": start,
                "result":{"value":{"assigned_agent":"Alice", "message_id":"m1",
                    "recipients":["Bob"]}, "delivery_receipts":[{"pool":"team", "to":"Bob"}]}}),
        ActivityContext {
            pool: Some("team".to_owned()),
            message: Some(message_context),
        },
    );
    let event = hub.snapshot(1).pop().unwrap();
    assert_eq!(event.id, finish);
    assert_eq!(event.kind, "tool.finish");
    assert_eq!(event.detail["start_event_id"], start);
    let lifecycle = event.lifecycle.unwrap();
    assert_eq!(lifecycle.event_type, ActivityLifecycleType::MessageSend);
    assert_eq!(lifecycle.phase, ActivityPhase::Finish);
    assert_eq!(lifecycle.start_event_id, Some(start));
    assert_eq!(lifecycle.delivery_receipts.len(), 1);
    let message = lifecycle.message.unwrap();
    assert_eq!(message.status, Some(ActivityMessageStatus::Queued));
    assert_eq!(message.pool.as_deref(), Some("team"));
    assert_eq!(message.target.as_deref(), Some("Bob"));
    assert_eq!(message.body.as_deref(), Some("hello"));
    assert_eq!(message.in_reply_to.as_deref(), Some("m0"));
    assert_eq!(message.from.as_deref(), Some("Alice"));
    assert_eq!(message.message_id.as_deref(), Some("m1"));
    assert_eq!(message.recipients, Some(vec!["Bob".to_owned()]));
    // A completion referring to a non-retained start must not resurrect it.
    assert!(hub.snapshot_page(3).correlated_events.is_empty());
}

#[test]
fn typed_message_error_and_tool_receives_use_explicit_lifecycle() {
    let hub = ActivityHub::new(12);
    let start = hub.emit(
        "tool.start",
        "info",
        None,
        vec![],
        json!({"tool":"pool_send", "arguments":{"pool":"team", "target":"Bob", "message":"hi"}}),
    );
    hub.emit(
        "tool.error",
        "error",
        None,
        vec![],
        json!({"tool":"pool_send", "start_event_id":start, "result":{"error":"not allowed"}}),
    );
    let failed = hub.snapshot(1).pop().unwrap();
    let lifecycle = failed.lifecycle.unwrap();
    assert_eq!(lifecycle.phase, ActivityPhase::Error);
    let message = lifecycle.message.unwrap();
    assert_eq!(message.status, Some(ActivityMessageStatus::Failed));
    assert_eq!(message.error.as_deref(), Some("not allowed"));
    assert_eq!(message.body.as_deref(), Some("hi"));

    hub.emit("tool.finish", "info", None, vec![],
            json!({"tool":"pool_wait", "start_event_id":123,
                "result":{"messages":[{"message_id":"m2", "pool":"team", "from":"Bob", "to":"Alice"}],
                    "delivery_receipts":[{"message_id":"m1", "pool":"team", "to":"Bob"}]}}));
    let lifecycle = hub.snapshot(1).pop().unwrap().lifecycle.unwrap();
    assert_eq!(lifecycle.event_type, ActivityLifecycleType::Tool);
    assert_eq!(lifecycle.peer_messages[0]["message_id"], "m2");
    assert_eq!(lifecycle.delivery_receipts[0]["message_id"], "m1");

    hub.emit(
        "tool.start",
        "info",
        None,
        vec![],
        json!({"tool":"pool_send", "arguments":{"pool":"team", "action":"leave"}}),
    );
    assert_eq!(
        hub.snapshot(1)[0].lifecycle.as_ref().unwrap().event_type,
        ActivityLifecycleType::Tool
    );
}

#[test]
fn legacy_activity_fixture_deserializes_and_retains_untyped_fields() {
    let legacy = json!({"id":3, "timestamp_ms":42, "kind":"tool.start", "level":"info",
            "session":"old", "agents":[], "detail":{"tool":"pool_send", "arguments":{"message":"hi"}}});
    let parsed: ActivityEvent = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(parsed.schema_version, 0);
    assert!(parsed.lifecycle.is_none());
    assert_eq!(parsed.kind, "tool.start");
    assert_eq!(parsed.detail, legacy["detail"]);
}

#[test]
fn cross_page_correlation_is_bounded_deduplicated_and_cleared() {
    let hub = ActivityHub::new(10);
    let start = hub.emit(
        "tool.start",
        "info",
        None,
        vec![],
        json!({"tool":"pool_send", "arguments":{"pool":"team", "message":"a"}}),
    );
    for _ in 0..8 {
        hub.emit("unrelated", "info", None, vec![], json!({}));
    }
    let finish = hub.emit(
        "tool.finish",
        "info",
        None,
        vec![],
        json!({"tool":"pool_send", "start_event_id": start, "result":{"value":{"recipients":[]}}}),
    );
    let latest = hub.snapshot_page(4);
    assert_eq!(latest.events.len(), 4);
    assert_eq!(
        latest
            .correlated_events
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>(),
        vec![start]
    );
    let older = hub.page(Some(3), 2);
    assert_eq!(
        older.events.iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        older
            .correlated_events
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>(),
        vec![finish]
    );
    let full = hub.snapshot_page(10);
    assert!(
        full.correlated_events.is_empty(),
        "same-page counterparts must not be repeated"
    );
    assert_eq!(hub.clear(), 10);
    assert!(hub.snapshot_page(10).correlated_events.is_empty());
    assert!(hub.page(None, 10).correlated_events.is_empty());
    assert!(lock_state(&hub.shared).correlations.is_empty());
    assert_eq!(hub.emit("new", "info", None, vec![], json!({})), finish + 1);
}

#[test]
fn archived_counterparts_are_available_without_snapshot_disk_reads() {
    let hub = ActivityHub::new(500);
    let start = hub.emit(
        "tool.start",
        "info",
        None,
        vec![],
        json!({"tool":"pool_send", "arguments":{"pool":"team", "message":"a"}}),
    );
    for _ in 0..498 {
        hub.emit("unrelated", "info", None, vec![], json!({}));
    }
    let finish = hub.emit(
        "tool.finish",
        "info",
        None,
        vec![],
        json!({"tool":"pool_send", "start_event_id":start, "result":{"value":{"recipients":[]}}}),
    );
    await_archive(&hub);
    assert_eq!(
        hub.snapshot_page(200)
            .correlated_events
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>(),
        vec![start]
    );
    // Compressed start is readable by the history route, and it can link
    // forward into the in-memory finish without paging in the browser.
    assert_eq!(
        hub.page(Some(2), 1)
            .correlated_events
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>(),
        vec![finish]
    );

    // Advance beyond the retained suffix: the old start cannot reappear.
    for i in 0..3500 {
        hub.emit("unrelated", "info", None, vec![], json!({"i":i}));
    }
    await_archive(&hub);
    assert!(lock_state(&hub.shared).correlations.len() <= MAX_RETAINED_EVENTS);
    assert!(hub.snapshot_page(200).correlated_events.is_empty());
}

/// The browser model and the Rust wire event schema consume the same fixture.
/// Legacy records omit a schema discriminator; v1 records must round-trip as-is.
#[test]
fn shared_v1_and_legacy_activity_event_fixture_is_wire_compatible() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/activity-events.json")).unwrap();
    let records = fixture["events"].as_array().unwrap();
    assert_eq!(records.len(), 4);
    for record in records {
        let event: ActivityEvent = serde_json::from_value(record.clone()).unwrap();
        let mut serialized = serde_json::to_value(&event).unwrap();
        if record.get("schema_version").is_none() {
            assert_eq!(event.schema_version, 0);
            assert!(event.lifecycle.is_none());
            serialized.as_object_mut().unwrap().remove("schema_version");
        } else {
            assert_eq!(event.schema_version, 1);
            assert!(event.lifecycle.is_some());
        }
        assert_eq!(
            serialized, *record,
            "event {} wire format changed",
            event.id
        );
    }
}

/// Producers use typed pool results and typed communications, never whichever
/// conflicting fields happen to be present in presentation-oriented JSON.
#[test]
fn typed_completion_context_overrides_misleading_legacy_result_and_comms() {
    use crate::agent_pool::{Communications, DeliveryReceipt, PeerMessage, ReceiptStatus};

    let fixture: Value =
        serde_json::from_str(include_str!("../../../tests/fixtures/activity-events.json")).unwrap();
    let start = &fixture["events"][0];
    let finish = &fixture["events"][1];
    let start_message: ActivityMessage =
        serde_json::from_value(start["lifecycle"]["message"].clone()).unwrap();
    let finish_message: ActivityMessage =
        serde_json::from_value(finish["lifecycle"]["message"].clone()).unwrap();

    let hub = ActivityHub::new(10);
    let start_id = hub.emit_with_context(
        "tool.start",
        "info",
        Some("chat-beta"),
        vec![],
        start["detail"].clone(),
        ActivityContext {
            pool: Some("beta".to_owned()),
            message: Some(start_message),
        },
    );
    let start_event = hub.snapshot(1).pop().unwrap();
    assert_eq!(
        serde_json::to_value(start_event.lifecycle).unwrap(),
        start["lifecycle"],
        "explicit start message must win over wrong argument JSON"
    );

    let communications = Communications {
        messages: vec![PeerMessage {
            pool: "beta".to_owned(),
            message_id: "message-recv".to_owned(),
            from: "Cara".to_owned(),
            to: "Bob".to_owned(),
            target: "Bob".to_owned(),
            sent_at_ms: 1001,
            message: "typed incoming".to_owned(),
            in_reply_to: None,
        }],
        delivery_receipts: vec![DeliveryReceipt {
            pool: "beta".to_owned(),
            message_id: "message-100".to_owned(),
            to: "Bob".to_owned(),
            status: ReceiptStatus::Offered,
            offered_at_ms: 1001,
        }],
    };
    let mut detail = finish["detail"].clone();
    // The real server already has the correct ID; only the fixture deliberately
    // contains an impossible legacy ID to test browser precedence.
    detail["start_event_id"] = json!(start_id);
    hub.emit_with_context_and_communications(
        "tool.finish",
        "info",
        Some("chat-beta"),
        vec![],
        detail,
        ActivityEmissionContext {
            activity: ActivityContext {
                pool: Some("beta".to_owned()),
                message: Some(finish_message),
            },
            communications: Some(&communications),
        },
    );
    let completed = hub.snapshot(1).pop().unwrap().lifecycle.unwrap();
    let mut expected = finish["lifecycle"].clone();
    expected["start_event_id"] = json!(start_id);
    assert_eq!(serde_json::to_value(completed).unwrap(), expected);

    // Even an empty typed result explicitly overrides misleading fallback
    // arrays; absence (None), not emptiness, enables legacy JSON fallback.
    hub.emit_with_context_and_communications(
        "tool.finish",
        "info",
        None,
        vec![],
        json!({
            "tool": "pool_wait",
            "result": {"messages": [{"message_id": "bad"}], "delivery_receipts": [{"message_id": "bad"}]}
        }),
        ActivityEmissionContext {
            activity: ActivityContext::default(),
            communications: Some(&Communications::default()),
        },
    );
    let empty = hub.snapshot(1).pop().unwrap().lifecycle.unwrap();
    assert!(empty.peer_messages.is_empty());
    assert!(empty.delivery_receipts.is_empty());
}
