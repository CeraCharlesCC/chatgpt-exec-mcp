use std::sync::Arc;

use serde_json::json;
use tempfile::TempDir;

use super::ExecMcpServer;
use crate::activity::{ActivityContext, ActivityMessage};
use crate::{
    Config, ProcessManager,
    agent_pool::{AgentPoolStore, PoolSendArgs},
};

fn server() -> (TempDir, ExecMcpServer) {
    let workspace = TempDir::new().unwrap();
    let config = workspace.path().join("config.json");
    std::fs::write(
        &config,
        json!({
            "version": 1,
            "workspace": ".",
            "shell": "/bin/bash",
            "output_store_dir": "outputs",
            "child_env": { "inherit": [], "rules": [] }
        })
        .to_string(),
    )
    .unwrap();
    let manager = ProcessManager::new(Config::load(&config).unwrap()).unwrap();
    (workspace, ExecMcpServer::new(manager))
}

#[tokio::test(flavor = "current_thread")]
async fn disabled_activity_never_builds_diagnostic_payloads_or_reads_pool_identities() {
    let (workspace, server) = server();
    let pool =
        Arc::new(AgentPoolStore::open(&workspace.path().join("private/pool.sqlite")).unwrap());
    // A pool is enabled for actual tool behavior, but there is no reason
    // to query its session identities for an unused diagnostic feed.
    let server = server.with_agent_pool(pool, "owner".into());
    let hub = server.activity_hub();
    assert!(!hub.is_enabled());

    let started = server
        .tool_started("pool_send", Some("session"), || {
            panic!("disabled activity must not build argument JSON")
        })
        .await;
    assert_eq!(started, 0);
    server
        .tool_finished("pool_send", Some("session"), started, true, || {
            panic!("disabled activity must not serialize results or build preview")
        })
        .await;
    assert!(hub.snapshot(200).is_empty());
    assert!(!hub.page(None, 200).has_more);
}

#[tokio::test]
async fn opted_in_activity_keeps_history_and_correlates_tool_events() {
    let (_workspace, server) = server();
    assert!(!server.activity_hub().is_enabled());
    let server = server.with_activity_feed();
    let hub = server.activity_hub();
    assert!(hub.is_enabled());

    let started = server
        .tool_started("pool_members", Some("session"), || json!({"pool":"team"}))
        .await;
    assert_eq!(started, 1);
    server
        .tool_finished(
            "pool_members",
            Some("session"),
            started,
            true,
            || json!({"value":{"agents":[]}}),
        )
        .await;
    let events = hub.snapshot(200);
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, "tool.start");
    assert_eq!(events[0].detail["arguments"]["pool"], "team");
    assert_eq!(events[1].kind, "tool.finish");
    assert_eq!(events[1].detail["start_event_id"], started);
    assert_eq!(events[1].detail["result"]["value"]["agents"], json!([]));

    let repeated_opt_in = server.with_activity_feed();
    assert!(Arc::ptr_eq(&hub, &repeated_opt_in.activity_hub()));
    assert_eq!(repeated_opt_in.activity_hub().snapshot(200).len(), 2);
}

#[tokio::test]
async fn typed_pool_lifecycles_are_self_contained_without_retaining_generic_tool_pairs() {
    let (_workspace, server) = server();
    let server = server.with_activity_feed();
    let hub = server.activity_hub();

    let members_start = server
        .tool_started("pool_members", Some("session"), || json!({"pool":"team"}))
        .await;
    server
        .tool_finished_with_context(
            "pool_members",
            Some("session"),
            members_start,
            true,
            ActivityContext {
                pool: Some("team".into()),
                ..ActivityContext::default()
            },
            || json!({"value":{"agents":[]}}),
        )
        .await;
    let members_finish = hub.snapshot(1).pop().unwrap();
    assert_eq!(
        members_finish.lifecycle.unwrap().pool.as_deref(),
        Some("team")
    );

    // Message context is captured before the async pool operation moves
    // its input, and a final event can be interpreted on its own.
    let args: PoolSendArgs = serde_json::from_value(json!({
        "pool":"team", "target":"bob", "message":"a message"
    }))
    .unwrap();
    let start = server
        .tool_started("pool_send", Some("session"), || json!(&args))
        .await;
    server
        .tool_finished_with_context(
            "pool_send",
            Some("session"),
            start,
            true,
            ActivityContext {
                pool: Some(args.pool.clone()),
                message: ActivityMessage::from_pool_send_args(&args),
            },
            || {
                json!({"value":{"message_id":"m1", "assigned_agent":"alice", "recipients":["bob"]},
            "peer_messages":[{"pool":"team","message_id":"m2","from":"bob","to":"alice"}]})
            },
        )
        .await;
    let sent = hub.snapshot(1).pop().unwrap();
    let typed = sent.lifecycle.unwrap();
    assert_eq!(typed.pool.as_deref(), Some("team"));
    assert_eq!(typed.start_event_id, Some(start));
    assert_eq!(typed.peer_messages.len(), 1);
    let message = typed.message.unwrap();
    assert_eq!(message.body.as_deref(), Some("a message"));
    assert_eq!(message.from.as_deref(), Some("alice"));
    assert_eq!(message.message_id.as_deref(), Some("m1"));

    // Leaving a pool is a tool lifecycle, not an outgoing message.
    let leave: PoolSendArgs =
        serde_json::from_value(json!({"pool":"team", "action":"leave"})).unwrap();
    let leave_start = server
        .tool_started("pool_send", Some("session"), || json!(&leave))
        .await;
    server
        .tool_finished_with_context(
            "pool_send",
            Some("session"),
            leave_start,
            true,
            ActivityContext {
                pool: Some(leave.pool.clone()),
                message: ActivityMessage::from_pool_send_args(&leave),
            },
            || json!({"value":{"recipients":[]}}),
        )
        .await;
    let leave_event = hub.snapshot(1).pop().unwrap();
    assert_eq!(
        leave_event.lifecycle.as_ref().unwrap().pool.as_deref(),
        Some("team")
    );
    assert!(leave_event.lifecycle.unwrap().message.is_none());
}
