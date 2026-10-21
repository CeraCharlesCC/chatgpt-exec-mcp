//! Activity event wire schema and lifecycle construction.
//!
//! Keep the versioned typed lifecycle alongside legacy `kind` and `detail`
//! for compatibility with older consumers and serialized history.

use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent_pool::{AgentIdentity, Communications, PoolSendArgs};

/// New activity events carry an explicit schema discriminator; older archive
/// records deserialize as version zero and keep their original `kind`/`detail`.
const ACTIVITY_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLifecycleType {
    Tool,
    MessageSend,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityPhase {
    Start,
    Finish,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActivityMessageStatus {
    Sending,
    Queued,
    Failed,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ActivityMessage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipients: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ActivityMessageStatus>,
}

impl ActivityMessage {
    /// Capture only when the diagnostic sink is enabled, without forcing
    /// another JSON serialization or borrowing args after pool_send consumes them.
    pub(crate) fn from_pool_send_args(args: &PoolSendArgs) -> Option<Self> {
        if args.action.is_some() {
            return None;
        }
        Some(Self {
            pool: Some(args.pool.clone()),
            target: Some(args.target.as_deref().unwrap_or("global").to_owned()),
            body: Some(args.message.as_ref()?.clone()),
            in_reply_to: args.in_reply_to.clone(),
            ..Self::default()
        })
    }

    fn from_start_detail(detail: &Value) -> Option<Self> {
        let arguments = detail.get("arguments")?;
        if arguments
            .get("action")
            .is_some_and(|action| !action.is_null())
        {
            return None;
        }
        let body = arguments.get("message")?.as_str()?.to_owned();
        Some(Self {
            pool: arguments
                .get("pool")
                .and_then(Value::as_str)
                .map(str::to_owned),
            target: Some(
                arguments
                    .get("target")
                    .and_then(Value::as_str)
                    .unwrap_or("global")
                    .to_owned(),
            ),
            body: Some(body),
            in_reply_to: arguments
                .get("in_reply_to")
                .and_then(Value::as_str)
                .map(str::to_owned),
            ..Self::default()
        })
    }
}

/// Captured from tool arguments only for an opted-in activity consumer.
/// The completion can then be rendered independently of the start row.
#[derive(Default)]
pub(crate) struct ActivityContext {
    pub pool: Option<String>,
    pub message: Option<ActivityMessage>,
}

/// Completion data supplied by the operation layer. A `None` communications
/// value keeps legacy result-JSON fallback; `Some(empty)` is authoritative.
pub(crate) struct ActivityEmissionContext<'a> {
    pub activity: ActivityContext,
    pub communications: Option<&'a Communications>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityLifecycle {
    #[serde(rename = "type")]
    pub event_type: ActivityLifecycleType,
    pub phase: ActivityPhase,
    pub tool: String,
    /// Pool identity even when the matching start event is on another page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_event_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<ActivityMessage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub peer_messages: Vec<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub delivery_receipts: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ActivityEvent {
    pub id: u64,
    pub timestamp_ms: i64,
    #[serde(default)]
    pub schema_version: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<ActivityLifecycle>,
    pub kind: String,
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    pub agents: Vec<AgentIdentity>,
    pub detail: Value,
}

impl ActivityEvent {
    /// Construct a versioned event while retaining its legacy wire fields.
    pub(super) fn new(
        id: u64,
        kind: String,
        level: String,
        session: Option<&str>,
        agents: Vec<AgentIdentity>,
        detail: Value,
        lifecycle: Option<ActivityLifecycle>,
    ) -> Self {
        Self {
            id,
            timestamp_ms: now_ms(),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            lifecycle,
            kind,
            level,
            session: session.map(str::to_owned),
            agents,
            detail,
        }
    }
}

/// `kind` and `detail` remain the legacy wire contract. New producers also
/// record a typed lifecycle so consumers need not infer message state by
/// examining adjacent events or digging through untyped tool result JSON.
pub(super) fn lifecycle_for_event(
    kind: &str,
    detail: &Value,
    agents: &[AgentIdentity],
    retained_start: Option<&ActivityEvent>,
    context: ActivityContext,
    communications: Option<&Communications>,
) -> Option<ActivityLifecycle> {
    let phase = match kind {
        "tool.start" => ActivityPhase::Start,
        "tool.finish" => ActivityPhase::Finish,
        "tool.error" => ActivityPhase::Error,
        _ => return None,
    };
    let tool = detail.get("tool")?.as_str()?.to_owned();
    let start_event_id = detail.get("start_event_id").and_then(Value::as_u64);
    let ActivityContext {
        pool: pool_context,
        message: message_context,
    } = context;
    let pool = pool_context
        .or_else(|| {
            message_context
                .as_ref()
                .and_then(|message| message.pool.clone())
        })
        .or_else(|| {
            detail
                .get("arguments")
                .and_then(|arguments| arguments.get("pool"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            retained_start
                .and_then(|event| event.lifecycle.as_ref())
                .and_then(|lifecycle| lifecycle.pool.clone())
        });

    // An explicitly provided completion is authoritative. A message recovered
    // from an earlier start is only a fallback: its `sending` status and any
    // other completion fields must still be updated for this operation.
    let explicit_message_context = message_context.is_some();
    let mut message = if tool == "pool_send" {
        message_context.or_else(|| {
            if phase == ActivityPhase::Start {
                ActivityMessage::from_start_detail(detail)
            } else {
                retained_start
                    .and_then(|event| event.lifecycle.as_ref())
                    .and_then(|lifecycle| lifecycle.message.clone())
            }
        })
    } else {
        None
    };

    if let Some(message) = message.as_mut() {
        let result = detail.get("result");
        let value = result.and_then(|result| result.get("value"));
        // A successful first join may assign the sender's name in the result;
        // otherwise use an already known member identity for this session.
        if (!explicit_message_context || message.from.is_none())
            && let Some(from) = value
                .and_then(|value| value.get("assigned_agent"))
                .and_then(Value::as_str)
                .or_else(|| {
                    agents
                        .iter()
                        .find(|agent| message.pool.as_deref() == Some(agent.pool.as_str()))
                        .map(|agent| agent.agent.as_str())
                })
        {
            message.from = Some(from.to_owned());
        }
        if phase != ActivityPhase::Start {
            if !explicit_message_context || message.message_id.is_none() {
                message.message_id = value
                    .and_then(|value| value.get("message_id"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            if !explicit_message_context || message.recipients.is_none() {
                message.recipients = value
                    .and_then(|value| value.get("recipients"))
                    .and_then(Value::as_array)
                    .map(|recipients| {
                        recipients
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    });
            }
            if phase == ActivityPhase::Error
                && (!explicit_message_context || message.error.is_none())
            {
                message.error = result
                    .and_then(|result| result.get("error"))
                    .and_then(Value::as_str)
                    .or_else(|| {
                        value
                            .and_then(|value| value.get("error"))
                            .and_then(Value::as_str)
                    })
                    .map(str::to_owned)
                    .or_else(|| Some("Send failed".to_owned()));
            }
        }
        if !explicit_message_context || message.status.is_none() {
            message.status = Some(match phase {
                ActivityPhase::Start => ActivityMessageStatus::Sending,
                ActivityPhase::Finish => ActivityMessageStatus::Queued,
                ActivityPhase::Error => ActivityMessageStatus::Failed,
            });
        }
    }

    // Typed communications override arbitrary legacy result JSON, including
    // when an operation intentionally produced no messages or receipts.
    let (peer_messages, delivery_receipts) = if let Some(communications) = communications {
        (
            communications
                .messages
                .iter()
                .map(|message| serde_json::to_value(message).expect("peer message is serializable"))
                .collect(),
            communications
                .delivery_receipts
                .iter()
                .map(|receipt| serde_json::to_value(receipt).expect("receipt is serializable"))
                .collect(),
        )
    } else {
        let result = detail.get("result");
        let result_value = result.and_then(|result| result.get("value"));
        let array = |key: &str| {
            result
                .and_then(|result| result.get(key))
                .or_else(|| result_value.and_then(|value| value.get(key)))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };
        let mut peer_messages = array("peer_messages");
        if peer_messages.is_empty() && tool == "pool_wait" {
            peer_messages = array("messages");
        }
        (peer_messages, array("delivery_receipts"))
    };
    Some(ActivityLifecycle {
        event_type: if message.is_some() {
            ActivityLifecycleType::MessageSend
        } else {
            ActivityLifecycleType::Tool
        },
        phase,
        tool,
        pool,
        start_event_id,
        message,
        peer_messages,
        delivery_receipts,
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
