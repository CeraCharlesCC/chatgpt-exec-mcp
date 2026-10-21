//! Agent-pool wire types, schemas, and argument deserialization.

use rmcp::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::MAX_NAME_CHARS;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersArgs {
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolWaitArgs {
    /// Pool to observe; messages and receipts are restricted to this pool.
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
    /// Maximum wait; returns early for messages or membership changes.
    #[serde(
        default = "default_pool_wait_seconds",
        deserialize_with = "deserialize_pool_wait_seconds"
    )]
    #[schemars(range(min = 5, max = 45), extend("default" = 35))]
    pub wait_seconds: u64,
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "rmcp::schemars")]
pub enum PoolWaitReason {
    Message,
    MembershipChanged,
    Timeout,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PoolWaitResult {
    pub reason: PoolWaitReason,
    pub messages: Vec<PeerMessage>,
    pub membership_changed: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[schemars(default)]
    pub delivery_receipts: Vec<DeliveryReceipt>,
}

/// Internal wait data retains timestamps for activity events and the WebUI.
#[derive(Debug, Serialize)]
pub struct PoolWaitOutcome {
    pub reason: PoolWaitReason,
    pub messages: Vec<MessageRecord>,
    pub membership_changed: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub delivery_receipts: Vec<ReceiptRecord>,
}

impl From<&PoolWaitOutcome> for PoolWaitResult {
    fn from(outcome: &PoolWaitOutcome) -> Self {
        Self {
            reason: outcome.reason,
            messages: outcome.messages.iter().map(PeerMessage::from).collect(),
            membership_changed: outcome.membership_changed,
            delivery_receipts: outcome
                .delivery_receipts
                .iter()
                .map(DeliveryReceipt::from)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolSendArgs {
    #[schemars(length(min = 1, max = MAX_NAME_CHARS))]
    pub pool: String,
    /// Member name, or global for other current members.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "String", length(min = 1, max = MAX_NAME_CHARS), skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// UTF-8 text, at most 65,536 bytes.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_string",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(
        with = "String",
        length(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub message: Option<String>,
    /// Omit to send; leave removes this membership (pool and action only).
    #[serde(
        default,
        deserialize_with = "deserialize_optional_non_null_action",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(with = "PoolAction", skip_serializing_if = "Option::is_none")]
    pub action: Option<PoolAction>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "rmcp::schemars")]
pub enum PoolAction {
    Leave,
}

/// Durable message data retained for activity events and the WebUI.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct MessageRecord {
    pub message_id: String,
    pub pool: String,
    pub from: String,
    pub to: String,
    pub target: String,
    pub sent_at_ms: i64,
    pub message: String,
}

/// Shared agent-facing message shape across every tool response.
#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PeerMessage {
    pub message_id: String,
    pub pool: String,
    pub from: String,
    pub to: String,
    /// Original send target.
    pub target: String,
    pub message: String,
}

impl From<&MessageRecord> for PeerMessage {
    fn from(record: &MessageRecord) -> Self {
        Self {
            message_id: record.message_id.clone(),
            pool: record.pool.clone(),
            from: record.from.clone(),
            to: record.to.clone(),
            target: record.target.clone(),
            message: record.message.clone(),
        }
    }
}

/// Durable receipt data retained for activity events and the WebUI.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ReceiptRecord {
    pub message_id: String,
    pub pool: String,
    pub to: String,
    pub status: ReceiptStatus,
    pub offered_at_ms: i64,
}

/// Shared agent-facing receipt shape across every tool response.
#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct DeliveryReceipt {
    pub message_id: String,
    pub pool: String,
    pub to: String,
    pub status: ReceiptStatus,
}

impl From<&ReceiptRecord> for DeliveryReceipt {
    fn from(record: &ReceiptRecord) -> Self {
        Self {
            message_id: record.message_id.clone(),
            pool: record.pool.clone(),
            to: record.to.clone(),
            status: record.status,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[schemars(crate = "rmcp::schemars")]
pub enum ReceiptStatus {
    Offered,
}

#[derive(Clone, Debug, Default)]
pub struct Communications {
    pub messages: Vec<MessageRecord>,
    pub delivery_receipts: Vec<ReceiptRecord>,
}

impl Communications {
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.delivery_receipts.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AgentIdentity {
    pub pool: String,
    pub agent: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminMember {
    pub pool: String,
    pub agent: String,
    pub session: String,
    pub last_seen_ms: i64,
    pub expires_ms: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdminMessage {
    pub message_id: String,
    pub pool: String,
    pub from: String,
    pub message: String,
    pub created_ms: i64,
    pub acknowledged: bool,
}

#[derive(Debug, Serialize)]
pub struct AdminMessagesPage {
    pub messages: Vec<AdminMessage>,
    /// Keyset cursor for the next (older) page, or None at the end of history.
    pub next_cursor: Option<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PoolMembersResult {
    pub agents: Vec<String>,
    /// This session's member name.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub self_agent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<PeerMessage>", default)]
    pub messages: Option<Vec<PeerMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<DeliveryReceipt>", default)]
    pub delivery_receipts: Option<Vec<DeliveryReceipt>>,
}

#[derive(Debug, Default, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct PoolSendResult {
    /// Name assigned on joining.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub assigned_agent: Option<String>,
    /// Present when a delivery is queued.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub message_id: Option<String>,
    /// Queued recipients.
    pub recipients: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<PeerMessage>", default)]
    pub messages: Option<Vec<PeerMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Vec<DeliveryReceipt>", default)]
    pub delivery_receipts: Option<Vec<DeliveryReceipt>>,
}

fn deserialize_optional_non_null_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?.map_or_else(
        || {
            Err(serde::de::Error::custom(
                "null is not allowed; omit the field instead",
            ))
        },
        |value| Ok(Some(value)),
    )
}

fn default_pool_wait_seconds() -> u64 {
    35
}

fn deserialize_pool_wait_seconds<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let seconds = u64::deserialize(deserializer)?;
    if !(5..=45).contains(&seconds) {
        return Err(serde::de::Error::custom(
            "wait_seconds must be between 5 and 45",
        ));
    }
    Ok(seconds)
}

fn deserialize_optional_non_null_action<'de, D>(
    deserializer: D,
) -> Result<Option<PoolAction>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<PoolAction>::deserialize(deserializer)?.map_or_else(
        || {
            Err(serde::de::Error::custom(
                "null is not allowed; omit the field instead",
            ))
        },
        |value| Ok(Some(value)),
    )
}
