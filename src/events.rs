//! Durable MCP Events subscriptions are also pool memberships. The owner is supplied
//! by trusted deployment configuration, never by request parameters or metadata.
use std::borrow::Cow;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use rmcp::ErrorData as McpError;
use rmcp::model::ErrorCode;
use rmcp::schemars::JsonSchema;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::webhook::{DeliveryOutcome, WebhookClient, validate_secret};

pub const EVENT_NAME: &str = "multiagent.message";
const DEFAULT_TTL_MS: i64 = 86_400_000;
const MAX_TTL_MS: i64 = 7 * DEFAULT_TTL_MS;
const MIN_TTL_MS: i64 = 60_000;
const ROTATION_MS: i64 = 300_000;
const MAX_PENDING: i64 = 10_000;
const MAX_ATTEMPTS: u32 = 6;

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct MembershipArgs {
    /// Pool name within this account.
    pub pool: String,
    /// Unique agent name within the pool. The name global is reserved.
    pub agent: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersArgs {
    pub pool: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolSendArgs {
    pub pool: String,
    /// Member name, or global to send to all other active members.
    pub agent: String,
    /// Message text, up to 64 KiB.
    pub message: String,
    #[serde(default)]
    pub in_reply_to: Option<String>,
    #[serde(default)]
    /// Fallback only for memberships subscribed without openai/session correlation.
    /// A correlated membership is resolved automatically and cannot be overridden.
    pub from_agent: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookDelivery {
    mode: String,
    url: String,
    secret: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct SubscribeArgs {
    name: String,
    arguments: MembershipArgs,
    delivery: WebhookDelivery,
    #[serde(default)]
    cursor: Option<Value>,
    #[serde(default)]
    ttl_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnsubscribeDelivery {
    mode: String,
    url: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnsubscribeArgs {
    name: String,
    arguments: MembershipArgs,
    delivery: UnsubscribeDelivery,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionResult {
    pub id: String,
    pub refresh_before: String,
    pub cursor: Option<Value>,
    pub truncated: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolMembersResult {
    pub pool: String,
    pub agents: Vec<String>,
}

#[derive(Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars")]
pub struct PoolSendResult {
    pub message_id: String,
    pub recipients: Vec<String>,
}

pub struct EventStore {
    connection: Mutex<Connection>,
    webhook: WebhookClient,
}

struct PendingDelivery {
    id: String,
    subscription_id: String,
    url: String,
    secret: String,
    previous_secret: Option<String>,
    body: String,
    attempts: u32,
    generation: String,
}

impl EventStore {
    #[cfg(test)]
    pub(crate) fn cache_verified_for_test(&self, owner: &str, url: &str) {
        self.webhook.cache_verified_for_test(owner, url);
    }

    pub fn open(path: &Path) -> anyhow::Result<Self> {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
            anyhow::ensure!(
                std::fs::metadata(parent)?.permissions().mode() & 0o077 == 0,
                "events database parent directory must be private to its owner"
            );
        }
        // SQLite contains callback credentials. Refuse symlinks and permissive files.
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
            "events database permissions must be restricted to its owner"
        );
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS subscriptions (
               id TEXT PRIMARY KEY, owner TEXT NOT NULL, pool TEXT NOT NULL,
               agent TEXT NOT NULL, callback TEXT NOT NULL, secret TEXT NOT NULL,
               previous_secret TEXT, previous_until INTEGER,
               expires INTEGER NOT NULL, session TEXT, generation TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS member_name
               ON subscriptions(owner,pool,agent);
             CREATE TABLE IF NOT EXISTS deliveries (
               id TEXT NOT NULL, subscription_id TEXT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
               body TEXT NOT NULL, created INTEGER NOT NULL, next_attempt INTEGER NOT NULL,
               attempts INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL DEFAULT 'pending',
               PRIMARY KEY(id,subscription_id)
             );
             CREATE INDEX IF NOT EXISTS pending_delivery ON deliveries(state,next_attempt);",
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
            webhook: WebhookClient::new(),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, McpError> {
        self.connection.lock().map_err(|_| storage_error())
    }

    pub fn catalog() -> Value {
        json!({"events": [{
            "name": EVENT_NAME,
            "description": "Receive messages addressed to a unique named agent in an account-scoped pool. Subscribing joins the pool; refresh preserves membership until expiration.",
            "delivery": ["webhook"],
            "inputSchema": {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "pool": {"type": "string", "minLength": 1, "maxLength": 128},
                    "agent": {"type": "string", "minLength": 1, "maxLength": 128, "description": "Unique name within the pool; global is reserved."}
                }, "required": ["pool", "agent"]
            },
            "payloadSchema": {
                "type": "object", "additionalProperties": false,
                "properties": {
                    "message_id": {"type": "string"}, "pool": {"type": "string"},
                    "from": {"type": "string"}, "to": {"type": "string"},
                    "message": {"type": "string"}, "in_reply_to": {"type": "string"}
                }, "required": ["message_id", "pool", "from", "to", "message"]
            }
        }]})
    }

    pub async fn custom_request(
        &self,
        owner: &str,
        method: &str,
        params: Option<Value>,
        session: Option<&str>,
    ) -> Result<Value, McpError> {
        validate_name(owner, "principal")?;
        match method {
            "events/list" => {
                let args = params.unwrap_or_else(|| json!({}));
                let object = args
                    .as_object()
                    .ok_or_else(|| invalid("events/list params must be an object"))?;
                if object.keys().any(|key| key != "cursor")
                    || object.get("cursor").is_some_and(|v| !v.is_null())
                {
                    return Err(invalid(
                        "events/list does not support a non-null cursor or unknown arguments",
                    ));
                }
                Ok(Self::catalog())
            }
            "events/subscribe" => {
                let args: SubscribeArgs = parse(params)?;
                validate_subscription(&args.name, &args.arguments, &args.delivery.mode)?;
                if args.cursor.is_some() {
                    return Err(invalid(
                        "multiagent.message does not support replay cursors",
                    ));
                }
                validate_secret(&args.delivery.secret)
                    .map_err(|_| invalid("invalid webhook signing secret"))?;
                let id = subscription_id(owner, &args.arguments, &args.delivery.url);
                self.check_available(owner, &args.arguments, &id, now_ms())?;
                self.webhook
                    .verify(&id, owner, &args.delivery.url, &args.delivery.secret)
                    .await
                    .map_err(|error| {
                        McpError::new(
                            ErrorCode(-32015),
                            "Callback endpoint verification failed",
                            Some(json!({"reason": error.reason()})),
                        )
                    })?;
                let lifetime = args
                    .ttl_ms
                    .map(|ttl| ttl.min(MAX_TTL_MS as u64) as i64)
                    .unwrap_or(DEFAULT_TTL_MS)
                    .clamp(MIN_TTL_MS, MAX_TTL_MS);
                let result = self.activate(
                    owner,
                    &args.arguments,
                    &args.delivery,
                    session,
                    now_ms(),
                    lifetime,
                )?;
                serde_json::to_value(result).map_err(|_| storage_error())
            }
            "events/unsubscribe" => {
                let args: UnsubscribeArgs = parse(params)?;
                validate_subscription(&args.name, &args.arguments, &args.delivery.mode)?;
                let id = subscription_id(owner, &args.arguments, &args.delivery.url);
                self.connection()?
                    .execute(
                        "DELETE FROM subscriptions WHERE owner=?1 AND id=?2",
                        params![owner, id],
                    )
                    .map_err(|_| storage_error())?;
                Ok(json!({}))
            }
            _ => Err(McpError::new(
                ErrorCode::METHOD_NOT_FOUND,
                "Unknown event method",
                None,
            )),
        }
    }

    fn check_available(
        &self,
        owner: &str,
        args: &MembershipArgs,
        id: &str,
        now: i64,
    ) -> Result<(), McpError> {
        let connection = self.connection()?;
        let other: Option<String> = connection.query_row("SELECT id FROM subscriptions WHERE owner=?1 AND pool=?2 AND agent=?3 AND expires>?4", params![owner,args.pool,args.agent,now], |row| row.get(0)).optional().map_err(|_| storage_error())?;
        if other.is_some_and(|other| other != id) {
            return Err(McpError::new(
                ErrorCode(-32000),
                "Agent name is already active for a different callback in this pool",
                Some(json!({"reason":"agent_conflict"})),
            ));
        }
        Ok(())
    }

    fn activate(
        &self,
        owner: &str,
        args: &MembershipArgs,
        delivery: &WebhookDelivery,
        session: Option<&str>,
        now: i64,
        lifetime: i64,
    ) -> Result<SubscriptionResult, McpError> {
        let WebhookDelivery { url, secret, .. } = delivery;
        let id = subscription_id(owner, args, url);
        let expires = now + lifetime;
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let other: Option<String> = transaction
            .query_row(
                "SELECT id FROM subscriptions WHERE owner=?1 AND pool=?2 AND agent=?3",
                params![owner, args.pool, args.agent],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| storage_error())?;
        if other.as_ref().is_some_and(|other| *other != id) {
            return Err(McpError::new(
                ErrorCode(-32000),
                "Agent name is already active for a different callback in this pool",
                Some(json!({"reason":"agent_conflict"})),
            ));
        }
        let count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM subscriptions WHERE owner=?1",
                [owner],
                |row| row.get(0),
            )
            .map_err(|_| storage_error())?;
        if other.is_none() && count >= 1000 {
            return Err(invalid("subscription limit reached"));
        }
        transaction.execute(
            "INSERT INTO subscriptions(id,owner,pool,agent,callback,secret,expires,session,generation) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?10)
             ON CONFLICT(id) DO UPDATE SET
               previous_secret=CASE WHEN secret<>excluded.secret THEN secret ELSE previous_secret END,
               previous_until=CASE WHEN secret<>excluded.secret THEN ?9 ELSE previous_until END,
               secret=excluded.secret, expires=excluded.expires, session=COALESCE(excluded.session,session), generation=excluded.generation",
            params![id,owner,args.pool,args.agent,url,secret,expires,session,now+ROTATION_MS,Uuid::new_v4().to_string()],
        ).map_err(|_| storage_error())?;
        transaction.commit().map_err(|_| storage_error())?;
        Ok(SubscriptionResult {
            id,
            refresh_before: timestamp(expires),
            cursor: None,
            truncated: false,
        })
    }

    pub fn members(
        &self,
        owner: &str,
        args: PoolMembersArgs,
    ) -> Result<PoolMembersResult, McpError> {
        validate_name(&args.pool, "pool")?;
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT agent FROM subscriptions WHERE owner=?1 AND pool=?2 AND expires>?3 ORDER BY agent").map_err(|_| storage_error())?;
        let agents = statement
            .query_map(params![owner, args.pool, now_ms()], |row| row.get(0))
            .map_err(|_| storage_error())?
            .collect::<rusqlite::Result<Vec<String>>>()
            .map_err(|_| storage_error())?;
        Ok(PoolMembersResult {
            pool: args.pool,
            agents,
        })
    }

    pub fn send(
        &self,
        owner: &str,
        args: PoolSendArgs,
        session: Option<&str>,
    ) -> Result<PoolSendResult, McpError> {
        validate_name(&args.pool, "pool")?;
        validate_name(&args.agent, "agent")?;
        if args.message.is_empty() || args.message.len() > 65_536 {
            return Err(invalid("message must contain 1 to 65536 bytes"));
        }
        if let Some(reply) = &args.in_reply_to {
            validate_name(reply, "in_reply_to")?;
        }
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let members: Vec<(String, String, Option<String>)> = {
            let mut statement = transaction.prepare("SELECT id,agent,session FROM subscriptions WHERE owner=?1 AND pool=?2 ORDER BY agent").map_err(|_| storage_error())?;
            statement
                .query_map(params![owner, args.pool], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<_>>()
                .map_err(|_| storage_error())?
        };
        let correlated: Vec<_> = members
            .iter()
            .filter(|member| session.is_some() && member.2.as_deref() == session)
            .collect();
        let sender = match correlated.as_slice() {
            [member] => {
                if args
                    .from_agent
                    .as_ref()
                    .is_some_and(|name| name != &member.1)
                {
                    return Err(invalid("from_agent cannot override the correlated sender"));
                }
                member.1.clone()
            }
            [] => {
                let name = args.from_agent.as_deref().ok_or_else(|| invalid("sender correlation unavailable; supply from_agent for a membership subscribed without openai/session"))?;
                let member = members
                    .iter()
                    .find(|member| member.1 == name)
                    .ok_or_else(|| invalid("from_agent is not an active pool member"))?;
                if member.2.is_some() {
                    return Err(invalid(
                        "from_agent cannot override a membership bound to another session",
                    ));
                }
                member.1.clone()
            }
            _ => {
                return Err(invalid(
                    "session is bound to multiple agents in this pool; unsubscribe duplicate memberships",
                ));
            }
        };
        let targets: Vec<_> = members
            .iter()
            .filter(|member| {
                if args.agent == "global" {
                    member.1 != sender
                } else {
                    member.1 == args.agent
                }
            })
            .collect();
        if args.agent != "global" && targets.is_empty() {
            return Err(invalid("target agent is not an active pool member"));
        }
        let pending: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM deliveries WHERE state='pending'",
                [],
                |row| row.get(0),
            )
            .map_err(|_| storage_error())?;
        if pending + targets.len() as i64 > MAX_PENDING {
            return Err(invalid("event delivery queue is full; retry later"));
        }
        let message_id = format!("evt_{}", Uuid::new_v4());
        let mut data = json!({"message_id":message_id,"pool":args.pool,"from":sender,"to":args.agent,"message":args.message});
        if let Some(reply) = args.in_reply_to {
            data["in_reply_to"] = json!(reply);
        }
        let body = serde_json::to_string(&json!({"eventId":message_id,"name":EVENT_NAME,"timestamp":timestamp(now),"data":data,"cursor":null})).map_err(|_| storage_error())?;
        if body.len() > 262_144 {
            return Err(invalid("serialized event exceeds 256 KiB"));
        }
        let recipients = targets.iter().map(|member| member.1.clone()).collect();
        for target in targets {
            transaction.execute("INSERT INTO deliveries(id,subscription_id,body,created,next_attempt) VALUES(?1,?2,?3,?4,?4)", params![message_id,target.0,body,now]).map_err(|_| storage_error())?;
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok(PoolSendResult {
            message_id,
            recipients,
        })
    }

    /// Queue leases make an interrupted delivery retryable after a restart. Every
    /// attempt reuses the stored body and event ID, and signs at the current time.
    pub fn spawn_worker(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let store = Arc::clone(self);
        tokio::spawn(async move {
            let mut last_storage_warning = None;
            loop {
                match store.claim(now_ms()) {
                    Ok(deliveries) if !deliveries.is_empty() => {
                        let mut tasks = tokio::task::JoinSet::new();
                        for delivery in deliveries {
                            let store = Arc::clone(&store);
                            tasks.spawn(async move {
                                let outcome = store
                                    .webhook
                                    .deliver(
                                        &delivery.subscription_id,
                                        &delivery.url,
                                        &delivery.secret,
                                        delivery.previous_secret.as_deref(),
                                        &delivery.id,
                                        &delivery.body,
                                    )
                                    .await;
                                store.complete(&delivery, outcome, now_ms())
                            });
                        }
                        while let Some(result) = tasks.join_next().await {
                            match result {
                                Ok(Ok(())) => {}
                                Ok(Err(_)) => warn_storage_failure(&mut last_storage_warning),
                                Err(_) => eprintln!(
                                    "events worker task failed; delivery will retry after its lease expires"
                                ),
                            }
                        }
                    }
                    Ok(_) => tokio::time::sleep(Duration::from_secs(1)).await,
                    Err(_) => {
                        warn_storage_failure(&mut last_storage_warning);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        })
    }

    fn claim(&self, now: i64) -> Result<Vec<PendingDelivery>, McpError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction().map_err(|_| storage_error())?;
        cleanup(&transaction, now)?;
        let pending = {
            let mut statement = transaction.prepare(
                "SELECT d.id,s.id,s.callback,s.secret,CASE WHEN s.previous_until>?1 THEN s.previous_secret ELSE NULL END,d.body,d.attempts,s.generation
                 FROM deliveries d JOIN subscriptions s ON s.id=d.subscription_id
                 WHERE d.state='pending' AND d.next_attempt<=?1 AND d.attempts<6 AND s.expires>?1 ORDER BY d.next_attempt,d.created LIMIT 8",
            ).map_err(|_| storage_error())?;
            statement
                .query_map([now], |row| {
                    Ok(PendingDelivery {
                        id: row.get(0)?,
                        subscription_id: row.get(1)?,
                        url: row.get(2)?,
                        secret: row.get(3)?,
                        previous_secret: row.get(4)?,
                        body: row.get(5)?,
                        attempts: row.get::<_, u32>(6)? + 1,
                        generation: row.get(7)?,
                    })
                })
                .map_err(|_| storage_error())?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(|_| storage_error())?
        };
        for delivery in &pending {
            transaction.execute("UPDATE deliveries SET attempts=?1,next_attempt=?2 WHERE id=?3 AND subscription_id=?4", params![delivery.attempts,now+30_000,delivery.id,delivery.subscription_id]).map_err(|_| storage_error())?;
        }
        transaction.commit().map_err(|_| storage_error())?;
        Ok(pending)
    }

    fn complete(
        &self,
        delivery: &PendingDelivery,
        outcome: DeliveryOutcome,
        now: i64,
    ) -> Result<(), McpError> {
        let connection = self.connection()?;
        if matches!(outcome, DeliveryOutcome::Gone) {
            connection
                .execute(
                    "DELETE FROM subscriptions WHERE id=?1 AND generation=?2",
                    params![delivery.subscription_id, delivery.generation],
                )
                .map_err(|_| storage_error())?;
        }
        if matches!(outcome, DeliveryOutcome::Retryable) && delivery.attempts < MAX_ATTEMPTS {
            connection
                .execute(
                    "UPDATE deliveries SET next_attempt=?1 WHERE id=?2 AND subscription_id=?3",
                    params![
                        now + 1000 * 2_i64.pow(delivery.attempts.min(10)),
                        delivery.id,
                        delivery.subscription_id
                    ],
                )
                .map_err(|_| storage_error())?;
        } else {
            // This event type has no replay/history API. Retain only outstanding
            // delivery state, so completed payloads cannot accumulate indefinitely.
            connection
                .execute(
                    "DELETE FROM deliveries WHERE id=?1 AND subscription_id=?2",
                    params![delivery.id, delivery.subscription_id],
                )
                .map_err(|_| storage_error())?;
            if !matches!(outcome, DeliveryOutcome::Delivered) {
                let reason = match outcome {
                    DeliveryOutcome::Gone => "callback returned 410",
                    DeliveryOutcome::PermanentFailure => {
                        "callback rejected delivery or failed validation"
                    }
                    DeliveryOutcome::Retryable => "retry limit exhausted",
                    DeliveryOutcome::Delivered => unreachable!(),
                };
                eprintln!(
                    "event delivery stopped after {} attempt(s): {reason}",
                    delivery.attempts
                );
            }
        }
        Ok(())
    }
}

fn warn_storage_failure(last_warning: &mut Option<Instant>) {
    let now = Instant::now();
    if last_warning.is_none_or(|previous| now.duration_since(previous) >= Duration::from_secs(60)) {
        eprintln!("events worker storage operation failed; pending deliveries will retry");
        *last_warning = Some(now);
    }
}

fn cleanup(connection: &Connection, now: i64) -> Result<(), McpError> {
    connection
        .execute("DELETE FROM subscriptions WHERE expires<=?1", [now])
        .map_err(|_| storage_error())?;
    connection
        .execute(
            "DELETE FROM deliveries WHERE created<?1",
            [now - MAX_TTL_MS],
        )
        .map_err(|_| storage_error())?;
    connection
        .execute(
            "DELETE FROM deliveries WHERE attempts>=6 AND next_attempt<=?1",
            [now],
        )
        .map_err(|_| storage_error())?;
    connection.execute("UPDATE subscriptions SET previous_secret=NULL,previous_until=NULL WHERE previous_until<=?1", [now]).map_err(|_| storage_error())?;
    Ok(())
}

fn subscription_id(owner: &str, args: &MembershipArgs, url: &str) -> String {
    // Typed arguments are serialized in a stable order, independent of request key order.
    let identity = json!([owner,url,EVENT_NAME,{"pool":args.pool,"agent":args.agent}]);
    format!("sub_{:x}", Sha256::digest(identity.to_string().as_bytes()))
}

fn validate_subscription(name: &str, args: &MembershipArgs, mode: &str) -> Result<(), McpError> {
    if name != EVENT_NAME {
        return Err(invalid("unsupported event name"));
    }
    if mode != "webhook" {
        return Err(invalid("only webhook delivery is supported"));
    }
    validate_name(&args.pool, "pool")?;
    validate_name(&args.agent, "agent")?;
    if args.agent == "global" {
        return Err(invalid("global is reserved as the broadcast target"));
    }
    Ok(())
}

fn validate_name(value: &str, field: &str) -> Result<(), McpError> {
    if value.is_empty()
        || value.chars().count() > 128
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(invalid(Cow::Owned(format!(
            "{field} must contain 1 to 128 characters with no control characters or surrounding whitespace"
        ))));
    }
    Ok(())
}
fn parse<T: serde::de::DeserializeOwned>(params: Option<Value>) -> Result<T, McpError> {
    serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|_| invalid("event request arguments do not match the event schema"))
}
fn invalid(message: impl Into<Cow<'static, str>>) -> McpError {
    McpError::invalid_params(message, None)
}
fn storage_error() -> McpError {
    McpError::internal_error("event storage operation failed", None)
}
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn timestamp(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .expect("bounded timestamp")
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine, engine::general_purpose::STANDARD};

    fn secret(byte: u8) -> String {
        format!("whsec_{}", STANDARD.encode([byte; 32]))
    }
    fn membership(pool: &str, agent: &str) -> MembershipArgs {
        MembershipArgs {
            pool: pool.into(),
            agent: agent.into(),
        }
    }
    fn store() -> (tempfile::TempDir, EventStore) {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::Builder::new()
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()
            .unwrap();
        let store = EventStore::open(&directory.path().join("events.sqlite3")).unwrap();
        (directory, store)
    }
    fn join(
        store: &EventStore,
        owner: &str,
        pool: &str,
        agent: &str,
        url: &str,
        session: Option<&str>,
    ) -> SubscriptionResult {
        store
            .activate(
                owner,
                &membership(pool, agent),
                &WebhookDelivery {
                    mode: "webhook".into(),
                    url: url.into(),
                    secret: secret(1),
                },
                session,
                now_ms(),
                DEFAULT_TTL_MS,
            )
            .unwrap()
    }
    fn send_args(target: &str, from_agent: Option<&str>) -> PoolSendArgs {
        PoolSendArgs {
            pool: "project".into(),
            agent: target.into(),
            message: "hello".into(),
            in_reply_to: None,
            from_agent: from_agent.map(str::to_string),
        }
    }
    fn queued(store: &EventStore) -> Vec<(String, String, String)> {
        let connection = store.connection().unwrap();
        let mut statement = connection
            .prepare("SELECT id,subscription_id,body FROM deliveries ORDER BY subscription_id")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    #[tokio::test]
    async fn official_event_contract_refresh_canonical_identity_and_unsubscribe() {
        let (_directory, store) = store();
        let owner = "account-a";
        let callback = "https://example.com/receiver";
        store.webhook.cache_verified_for_test(owner, callback);
        let params = json!({"name":EVENT_NAME,"arguments":{"pool":"project","agent":"alice"},"delivery":{"mode":"webhook","url":callback,"secret":secret(1)},"cursor":null,"ttlMs":120000});
        let first = store
            .custom_request(owner, "events/subscribe", Some(params), Some("chat-a"))
            .await
            .unwrap();
        assert!(first["id"].as_str().unwrap().starts_with("sub_"));
        assert_eq!(first["cursor"], Value::Null);
        assert_eq!(first["truncated"], false);
        let expiry = DateTime::parse_from_rfc3339(first["refreshBefore"].as_str().unwrap())
            .unwrap()
            .timestamp_millis();
        assert!((expiry - now_ms() - 120000).abs() < 1000);
        let reordered = json!({"name":EVENT_NAME,"arguments":{"agent":"alice","pool":"project"},"delivery":{"mode":"webhook","url":callback,"secret":secret(2)},"ttlMs":null});
        let refreshed = store
            .custom_request(owner, "events/subscribe", Some(reordered), None)
            .await
            .unwrap();
        assert_eq!(first["id"], refreshed["id"]);
        assert!(refreshed["refreshBefore"].is_string());
        assert_eq!(
            store
                .members(
                    owner,
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents,
            ["alice"]
        );
        let saved: (String, Option<String>, Option<String>) = {
            let connection = store.connection().unwrap();
            connection
                .query_row(
                    "SELECT secret,previous_secret,session FROM subscriptions",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap()
        };
        assert_eq!(saved, (secret(2), Some(secret(1)), Some("chat-a".into())));
        let unsubscribe = json!({"name":EVENT_NAME,"arguments":{"pool":"project","agent":"alice"},"delivery":{"mode":"webhook","url":callback}});
        for _ in 0..2 {
            assert_eq!(
                store
                    .custom_request(owner, "events/unsubscribe", Some(unsubscribe.clone()), None)
                    .await
                    .unwrap(),
                json!({})
            );
        }
        assert!(
            store
                .members(
                    owner,
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents
                .is_empty()
        );
    }

    #[tokio::test]
    async fn name_conflict_and_reserved_global_fail_before_callback_verification() {
        let (_directory, store) = store();
        join(
            &store,
            "account-a",
            "project",
            "alice",
            "https://example.com/one",
            None,
        );
        let request = |agent, url| json!({"name":EVENT_NAME,"arguments":{"pool":"project","agent":agent},"delivery":{"mode":"webhook","url":url,"secret":secret(1)}});
        let conflict = store
            .custom_request(
                "account-a",
                "events/subscribe",
                Some(request("alice", "https://example.com/two")),
                None,
            )
            .await
            .unwrap_err();
        assert_eq!(conflict.data.unwrap()["reason"], "agent_conflict");
        assert!(
            store
                .custom_request(
                    "account-a",
                    "events/subscribe",
                    Some(request("global", "https://example.com/two")),
                    None
                )
                .await
                .unwrap_err()
                .message
                .contains("reserved")
        );
        // Same pool/name is independent under another trusted account.
        join(
            &store,
            "account-b",
            "project",
            "alice",
            "https://example.com/two",
            None,
        );
        assert_eq!(
            store
                .members(
                    "account-b",
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents,
            ["alice"]
        );
    }

    #[test]
    fn targeted_and_global_are_account_scoped_and_global_excludes_sender() {
        let (_directory, store) = store();
        join(
            &store,
            "account-a",
            "project",
            "alice",
            "https://example.com/a",
            Some("chat-a"),
        );
        let bob = join(
            &store,
            "account-a",
            "project",
            "bob",
            "https://example.com/b",
            Some("chat-b"),
        );
        join(
            &store,
            "account-a",
            "project",
            "charlie",
            "https://example.com/c",
            None,
        );
        join(
            &store,
            "account-b",
            "project",
            "outside",
            "https://example.com/x",
            None,
        );
        join(
            &store,
            "account-a",
            "other",
            "outside",
            "https://example.com/y",
            None,
        );
        let targeted = store
            .send("account-a", send_args("bob", None), Some("chat-a"))
            .unwrap();
        assert_eq!(targeted.recipients, ["bob"]);
        let queue = queued(&store);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].1, bob.id);
        let body: Value = serde_json::from_str(&queue[0].2).unwrap();
        assert_eq!(body["eventId"], targeted.message_id);
        assert_eq!(body["name"], EVENT_NAME);
        assert_eq!(body["data"]["from"], "alice");
        assert_eq!(body["data"]["to"], "bob");
        assert!(body["data"].get("in_reply_to").is_none());
        let broadcast = store
            .send("account-a", send_args("global", None), Some("chat-a"))
            .unwrap();
        assert_eq!(broadcast.recipients, ["bob", "charlie"]);
        assert_eq!(queued(&store).len(), 3);
    }

    #[test]
    fn sender_fallback_cannot_override_correlated_memberships_or_ambiguity() {
        let (_directory, store) = store();
        join(
            &store,
            "account",
            "project",
            "alice",
            "https://example.com/a",
            Some("chat-a"),
        );
        join(
            &store,
            "account",
            "project",
            "bob",
            "https://example.com/b",
            None,
        );
        assert!(
            store
                .send("account", send_args("alice", None), None)
                .is_err()
        );
        assert!(
            store
                .send("account", send_args("bob", Some("alice")), None)
                .is_err()
        );
        assert!(
            store
                .send(
                    "account",
                    send_args("bob", Some("alice")),
                    Some("chat-wrong")
                )
                .is_err()
        );
        assert!(
            store
                .send("account", send_args("bob", Some("bob")), Some("chat-a"))
                .is_err()
        );
        assert_eq!(
            store
                .send("account", send_args("alice", Some("bob")), None)
                .unwrap()
                .recipients,
            ["alice"]
        );
        join(
            &store,
            "account",
            "project",
            "duplicate",
            "https://example.com/c",
            Some("chat-a"),
        );
        assert!(
            store
                .send(
                    "account",
                    send_args("global", Some("alice")),
                    Some("chat-a")
                )
                .unwrap_err()
                .message
                .contains("multiple")
        );
    }

    #[test]
    fn expiration_and_restart_preserve_only_active_memberships_and_pending_events() {
        let (directory, store) = store();
        join(
            &store,
            "account",
            "project",
            "alice",
            "https://example.com/a",
            None,
        );
        join(
            &store,
            "account",
            "project",
            "bob",
            "https://example.com/b",
            None,
        );
        let sent = store
            .send("account", send_args("bob", Some("alice")), None)
            .unwrap();
        let before = queued(&store);
        drop(store);
        let restarted = EventStore::open(&directory.path().join("events.sqlite3")).unwrap();
        assert_eq!(
            restarted
                .members(
                    "account",
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents,
            ["alice", "bob"]
        );
        assert_eq!(queued(&restarted), before);
        assert_eq!(before[0].0, sent.message_id);
        restarted
            .connection()
            .unwrap()
            .execute(
                "UPDATE subscriptions SET expires=?1 WHERE agent='bob'",
                [now_ms() - 1],
            )
            .unwrap();
        assert_eq!(
            restarted
                .members(
                    "account",
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents,
            ["alice"]
        );
        assert!(
            restarted
                .send("account", send_args("bob", Some("alice")), None)
                .is_err()
        );
        assert!(restarted.claim(now_ms()).unwrap().is_empty());
        assert!(queued(&restarted).is_empty());
        // An expired name can be acquired by a different callback.
        join(
            &restarted,
            "account",
            "project",
            "bob",
            "https://example.com/new",
            None,
        );
    }

    #[test]
    fn refresh_rotates_secrets_without_changing_pending_ids_or_bodies() {
        let (_directory, store) = store();
        join(
            &store,
            "account",
            "project",
            "alice",
            "https://example.com/a",
            None,
        );
        let bob = join(
            &store,
            "account",
            "project",
            "bob",
            "https://example.com/b",
            None,
        );
        store
            .send("account", send_args("bob", Some("alice")), None)
            .unwrap();
        let before = queued(&store);
        let now = now_ms();
        let refresh = store
            .activate(
                "account",
                &membership("project", "bob"),
                &WebhookDelivery {
                    mode: "webhook".into(),
                    url: "https://example.com/b".into(),
                    secret: secret(2),
                },
                None,
                now,
                DEFAULT_TTL_MS,
            )
            .unwrap();
        assert_eq!(refresh.id, bob.id);
        assert_eq!(queued(&store), before);
        let attempts = store.claim(now).unwrap();
        assert_eq!(attempts[0].secret, secret(2));
        assert_eq!(attempts[0].previous_secret, Some(secret(1)));
        store
            .complete(&attempts[0], DeliveryOutcome::Retryable, now)
            .unwrap();
        assert!(store.claim(now + 1000).unwrap().is_empty());
        let retried = store.claim(now + ROTATION_MS + 1).unwrap();
        assert_eq!(retried[0].id, attempts[0].id);
        assert_eq!(retried[0].body, attempts[0].body);
        assert_eq!(retried[0].previous_secret, None);
        store
            .complete(
                &retried[0],
                DeliveryOutcome::Delivered,
                now + ROTATION_MS + 1,
            )
            .unwrap();
        assert!(queued(&store).is_empty());
    }

    #[test]
    fn crash_leases_cannot_exceed_attempt_limit_and_stale_gone_keeps_refreshed_member() {
        let (_directory, store) = store();
        join(
            &store,
            "account",
            "project",
            "alice",
            "https://example.com/a",
            None,
        );
        join(
            &store,
            "account",
            "project",
            "bob",
            "https://example.com/b",
            None,
        );
        store
            .send("account", send_args("bob", Some("alice")), None)
            .unwrap();
        let now = now_ms();
        let first = store.claim(now).unwrap().pop().unwrap();
        assert!(store.claim(now + 29000).unwrap().is_empty());
        join(
            &store,
            "account",
            "project",
            "bob",
            "https://example.com/b",
            None,
        );
        store
            .complete(&first, DeliveryOutcome::Gone, now + 1000)
            .unwrap();
        assert!(
            store
                .members(
                    "account",
                    PoolMembersArgs {
                        pool: "project".into()
                    }
                )
                .unwrap()
                .agents
                .contains(&"bob".into())
        );
        store
            .send("account", send_args("bob", Some("alice")), None)
            .unwrap();
        for attempt in 1..=MAX_ATTEMPTS {
            let deliveries = store.claim(now + attempt as i64 * 30000).unwrap();
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].attempts, attempt);
            // Simulate process death before recording the outbound result.
        }
        assert!(
            store
                .claim(now + (MAX_ATTEMPTS as i64 + 1) * 30000)
                .unwrap()
                .is_empty()
        );
        assert!(queued(&store).is_empty());
    }

    #[test]
    fn escaped_payload_cap_and_queue_limit_are_visible_to_sender() {
        let (_directory, store) = store();
        join(
            &store,
            "account",
            "project",
            "alice",
            "https://example.com/a",
            None,
        );
        let bob = join(
            &store,
            "account",
            "project",
            "bob",
            "https://example.com/b",
            None,
        );
        let mut args = send_args("bob", Some("alice"));
        args.message = "\0".repeat(65536);
        assert!(
            store
                .send("account", args, None)
                .unwrap_err()
                .message
                .contains("256 KiB")
        );
        assert!(queued(&store).is_empty());
        {
            let mut connection = store.connection().unwrap();
            let transaction = connection.transaction().unwrap();
            for index in 0..MAX_PENDING {
                transaction.execute("INSERT INTO deliveries(id,subscription_id,body,created,next_attempt) VALUES(?1,?2,'{}',?3,?3)",params![format!("fixture-{index}"),bob.id,now_ms()]).unwrap();
            }
            transaction.commit().unwrap();
        }
        assert!(
            store
                .send("account", send_args("bob", Some("alice")), None)
                .unwrap_err()
                .message
                .contains("queue is full")
        );
        assert_eq!(queued(&store).len(), MAX_PENDING as usize);
    }

    #[test]
    fn sqlite_credentials_are_private_and_symlinks_rejected() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let (directory, store) = store();
        let path = directory.path().join("events.sqlite3");
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        drop(store);
        let link = directory.path().join("link");
        symlink(&path, &link).unwrap();
        assert!(EventStore::open(&link).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(EventStore::open(&path).is_err());
    }
}
