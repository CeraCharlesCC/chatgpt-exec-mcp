use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::Value;

use crate::agent_pool::AgentIdentity;

#[derive(Clone, Debug, Serialize)]
pub struct ActivityEvent {
    pub id: u64,
    pub timestamp_ms: i64,
    pub kind: String,
    pub level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    pub agents: Vec<AgentIdentity>,
    pub detail: Value,
}

pub struct ActivityHub {
    next_id: AtomicU64,
    events: Mutex<VecDeque<ActivityEvent>>,
    capacity: usize,
}

impl ActivityHub {
    pub fn new(capacity: usize) -> Self {
        Self {
            next_id: AtomicU64::new(0),
            events: Mutex::new(VecDeque::with_capacity(capacity.min(1024))),
            capacity: capacity.max(1),
        }
    }

    pub fn emit(
        &self,
        kind: impl Into<String>,
        level: impl Into<String>,
        session: Option<&str>,
        agents: Vec<AgentIdentity>,
        detail: Value,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let event = ActivityEvent {
            id,
            timestamp_ms: now_ms(),
            kind: kind.into(),
            level: level.into(),
            session: session.map(str::to_owned),
            agents,
            detail,
        };
        if let Ok(mut events) = self.events.lock() {
            while events.len() >= self.capacity {
                events.pop_front();
            }
            events.push_back(event);
        }
        id
    }

    pub fn snapshot(&self, limit: usize) -> Vec<ActivityEvent> {
        let Ok(events) = self.events.lock() else {
            return Vec::new();
        };
        let take = limit.min(events.len());
        events.iter().skip(events.len() - take).cloned().collect()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
