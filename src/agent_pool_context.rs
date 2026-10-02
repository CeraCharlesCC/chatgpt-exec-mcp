use std::sync::Arc;

use crate::agent_pool::AgentPoolStore;

/// A configured agent-pool store paired with the authenticated principal that
/// owns it. Keeping the pair in one value prevents partially configured states
/// from leaking across the MCP, transport, and WebUI layers.
#[derive(Clone)]
pub(crate) struct AgentPoolContext {
    store: Arc<AgentPoolStore>,
    principal: String,
}

impl AgentPoolContext {
    pub(crate) fn new(store: Arc<AgentPoolStore>, principal: String) -> Self {
        Self { store, principal }
    }

    pub(crate) fn store(&self) -> &AgentPoolStore {
        &self.store
    }

    pub(crate) fn principal(&self) -> &str {
        &self.principal
    }
}
