use std::{borrow::Cow, sync::Arc};

use rmcp::handler::server::{
    router::tool::ToolRouter, tool::schema_for_output, wrapper::Parameters,
};
use rmcp::model::{
    CallToolResult, ContentBlock, ErrorCode, Implementation, ProtocolVersion, ServerCapabilities,
    ServerConfig,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler, tool, tool_handler, tool_router};

use crate::agent_pool::{
    AgentPoolStore, PeerMessage, PoolMembersArgs, PoolMembersResult, PoolSendArgs, PoolSendResult,
    SessionTurn,
};
use crate::process_manager::ProcessManager;
use crate::tools::{
    ExecCommandArgs, ExecResponse, StartSessionArgs, WaitForExitArgs, WriteStdinArgs,
};

#[derive(Clone)]
pub struct ExecMcpServer {
    manager: Arc<ProcessManager>,
    tool_router: ToolRouter<Self>,
    agent_pool: Option<Arc<AgentPoolStore>>,
    principal: Option<String>,
    strict_http: bool,
}

#[tool_router]
impl ExecMcpServer {
    pub fn new(manager: Arc<ProcessManager>) -> Self {
        let mut tool_router = Self::tool_router();
        tool_router.disable_route("pool_members");
        tool_router.disable_route("pool_send");
        Self {
            manager,
            tool_router,
            agent_pool: None,
            principal: None,
            strict_http: false,
        }
    }

    /// The dedicated, authenticated tunnel belongs to this configured account.
    /// Request metadata cannot change the principal.
    pub fn with_agent_pool(mut self, store: Arc<AgentPoolStore>, principal: String) -> Self {
        self.agent_pool = Some(store);
        self.principal = Some(principal);
        self.tool_router.enable_route("pool_members");
        self.tool_router.enable_route("pool_send");
        self
    }

    pub fn with_http_protocol(mut self) -> Self {
        self.strict_http = true;
        self
    }

    fn agent_pool_context(&self) -> Result<(&AgentPoolStore, &str), McpError> {
        match (&self.agent_pool, &self.principal) {
            (Some(pool), Some(principal)) if !principal.is_empty() => Ok((pool, principal)),
            _ => Err(McpError::new(
                ErrorCode(-32000),
                "Agent pools require an authenticated account configuration",
                None,
            )),
        }
    }

    fn session(context: &RequestContext<RoleServer>) -> Option<String> {
        context
            .meta
            .get("openai/session")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    }

    async fn begin_tool(&self, session: Option<&str>) -> Result<Option<SessionTurn>, McpError> {
        let (Some(pool), Some(principal)) = (&self.agent_pool, &self.principal) else {
            return Ok(None);
        };
        pool.start_tool(principal, session).await
    }

    async fn finish_tool(
        &self,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Result<Vec<PeerMessage>, McpError> {
        let (Some(pool), Some(principal)) = (&self.agent_pool, &self.principal) else {
            return Ok(Vec::new());
        };
        pool.finish_tool_turn(principal, session, turn).await
    }

    /// Piggyback collection happens after the primary tool operation. Storage
    /// failure here must not hide a command or send that already succeeded,
    /// because a caller could otherwise retry a non-idempotent operation.
    async fn finish_tool_preserving_result(
        &self,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Vec<PeerMessage> {
        match self.finish_tool(session, turn).await {
            Ok(peers) => peers,
            Err(_) => {
                eprintln!("agent pool piggyback collection failed; preserving primary tool result");
                Vec::new()
            }
        }
    }

    /// List active agent names in the caller's account-scoped pool.
    #[tool(output_schema = schema_for_output::<PoolMembersResult>(), annotations(read_only_hint = true))]
    async fn pool_members(
        &self,
        Parameters(args): Parameters<PoolMembersArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let turn = self.begin_tool(session.as_deref()).await?;
        let (pool, principal) = self.agent_pool_context()?;
        let result = pool.members(principal, args);
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        Self::pool_result(result, peers)
    }

    /// Queue a message to a named member, or to global for all other active members.
    /// The first send in a pool binds from_agent to the current openai/session.
    #[tool(output_schema = schema_for_output::<PoolSendResult>(), annotations(read_only_hint = false, destructive_hint = false, open_world_hint = true, idempotent_hint = false))]
    async fn pool_send(
        &self,
        Parameters(args): Parameters<PoolSendArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let turn = self.begin_tool(session.as_deref()).await?;
        let (pool, principal) = self.agent_pool_context()?;
        let result = pool.send(principal, args, session.as_deref());
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        Self::pool_result(result, peers)
    }

    fn pool_result<T: serde::Serialize>(
        result: Result<T, McpError>,
        peers: Vec<PeerMessage>,
    ) -> Result<CallToolResult, McpError> {
        match result {
            Ok(value) => {
                let mut structured = serde_json::to_value(value).map_err(|_| {
                    McpError::internal_error("pool response serialization failed", None)
                })?;
                if !peers.is_empty()
                    && let Some(object) = structured.as_object_mut()
                {
                    object.insert(
                        "peer_messages".into(),
                        serde_json::to_value(&peers).map_err(|_| {
                            McpError::internal_error("peer message serialization failed", None)
                        })?,
                    );
                }
                let mut text = structured.to_string();
                Self::append_peer_block(&mut text, &peers);
                let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
                result.structured_content = Some(structured);
                Ok(result)
            }
            Err(error) => {
                let mut text = error.message.to_string();
                Self::append_peer_block(&mut text, &peers);
                let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
                if !peers.is_empty() {
                    result.structured_content = Some(serde_json::json!({"peer_messages": peers}));
                }
                Ok(result)
            }
        }
    }

    /// Run a stateless shell command. Returns exit_code when finished, or a memorable session_id when still running after yield_time_ms.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn exec_command(
        &self,
        Parameters(args): Parameters<ExecCommandArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self.manager.exec_command(args).await;
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        Self::respond(response, peers)
    }

    /// Explicitly start a stateful shell, REPL, or long-running process. The process uses a PTY by default.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn start_session(
        &self,
        Parameters(args): Parameters<StartSessionArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self.manager.start_session(args).await;
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        Self::respond(response, peers)
    }

    /// Write raw characters to a running process or poll it with empty chars. A chars value containing only Ctrl-C interrupts the process group.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn write_stdin(
        &self,
        Parameters(args): Parameters<WriteStdinArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self.manager.write_stdin(args).await;
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        Self::respond(response, peers)
    }

    /// Wait for a running process to exit or for wait_seconds to elapse. Ordinary stdout/stderr output does not wake the wait; use write_stdin for immediate output polling, input, or interruption.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn wait_for_exit(
        &self,
        Parameters(args): Parameters<WaitForExitArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self
            .manager
            .wait_for_exit_cancellable(args, context.ct.cancelled())
            .await;
        match response {
            Ok(Some(response)) => {
                let peers = self
                    .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
                    .await;
                Self::success(response, peers)
            }
            Ok(None) => Err(McpError::internal_error("wait_for_exit cancelled", None)),
            Err(error) => {
                let peers = self
                    .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
                    .await;
                Ok(Self::tool_error(error, peers))
            }
        }
    }

    fn respond(
        result: anyhow::Result<ExecResponse>,
        peers: Vec<PeerMessage>,
    ) -> Result<CallToolResult, McpError> {
        match result {
            Ok(response) => Self::success(response, peers),
            Err(error) => Ok(Self::tool_error(error, peers)),
        }
    }

    fn success(
        mut response: ExecResponse,
        peers: Vec<PeerMessage>,
    ) -> Result<CallToolResult, McpError> {
        if !peers.is_empty() {
            response.peer_messages = Some(peers.clone());
        }
        let structured = serde_json::to_value(&response)
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        let mut text = Self::response_summary(&response);
        Self::append_peer_block(&mut text, &peers);
        let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
        result.structured_content = Some(structured);
        Ok(result)
    }

    fn response_summary(response: &ExecResponse) -> String {
        let mut parts = Vec::with_capacity(7);
        if let Some(exit_code) = response.exit_code {
            parts.push(format!("exit_code={exit_code}"));
        } else if let Some(session_id) = response.session_id.as_deref() {
            parts.push(format!("session_id={session_id}"));
        }
        parts.push(format!("output_bytes={}", response.output.len()));
        if response.output_truncated {
            parts.push("output_truncated=true".into());
        }
        if response.output_encoding_loss {
            parts.push("output_encoding_loss=true".into());
        }
        if response.capture_error.is_some() {
            parts.push("capture_error=present".into());
        }
        if let Some(output_ref) = response.output_ref.as_ref() {
            parts.push(format!("output_ref={}", output_ref.path));
        }
        parts.join("; ")
    }

    fn append_peer_block(text: &mut String, peers: &[PeerMessage]) {
        if peers.is_empty() {
            return;
        }
        text.push_str("\n\n=== PEER MESSAGES ===");
        for message in peers {
            text.push_str(&format!(
                "\n[{} {} -> {} in {}] {}",
                message.message_id, message.from, message.to, message.pool, message.message
            ));
            if let Some(reply) = message.in_reply_to.as_deref() {
                text.push_str(&format!(" (in_reply_to={reply})"));
            }
        }
        text.push_str("\n=== END PEER MESSAGES ===");
    }

    fn tool_error(error: anyhow::Error, peers: Vec<PeerMessage>) -> CallToolResult {
        let mut text = error.to_string();
        Self::append_peer_block(&mut text, &peers);
        let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
        if !peers.is_empty() {
            result.structured_content = Some(serde_json::json!({"peer_messages": peers}));
        }
        result
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ExecMcpServer {
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        if self.strict_http {
            Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
        } else {
            Cow::Borrowed(ProtocolVersion::KNOWN_VERSIONS)
        }
    }

    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("chatgpt-exec-mcp", env!("CARGO_PKG_VERSION"))
                    .with_title("Exec MCP"),
            )
            .with_instructions(self.manager.instructions())
    }
}
