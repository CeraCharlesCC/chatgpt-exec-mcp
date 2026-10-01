use std::{borrow::Cow, sync::Arc};

use serde_json::{Value, json};

use rmcp::handler::server::{
    router::tool::ToolRouter, tool::schema_for_output, wrapper::Parameters,
};
use rmcp::model::{
    CallToolResult, ContentBlock, ErrorCode, Implementation, ProtocolVersion, ServerCapabilities,
    ServerConfig,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler, tool, tool_handler, tool_router};

use crate::activity::ActivityHub;
use crate::agent_pool::{
    AgentIdentity, AgentPoolStore, PeerMessage, PoolMembersArgs, PoolMembersResult, PoolSendArgs,
    PoolSendResult, SessionTurn,
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
    activity: Arc<ActivityHub>,
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
            activity: Arc::new(ActivityHub::new(500)),
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

    pub(crate) fn activity_hub(&self) -> Arc<ActivityHub> {
        Arc::clone(&self.activity)
    }

    pub(crate) fn admin_context(&self) -> Option<(Arc<AgentPoolStore>, String)> {
        match (&self.agent_pool, &self.principal) {
            (Some(pool), Some(principal)) => Some((Arc::clone(pool), principal.clone())),
            _ => None,
        }
    }

    fn activity_agents(&self, session: Option<&str>) -> Vec<AgentIdentity> {
        match (&self.agent_pool, &self.principal) {
            (Some(pool), Some(principal)) => pool
                .identities_for_session(principal, session)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    fn tool_started(&self, tool: &str, session: Option<&str>, arguments: Value) -> u64 {
        self.activity.emit(
            "tool.start",
            "info",
            session,
            self.activity_agents(session),
            json!({"tool": tool, "arguments": arguments}),
        )
    }

    fn tool_finished(
        &self,
        tool: &str,
        session: Option<&str>,
        start_event_id: u64,
        success: bool,
        detail: Value,
    ) {
        self.activity.emit(
            if success { "tool.finish" } else { "tool.error" },
            if success { "info" } else { "error" },
            session,
            self.activity_agents(session),
            json!({
                "tool": tool,
                "start_event_id": start_event_id,
                "result": detail,
            }),
        );
    }

    fn pool_activity_detail<T: serde::Serialize>(
        result: &Result<T, McpError>,
        peers: &[PeerMessage],
    ) -> (bool, Value) {
        match result {
            Ok(value) => (
                true,
                Self::activity_with_peers(json!({"value": value}), peers),
            ),
            Err(error) => (
                false,
                Self::activity_with_peers(json!({"error": error.message.to_string()}), peers),
            ),
        }
    }

    fn exec_activity_detail(
        result: &anyhow::Result<ExecResponse>,
        peers: &[PeerMessage],
    ) -> (bool, Value) {
        match result {
            Ok(response) => (true, Self::exec_response_detail(response, peers)),
            Err(error) => (
                false,
                Self::activity_with_peers(json!({"error": error.to_string()}), peers),
            ),
        }
    }

    fn exec_response_detail(response: &ExecResponse, peers: &[PeerMessage]) -> Value {
        // Use the wire result's sparse fields; add diagnostics only to activity.
        let mut detail = json!(response);
        if let Some(object) = detail.as_object_mut() {
            if !response.output.is_empty() {
                object.insert(
                    "output".into(),
                    json!(Self::activity_preview(&response.output)),
                );
            }
            object.insert("output_bytes".into(), json!(response.output.len()));
            object.insert(
                "call_wall_time_seconds".into(),
                json!(response.call_wall_time_seconds),
            );
        }
        Self::activity_with_peers(detail, peers)
    }

    fn activity_with_peers(mut detail: Value, peers: &[PeerMessage]) -> Value {
        if !peers.is_empty()
            && let Some(object) = detail.as_object_mut()
        {
            object.insert("peer_messages".into(), json!(peers));
        }
        detail
    }

    fn activity_preview(value: &str) -> String {
        const MAX: usize = 8 * 1024;
        if value.len() <= MAX {
            return value.to_owned();
        }
        let mut end = MAX;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}\n… <{} bytes omitted>", &value[..end], value.len() - end)
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
            Err(error) => {
                eprintln!("agent pool piggyback collection failed; preserving primary tool result");
                self.activity.emit(
                    "mcp.warning",
                    "warn",
                    session,
                    self.activity_agents(session),
                    json!({
                        "message": "agent pool piggyback collection failed; preserving primary tool result",
                        "error": error.message.to_string(),
                    }),
                );
                Vec::new()
            }
        }
    }

    /// List leased members; optionally wait for session messages (any pool) or
    /// membership changes here. Does not join.
    #[tool(output_schema = schema_for_output::<PoolMembersResult>(), annotations(read_only_hint = true))]
    async fn pool_members(
        &self,
        Parameters(args): Parameters<PoolMembersArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let started = self.tool_started(
            "pool_members",
            session.as_deref(),
            serde_json::to_value(&args).unwrap_or(Value::Null),
        );
        let turn = self.begin_tool(session.as_deref()).await?;
        let (pool, principal) = self.agent_pool_context()?;
        let result = tokio::select! {
            biased;
            _ = context.ct.cancelled() => {
                self.tool_finished("pool_members", session.as_deref(), started, false,
                    json!({"error": "pool_members cancelled"}));
                // Cancellation must not offer unread inbox rows in a response
                // the caller will discard. Membership and pending rows remain.
                return Err(McpError::internal_error("pool_members cancelled", None));
            },
            result = pool.wait_members(principal, args, session.as_deref()) => result,
        };
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        let (success, detail) = Self::pool_activity_detail(&result, &peers);
        self.tool_finished("pool_members", session.as_deref(), started, success, detail);
        Self::pool_result(result, peers)
    }

    /// Queue a message; first send joins and assigns a name. Messages arrive on
    /// tool calls, never wake idle chats; no ACK reply needed.
    #[tool(output_schema = schema_for_output::<PoolSendResult>(), annotations(read_only_hint = false, destructive_hint = false, open_world_hint = true, idempotent_hint = false))]
    async fn pool_send(
        &self,
        Parameters(args): Parameters<PoolSendArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let started = self.tool_started(
            "pool_send",
            session.as_deref(),
            serde_json::to_value(&args).unwrap_or(Value::Null),
        );
        let turn = self.begin_tool(session.as_deref()).await?;
        let (pool, principal) = self.agent_pool_context()?;
        let result = pool.send(principal, args, session.as_deref());
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        let (success, detail) = Self::pool_activity_detail(&result, &peers);
        self.tool_finished("pool_send", session.as_deref(), started, success, detail);
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
                let mut result =
                    CallToolResult::success(vec![ContentBlock::text(structured.to_string())]);
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

    /// Run a stateless shell command; returns exit_code when done, session_id while running.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn exec_command(
        &self,
        Parameters(args): Parameters<ExecCommandArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let started = self.tool_started(
            "exec_command",
            session.as_deref(),
            json!({
                "cmd": &args.cmd,
                "workdir": &args.workdir,
                "tty": args.tty,
                "yield_time_ms": args.yield_time_ms,
                "max_output_tokens": args.max_output_tokens,
            }),
        );
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self.manager.exec_command(args).await;
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        let (success, detail) = Self::exec_activity_detail(&response, &peers);
        self.tool_finished("exec_command", session.as_deref(), started, success, detail);
        Self::respond(response, peers)
    }

    /// Start a persistent shell, REPL, or long-running process.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn start_session(
        &self,
        Parameters(args): Parameters<StartSessionArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let started = self.tool_started(
            "start_session",
            session.as_deref(),
            json!({
                "cmd": &args.cmd,
                "workdir": &args.workdir,
                "tty": args.tty,
                "max_output_tokens": args.max_output_tokens,
            }),
        );
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self.manager.start_session(args).await;
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        let (success, detail) = Self::exec_activity_detail(&response, &peers);
        self.tool_finished(
            "start_session",
            session.as_deref(),
            started,
            success,
            detail,
        );
        Self::respond(response, peers)
    }

    /// Send raw input or poll output; Ctrl-C alone interrupts the process group.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn write_stdin(
        &self,
        Parameters(args): Parameters<WriteStdinArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let started = self.tool_started(
            "write_stdin",
            session.as_deref(),
            json!({
                "session_id": &args.session_id,
                "chars": &args.chars,
                "max_output_tokens": args.max_output_tokens,
            }),
        );
        let turn = self.begin_tool(session.as_deref()).await?;
        let response = self.manager.write_stdin(args).await;
        let peers = self
            .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
            .await;
        let (success, detail) = Self::exec_activity_detail(&response, &peers);
        self.tool_finished("write_stdin", session.as_deref(), started, success, detail);
        Self::respond(response, peers)
    }

    /// Wait for process exit or timeout; ordinary output does not wake the wait.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn wait_for_exit(
        &self,
        Parameters(args): Parameters<WaitForExitArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let started = self.tool_started(
            "wait_for_exit",
            session.as_deref(),
            json!({
                "session_id": &args.session_id,
                "wait_seconds": args.wait_seconds,
                "max_output_tokens": args.max_output_tokens,
            }),
        );
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
                let detail = Self::exec_response_detail(&response, &peers);
                self.tool_finished("wait_for_exit", session.as_deref(), started, true, detail);
                Self::success(response, peers)
            }
            Ok(None) => {
                self.tool_finished(
                    "wait_for_exit",
                    session.as_deref(),
                    started,
                    false,
                    json!({"error": "wait_for_exit cancelled"}),
                );
                Err(McpError::internal_error("wait_for_exit cancelled", None))
            }
            Err(error) => {
                let peers = self
                    .finish_tool_preserving_result(session.as_deref(), turn.as_ref())
                    .await;
                self.tool_finished(
                    "wait_for_exit",
                    session.as_deref(),
                    started,
                    false,
                    Self::activity_with_peers(json!({"error": error.to_string()}), &peers),
                );
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
        let mut parts = Vec::with_capacity(6);
        if let Some(exit_code) = response.exit_code {
            parts.push(format!("exit_code={exit_code}"));
        } else if let Some(session_id) = response.session_id.as_deref() {
            parts.push(format!("session_id={session_id}"));
        }
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
