use std::{borrow::Cow, sync::Arc};

use serde_json::{Value, json};

use rmcp::handler::server::{
    router::tool::ToolRouter, tool::schema_for_output, wrapper::Parameters,
};
use rmcp::model::{
    CallToolResult, ErrorCode, Implementation, ProtocolVersion, ServerCapabilities, ServerConfig,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler, tool, tool_handler, tool_router};

use crate::activity::{ActivityContext, ActivityEmissionContext, ActivityHub, ActivityMessage};
use crate::agent_pool::{
    AgentIdentity, AgentPoolStore, Communications, PoolMembersArgs, PoolMembersResult,
    PoolSendArgs, PoolSendResult, PoolWaitArgs, PoolWaitResult, SessionTurn,
};
use crate::agent_pool_context::AgentPoolContext;
use crate::process_manager::ProcessManager;
use crate::tools::{ExecCommandArgs, ExecResponse, WaitForExitArgs, WriteStdinArgs};

mod coordination;
mod response;
use coordination::ToolCall;

#[derive(Clone)]
pub struct ExecMcpServer {
    manager: Arc<ProcessManager>,
    tool_router: ToolRouter<Self>,
    agent_pool: Option<AgentPoolContext>,
    activity: Arc<ActivityHub>,
    strict_http: bool,
}

#[tool_router]
impl ExecMcpServer {
    pub fn new(manager: Arc<ProcessManager>) -> Self {
        let mut tool_router = Self::tool_router();
        tool_router.disable_route("pool_members");
        tool_router.disable_route("pool_send");
        tool_router.disable_route("pool_wait");
        Self {
            manager,
            tool_router,
            agent_pool: None,
            activity: Arc::new(ActivityHub::disabled()),
            strict_http: false,
        }
    }

    /// The dedicated, authenticated tunnel belongs to this configured account.
    /// Request metadata cannot change the principal.
    pub fn with_agent_pool(mut self, store: Arc<AgentPoolStore>, principal: String) -> Self {
        self.agent_pool = Some(AgentPoolContext::new(store, principal));
        self.tool_router.enable_route("pool_members");
        self.tool_router.enable_route("pool_send");
        self.tool_router.enable_route("pool_wait");
        self
    }

    /// Enable the optional diagnostic activity feed, including its historical
    /// 500-event in-memory window and compressed history. Without this opt-in,
    /// MCP-only runs avoid diagnostic allocations and database identity reads.
    /// Repeated calls preserve any already collected activity history.
    pub fn with_activity_feed(mut self) -> Self {
        if !self.activity.is_enabled() {
            self.activity = Arc::new(ActivityHub::new(500));
        }
        self
    }

    pub fn with_http_protocol(mut self) -> Self {
        self.strict_http = true;
        self
    }

    pub(crate) fn activity_hub(&self) -> Arc<ActivityHub> {
        Arc::clone(&self.activity)
    }

    pub(crate) fn admin_context(&self) -> Option<AgentPoolContext> {
        self.agent_pool.clone()
    }

    async fn activity_agents(&self, session: Option<&str>) -> Vec<AgentIdentity> {
        if !self.activity.is_enabled() {
            return Vec::new();
        }
        let Some(context) = self.agent_pool.as_ref() else {
            return Vec::new();
        };
        context
            .store()
            .identities_for_session_async(context.principal(), session)
            .await
            .unwrap_or_default()
    }

    async fn tool_started(
        &self,
        tool: &str,
        session: Option<&str>,
        arguments: impl FnOnce() -> Value,
    ) -> u64 {
        if !self.activity.is_enabled() {
            return 0;
        }
        self.activity.emit(
            "tool.start",
            "info",
            session,
            self.activity_agents(session).await,
            json!({"tool": tool, "arguments": arguments()}),
        )
    }

    #[cfg(test)]
    async fn tool_finished(
        &self,
        tool: &str,
        session: Option<&str>,
        start_event_id: u64,
        success: bool,
        detail: impl FnOnce() -> Value,
    ) {
        self.tool_finished_with_context(
            tool,
            session,
            start_event_id,
            success,
            ActivityContext::default(),
            detail,
        )
        .await;
    }

    async fn tool_finished_with_context(
        &self,
        tool: &str,
        session: Option<&str>,
        start_event_id: u64,
        success: bool,
        activity_context: ActivityContext,
        detail: impl FnOnce() -> Value,
    ) {
        self.tool_finished_with_context_and_communications(
            tool,
            session,
            start_event_id,
            success,
            ActivityEmissionContext {
                activity: activity_context,
                communications: None,
            },
            detail,
        )
        .await;
    }

    async fn tool_finished_with_context_and_communications(
        &self,
        tool: &str,
        session: Option<&str>,
        start_event_id: u64,
        success: bool,
        emission: ActivityEmissionContext<'_>,
        detail: impl FnOnce() -> Value,
    ) {
        if !self.activity.is_enabled() {
            return;
        }
        self.activity.emit_with_context_and_communications(
            if success { "tool.finish" } else { "tool.error" },
            if success { "info" } else { "error" },
            session,
            self.activity_agents(session).await,
            json!({
                "tool": tool,
                "start_event_id": start_event_id,
                "result": detail(),
            }),
            emission,
        );
    }

    fn pool_activity_detail<T: serde::Serialize>(
        result: &Result<T, McpError>,
        comms: &Communications,
    ) -> (bool, Value) {
        match result {
            Ok(value) => (true, response::with_comms(json!({"value": value}), comms)),
            Err(error) => (
                false,
                response::with_comms(json!({"error": error.message.to_string()}), comms),
            ),
        }
    }

    fn exec_activity_detail(
        result: &anyhow::Result<ExecResponse>,
        comms: &Communications,
    ) -> (bool, Value) {
        match result {
            Ok(response) => (true, Self::exec_response_detail(response, comms)),
            Err(error) => (
                false,
                response::with_comms(json!({"error": error.to_string()}), comms),
            ),
        }
    }

    fn exec_response_detail(response: &ExecResponse, comms: &Communications) -> Value {
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
        response::with_comms(detail, comms)
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
        match self.agent_pool.as_ref() {
            Some(context) if !context.principal().is_empty() => {
                Ok((context.store(), context.principal()))
            }
            None | Some(_) => Err(McpError::new(
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
        let Some(context) = self.agent_pool.as_ref() else {
            return Ok(None);
        };
        context
            .store()
            .start_tool(context.principal(), session)
            .await
    }

    async fn finish_tool(
        &self,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Result<Communications, McpError> {
        let Some(context) = self.agent_pool.as_ref() else {
            return Ok(Communications::default());
        };
        context
            .store()
            .finish_tool_turn(context.principal(), session, turn)
            .await
    }

    /// Piggyback collection happens after the primary tool operation. Storage
    /// failure here must not hide a command or send that already succeeded,
    /// because a caller could otherwise retry a non-idempotent operation.
    async fn finish_tool_preserving_result(
        &self,
        session: Option<&str>,
        turn: Option<&SessionTurn>,
    ) -> Communications {
        match self.finish_tool(session, turn).await {
            Ok(comms) => comms,
            Err(error) => {
                eprintln!("agent pool piggyback collection failed; preserving primary tool result");
                if self.activity.is_enabled() {
                    self.activity.emit(
                        "mcp.warning",
                        "warn",
                        session,
                        self.activity_agents(session).await,
                        json!({
                            "message": "agent pool piggyback collection failed; preserving primary tool result",
                            "error": error.message.to_string(),
                        }),
                    );
                }
                Communications::default()
            }
        }
    }

    /// List leased members immediately. Does not join; use pool_wait to wait.
    #[tool(output_schema = schema_for_output::<PoolMembersResult>(), annotations(read_only_hint = true))]
    async fn pool_members(
        &self,
        Parameters(args): Parameters<PoolMembersArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let activity_context = if self.activity.is_enabled() {
            ActivityContext {
                pool: Some(args.pool.clone()),
                ..ActivityContext::default()
            }
        } else {
            ActivityContext::default()
        };
        let call = ToolCall::begin(
            self,
            "pool_members",
            &context,
            || serde_json::to_value(&args).unwrap_or(Value::Null),
            activity_context,
            false,
        )
        .await?;
        let (pool, principal) = self.agent_pool_context()?;
        let result = pool.members_async(principal, args, call.session()).await;
        call.finish_pool(result).await
    }

    /// Wait for this pool's messages, offered receipts, membership changes, or timeout.
    /// Returns messages directly; timeout is a normal result. Does not join.
    #[tool(output_schema = schema_for_output::<PoolWaitResult>(), annotations(read_only_hint = true))]
    async fn pool_wait(
        &self,
        Parameters(args): Parameters<PoolWaitArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let session = Self::session(&context);
        let activity_context = if self.activity.is_enabled() {
            ActivityContext {
                pool: Some(args.pool.clone()),
                ..ActivityContext::default()
            }
        } else {
            ActivityContext::default()
        };
        let started = self
            .tool_started("pool_wait", session.as_deref(), || json!(&args))
            .await;
        let (pool, principal) = self.agent_pool_context()?;
        let result = tokio::select! {
            biased;
            _ = context.ct.cancelled() => {
                self.tool_finished_with_context("pool_wait", session.as_deref(), started, false, activity_context,
                    || json!({"error": "pool_wait cancelled"})).await;
                return Err(McpError::internal_error("pool_wait cancelled", None));
            },
            result = pool.wait(principal, args, session.as_deref()) => result,
        };
        // A wait's own typed operation result is the source of peer messages.
        // It is not a piggyback turn and must not collect communications again.
        let lifecycle_comms = match &result {
            Ok(value) => Communications {
                messages: value.messages.clone(),
                delivery_receipts: value.delivery_receipts.clone(),
            },
            Err(_) => Communications::default(),
        };
        self.tool_finished_with_context_and_communications(
            "pool_wait",
            session.as_deref(),
            started,
            result.is_ok(),
            ActivityEmissionContext {
                activity: activity_context,
                communications: Some(&lifecycle_comms),
            },
            || match &result {
                Ok(value) => json!(value),
                Err(error) => json!({"error": error.message.to_string()}),
            },
        )
        .await;
        response::pool_result(result, Communications::default())
    }

    /// Queue a message; first send joins and assigns a name. Messages arrive on
    /// tool calls, never wake idle chats. Direct sends later get offered receipts;
    /// these confirm response preparation, not receipt, acceptance, or completion.
    #[tool(output_schema = schema_for_output::<PoolSendResult>(), annotations(read_only_hint = false, destructive_hint = false, open_world_hint = true, idempotent_hint = false))]
    async fn pool_send(
        &self,
        Parameters(args): Parameters<PoolSendArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        // PoolSendArgs is consumed by send_async. Keep its message metadata
        // only for opted-in activity so completion events are self-contained.
        let activity_context = if self.activity.is_enabled() {
            ActivityContext {
                pool: Some(args.pool.clone()),
                message: ActivityMessage::from_pool_send_args(&args),
            }
        } else {
            ActivityContext::default()
        };
        let call = ToolCall::begin(
            self,
            "pool_send",
            &context,
            || serde_json::to_value(&args).unwrap_or(Value::Null),
            activity_context,
            true,
        )
        .await?;
        let (pool, principal) = match self.agent_pool_context() {
            Ok(pool) => pool,
            Err(error) => return Err(call.fail(error).await),
        };
        let result = pool.send_async(principal, args, call.session()).await;
        call.finish_pool_send(result).await
    }

    /// Start a command, shell, or REPL. Returns exit_code when done, session_id while running.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn exec_command(
        &self,
        Parameters(args): Parameters<ExecCommandArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let call = ToolCall::begin(
            self,
            "exec_command",
            &context,
            || {
                json!({
                    "cmd": &args.cmd,
                    "workdir": &args.workdir,
                    "tty": args.tty,
                    "yield_time_ms": args.yield_time_ms,
                    "max_output_tokens": args.max_output_tokens,
                })
            },
            ActivityContext::default(),
            false,
        )
        .await?;
        let response = self.manager.exec_command(args).await;
        call.finish_exec(response).await
    }

    /// Send raw input or poll output; Ctrl-C alone interrupts the process group.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn write_stdin(
        &self,
        Parameters(args): Parameters<WriteStdinArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let call = ToolCall::begin(
            self,
            "write_stdin",
            &context,
            || {
                json!({
                    "session_id": &args.session_id,
                    "chars": &args.chars,
                    "max_output_tokens": args.max_output_tokens,
                })
            },
            ActivityContext::default(),
            false,
        )
        .await?;
        let response = self.manager.write_stdin(args).await;
        call.finish_exec(response).await
    }

    /// Wait for process exit or timeout; ordinary output does not wake the wait.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn wait_for_exit(
        &self,
        Parameters(args): Parameters<WaitForExitArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        let call = ToolCall::begin(
            self,
            "wait_for_exit",
            &context,
            || {
                json!({
                    "session_id": &args.session_id,
                    "wait_seconds": args.wait_seconds,
                    "max_output_tokens": args.max_output_tokens,
                })
            },
            ActivityContext::default(),
            false,
        )
        .await?;
        let response = self
            .manager
            .wait_for_exit_cancellable(args, context.ct.cancelled())
            .await;
        match response {
            Ok(Some(response)) => call.finish_wait_success(response).await,
            Ok(None) => call.cancelled("wait_for_exit cancelled").await,
            Err(error) => call.finish_wait_error(error).await,
        }
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

#[cfg(test)]
mod tests;
