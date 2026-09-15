use std::sync::Arc;

use rmcp::handler::server::{
    router::tool::ToolRouter, tool::schema_for_output, wrapper::Parameters,
};
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerInfo};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler, tool, tool_handler, tool_router};

use crate::process_manager::ProcessManager;
use crate::tools::{
    ExecCommandArgs, ExecResponse, StartSessionArgs, WaitForExitArgs, WriteStdinArgs,
};

#[derive(Clone)]
pub struct ExecMcpServer {
    manager: Arc<ProcessManager>,
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl ExecMcpServer {
    pub fn new(manager: Arc<ProcessManager>) -> Self {
        Self {
            manager,
            tool_router: Self::tool_router(),
        }
    }

    /// Run a stateless shell command. Returns exit_code when finished, or a memorable session_id when still running after yield_time_ms.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn exec_command(
        &self,
        Parameters(args): Parameters<ExecCommandArgs>,
    ) -> Result<CallToolResult, McpError> {
        Self::respond(self.manager.exec_command(args).await)
    }

    /// Explicitly start a stateful shell, REPL, or long-running process. The process uses a PTY by default.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn start_session(
        &self,
        Parameters(args): Parameters<StartSessionArgs>,
    ) -> Result<CallToolResult, McpError> {
        Self::respond(self.manager.start_session(args).await)
    }

    /// Write raw characters to a running process or poll it with empty chars. A chars value containing only Ctrl-C interrupts the process group.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn write_stdin(
        &self,
        Parameters(args): Parameters<WriteStdinArgs>,
    ) -> Result<CallToolResult, McpError> {
        Self::respond(self.manager.write_stdin(args).await)
    }

    /// Wait for a running process to exit or for wait_seconds to elapse. Ordinary stdout/stderr output does not wake the wait; use write_stdin for immediate output polling, input, or interruption.
    #[tool(output_schema = schema_for_output::<ExecResponse>())]
    async fn wait_for_exit(
        &self,
        Parameters(args): Parameters<WaitForExitArgs>,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        match self
            .manager
            .wait_for_exit_cancellable(args, context.ct.cancelled())
            .await
        {
            Ok(Some(response)) => Self::success(response),
            Ok(None) => Err(McpError::internal_error("wait_for_exit cancelled", None)),
            Err(error) => Ok(Self::tool_error(error)),
        }
    }

    fn respond(result: anyhow::Result<ExecResponse>) -> Result<CallToolResult, McpError> {
        match result {
            Ok(response) => Self::success(response),
            Err(error) => Ok(Self::tool_error(error)),
        }
    }

    fn success(response: ExecResponse) -> Result<CallToolResult, McpError> {
        let structured = serde_json::to_value(&response)
            .map_err(|error| McpError::internal_error(error.to_string(), None))?;
        let text = Self::response_summary(&response);
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

    fn tool_error(error: anyhow::Error) -> CallToolResult {
        CallToolResult::error(vec![ContentBlock::text(error.to_string())])
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for ExecMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("chatgpt-exec-mcp", env!("CARGO_PKG_VERSION"))
                    .with_title("Exec MCP"),
            )
            .with_instructions(self.manager.instructions())
    }
}
