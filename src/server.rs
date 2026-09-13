use std::borrow::Cow;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::handler::server::ServerHandler;
use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::model::ContentBlock;
use rmcp::model::Implementation;
use rmcp::model::JsonObject;
use rmcp::model::ListToolsResult;
use rmcp::model::PaginatedRequestParams;
use rmcp::model::ServerCapabilities;
use rmcp::model::ServerInfo;
use rmcp::model::Tool;
use serde::de::DeserializeOwned;
use serde_json::Value;
use serde_json::json;

use crate::process_manager::ProcessManager;
use crate::tools::ExecCommandArgs;
use crate::tools::ExecResponse;
use crate::tools::StartSessionArgs;
use crate::tools::WaitForExitArgs;
use crate::tools::WriteStdinArgs;

#[derive(Clone)]
pub struct ExecMcpServer {
    manager: Arc<ProcessManager>,
    tools: Arc<Vec<Tool>>,
}

impl ExecMcpServer {
    pub fn new(manager: Arc<ProcessManager>) -> Self {
        Self {
            manager,
            tools: Arc::new(vec![
                exec_command_tool(),
                start_session_tool(),
                write_stdin_tool(),
                wait_for_exit_tool(),
            ]),
        }
    }

    fn parse_args<T: DeserializeOwned>(
        arguments: Option<JsonObject>,
        tool_name: &str,
        arguments_optional: bool,
    ) -> Result<T, McpError> {
        let value = match arguments {
            Some(arguments) => Value::Object(arguments.into_iter().collect()),
            None if arguments_optional => Value::Object(Default::default()),
            None => {
                return Err(McpError::invalid_params(
                    format!("missing arguments for {tool_name}"),
                    None,
                ));
            }
        };
        serde_json::from_value(value)
            .map_err(|error| McpError::invalid_params(error.to_string(), None))
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

impl ServerHandler for ExecMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("chatgpt-exec-mcp", env!("CARGO_PKG_VERSION"))
                    .with_title("Exec MCP"),
            )
            .with_instructions(self.manager.instructions())
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        let tools = Arc::clone(&self.tools);
        async move { Ok(ListToolsResult::with_all_items((*tools).clone())) }
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::service::RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, McpError> {
        let result = match request.name.as_ref() {
            "exec_command" => {
                let args =
                    Self::parse_args::<ExecCommandArgs>(request.arguments, "exec_command", false)?;
                self.manager.exec_command(args).await
            }
            "start_session" => {
                let args =
                    Self::parse_args::<StartSessionArgs>(request.arguments, "start_session", true)?;
                self.manager.start_session(args).await
            }
            "write_stdin" => {
                let args =
                    Self::parse_args::<WriteStdinArgs>(request.arguments, "write_stdin", false)?;
                self.manager.write_stdin(args).await
            }
            "wait_for_exit" => {
                let args =
                    Self::parse_args::<WaitForExitArgs>(request.arguments, "wait_for_exit", false)?;
                match self
                    .manager
                    .wait_for_exit_cancellable(args, context.ct.cancelled())
                    .await
                {
                    Ok(Some(response)) => Ok(response),
                    Ok(None) => {
                        return Err(McpError::internal_error("wait_for_exit cancelled", None));
                    }
                    Err(error) => Err(error),
                }
            }
            other => {
                return Err(McpError::invalid_params(
                    format!("unknown tool: {other}"),
                    None,
                ));
            }
        };

        match result {
            Ok(response) => Self::success(response).map(Into::into),
            Err(error) => Ok(Self::tool_error(error).into()),
        }
    }
}

fn object_schema(properties: Value, required: &[&str]) -> Arc<JsonObject> {
    let schema = json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    });
    Arc::new(serde_json::from_value(schema).expect("static MCP tool schema must be a JSON object"))
}

fn output_schema() -> Arc<JsonObject> {
    object_schema(
        json!({
            "call_wall_time_seconds": {
                "type": "number",
                "minimum": 0,
                "description": "Wall-clock time spent servicing this tool call, not total session runtime."
            },
            "exit_code": { "type": "integer" },
            "session_id": { "type": "string", "pattern": "^[a-z]+-[a-z]+$" },
            "output": { "type": "string", "description": "Output text within the byte/token budget. Large output may use a bounded projection." },
            "output_truncated": { "type": "boolean" },
            "output_encoding_loss": { "type": "boolean" },
            "capture_error": { "type": "string" },
            "output_ref": {
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "range_start": { "type": "integer", "minimum": 0 },
                    "range_end": { "type": "integer", "minimum": 0 },
                    "stored_bytes": { "type": "integer", "minimum": 0 },
                    "capture_status": {
                        "type": "string",
                        "enum": ["open", "complete", "incomplete"]
                    },
                    "expires_at_unix_seconds": { "type": "integer", "minimum": 0 },
                    "incomplete_reason": { "type": "string" }
                },
                "required": ["path", "range_start", "range_end", "stored_bytes", "capture_status"],
                "additionalProperties": false
            }
        }),
        &[
            "call_wall_time_seconds",
            "output",
            "output_truncated",
            "output_encoding_loss",
        ],
    )
}

fn exec_command_tool() -> Tool {
    let mut tool = Tool::new(
        Cow::Borrowed("exec_command"),
        Cow::Borrowed(
            "Run a stateless shell command. Returns exit_code when finished, or a memorable session_id when still running after yield_time_ms.",
        ),
        object_schema(
            json!({
                "cmd": { "type": "string", "minLength": 1, "description": "Shell command passed to the server-configured shell with -lc." },
                "workdir": { "type": "string", "description": "Absolute path, or a path resolved relative to the workspace base directory. The workspace is not a sandbox boundary." },
                "tty": { "type": "boolean", "default": false, "description": "Allocate a PTY." },
                "yield_time_ms": { "type": "integer", "minimum": 10, "maximum": 120000, "default": 10000, "description": "Initial wait before a still-running command becomes a continuation." },
                "max_output_tokens": { "type": "integer", "minimum": 1, "description": "Optional approximate display budget. One token is treated as four bytes; this is not a tokenizer guarantee." }
            }),
            &["cmd"],
        ),
    );
    tool.output_schema = Some(output_schema());
    tool
}

fn start_session_tool() -> Tool {
    let mut tool = Tool::new(
        Cow::Borrowed("start_session"),
        Cow::Borrowed(
            "Explicitly start a stateful shell, REPL, or long-running process. The process uses a PTY by default.",
        ),
        object_schema(
            json!({
                "cmd": { "type": "string", "minLength": 1, "description": "Long-lived shell command. Omit to use the server-configured shell." },
                "workdir": { "type": "string", "description": "Initial cwd as an absolute path, or relative to the workspace base directory. The workspace is not a sandbox boundary." },
                "tty": { "type": "boolean", "default": true, "description": "Allocate a PTY." },
                "max_output_tokens": { "type": "integer", "minimum": 1, "description": "Optional approximate display budget. One token is treated as four bytes; this is not a tokenizer guarantee." }
            }),
            &[],
        ),
    );
    tool.output_schema = Some(output_schema());
    tool
}

fn write_stdin_tool() -> Tool {
    let mut tool = Tool::new(
        Cow::Borrowed("write_stdin"),
        Cow::Borrowed(
            "Write raw characters to a running process or poll it with empty chars. A chars value containing only Ctrl-C interrupts the process group.",
        ),
        object_schema(
            json!({
                "session_id": { "type": "string", "pattern": "^[a-z]+-[a-z]+$", "description": "Memorable process handle returned by exec_command or start_session." },
                "chars": { "type": "string", "default": "", "description": "Raw input. Empty input returns pending output immediately, otherwise briefly polls for activity." },
                "max_output_tokens": { "type": "integer", "minimum": 1, "description": "Optional approximate display budget. One token is treated as four bytes; this is not a tokenizer guarantee." }
            }),
            &["session_id"],
        ),
    );
    tool.output_schema = Some(output_schema());
    tool
}

fn wait_for_exit_tool() -> Tool {
    let mut tool = Tool::new(
        Cow::Borrowed("wait_for_exit"),
        Cow::Borrowed(
            "Wait for a running process to exit or for wait_seconds to elapse. Ordinary stdout/stderr output does not wake the wait; use write_stdin for immediate output polling, input, or interruption.",
        ),
        object_schema(
            json!({
                "session_id": { "type": "string", "pattern": "^[a-z]+-[a-z]+$", "description": "Memorable process handle returned by exec_command or start_session." },
                "wait_seconds": { "type": "integer", "minimum": 15, "maximum": 100, "default": 35, "description": "Maximum time to wait for process exit. Ordinary output does not end the wait." },
                "max_output_tokens": { "type": "integer", "minimum": 1, "description": "Optional approximate display budget. One token is treated as four bytes; this is not a tokenizer guarantee." }
            }),
            &["session_id"],
        ),
    );
    tool.output_schema = Some(output_schema());
    tool
}
