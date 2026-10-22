//! Common tool-call lifecycle: begin the chat turn, perform the caller's
//! operation, collect offered communications, record the outcome, and hand
//! response presentation to `response`. Wait/cancellation remain explicit in
//! the caller: they have different boundaries from ordinary tool operations.

use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, model::CallToolResult};
use serde::Serialize;
use serde_json::{Value, json};

use super::{ExecMcpServer, response};
use crate::activity::{ActivityContext, ActivityEmissionContext, ActivityMessageStatus};
use crate::agent_pool::{Communications, PoolSendResult, SessionTurn};
use crate::tools::ExecResponse;
use crate::view_image::LoadedImage;

pub(super) struct ToolCall<'a> {
    server: &'a ExecMcpServer,
    tool: &'static str,
    session: Option<String>,
    started: u64,
    turn: Option<SessionTurn>,
    activity: ActivityContext,
}

impl<'a> ToolCall<'a> {
    pub(super) async fn begin(
        server: &'a ExecMcpServer,
        tool: &'static str,
        context: &RequestContext<RoleServer>,
        arguments: impl FnOnce() -> Value,
        activity: ActivityContext,
        record_begin_error: bool,
    ) -> Result<Self, McpError> {
        let session = ExecMcpServer::session(context);
        let started = server
            .tool_started(tool, session.as_deref(), arguments)
            .await;
        let turn = match server.begin_tool(session.as_deref()).await {
            Ok(turn) => turn,
            Err(error) => {
                if record_begin_error {
                    server
                        .tool_finished_with_context(
                            tool,
                            session.as_deref(),
                            started,
                            false,
                            activity,
                            || json!({"error": error.message.to_string()}),
                        )
                        .await;
                }
                return Err(error);
            }
        };
        Ok(Self {
            server,
            tool,
            session,
            started,
            turn,
            activity,
        })
    }

    pub(super) fn session(&self) -> Option<&str> {
        self.session.as_deref()
    }

    async fn collect(&self) -> Communications {
        self.server
            .finish_tool_preserving_result(self.session(), self.turn.as_ref())
            .await
    }

    async fn record(
        &mut self,
        success: bool,
        comms: Option<&Communications>,
        detail: impl FnOnce() -> Value,
    ) {
        // The SessionTurn guard must survive until response formatting ends:
        // dropping it here would let a subsequent non-overlapping tool call
        // retire offered messages before this call prepares its response.
        self.server
            .tool_finished_with_context_and_communications(
                self.tool,
                self.session.as_deref(),
                self.started,
                success,
                ActivityEmissionContext {
                    activity: std::mem::take(&mut self.activity),
                    communications: comms,
                },
                detail,
            )
            .await;
    }

    pub(super) async fn fail(mut self, error: McpError) -> McpError {
        self.record(false, None, || json!({"error": error.message.to_string()}))
            .await;
        error
    }

    pub(super) async fn finish_pool<T: Serialize>(
        mut self,
        result: Result<T, McpError>,
    ) -> Result<CallToolResult, McpError> {
        let comms = self.collect().await;
        self.record(result.is_ok(), Some(&comms), || {
            ExecMcpServer::pool_activity_detail(&result, &comms).1
        })
        .await;
        response::pool_result(result, comms)
    }

    pub(super) async fn finish_pool_send(
        mut self,
        result: Result<PoolSendResult, McpError>,
    ) -> Result<CallToolResult, McpError> {
        // Typed operation effects are authoritative for v1 activity. Legacy
        // detail remains in the event for old consumers only.
        if let Some(message) = self.activity.message.as_mut() {
            match &result {
                Ok(value) => {
                    message.from = value.assigned_agent.clone().or(message.from.take());
                    message.message_id = value.message_id.clone();
                    message.recipients = Some(value.recipients.clone());
                    message.status = Some(ActivityMessageStatus::Queued);
                }
                Err(error) => {
                    message.error = Some(error.message.to_string());
                    message.status = Some(ActivityMessageStatus::Failed);
                }
            }
        }
        self.finish_pool(result).await
    }

    pub(super) async fn finish_exec(
        mut self,
        result: anyhow::Result<ExecResponse>,
    ) -> Result<CallToolResult, McpError> {
        let comms = self.collect().await;
        self.record(result.is_ok(), Some(&comms), || {
            ExecMcpServer::exec_activity_detail(&result, &comms).1
        })
        .await;
        response::respond(result, comms)
    }

    pub(super) async fn finish_image(
        mut self,
        result: anyhow::Result<LoadedImage>,
    ) -> Result<CallToolResult, McpError> {
        let comms = self.collect().await;
        self.record(result.is_ok(), Some(&comms), || {
            // Keep image bytes out of diagnostic activity history.
            let detail = match &result {
                Ok(image) => json!({"mime_type": image.mime_type, "bytes": image.byte_len}),
                Err(error) => json!({"error": error.to_string()}),
            };
            response::with_internal_comms(detail, &comms)
        })
        .await;
        Ok(response::image_result(result, comms))
    }

    pub(super) async fn finish_wait_success(
        mut self,
        value: ExecResponse,
    ) -> Result<CallToolResult, McpError> {
        let comms = self.collect().await;
        self.record(true, Some(&comms), || {
            ExecMcpServer::exec_response_detail(&value, &comms)
        })
        .await;
        response::success(value, comms)
    }

    pub(super) async fn finish_wait_error(
        mut self,
        error: anyhow::Error,
    ) -> Result<CallToolResult, McpError> {
        let comms = self.collect().await;
        self.record(false, Some(&comms), || {
            response::with_internal_comms(json!({"error": error.to_string()}), &comms)
        })
        .await;
        Ok(response::tool_error(error, comms))
    }

    pub(super) async fn cancelled(
        mut self,
        message: &'static str,
    ) -> Result<CallToolResult, McpError> {
        self.record(false, None, || json!({"error": message})).await;
        Err(McpError::internal_error(message, None))
    }
}
