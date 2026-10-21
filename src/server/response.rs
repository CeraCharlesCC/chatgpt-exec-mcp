//! MCP tool response presentation. No tool execution, pool mutation, or
//! diagnostic history state belongs in this module.

use rmcp::ErrorData as McpError;
use rmcp::model::{CallToolResult, ContentBlock};
use serde::Serialize;
use serde_json::{Value, json};

use crate::agent_pool::{Communications, DeliveryReceipt, PeerMessage};
use crate::tools::ExecResponse;

/// Activity diagnostics retain internal delivery timestamps.
pub(super) fn with_internal_comms(mut detail: Value, comms: &Communications) -> Value {
    if let Some(object) = detail.as_object_mut() {
        if !comms.messages.is_empty() {
            object.insert("messages".into(), json!(comms.messages));
        }
        if !comms.delivery_receipts.is_empty() {
            object.insert("delivery_receipts".into(), json!(comms.delivery_receipts));
        }
    }
    detail
}

fn with_agent_comms(mut detail: Value, comms: &Communications) -> Value {
    if let Some(object) = detail.as_object_mut() {
        if !comms.messages.is_empty() {
            object.insert(
                "messages".into(),
                json!(
                    comms
                        .messages
                        .iter()
                        .map(PeerMessage::from)
                        .collect::<Vec<_>>()
                ),
            );
        }
        if !comms.delivery_receipts.is_empty() {
            object.insert(
                "delivery_receipts".into(),
                json!(
                    comms
                        .delivery_receipts
                        .iter()
                        .map(DeliveryReceipt::from)
                        .collect::<Vec<_>>()
                ),
            );
        }
    }
    detail
}

pub(super) fn pool_result<T: Serialize>(
    result: Result<T, McpError>,
    comms: Communications,
) -> Result<CallToolResult, McpError> {
    match result {
        Ok(value) => {
            let mut structured = serde_json::to_value(value).map_err(|_| {
                McpError::internal_error("pool response serialization failed", None)
            })?;
            structured = with_agent_comms(structured, &comms);
            let mut result =
                CallToolResult::success(vec![ContentBlock::text(structured.to_string())]);
            result.structured_content = Some(structured);
            Ok(result)
        }
        Err(error) => {
            let mut text = error.message.to_string();
            append_comms_block(&mut text, &comms);
            let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
            if !comms.is_empty() {
                result.structured_content = Some(with_agent_comms(json!({}), &comms));
            }
            Ok(result)
        }
    }
}

pub(super) fn respond(
    result: anyhow::Result<ExecResponse>,
    comms: Communications,
) -> Result<CallToolResult, McpError> {
    match result {
        Ok(response) => success(response, comms),
        Err(error) => Ok(tool_error(error, comms)),
    }
}

pub(super) fn success(
    mut response: ExecResponse,
    comms: Communications,
) -> Result<CallToolResult, McpError> {
    if !comms.messages.is_empty() {
        response.messages = Some(comms.messages.iter().map(PeerMessage::from).collect());
    }
    if !comms.delivery_receipts.is_empty() {
        response.delivery_receipts = Some(
            comms
                .delivery_receipts
                .iter()
                .map(DeliveryReceipt::from)
                .collect(),
        );
    }
    let structured = serde_json::to_value(&response)
        .map_err(|error| McpError::internal_error(error.to_string(), None))?;
    let mut text = response_summary(&response);
    append_comms_block(&mut text, &comms);
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

fn append_comms_block(text: &mut String, comms: &Communications) {
    if !comms.messages.is_empty() {
        text.push_str("\n\n=== MESSAGES ===");
        for message in &comms.messages {
            text.push_str(&format!(
                "\n[{} {} -> {} in {}] {}",
                message.message_id, message.from, message.to, message.pool, message.message
            ));
        }
        text.push_str("\n=== END MESSAGES ===");
    }
    if !comms.delivery_receipts.is_empty() {
        text.push_str("\n\n=== DELIVERY RECEIPTS (offered, not read or accepted) ===");
        for receipt in &comms.delivery_receipts {
            text.push_str(&format!(
                "\n[{} in {}] offered to {}",
                receipt.message_id, receipt.pool, receipt.to
            ));
        }
        text.push_str("\n=== END DELIVERY RECEIPTS ===");
    }
}

pub(super) fn tool_error(error: anyhow::Error, comms: Communications) -> CallToolResult {
    let mut text = error.to_string();
    append_comms_block(&mut text, &comms);
    let mut result = CallToolResult::error(vec![ContentBlock::text(text)]);
    if !comms.is_empty() {
        result.structured_content = Some(with_agent_comms(json!({}), &comms));
    }
    result
}
