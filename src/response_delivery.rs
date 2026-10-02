//! Output response preparation and recovery policy. The process manager remains
//! the coordinated owner of the synchronous cursor/session delivery commit.

use std::io;
use std::sync::Arc;
use std::time::Instant;

use crate::session::{OutputReference, Session, TerminalDelivery};
use crate::tools::{ExecResponse, OutputRef};

pub(crate) struct PreparedExecResponse {
    pub response: ExecResponse,
    pub commit_end: Option<u64>,
    range_start: u64,
    range_end: u64,
    pub terminal: bool,
    delete_output: bool,
    output_ref: Option<OutputReference>,
}

impl PreparedExecResponse {
    pub(crate) fn terminal_delivery(&self) -> TerminalDelivery {
        TerminalDelivery {
            capture_error: self.response.capture_error.clone(),
            output_ref: self.output_ref.clone(),
        }
    }
}

pub(crate) async fn prepare_response(
    session: &Arc<Session>,
    started: Instant,
    max_bytes: usize,
) -> anyhow::Result<PreparedExecResponse> {
    let mut prepared = session.prepare_output(max_bytes).await?;
    let mut capture_error = session.terminal_reason();

    if let Some(read_error) = prepared.read_error.as_deref() {
        let reason = format!("output projection failed: {read_error}");
        merge_error(&mut capture_error, reason.clone());
        session.mark_forced_incomplete(&reason);
        session.terminate().await;
    }

    // Latch terminal state before committing the response cursor. If capture
    // completed after the first snapshot, refresh from the same cursor so the
    // final response includes every committed byte. If it completes later,
    // keep the session for one more poll instead of deleting unseen output.
    let terminal = session.is_terminal();
    if terminal && prepared.read_error.is_none() {
        prepared = session.prepare_output(max_bytes).await?;
        if let Some(read_error) = prepared.read_error.as_deref() {
            let reason = format!("output projection failed: {read_error}");
            merge_error(&mut capture_error, reason.clone());
            session.mark_forced_incomplete(&reason);
            session.terminate().await;
        }
    }

    if terminal {
        if let Some(reason) = session.terminal_reason().or_else(|| capture_error.clone()) {
            if let Err(error) = session.seal_incomplete(&reason).await {
                merge_error(
                    &mut capture_error,
                    format!("failed to seal incomplete output artifact: {error}"),
                );
            }
        } else if let Err(error) = session.seal_complete().await {
            let reason = format!("failed to seal output artifact: {error}");
            merge_error(&mut capture_error, reason.clone());
            session.mark_forced_incomplete(&reason);
            let _ = session.seal_incomplete(&reason).await;
        }
    }

    if let Some(reason) = session.terminal_reason() {
        merge_error(&mut capture_error, reason);
    }

    let recovery_published = session.recovery_path_published().await?;
    let need_ref = recovery_published
        || prepared.truncated
        || prepared.encoding_loss
        || capture_error.is_some();
    let output_ref = if need_ref {
        Some(
            output_reference_for_range(session, prepared.snapshot.start, prepared.snapshot.end)
                .await?,
        )
    } else {
        None
    };

    Ok(PreparedExecResponse {
        response: ExecResponse {
            call_wall_time_seconds: started.elapsed().as_secs_f64(),
            exit_code: terminal.then(|| session.known_exit_code()).flatten(),
            session_id: (!terminal).then(|| session.id.to_string()),
            output: prepared.output,
            output_truncated: prepared.truncated,
            output_encoding_loss: prepared.encoding_loss,
            capture_error,
            output_ref: output_ref.as_ref().map(wire_output_ref),
            peer_messages: None,
        },
        commit_end: prepared
            .read_error
            .is_none()
            .then_some(prepared.snapshot.end),
        range_start: prepared.snapshot.start,
        range_end: prepared.snapshot.end,
        terminal,
        delete_output: terminal && !need_ref,
        output_ref,
    })
}

pub(crate) fn delivered_terminal_response(
    session: &Session,
    delivery: TerminalDelivery,
    started: Instant,
) -> ExecResponse {
    ExecResponse {
        call_wall_time_seconds: started.elapsed().as_secs_f64(),
        exit_code: session.known_exit_code(),
        session_id: None,
        output: String::new(),
        output_truncated: false,
        output_encoding_loss: false,
        capture_error: delivery.capture_error,
        output_ref: delivery.output_ref.as_ref().map(wire_output_ref),
        peer_messages: None,
    }
}

pub(crate) async fn finish_prepared_response(
    session: &Arc<Session>,
    mut prepared: PreparedExecResponse,
    started: Instant,
) -> anyhow::Result<ExecResponse> {
    if prepared.delete_output
        && let Err(error) = session.delete_output().await
    {
        let cleanup_error = format!("failed to remove delivered output artifact: {error}");
        merge_error(&mut prepared.response.capture_error, cleanup_error);
        // The response has already committed, so preserve the artifact as a
        // recovery target instead of leaving an unreachable complete spool.
        match output_reference_for_range(session, prepared.range_start, prepared.range_end).await {
            Ok(output_ref) => {
                session.publish_output_ref()?;
                prepared.response.output_ref = Some(wire_output_ref(&output_ref));
                prepared.output_ref = Some(output_ref);
            }
            Err(error) => merge_error(
                &mut prepared.response.capture_error,
                format!("failed to publish output recovery reference: {error}"),
            ),
        }
    }

    prepared.response.call_wall_time_seconds = started.elapsed().as_secs_f64();
    if prepared.terminal {
        // Include post-commit cleanup errors before releasing the interaction
        // lock to callers already pinned to this terminal session.
        session.record_terminal_delivery(prepared.terminal_delivery());
    }
    Ok(prepared.response)
}

async fn output_reference_for_range(
    session: &Session,
    range_start: u64,
    range_end: u64,
) -> io::Result<OutputReference> {
    let info = session.output_ref_info().await?;
    Ok(OutputReference {
        info,
        range_start,
        range_end,
    })
}

fn wire_output_ref(reference: &OutputReference) -> OutputRef {
    let info = &reference.info;
    OutputRef {
        path: info.path.display().to_string(),
        range_start: reference.range_start,
        range_end: reference.range_end,
        stored_bytes: info.stored_bytes,
        capture_status: info.capture_status.into(),
        expires_at_unix_seconds: info.expires_at_unix_seconds,
        incomplete_reason: info.incomplete_reason.clone(),
    }
}

fn merge_error(target: &mut Option<String>, message: String) {
    if target.is_none() {
        *target = Some(message);
    }
}
