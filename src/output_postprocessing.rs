//! Optional display processing after the ordinary output projection.
//!
//! Processors can suggest replacement text, never change the raw snapshot,
//! truncation/encoding flags, capture state, or the delivery cursor. Keep
//! processor selection and display budgets here, outside process execution.

use std::io;

use crate::output_projection::Projection;
use crate::output_summary::BuildSummary;

pub(crate) trait OutputProcessor {
    fn default_max_output_tokens(&self) -> usize;
    fn replacement(&self, baseline: &Projection, budget: usize) -> io::Result<Option<String>>;
}

/// Selected once per command and reused for subsequent output polls.
/// The empty/default stage leaves the ordinary head/tail projection untouched.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct OutputPostprocessing {
    build_summary: Option<BuildSummary>,
}

impl OutputPostprocessing {
    pub(crate) fn for_command(command: &str) -> Self {
        Self {
            build_summary: BuildSummary::for_command(command),
        }
    }

    fn processor(&self) -> Option<&dyn OutputProcessor> {
        self.build_summary
            .as_ref()
            .map(|summary| summary as &dyn OutputProcessor)
    }

    pub(crate) fn default_max_output_tokens(&self) -> usize {
        self.processor().map_or(
            crate::tools::DEFAULT_MAX_OUTPUT_TOKENS,
            OutputProcessor::default_max_output_tokens,
        )
    }

    pub(crate) fn apply(&self, baseline: Projection, budget: usize) -> Projection {
        apply_processor(baseline, self.processor(), budget)
    }
}

pub(crate) fn apply_processor(
    mut baseline: Projection,
    processor: Option<&dyn OutputProcessor>,
    budget: usize,
) -> Projection {
    if let Some(processor) = processor
        && let Ok(Some(output)) = processor.replacement(&baseline, budget)
        && output.len() <= budget
    {
        baseline.output = output;
    }
    // Optional read/recognition failures preserve the successful base read.
    baseline
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_projection::Snapshot;

    struct Replacement(io::Result<Option<String>>);

    impl OutputProcessor for Replacement {
        fn default_max_output_tokens(&self) -> usize {
            123
        }

        fn replacement(&self, _: &Projection, _: usize) -> io::Result<Option<String>> {
            match &self.0 {
                Ok(output) => Ok(output.clone()),
                Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
            }
        }
    }

    fn baseline() -> Projection {
        Projection {
            snapshot: Snapshot {
                start: 7,
                end: 123,
                stored_bytes: 456,
                path: "unused-raw-log".into(),
            },
            output: "head...tail".into(),
            truncated: true,
            encoding_loss: false,
        }
    }

    #[test]
    fn disabled_and_unrecognized_commands_leave_projection_untouched() {
        for stage in [
            OutputPostprocessing::default(),
            OutputPostprocessing::for_command("cat build.log"),
            OutputPostprocessing::for_command("cargo check | cat"),
        ] {
            // No file exists at this path. An empty stage needs no raw-log IO.
            let original = baseline();
            let result = stage.apply(original.clone(), 1024);
            assert_eq!(result.output, original.output);
            assert_eq!(stage.default_max_output_tokens(), 8_000);
        }
        for command in ["rustc main.rs", "cargo test", "./gradlew build"] {
            assert_eq!(
                OutputPostprocessing::for_command(command).default_max_output_tokens(),
                2_000
            );
        }
    }

    #[test]
    fn processing_only_replaces_display_text_and_falls_back_on_failure_or_over_budget() {
        for (replacement, expected) in [
            (Ok(Some("excerpt".into())), "excerpt"),
            (Ok(None), "head...tail"),
            (Err(io::Error::other("optional read failed")), "head...tail"),
            (Ok(Some("too large".repeat(20))), "head...tail"),
        ] {
            let original = baseline();
            let result = apply_processor(original.clone(), Some(&Replacement(replacement)), 16);
            assert_eq!(result.output, expected);
            assert_eq!(result.snapshot.start, original.snapshot.start);
            assert_eq!(result.snapshot.end, original.snapshot.end);
            assert_eq!(result.snapshot.stored_bytes, original.snapshot.stored_bytes);
            assert_eq!(result.snapshot.path, original.snapshot.path);
            assert_eq!(result.truncated, original.truncated);
            assert_eq!(result.encoding_loss, original.encoding_loss);
        }
    }
}
