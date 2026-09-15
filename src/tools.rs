use std::fmt;
use std::marker::PhantomData;

use rmcp::schemars::JsonSchema;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::de::Error as _;
use serde::de::Visitor;

pub const DEFAULT_EXEC_YIELD_MS: u64 = 10_000;
pub const DEFAULT_SESSION_YIELD_MS: u64 = 250;
pub const DEFAULT_WRITE_YIELD_MS: u64 = 250;
pub const DEFAULT_POLL_YIELD_MS: u64 = 5_000;
pub const DEFAULT_WAIT_SECONDS: u64 = 35;
pub const DEFAULT_MAX_OUTPUT_TOKENS: usize = 8_000;
pub const MIN_YIELD_MS: u64 = 10;
pub const MAX_YIELD_MS: u64 = 120_000;
pub const MIN_WAIT_SECONDS: u64 = 15;
pub const MAX_WAIT_SECONDS: u64 = 100;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct ExecCommandArgs {
    /// Shell command passed to the server-configured shell with -c.
    #[schemars(length(min = 1))]
    pub cmd: String,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    /// Absolute path, or relative to the workspace base directory. The workspace is not a sandbox boundary.
    #[schemars(with = "String", skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default)]
    /// Allocate a PTY.
    pub tty: bool,
    #[serde(default, deserialize_with = "deserialize_optional_yield_ms")]
    /// Initial wait before a still-running command becomes a continuation.
    #[schemars(with = "u64", range(min = MIN_YIELD_MS, max = MAX_YIELD_MS), extend("default" = DEFAULT_EXEC_YIELD_MS))]
    pub yield_time_ms: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    /// Optional display budget: approximately four bytes per token.
    #[schemars(
        with = "usize",
        range(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<usize>,
}

impl ExecCommandArgs {
    pub fn yield_ms(&self) -> u64 {
        self.yield_time_ms.unwrap_or(DEFAULT_EXEC_YIELD_MS)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct StartSessionArgs {
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    /// Long-lived shell command. Omit to use the server-configured shell.
    #[schemars(
        with = "String",
        length(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub cmd: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    /// Absolute path, or relative to the workspace base directory. The workspace is not a sandbox boundary.
    #[schemars(with = "String", skip_serializing_if = "Option::is_none")]
    pub workdir: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    /// Allocate a PTY (default true).
    #[schemars(with = "bool", extend("default" = true))]
    pub tty: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    /// Optional display budget: approximately four bytes per token.
    #[schemars(
        with = "usize",
        range(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct WriteStdinArgs {
    /// Memorable process handle returned by exec_command or start_session.
    #[schemars(regex(pattern = "^[a-z]+-[a-z]+$"))]
    pub session_id: String,
    #[serde(default)]
    /// Raw input. Empty input returns pending output immediately.
    pub chars: String,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    /// Optional display budget: approximately four bytes per token.
    #[schemars(
        with = "usize",
        range(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(crate = "rmcp::schemars")]
pub struct WaitForExitArgs {
    /// Memorable process handle returned by exec_command or start_session.
    #[schemars(regex(pattern = "^[a-z]+-[a-z]+$"))]
    pub session_id: String,
    #[serde(
        default = "default_wait_seconds",
        deserialize_with = "deserialize_wait_seconds"
    )]
    /// Maximum time to wait for process exit. Ordinary output does not end the wait.
    #[schemars(with = "u64", range(min = MIN_WAIT_SECONDS, max = MAX_WAIT_SECONDS))]
    pub wait_seconds: u64,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    /// Optional display budget: approximately four bytes per token.
    #[schemars(
        with = "usize",
        range(min = 1),
        skip_serializing_if = "Option::is_none"
    )]
    pub max_output_tokens: Option<usize>,
}

fn deserialize_optional_yield_ms<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = deserialize_optional_non_null(deserializer)?;
    if let Some(value) = value
        && !(MIN_YIELD_MS..=MAX_YIELD_MS).contains(&value)
    {
        return Err(D::Error::custom(format!(
            "yield_time_ms must be between {MIN_YIELD_MS} and {MAX_YIELD_MS}"
        )));
    }
    Ok(value)
}

fn deserialize_optional_positive_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = deserialize_optional_non_null(deserializer)?;
    if value == Some(0) {
        return Err(D::Error::custom("max_output_tokens must be at least 1"));
    }
    Ok(value)
}

fn deserialize_optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct NonNullOption<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for NonNullOption<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Option<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a non-null value or an omitted field")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Err(E::custom("null is not allowed; omit the field instead"))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Err(E::custom("null is not allowed; omit the field instead"))
        }

        fn visit_some<D2>(self, deserializer: D2) -> Result<Self::Value, D2::Error>
        where
            D2: Deserializer<'de>,
        {
            T::deserialize(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(NonNullOption(PhantomData))
}

fn default_wait_seconds() -> u64 {
    DEFAULT_WAIT_SECONDS
}

fn deserialize_wait_seconds<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u64::deserialize(deserializer)?;
    if (MIN_WAIT_SECONDS..=MAX_WAIT_SECONDS).contains(&value) {
        Ok(value)
    } else {
        Err(D::Error::custom(format!(
            "wait_seconds must be between {MIN_WAIT_SECONDS} and {MAX_WAIT_SECONDS}"
        )))
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields)]
pub struct ExecResponse {
    /// Wall-clock time servicing this call, not total session runtime.
    #[schemars(range(min = 0))]
    pub call_wall_time_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "i32", default)]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub session_id: Option<String>,
    pub output: String,
    pub output_truncated: bool,
    pub output_encoding_loss: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub capture_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "OutputRef", default)]
    pub output_ref: Option<OutputRef>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
#[schemars(crate = "rmcp::schemars", deny_unknown_fields, inline)]
pub struct OutputRef {
    pub path: String,
    pub range_start: u64,
    pub range_end: u64,
    pub stored_bytes: u64,
    #[schemars(extend("enum" = ["open", "complete", "incomplete"]))]
    pub capture_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "u64", default)]
    pub expires_at_unix_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", default)]
    pub incomplete_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_yield_values_are_strict() {
        for yield_time_ms in [MIN_YIELD_MS, DEFAULT_EXEC_YIELD_MS, MAX_YIELD_MS] {
            let args: ExecCommandArgs = serde_json::from_value(serde_json::json!({
                "cmd": "true",
                "yield_time_ms": yield_time_ms
            }))
            .unwrap();
            assert_eq!(args.yield_ms(), yield_time_ms);
        }
        for yield_time_ms in [0, MIN_YIELD_MS - 1, MAX_YIELD_MS + 1] {
            assert!(
                serde_json::from_value::<ExecCommandArgs>(serde_json::json!({
                    "cmd": "true",
                    "yield_time_ms": yield_time_ms
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn wait_for_exit_args_enforce_defaults_bounds_and_strict_fields() {
        let valid: WaitForExitArgs = serde_json::from_value(serde_json::json!({
            "session_id": "maple-comet",
            "wait_seconds": 15,
            "max_output_tokens": 1
        }))
        .unwrap();
        assert_eq!(valid.wait_seconds, MIN_WAIT_SECONDS);

        let defaulted: WaitForExitArgs = serde_json::from_value(serde_json::json!({
            "session_id": "maple-comet"
        }))
        .unwrap();
        assert_eq!(defaulted.wait_seconds, DEFAULT_WAIT_SECONDS);

        for wait_seconds in [14, 101] {
            let error = serde_json::from_value::<WaitForExitArgs>(serde_json::json!({
                "session_id": "maple-comet",
                "wait_seconds": wait_seconds
            }))
            .unwrap_err();
            assert!(error.to_string().contains("wait_seconds must be between"));
        }

        assert!(
            serde_json::from_value::<WaitForExitArgs>(serde_json::json!({
                "session_id": "maple-comet",
                "wait_seconds": 15,
                "unexpected": true
            }))
            .is_err()
        );

        assert!(
            serde_json::from_value::<ExecCommandArgs>(serde_json::json!({
                "cmd": "true",
                "max_output_tokens": 0
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<WriteStdinArgs>(serde_json::json!({
                "session_id": "maple-comet",
                "max_output_tokens": 0
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<StartSessionArgs>(serde_json::json!({
                "cmd": null
            }))
            .is_err()
        );
        assert!(
            serde_json::from_value::<ExecCommandArgs>(serde_json::json!({
                "cmd": "true",
                "workdir": null
            }))
            .is_err()
        );
    }
}
