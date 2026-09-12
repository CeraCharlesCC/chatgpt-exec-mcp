use std::fmt;
use std::marker::PhantomData;

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

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecCommandArgs {
    pub cmd: String,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub workdir: Option<String>,
    #[serde(default)]
    pub tty: bool,
    #[serde(default, deserialize_with = "deserialize_optional_yield_ms")]
    pub yield_time_ms: Option<u64>,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    pub max_output_tokens: Option<usize>,
}

impl ExecCommandArgs {
    pub fn yield_ms(&self) -> u64 {
        self.yield_time_ms.unwrap_or(DEFAULT_EXEC_YIELD_MS)
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartSessionArgs {
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub cmd: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub workdir: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_non_null")]
    pub tty: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    pub max_output_tokens: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteStdinArgs {
    pub session_id: String,
    #[serde(default)]
    pub chars: String,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
    pub max_output_tokens: Option<usize>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitForExitArgs {
    pub session_id: String,
    #[serde(
        default = "default_wait_seconds",
        deserialize_with = "deserialize_wait_seconds"
    )]
    pub wait_seconds: u64,
    #[serde(default, deserialize_with = "deserialize_optional_positive_usize")]
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

#[derive(Clone, Debug, Serialize)]
pub struct ExecResponse {
    pub call_wall_time_seconds: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub output: String,
    pub output_truncated: bool,
    pub output_encoding_loss: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_ref: Option<OutputRef>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OutputRef {
    pub path: String,
    pub range_start: u64,
    pub range_end: u64,
    pub stored_bytes: u64,
    pub capture_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
