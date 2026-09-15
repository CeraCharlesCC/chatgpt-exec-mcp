use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::session::SessionOrigin;

/// Validated startup settings. Environment values are deliberately redacted.
#[derive(Clone, Debug)]
pub struct Config {
    pub workspace: PathBuf,
    pub shell: PathBuf,
    pub max_explicit_sessions: usize,
    pub max_exec_continuations: usize,
    pub explicit_session_idle_timeout: Duration,
    pub exec_continuation_idle_timeout: Duration,
    pub reaper_interval: Duration,
    pub output_cap_bytes: usize,
    pub output_store_dir: PathBuf,
    pub output_store_retention: Duration,
    pub output_store_max_bytes: u64,
    pub(crate) additional_instructions: Option<String>,
    pub(crate) child_env: ChildEnv,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    version: u64,
    workspace: PathBuf,
    shell: PathBuf,
    output_store_dir: PathBuf,
    child_env: EnvConfig,
    #[serde(default, deserialize_with = "present_path")]
    instructions_file: Option<PathBuf>,
    #[serde(default = "three")]
    max_explicit_sessions: usize,
    #[serde(default = "three")]
    max_exec_continuations: usize,
    #[serde(default = "hour")]
    explicit_session_idle_timeout: u64,
    #[serde(default = "quarter_hour")]
    exec_continuation_idle_timeout: u64,
    #[serde(default = "thirty")]
    reaper_interval: u64,
    #[serde(default = "megabyte")]
    output_cap_bytes: usize,
    #[serde(default = "week")]
    output_store_retention: u64,
    #[serde(default = "four_gigabytes")]
    output_store_max_bytes: u64,
}
fn present_path<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<PathBuf>, D::Error> {
    PathBuf::deserialize(d).map(Some)
}
fn three() -> usize {
    3
}
fn hour() -> u64 {
    3600
}
fn quarter_hour() -> u64 {
    900
}
fn thirty() -> u64 {
    30
}
fn megabyte() -> usize {
    1_048_576
}
fn week() -> u64 {
    604800
}
fn four_gigabytes() -> u64 {
    4_294_967_296
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvConfig {
    inherit: Vec<String>,
    rules: Vec<EnvRule>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvRule {
    tool: Tool,
    workdir_under: PathBuf,
    set_from_env: BTreeMap<String, String>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Tool {
    ExecCommand,
    StartSession,
}

#[derive(Clone)]
pub(crate) struct ChildEnv {
    inherited: HashMap<String, String>,
    rules: Vec<ResolvedRule>,
}
#[derive(Clone)]
struct ResolvedRule {
    tool: Tool,
    root: PathBuf,
    values: HashMap<String, String>,
}
impl std::fmt::Debug for ChildEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ChildEnv(<redacted>)")
    }
}
impl ChildEnv {
    pub(crate) fn for_spawn(&self, origin: SessionOrigin, cwd: &Path) -> HashMap<String, String> {
        let mut env = self.inherited.clone();
        for rule in &self.rules {
            let matches = matches!(
                (rule.tool, origin),
                (Tool::ExecCommand, SessionOrigin::ExecContinuation)
                    | (Tool::StartSession, SessionOrigin::ExplicitSession)
            );
            if matches && cwd.starts_with(&rule.root) {
                env.extend(rule.values.clone());
            }
        }
        env
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        Self::load_with_env(path, &std::env::vars_os().collect())
    }

    fn load_with_env(
        path: &Path,
        env: &HashMap<std::ffi::OsString, std::ffi::OsString>,
    ) -> anyhow::Result<Self> {
        // Reserve the former settings namespace, including old credential sources.
        // SESSION is runtime metadata, not a configuration input.
        for name in env.keys() {
            let name = name.to_string_lossy();
            if name.starts_with("CHATGPT_EXEC_") && name != "CHATGPT_EXEC_SESSION" {
                bail!("startup environment: obsolete CHATGPT_EXEC_* input is present");
            }
        }
        if path.as_os_str().is_empty() {
            bail!("config path must not be empty");
        }
        let path = std::path::absolute(path).context("config path resolution failed")?;
        let base = path.parent().context("config path has no parent")?;
        let bytes = std::fs::read(&path).context("config read failed")?;
        // Validate duplicates recursively before serde's maps can discard them.
        let mut deserializer = serde_json::Deserializer::from_slice(&bytes);
        StrictJson::deserialize(&mut deserializer).map_err(|e| {
            anyhow::anyhow!(
                "config JSON invalid or duplicate key at line {}, column {}",
                e.line(),
                e.column()
            )
        })?;
        deserializer
            .end()
            .context("config JSON has trailing input")?;
        // Serde errors can include an offending scalar. Never echo config values.
        let raw: FileConfig = serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!(
            "config schema invalid: unknown/missing field or invalid field type (see configuration.md)"))?;
        if raw.version != 1 {
            bail!("config version must be 1");
        }
        let workspace = canonical_directory(base, &raw.workspace, "workspace")?;
        let shell = canonical_path(base, &raw.shell, "shell")?;
        shell
            .to_str()
            .context("shell canonical path must be UTF-8")?;
        if !shell.is_file() {
            bail!("shell must be a regular executable file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let name = std::ffi::CString::new(shell.as_os_str().as_bytes())?;
            if unsafe { libc::access(name.as_ptr(), libc::X_OK) } != 0 {
                bail!("shell is not executable");
            }
        }
        let additional_instructions = raw
            .instructions_file
            .as_ref()
            .map(|p| {
                let path = resolve(base, p, "instructions_file")?;
                std::fs::read_to_string(path).context("instructions_file read failed")
            })
            .transpose()?;
        let output_store_dir = resolve(base, &raw.output_store_dir, "output_store_dir")?;
        let child_env = resolve_env(raw.child_env, base, env)?;
        let config = Self {
            workspace,
            shell,
            output_store_dir,
            child_env,
            additional_instructions,
            max_explicit_sessions: raw.max_explicit_sessions,
            max_exec_continuations: raw.max_exec_continuations,
            explicit_session_idle_timeout: Duration::from_secs(raw.explicit_session_idle_timeout),
            exec_continuation_idle_timeout: Duration::from_secs(raw.exec_continuation_idle_timeout),
            reaper_interval: Duration::from_secs(raw.reaper_interval),
            output_cap_bytes: raw.output_cap_bytes,
            output_store_retention: Duration::from_secs(raw.output_store_retention),
            output_store_max_bytes: raw.output_store_max_bytes,
        };
        config.validate_limits()?;
        Ok(config)
    }

    pub(crate) fn validate_limits(&self) -> anyhow::Result<()> {
        for (name, n, max) in [
            (
                "max_explicit_sessions",
                self.max_explicit_sessions as u64,
                1024,
            ),
            (
                "max_exec_continuations",
                self.max_exec_continuations as u64,
                1024,
            ),
            (
                "explicit_session_idle_timeout",
                self.explicit_session_idle_timeout.as_secs(),
                31_536_000,
            ),
            (
                "exec_continuation_idle_timeout",
                self.exec_continuation_idle_timeout.as_secs(),
                31_536_000,
            ),
            ("reaper_interval", self.reaper_interval.as_secs(), 86400),
            (
                "output_cap_bytes",
                self.output_cap_bytes as u64,
                1_073_741_824,
            ),
            (
                "output_store_retention",
                self.output_store_retention.as_secs(),
                31_536_000,
            ),
            (
                "output_store_max_bytes",
                self.output_store_max_bytes,
                1_125_899_906_842_624,
            ),
        ] {
            if !(1..=max).contains(&n) {
                bail!("{name} must be in 1..={max}");
            }
        }
        Ok(())
    }
}

fn resolve(base: &Path, path: &Path, field: &str) -> anyhow::Result<PathBuf> {
    if path.as_os_str().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(base.join(path))
}
fn canonical_path(base: &Path, path: &Path, field: &str) -> anyhow::Result<PathBuf> {
    std::fs::canonicalize(resolve(base, path, field)?)
        .with_context(|| format!("{field} canonicalize failed"))
}
fn canonical_directory(base: &Path, path: &Path, field: &str) -> anyhow::Result<PathBuf> {
    let path = canonical_path(base, path, field)?;
    if !path.is_dir() {
        bail!("{field} must be a directory");
    }
    std::fs::read_dir(&path).with_context(|| format!("{field} is not readable"))?;
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(path.as_os_str().as_bytes())?;
        if unsafe { libc::access(name.as_ptr(), libc::X_OK) } != 0 {
            bail!("{field} is not searchable");
        }
    }
    Ok(path)
}
fn validate_name(name: &str) -> anyhow::Result<()> {
    let mut chars = name.bytes();
    if !chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == b'_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == b'_')
    {
        bail!("child_env contains an invalid variable name");
    }
    if name.starts_with("CHATGPT_EXEC_") {
        bail!("child_env conflicts with the reserved CHATGPT_EXEC_* namespace");
    }
    Ok(())
}
fn resolve_env(
    raw: EnvConfig,
    base: &Path,
    env: &HashMap<std::ffi::OsString, std::ffi::OsString>,
) -> anyhow::Result<ChildEnv> {
    let mut inherited = HashSet::new();
    let mut sources = HashSet::new();
    let mut targets = HashSet::new();
    for name in &raw.inherit {
        validate_name(name)?;
        if !inherited.insert(name.clone()) {
            bail!("child_env.inherit contains duplicate names");
        }
    }
    for rule in &raw.rules {
        if rule.set_from_env.is_empty() {
            bail!("child_env rule set_from_env must not be empty");
        }
        for (target, source) in &rule.set_from_env {
            validate_name(target)?;
            validate_name(source)?;
            if !targets.insert(target.clone()) {
                bail!("child_env has duplicate destination definitions");
            }
            sources.insert(source.clone());
        }
    }
    if !sources.is_disjoint(&targets)
        || !inherited.is_disjoint(&sources)
        || !inherited.is_disjoint(&targets)
    {
        bail!("child_env inherit/source/destination sets must not overlap");
    }
    let read = |name: &str| -> anyhow::Result<String> {
        let value = env
            .get(std::ffi::OsStr::new(name))
            .and_then(|s| s.to_str())
            .filter(|s| !s.is_empty() && !s.contains('\0'))
            .with_context(|| {
                format!("child_env reference {name}: missing, empty, non-UTF-8 or NUL value")
            })?;
        Ok(value.to_owned())
    };
    let inherited = raw
        .inherit
        .iter()
        .map(|name| Ok((name.clone(), read(name)?)))
        .collect::<anyhow::Result<_>>()?;
    let mut rules = Vec::new();
    for rule in raw.rules {
        rules.push(ResolvedRule {
            tool: rule.tool,
            root: canonical_directory(base, &rule.workdir_under, "child_env.workdir_under")?,
            values: rule
                .set_from_env
                .iter()
                .map(|(target, source)| Ok((target.clone(), read(source)?)))
                .collect::<anyhow::Result<_>>()?,
        });
    }
    Ok(ChildEnv { inherited, rules })
}

/// Reject duplicate keys at every depth, including arbitrary set_from_env maps.
struct StrictJson;
impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = StrictJson;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("JSON with unique keys")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<StrictJson, M::Error> {
                let mut keys = HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate key"));
                    }
                    map.next_value::<StrictJson>()?;
                }
                Ok(StrictJson)
            }
            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut seq: S,
            ) -> Result<StrictJson, S::Error> {
                while seq.next_element::<StrictJson>()?.is_some() {}
                Ok(StrictJson)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<StrictJson, E> {
                Ok(StrictJson)
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<StrictJson, E> {
                Ok(StrictJson)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<StrictJson, E> {
                Ok(StrictJson)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<StrictJson, E> {
                Ok(StrictJson)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<StrictJson, E> {
                Ok(StrictJson)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<StrictJson, E> {
                Ok(StrictJson)
            }
        }
        d.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn environment_is_snapshotted_redacted_and_start_session_rule_is_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            json!({
                "version":1, "workspace":".", "shell":"/bin/bash", "output_store_dir":"outputs",
                "child_env":{"inherit":["ALLOWED"], "rules":[{
                    "tool":"start_session", "workdir_under":".", "set_from_env":{"TARGET":"SOURCE"}
                }]}
            })
            .to_string(),
        )
        .unwrap();
        let mut env = HashMap::from([
            ("ALLOWED".into(), "allowed-original".into()),
            ("SOURCE".into(), "SECRET_SENTINEL".into()),
            ("UNLISTED".into(), "must-not-leak".into()),
            ("TARGET".into(), "must-not-leak".into()),
        ]);
        let config = Config::load_with_env(&path, &env).unwrap();
        env.insert("SOURCE".into(), "changed".into());
        env.insert("ALLOWED".into(), "changed".into());
        let cwd = dir.path().canonicalize().unwrap();
        assert_eq!(
            config
                .child_env
                .for_spawn(SessionOrigin::ExecContinuation, &cwd),
            HashMap::from([("ALLOWED".into(), "allowed-original".into())])
        );
        assert_eq!(
            config
                .child_env
                .for_spawn(SessionOrigin::ExplicitSession, &cwd),
            HashMap::from([
                ("ALLOWED".into(), "allowed-original".into()),
                ("TARGET".into(), "SECRET_SENTINEL".into())
            ])
        );
        let debug = format!("{config:?}");
        for value in ["SECRET_SENTINEL", "allowed-original", "must-not-leak"] {
            assert!(!debug.contains(value));
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_canonical_shell_is_rejected_before_spawn() {
        use std::os::unix::ffi::OsStringExt;
        let dir = tempfile::tempdir().unwrap();
        let shell = dir.path().join(std::ffi::OsString::from_vec(vec![255]));
        std::fs::copy("/bin/bash", &shell).unwrap();
        std::os::unix::fs::symlink(&shell, dir.path().join("shell-link")).unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            json!({
                "version":1, "workspace":".", "shell":"shell-link", "output_store_dir":"outputs",
                "child_env":{"inherit":[], "rules":[]}
            })
            .to_string(),
        )
        .unwrap();
        assert!(
            Config::load_with_env(&path, &HashMap::new())
                .unwrap_err()
                .to_string()
                .contains("UTF-8")
        );
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_reference_is_rejected_without_echoing_value() {
        use std::os::unix::ffi::OsStringExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        std::fs::write(
            &path,
            json!({
                "version":1, "workspace":".", "shell":"/bin/bash", "output_store_dir":"outputs",
                "child_env":{"inherit":["ALLOWED"], "rules":[]}
            })
            .to_string(),
        )
        .unwrap();
        let env = HashMap::from([("ALLOWED".into(), std::ffi::OsString::from_vec(vec![255]))]);
        assert!(
            Config::load_with_env(&path, &env)
                .unwrap_err()
                .to_string()
                .contains("ALLOWED")
        );
    }
}
