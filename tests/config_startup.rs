use serde_json::{Value, json};
use std::process::{Command, Stdio};
use tempfile::TempDir;

fn config() -> Value {
    json!({"version": 1, "workspace": ".", "shell": "/bin/bash", "output_store_dir": "outputs",
        "child_env": {"inherit": [], "rules": []}})
}
fn command() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"));
    cmd.env_clear().stdin(Stdio::null());
    cmd
}
fn rejected(raw: &str, env: &[(&str, &str)], expected_error: &str) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.json");
    std::fs::write(&path, raw).unwrap();
    let result = command()
        .arg("--config")
        .arg(path)
        .envs(env.iter().copied())
        .output()
        .unwrap();
    assert!(!result.status.success(), "unexpected startup: {raw}");
    assert!(
        result.stdout.is_empty(),
        "startup refusal must not emit MCP readiness"
    );
    let diagnostics = String::from_utf8_lossy(&result.stderr);
    assert!(!diagnostics.is_empty());
    assert!(
        diagnostics.contains(expected_error),
        "expected diagnostic {expected_error:?}, got {diagnostics:?}"
    );
    assert!(
        !diagnostics.contains("SECRET_SENTINEL"),
        "leaked value: {diagnostics}"
    );
    assert!(
        !dir.path().join("outputs").exists(),
        "invalid config must fail before opening output store"
    );
}

#[test]
fn required_config_and_removed_cli_are_rejected() {
    for args in [
        vec![],
        vec!["--workspace", "."],
        vec!["--shell", "/bin/bash"],
        vec!["--scoped-gradle-auth-root", "."],
        vec!["--output-cap-bytes", "1"],
        vec!["--config"],
        vec!["--config", "one.json", "--config", "two.json"],
        vec!["--config", "one.json", "unexpected"],
        vec!["--help", "unexpected"],
        vec!["--version", "unexpected"],
        vec!["--config", ""],
        vec!["--config", "/nonexistent-config.json"],
    ] {
        let result = command().args(args).output().unwrap();
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
    }
    for flag in ["--help", "--version"] {
        assert!(command().arg(flag).output().unwrap().status.success());
    }
}

#[test]
fn rejects_schema_errors_and_invalid_paths_and_limits() {
    rejected("{", &[], "config JSON invalid");
    rejected("{}", &[], "config schema invalid");
    for field in [
        "version",
        "workspace",
        "shell",
        "output_store_dir",
        "child_env",
    ] {
        let mut value = config();
        value.as_object_mut().unwrap().remove(field);
        rejected(&value.to_string(), &[], "config schema invalid");
    }
    for (field, bad, expected_error) in [
        ("version", json!(2), "config version must be 1"),
        ("workspace", json!(""), "workspace must not be empty"),
        (
            "workspace",
            json!("missing"),
            "workspace canonicalize failed",
        ),
        ("shell", json!(""), "shell must not be empty"),
        (
            "shell",
            json!("."),
            "shell must be a regular executable file",
        ),
        ("shell", json!("config.json"), "shell is not executable"),
        (
            "output_store_dir",
            json!(""),
            "output_store_dir must not be empty",
        ),
        (
            "instructions_file",
            json!(""),
            "instructions_file must not be empty",
        ),
        (
            "instructions_file",
            json!("missing"),
            "instructions_file read failed",
        ),
        ("instructions_file", Value::Null, "config schema invalid"),
        (
            "unexpected",
            json!("SECRET_SENTINEL"),
            "config schema invalid",
        ),
        ("max_sessions", json!(0), "max_sessions must be in"),
        ("max_sessions", json!(2049), "max_sessions must be in"),
        (
            "session_idle_timeout",
            json!(0),
            "session_idle_timeout must be in",
        ),
        (
            "session_idle_timeout",
            json!(31536001),
            "session_idle_timeout must be in",
        ),
        ("reaper_interval", json!(0), "reaper_interval must be in"),
        ("output_cap_bytes", json!(0), "output_cap_bytes must be in"),
        (
            "output_store_retention",
            json!(0),
            "output_store_retention must be in",
        ),
        (
            "output_store_min_retention",
            json!(0),
            "config schema invalid",
        ),
        (
            "output_store_max_bytes",
            json!(0),
            "output_store_max_bytes must be in",
        ),
        ("output_store_max_files", json!(0), "config schema invalid"),
        (
            "output_store_headroom_bytes",
            json!(4294967297_u64),
            "config schema invalid",
        ),
    ] {
        let mut value = config();
        value[field] = bad;
        rejected(&value.to_string(), &[], expected_error);
    }
    // Repeated struct fields and nested map keys must both be rejected.
    rejected(
        &config()
            .to_string()
            .replacen("\"version\":1", "\"version\":1,\"version\":1", 1),
        &[],
        "config JSON invalid or duplicate key",
    );
    rejected(
        &config()
            .to_string()
            .replace("\"rules\":[]", "\"rules\":[],\"rules\":[]"),
        &[],
        "config JSON invalid or duplicate key",
    );
    rejected(&config().to_string().replace("\"rules\":[]", r#""rules":[{"tool":"exec_command","workdir_under":".","set_from_env":{"TARGET":"SOURCE","TARGET":"SOURCE"}}]"#), &[("SOURCE", "SECRET_SENTINEL")], "config JSON invalid or duplicate key");
}

#[test]
fn removed_session_settings_are_rejected() {
    for field in [
        "max_explicit_sessions",
        "max_exec_continuations",
        "explicit_session_idle_timeout",
        "exec_continuation_idle_timeout",
    ] {
        // Old values must never be silently ignored, even alongside new limits.
        for old in [json!(3), json!(null), json!("SECRET_SENTINEL")] {
            let mut value = config();
            value[field] = old;
            for unified in [false, true] {
                if unified {
                    value["max_sessions"] = json!(6);
                    value["session_idle_timeout"] = json!(3600);
                }
                rejected(&value.to_string(), &[], "docs/configuration.md");
            }
        }
    }
    let mut value = config();
    value["child_env"]["rules"] = json!([{
        "tool":"start_session", "workdir_under":".", "set_from_env":{"TARGET":"SOURCE"}
    }]);
    for env in [vec![], vec![("SOURCE", "SECRET_SENTINEL")]] {
        rejected(&value.to_string(), &env, "docs/configuration.md");
    }
}

#[test]
fn agent_pool_requires_an_explicit_principal_and_database_path() {
    for (agent_pool, expected_error) in [
        (Value::Null, "config schema invalid"),
        (json!({}), "config schema invalid"),
        (
            json!({"database_path":"agent_pool.sqlite3"}),
            "config schema invalid",
        ),
        (json!({"principal":"owner"}), "config schema invalid"),
        (
            json!({"principal":"", "database_path":"agent_pool.sqlite3"}),
            "agent_pool.principal must be",
        ),
        (
            json!({"principal":" owner", "database_path":"agent_pool.sqlite3"}),
            "agent_pool.principal must be",
        ),
        (
            json!({"principal":"owner\n", "database_path":"agent_pool.sqlite3"}),
            "agent_pool.principal must be",
        ),
        (
            json!({"principal":"owner", "database_path":""}),
            "agent_pool.database_path must not be empty",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "membership_ttl_seconds":179}),
            "agent_pool.membership_ttl_seconds must be between",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "membership_ttl_seconds":604801}),
            "agent_pool.membership_ttl_seconds must be between",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "membership_ttl_seconds":null}),
            "config schema invalid",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":null}),
            "config schema invalid",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_allocation":null}),
            "config schema invalid",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_allocation":"unknown"}),
            "config schema invalid",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_allocation":true}),
            "config schema invalid",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":[]}),
            "agent_pool.agent_name_dictionary must not be empty",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":["Caesar", "Caesar"]}),
            "agent_pool.agent_name_dictionary entries must be unique",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":["global"]}),
            "agent_pool.agent_name_dictionary entries must be unique usable",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":["admin"]}),
            "agent_pool.agent_name_dictionary entries must be unique usable",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":[" Caesar"]}),
            "agent_pool.agent_name_dictionary entries must be unique usable",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":["Caesar\n"]}),
            "agent_pool.agent_name_dictionary entries must be unique usable",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "agent_name_dictionary":["x".repeat(129)]}),
            "agent_pool.agent_name_dictionary entries must be unique usable",
        ),
        (
            json!({"principal":"owner", "database_path":"agent_pool.sqlite3", "unknown":"SECRET_SENTINEL"}),
            "config schema invalid",
        ),
    ] {
        let mut value = config();
        value["agent_pool"] = agent_pool;
        rejected(&value.to_string(), &[], expected_error);
    }
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("config.json");
    let mut value = config();
    value["agent_pool"] =
        json!({"principal":"dedicated-owner", "database_path":"agent_pool.sqlite3"});
    std::fs::write(&path, value.to_string()).unwrap();
    let settings = chatgpt_exec_mcp::Config::load(&path).unwrap();
    let agent_pool = settings.agent_pool.unwrap();
    assert_eq!(agent_pool.principal, "dedicated-owner");
    assert_eq!(
        agent_pool.membership_ttl,
        std::time::Duration::from_secs(86_400)
    );
    assert_eq!(
        agent_pool.database_path,
        dir.path().join("agent_pool.sqlite3")
    );
    assert_eq!(agent_pool.agent_name_dictionary.first().unwrap(), "Caesar");
    assert_eq!(
        agent_pool.agent_name_allocation,
        chatgpt_exec_mcp::config::AgentNameAllocation::FirstAvailable
    );
    assert!(
        !agent_pool.database_path.exists(),
        "validation must not create the database"
    );

    let mut minimum = config();
    minimum["agent_pool"] = json!({
        "principal":"dedicated-owner",
        "database_path":"agent_pool.sqlite3",
        "membership_ttl_seconds":180
    });
    std::fs::write(&path, minimum.to_string()).unwrap();
    let settings = chatgpt_exec_mcp::Config::load(&path).unwrap();
    assert_eq!(
        settings.agent_pool.unwrap().membership_ttl,
        std::time::Duration::from_secs(180)
    );

    let mut custom_names = config();
    custom_names["agent_pool"] = json!({
        "principal":"dedicated-owner",
        "database_path":"agent_pool.sqlite3",
        "agent_name_dictionary":["Ada", "Grace", "Linus"]
    });
    std::fs::write(&path, custom_names.to_string()).unwrap();
    let settings = chatgpt_exec_mcp::Config::load(&path).unwrap();
    assert_eq!(
        settings.agent_pool.unwrap().agent_name_dictionary,
        ["Ada", "Grace", "Linus"]
    );

    for (value, expected) in [
        (
            "first_available",
            chatgpt_exec_mcp::config::AgentNameAllocation::FirstAvailable,
        ),
        (
            "round_robin",
            chatgpt_exec_mcp::config::AgentNameAllocation::RoundRobin,
        ),
    ] {
        custom_names["agent_pool"]["agent_name_allocation"] = json!(value);
        std::fs::write(&path, custom_names.to_string()).unwrap();
        let settings = chatgpt_exec_mcp::Config::load(&path).unwrap();
        assert_eq!(settings.agent_pool.unwrap().agent_name_allocation, expected);
    }
}

#[test]
fn rejects_legacy_environment_and_invalid_policy() {
    for name in [
        "CHATGPT_EXEC_WORKSPACE",
        "CHATGPT_EXEC_SHELL",
        "CHATGPT_EXEC_OUTPUT_STORE_DIR",
        "CHATGPT_EXEC_SCOPED_GRADLE_AUTH_ROOT",
        "CHATGPT_EXEC_SCOPED_GRADLE_GITHUB_TOKEN",
    ] {
        rejected(
            &config().to_string(),
            &[(name, "SECRET_SENTINEL")],
            "startup environment",
        );
        rejected(&config().to_string(), &[(name, "")], "startup environment");
    }
    let rule =
        json!({"tool":"exec_command", "workdir_under":".", "set_from_env":{"TARGET":"SOURCE"}});
    let base = json!({"inherit":[], "rules":[rule.clone()]});
    let mut policies = vec![
        (
            json!({"inherit":["SOURCE"], "rules":[]}),
            "child_env reference SOURCE",
            false,
        ),
        (
            json!({"inherit":["1BAD"], "rules":[]}),
            "child_env contains an invalid variable name",
            true,
        ),
        (
            json!({"inherit":["SOURCE", "SOURCE"], "rules":[]}),
            "child_env.inherit contains duplicate names",
            true,
        ),
        (
            json!({"inherit":["CHATGPT_EXEC_SESSION"], "rules":[]}),
            "child_env conflicts with the reserved",
            true,
        ),
        (
            json!({"inherit":["SOURCE"], "rules":[rule.clone()]}),
            "child_env inherit/source/destination sets must not overlap",
            true,
        ),
        (
            json!({"inherit":["TARGET"], "rules":[rule.clone()]}),
            "child_env inherit/source/destination sets must not overlap",
            true,
        ),
        (
            json!({"inherit":[], "rules":[rule.clone(),rule.clone()]}),
            "child_env has duplicate destination definitions",
            true,
        ),
    ];
    for (key, bad, expected_error) in [
        ("tool", json!("unknown"), "config schema invalid"),
        (
            "workdir_under",
            json!("missing"),
            "child_env.workdir_under canonicalize failed",
        ),
        (
            "workdir_under",
            json!(""),
            "child_env.workdir_under must not be empty",
        ),
        (
            "set_from_env",
            json!({}),
            "child_env rule set_from_env must not be empty",
        ),
        (
            "set_from_env",
            json!({"SOURCE":"SOURCE"}),
            "child_env inherit/source/destination sets must not overlap",
        ),
        (
            "set_from_env",
            json!({"CHATGPT_EXEC_SESSION":"SOURCE"}),
            "child_env conflicts with the reserved",
        ),
        (
            "set_from_env",
            json!({"TARGET":"CHATGPT_EXEC_SESSION"}),
            "child_env conflicts with the reserved",
        ),
        ("required", json!(false), "config schema invalid"),
    ] {
        let mut policy = base.clone();
        policy["rules"][0][key] = bad;
        policies.push((policy, expected_error, true));
    }
    for (policy, expected_error, reject_with_credentials) in policies {
        let mut value = config();
        value["child_env"] = policy;
        rejected(&value.to_string(), &[], expected_error);
        // Also reject structural errors when referenced credentials are available.
        if reject_with_credentials {
            rejected(
                &value.to_string(),
                &[("SOURCE", "SECRET_SENTINEL"), ("TARGET", "SECRET_SENTINEL")],
                expected_error,
            );
        }
    }
    let mut value = config();
    value["child_env"] = base;
    rejected(&value.to_string(), &[], "child_env reference SOURCE");
    rejected(
        &value.to_string(),
        &[("SOURCE", "")],
        "child_env reference SOURCE",
    );
}

#[test]
fn output_store_failure_prevents_mcp_startup() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("outputs"), "blocked").unwrap();
    let path = dir.path().join("config.json");
    std::fs::write(&path, config().to_string()).unwrap();
    let result = command().arg("--config").arg(&path).output().unwrap();
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert_eq!(
        std::fs::read_to_string(dir.path().join("outputs")).unwrap(),
        "blocked"
    );
    assert!(!dir.path().join(".chatgpt-exec-outputs").exists());
}
