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
fn rejected(raw: &str, env: &[(&str, &str)]) {
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
        vec!["--config", "one.json", "--listen-unix"],
        vec!["--config", "one.json", "--listen-unix", "relative.sock"],
        vec![
            "--config",
            "one.json",
            "--listen-unix",
            "/tmp/a",
            "--listen-unix",
            "/tmp/b",
        ],
        vec!["--listen-unix", "/tmp/a"],
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
    rejected("{", &[]);
    rejected("{}", &[]);
    for field in [
        "version",
        "workspace",
        "shell",
        "output_store_dir",
        "child_env",
    ] {
        let mut value = config();
        value.as_object_mut().unwrap().remove(field);
        rejected(&value.to_string(), &[]);
    }
    for (field, bad) in [
        ("version", json!(2)),
        ("workspace", json!("")),
        ("workspace", json!("missing")),
        ("shell", json!("")),
        ("shell", json!(".")),
        ("shell", json!("config.json")),
        ("output_store_dir", json!("")),
        ("instructions_file", json!("")),
        ("instructions_file", json!("missing")),
        ("instructions_file", Value::Null),
        ("unexpected", json!("SECRET_SENTINEL")),
        ("max_explicit_sessions", json!(0)),
        ("max_exec_continuations", json!(1025)),
        ("reaper_interval", json!(0)),
        ("exec_continuation_idle_timeout", json!(-1)),
        ("explicit_session_idle_timeout", json!(31536001)),
        ("output_cap_bytes", json!(0)),
        ("output_store_retention", json!(0)),
        ("output_store_min_retention", json!(0)),
        ("output_store_max_bytes", json!(0)),
        ("output_store_max_files", json!(0)),
        ("output_store_headroom_bytes", json!(4294967297_u64)),
    ] {
        let mut value = config();
        value[field] = bad;
        rejected(&value.to_string(), &[]);
    }
    // Repeated struct fields and nested map keys must both be rejected.
    rejected(
        &config()
            .to_string()
            .replacen("\"version\":1", "\"version\":1,\"version\":1", 1),
        &[],
    );
    rejected(
        &config()
            .to_string()
            .replace("\"rules\":[]", "\"rules\":[],\"rules\":[]"),
        &[],
    );
    rejected(&config().to_string().replace("\"rules\":[]", r#""rules":[{"tool":"exec_command","workdir_under":".","set_from_env":{"TARGET":"SOURCE","TARGET":"SOURCE"}}]"#), &[("SOURCE", "SECRET_SENTINEL")]);
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
        rejected(&config().to_string(), &[(name, "SECRET_SENTINEL")]);
        rejected(&config().to_string(), &[(name, "")]);
    }
    let rule =
        json!({"tool":"exec_command", "workdir_under":".", "set_from_env":{"TARGET":"SOURCE"}});
    let base = json!({"inherit":[], "rules":[rule.clone()]});
    let mut policies = vec![
        json!({"inherit":["SOURCE"], "rules":[]}),
        json!({"inherit":["1BAD"], "rules":[]}),
        json!({"inherit":["SOURCE", "SOURCE"], "rules":[]}),
        json!({"inherit":["CHATGPT_EXEC_SESSION"], "rules":[]}),
        json!({"inherit":["SOURCE"], "rules":[rule.clone()]}),
        json!({"inherit":["TARGET"], "rules":[rule.clone()]}),
        json!({"inherit":[], "rules":[rule.clone(),rule.clone()]}),
    ];
    for (key, bad) in [
        ("tool", json!("unknown")),
        ("workdir_under", json!("missing")),
        ("workdir_under", json!("")),
        ("set_from_env", json!({})),
        ("set_from_env", json!({"SOURCE":"SOURCE"})),
        ("set_from_env", json!({"CHATGPT_EXEC_SESSION":"SOURCE"})),
        ("set_from_env", json!({"TARGET":"CHATGPT_EXEC_SESSION"})),
        ("required", json!(false)),
    ] {
        let mut policy = base.clone();
        policy["rules"][0][key] = bad;
        policies.push(policy);
    }
    for policy in policies {
        let mut value = config();
        value["child_env"] = policy;
        rejected(&value.to_string(), &[]);
        // Also reject structural errors when referenced credentials are available.
        if value["child_env"] != json!({"inherit":["SOURCE"], "rules":[]}) {
            rejected(
                &value.to_string(),
                &[("SOURCE", "SECRET_SENTINEL"), ("TARGET", "SECRET_SENTINEL")],
            );
        }
    }
    let mut value = config();
    value["child_env"] = base;
    rejected(&value.to_string(), &[]);
    rejected(&value.to_string(), &[("SOURCE", "")]);
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
