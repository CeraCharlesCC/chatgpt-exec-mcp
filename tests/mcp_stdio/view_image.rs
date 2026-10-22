use std::process::Stdio;
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

use super::{request, write_config};

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=";

#[tokio::test]
async fn image_tool_requires_opt_in_and_returns_mcp_image_content() {
    for enabled in [None, Some(false), Some(true)] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, false);
        let mut config: Value =
            serde_json::from_slice(&std::fs::read(&config_path).unwrap()).unwrap();
        config["workspace"] = json!("project");
        if let Some(enabled) = enabled {
            config["view_image"] = json!(enabled);
        }
        std::fs::write(&config_path, config.to_string()).unwrap();
        std::fs::create_dir(dir.path().join("project")).unwrap();
        // Deliberately misleading extension: MIME type must come from bytes.
        let image_path = dir.path().join("project/image.jpg");
        std::fs::write(&image_path, STANDARD.decode(PNG).unwrap()).unwrap();

        let mut child = Command::new(env!("CARGO_BIN_EXE_chatgpt-exec-mcp"))
            .arg("--config")
            .arg(&config_path)
            .current_dir(dir.path())
            .env_clear()
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let initialized = request(
            &mut stdin,
            &mut stdout,
            json!({
                "jsonrpc":"2.0", "id":1, "method":"initialize",
                "params":{
                    "protocolVersion":"2025-06-18", "capabilities":{},
                    "clientInfo":{"name":"image-test", "version":"1"}
                }
            }),
        )
        .await;
        assert!(initialized.get("error").is_none(), "{initialized}");
        stdin
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        let listed = request(
            &mut stdin,
            &mut stdout,
            json!({"jsonrpc":"2.0", "id":2, "method":"tools/list", "params":{}}),
        )
        .await;
        let image_tool = listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tool| tool["name"] == "view_image");
        assert_eq!(image_tool.is_some(), enabled.unwrap_or(false));
        if let Some(tool) = image_tool {
            assert_eq!(tool["annotations"]["readOnlyHint"], true);
            assert_eq!(tool["inputSchema"]["required"], json!(["path"]));
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
            assert_eq!(tool["inputSchema"]["properties"]["path"]["minLength"], 1);
            assert!(tool.get("outputSchema").is_none());
        }
        for path in ["image.jpg".to_owned(), image_path.display().to_string()] {
            let response = request(
                &mut stdin,
                &mut stdout,
                json!({
                    "jsonrpc":"2.0", "id":3, "method":"tools/call",
                    "params":{"name":"view_image", "arguments":{"path":path}}
                }),
            )
            .await;
            if enabled == Some(true) {
                assert_ne!(response["result"]["isError"], true, "{response}");
                let content = response["result"]["content"].as_array().unwrap();
                assert_eq!(content.len(), 1);
                assert_eq!(content[0]["type"], "image");
                assert_eq!(content[0]["mimeType"], "image/png");
                assert_eq!(content[0]["data"], PNG);
                assert!(content[0].get("text").is_none());
            } else {
                assert_eq!(response["error"]["code"], -32602, "{response}");
            }
        }
        if enabled == Some(true) {
            for args in [
                json!({}),
                json!({"path":null}),
                json!({"path":""}),
                json!({"path":"missing.png"}),
                json!({"path":"."}),
                json!({"path":"../config.json"}),
                json!({"path":"image.jpg", "unknown":true}),
            ] {
                let response = request(
                    &mut stdin,
                    &mut stdout,
                    json!({
                        "jsonrpc":"2.0", "id":4, "method":"tools/call",
                        "params":{"name":"view_image", "arguments":args}
                    }),
                )
                .await;
                assert_eq!(response["result"]["isError"], true, "{response}");
                assert_eq!(response["result"]["content"][0]["type"], "text");
            }
        }
        drop(stdin);
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
        let mut diagnostics = String::new();
        child
            .stderr
            .take()
            .unwrap()
            .read_to_string(&mut diagnostics)
            .await
            .unwrap();
        assert!(diagnostics.is_empty(), "{diagnostics}");
    }
}
