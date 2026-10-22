use base64::{Engine as _, engine::general_purpose::STANDARD};

use super::*;

#[tokio::test]
async fn images_over_http_preserve_pool_delivery_without_retaining_image_bytes_in_activity() {
    let mut server = Server::new().await;
    server.stop();
    let path = server.workspace.path().join("config.json");
    let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["view_image"] = json!(true);
    std::fs::write(path, config.to_string()).unwrap();
    server.start().await;

    let gif = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7";
    std::fs::write(
        server.workspace.path().join("image.dat"),
        STANDARD.decode(gif).unwrap(),
    )
    .unwrap();
    let joined = server.send("image-viewer", "global", "join").await;
    assert_eq!(structured(&joined)["assigned_agent"], "Augustus");
    let sent = server
        .send("image-sender", "Augustus", "inspect this image")
        .await;
    let image = server
        .tool(
            "view_image",
            json!({"path":"image.dat"}),
            Some("image-viewer"),
        )
        .await;
    assert_ne!(image["result"]["isError"], true, "{image}");
    assert_eq!(image["result"]["content"][0]["type"], "image");
    assert_eq!(image["result"]["content"][0]["mimeType"], "image/gif");
    assert_eq!(image["result"]["content"][0]["data"], gif);
    assert_eq!(
        structured(&image)["messages"][0]["message_id"],
        structured(&sent)["message_id"]
    );
    assert!(
        image["result"]["content"][1]["text"]
            .as_str()
            .unwrap()
            .contains("inspect this image")
    );

    let receipt = server
        .tool(
            "view_image",
            json!({"path":"image.dat"}),
            Some("image-sender"),
        )
        .await;
    assert_eq!(
        structured(&receipt)["delivery_receipts"][0]["message_id"],
        structured(&sent)["message_id"]
    );
    let sent = server
        .send("image-sender", "Augustus", "delivered with an image error")
        .await;
    let error = server
        .tool(
            "view_image",
            json!({"path":"missing.png"}),
            Some("image-viewer"),
        )
        .await;
    assert_eq!(error["result"]["isError"], true);
    assert_eq!(error["result"]["content"][0]["type"], "text");
    assert_eq!(
        structured(&error)["messages"][0]["message_id"],
        structured(&sent)["message_id"]
    );

    let snapshot: Value = server
        .webui_client
        .get(format!(
            "http://{}/_admin/api/snapshot",
            server.webui_address
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!snapshot.to_string().contains(gif));
    let events = snapshot["events"].as_array().unwrap();
    let finished = events
        .iter()
        .find(|event| event["kind"] == "tool.finish" && event["detail"]["tool"] == "view_image")
        .unwrap();
    assert_eq!(finished["detail"]["result"]["mime_type"], "image/gif");
    assert!(finished["detail"]["result"]["bytes"].as_u64().unwrap() > 0);
    assert!(
        events.iter().any(|event| {
            event["kind"] == "tool.error" && event["detail"]["tool"] == "view_image"
        })
    );
}
