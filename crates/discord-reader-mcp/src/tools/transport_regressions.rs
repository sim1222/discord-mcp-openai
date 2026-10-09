//! Exercises the MCP router, daemon client, and response transport in one session.

use super::*;
use futures::{channel::mpsc, StreamExt};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[tokio::test]
async fn account_tools_handle_large_inventory_optional_arguments_and_errors_in_one_session() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("daemon.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let observed = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let daemon_observed = Arc::clone(&observed);
    let daemon = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let observed = Arc::clone(&daemon_observed);
            tokio::spawn(async move {
                let (read, mut write) = stream.into_split();
                let line = BufReader::new(read)
                    .lines()
                    .next_line()
                    .await
                    .unwrap()
                    .unwrap();
                let request: Value = serde_json::from_str(&line).unwrap();
                let ordinal = {
                    let mut requests = observed.lock().unwrap();
                    requests.push(request.clone());
                    requests.len()
                };
                let response = match request["method"].as_str().unwrap() {
                    "get_read_state" if ordinal == 1 => {
                        let rows: Vec<Value> = (1..=13_000).map(|id| json!({
                            "channel_id":id.to_string(),"last_read_message_id":null,
                            "discord_mention_count":null,"availability":"unobserved",
                            "reason":"synthetic_read_position_not_observed","observed_at":null,
                            "version":null,"source":"synthetic_regression"
                        })).collect();
                        json!({"id":request["id"],"result":{"scope":{"guild_id":"42"},"complete":false,"read_state":rows}})
                    }
                    "get_account_coverage" if ordinal == 4 => json!({
                        "id":request["id"],"error":{"code":-32001,"message":"synthetic temporary failure",
                        "data":{"error":{"error_source":"transport","code":"temporary_failure","operation":"get_account_coverage","retryable":true}}}
                    }),
                    "get_account_coverage" => {
                        json!({"id":request["id"],"result":{"coverage":{"targets_total":13_000,"complete":false,"targets":[]}}})
                    }
                    "get_read_state"
                        if request["params"]["cursor"].is_string()
                            && request["params"]["refresh"] != false =>
                    {
                        json!({
                            "id":request["id"],"error":{"code":-32602,"message":"cannot refresh immutable continuation",
                            "data":{"error":{"error_source":"client","code":"INVALID_PARAMS","operation":"get_read_state","retryable":false}}}
                        })
                    }
                    "get_read_state" => {
                        json!({"id":request["id"],"result":{"read_state":[],"complete":false}})
                    }
                    "get_me" => json!({"id":request["id"],"result":{"me":{"id":"7"}}}),
                    _ => panic!("unexpected synthetic RPC method"),
                };
                let mut payload = response.to_string();
                payload.push('\n');
                write.write_all(payload.as_bytes()).await.unwrap();
            });
        }
    });

    let (sender, incoming) = mpsc::unbounded::<rmcp::model::ClientJsonRpcMessage>();
    let (outgoing, mut receiver) = mpsc::unbounded::<rmcp::model::ServerJsonRpcMessage>();
    let tools = DiscordReaderTools::new(Arc::new(RpcClient::new(socket)));
    let server = tokio::spawn(async move {
        rmcp::serve_server(tools, (outgoing, incoming))
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap()
    });
    sender.unbounded_send(serde_json::from_value(json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"account-regression","version":"1"}}})).unwrap()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), receiver.next())
        .await
        .unwrap()
        .unwrap();
    sender
        .unbounded_send(
            serde_json::from_value(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
                .unwrap(),
        )
        .unwrap();

    let calls = [
        (
            "get_read_state",
            Some(json!({"guild_id":"42","refresh":false})),
            false,
        ),
        ("get_read_state", None, false),
        ("get_account_coverage", None, false),
        ("get_account_coverage", Some(json!({})), true),
        ("get_read_state", Some(json!({})), false),
        ("get_account_coverage", Some(json!({})), false),
        (
            "get_read_state",
            Some(json!({"guild_id":"42","cursor":"synthetic-snapshot:100"})),
            false,
        ),
        ("get_me", None, false),
    ];
    for (index, (name, arguments, expected_error)) in calls.into_iter().enumerate() {
        let id = index + 2;
        let mut params = json!({"name":name});
        if let Some(arguments) = arguments {
            params["arguments"] = arguments;
        }
        sender
            .unbounded_send(
                serde_json::from_value(
                    json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":params}),
                )
                .unwrap(),
            )
            .unwrap();
        let response = tokio::time::timeout(std::time::Duration::from_secs(10), receiver.next())
            .await
            .expect("same MCP session must remain responsive")
            .expect("MCP transport closed");
        let response = serde_json::to_value(response).unwrap();
        assert_eq!(response["id"], id);
        assert!(
            response["error"].is_null(),
            "protocol error for {name}: {response}"
        );
        assert_eq!(
            response["result"]["isError"].as_bool().unwrap_or(false),
            expected_error,
            "unexpected tool result for {name}"
        );
        let text = response["result"]["content"][0]["text"].as_str().unwrap();
        let result: Value = serde_json::from_str(text).unwrap();
        if index == 0 {
            assert_eq!(result["read_state"].as_array().unwrap().len(), 13_000);
            assert!(
                text.len() > 1024 * 1024,
                "exercise a response larger than one MiB"
            );
        } else if expected_error {
            assert_eq!(result["error"]["code"], "temporary_failure");
            assert_eq!(result["error"]["retryable"], true);
        } else if name == "get_account_coverage" {
            assert_eq!(result["coverage"]["targets_total"], 13_000);
        } else if name == "get_me" {
            assert_eq!(result["me"]["id"], "7");
        }
    }
    let requests = observed.lock().unwrap();
    assert_eq!(requests.len(), 8);
    assert_eq!(requests[0]["params"]["guild_id"], "42");
    assert_eq!(requests[0]["params"]["refresh"], false);
    assert_eq!(requests[1]["params"]["refresh"], true);
    assert_eq!(requests[6]["params"]["cursor"], "synthetic-snapshot:100");
    assert_eq!(requests[6]["params"]["refresh"], false);
    drop(requests);
    drop(sender);
    server.abort();
    daemon.abort();
}
