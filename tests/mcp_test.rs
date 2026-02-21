//! MCP JSON-RPC integration tests.
//!
//! These tests spawn `topology --mcp` as a child process and communicate
//! via stdin/stdout using newline-delimited JSON-RPC (rmcp's stdio transport).

#![cfg(all(feature = "cli", feature = "mcp"))]

use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn topology_binary() -> String {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("topology");
    path.to_string_lossy().into_owned()
}

fn send_jsonrpc(stdin: &mut impl Write, msg: &Value) {
    let body = serde_json::to_string(msg).unwrap();
    writeln!(stdin, "{body}").unwrap();
    stdin.flush().unwrap();
}

fn read_jsonrpc_line(reader: &mut BufReader<impl std::io::Read>) -> Option<Value> {
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => None, // EOF
        Ok(_) => {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return read_jsonrpc_line(reader); // skip blank lines
            }
            serde_json::from_str(trimmed).ok()
        }
        Err(_) => None,
    }
}

/// Spawn the MCP server, send requests, collect responses.
fn mcp_session(requests: Vec<Value>) -> Vec<Value> {
    let bin = topology_binary();
    let mut child = Command::new(&bin)
        .arg("--mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn {bin}: {e}"));

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);

    let mut responses = Vec::new();
    for req in &requests {
        send_jsonrpc(&mut stdin, req);
        // Only read a response for requests with an "id" (not notifications)
        if req.get("id").is_some() {
            if let Some(resp) = read_jsonrpc_line(&mut reader) {
                responses.push(resp);
            }
        }
    }

    drop(stdin);
    let _ = child.wait();
    responses
}

#[test]
fn mcp_initialize_returns_server_info() {
    let responses = mcp_session(vec![json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "0.1.0"}
        }
    })]);

    assert!(!responses.is_empty(), "No response received");
    let resp = &responses[0];
    assert_eq!(resp["jsonrpc"], "2.0");
    assert_eq!(resp["id"], 1);
    let result = &resp["result"];
    assert!(result.get("serverInfo").is_some());
    assert_eq!(result["serverInfo"]["name"], "topology");
}

#[test]
fn mcp_tools_list_has_all_tools() {
    let responses = mcp_session(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0.1.0"}
            }
        }),
        // Send initialized notification (no id)
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        }),
    ]);

    assert!(responses.len() >= 2, "Expected at least 2 responses, got {}", responses.len());
    let tools_resp = responses.iter().find(|r| r["id"] == 2).unwrap();
    let tools = tools_resp["result"]["tools"].as_array().unwrap();

    let tool_names: Vec<&str> = tools
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();

    let expected = [
        "fingerprint", "sample", "analyze", "classify", "tags", "dedup",
        "similarity", "normalize_url", "generate", "topics", "organize",
    ];

    for name in &expected {
        assert!(
            tool_names.contains(name),
            "Missing tool: {name}. Found: {tool_names:?}"
        );
    }
    assert_eq!(tool_names.len(), 11, "Expected 11 tools, got {}", tool_names.len());
}

#[test]
fn mcp_tools_call_similarity() {
    let responses = mcp_session(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0.1.0"}
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "similarity",
                "arguments": {
                    "a": "hello",
                    "b": "world",
                    "all": true
                }
            }
        }),
    ]);

    let call_resp = responses.iter().find(|r| r["id"] == 2).unwrap();
    let content = &call_resp["result"]["content"][0];
    assert_eq!(content["type"], "text");
    let text = content["text"].as_str().unwrap();
    let parsed: Value = serde_json::from_str(text).unwrap();
    assert!(parsed.get("levenshtein").is_some());
    assert!(parsed.get("jaro-winkler").is_some());
    assert!(parsed.get("cosine").is_some());
}

#[test]
fn mcp_tools_call_normalize_url() {
    let responses = mcp_session(vec![
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "0.1.0"}
            }
        }),
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }),
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "normalize_url",
                "arguments": {
                    "url": "https://www.Example.COM:443/path?utm_source=test&id=1"
                }
            }
        }),
    ]);

    let call_resp = responses.iter().find(|r| r["id"] == 2).unwrap();
    let content = &call_resp["result"]["content"][0];
    let text = content["text"].as_str().unwrap();
    let parsed: Value = serde_json::from_str(text).unwrap();
    let normalized = parsed["normalized"].as_str().unwrap();
    assert!(!normalized.contains("utm_source"));
    assert!(!normalized.contains("www."));
    assert!(normalized.contains("id=1"));
}
