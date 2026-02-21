//! LSP (Language Server Protocol) server for topology.
//!
//! Exposes every algorithm in `algo/` as a custom `workspace/executeCommand`
//! so that editors (VS Code, Zed, Neovim, etc.) can invoke them over stdio.
//!
//! Start with: `topology --lsp`
//!
//! ## Commands
//!
//! All commands accept a single JSON object argument and return JSON results:
//!
//! | Command                  | Description                                    |
//! |--------------------------|------------------------------------------------|
//! | `topology.fingerprint`   | Compute SimHash fingerprints                   |
//! | `topology.sample`        | Sample rows from a JSON array                  |
//! | `topology.analyze`       | Analyze table structure and field statistics    |
//! | `topology.classify`      | Auto-classify into categories                  |
//! | `topology.tags`          | Extract top TF-IDF tags                        |
//! | `topology.dedup`         | Find duplicates via SimHash + LSH + URL        |
//! | `topology.similarity`    | String similarity (Levenshtein/Jaro/Cosine)    |
//! | `topology.normalize_url` | Normalize a URL for deduplication               |
//! | `topology.generate`      | Auto-generate taxonomy via HAC clustering       |
//! | `topology.topics`        | Discover topics via NMF                         |
//! | `topology.organize`      | Generate output paths from classified items     |

use serde_json::Value;
use tower_lsp::jsonrpc::{Error as RpcError, Result as RpcResult};
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

use crate::ops;

// ── Constants ───────────────────────────────────────────────────────────────

const COMMAND_FINGERPRINT: &str = "topology.fingerprint";
const COMMAND_SAMPLE: &str = "topology.sample";
const COMMAND_ANALYZE: &str = "topology.analyze";
const COMMAND_CLASSIFY: &str = "topology.classify";
const COMMAND_TAGS: &str = "topology.tags";
const COMMAND_DEDUP: &str = "topology.dedup";
const COMMAND_SIMILARITY: &str = "topology.similarity";
const COMMAND_NORMALIZE_URL: &str = "topology.normalize_url";
const COMMAND_GENERATE: &str = "topology.generate";
const COMMAND_TOPICS: &str = "topology.topics";
const COMMAND_ORGANIZE: &str = "topology.organize";
const COMMAND_CACHE_INFO: &str = "topology.cache_info";
const COMMAND_CACHE_CLEAR: &str = "topology.cache_clear";

const ALL_COMMANDS: &[&str] = &[
    COMMAND_FINGERPRINT,
    COMMAND_SAMPLE,
    COMMAND_ANALYZE,
    COMMAND_CLASSIFY,
    COMMAND_TAGS,
    COMMAND_DEDUP,
    COMMAND_SIMILARITY,
    COMMAND_NORMALIZE_URL,
    COMMAND_GENERATE,
    COMMAND_TOPICS,
    COMMAND_ORGANIZE,
    COMMAND_CACHE_INFO,
    COMMAND_CACHE_CLEAR,
];

// ── Server struct ───────────────────────────────────────────────────────────

pub struct TopologyLsp {
    client: Client,
}

impl TopologyLsp {
    pub fn new(client: Client) -> Self {
        Self { client }
    }
}

// ── LanguageServer impl ─────────────────────────────────────────────────────

#[tower_lsp::async_trait]
impl LanguageServer for TopologyLsp {
    async fn initialize(&self, _: InitializeParams) -> RpcResult<InitializeResult> {
        Ok(InitializeResult {
            server_info: Some(ServerInfo {
                name: "topology".into(),
                version: Some(env!("CARGO_PKG_VERSION").into()),
            }),
            capabilities: ServerCapabilities {
                execute_command_provider: Some(ExecuteCommandOptions {
                    commands: ALL_COMMANDS.iter().map(|s| s.to_string()).collect(),
                    work_done_progress_options: WorkDoneProgressOptions {
                        work_done_progress: Some(false),
                    },
                }),
                text_document_sync: Some(TextDocumentSyncCapability::Kind(
                    TextDocumentSyncKind::NONE,
                )),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.client
            .log_message(
                MessageType::INFO,
                format!(
                    "topology LSP server v{} ready — {} commands registered",
                    env!("CARGO_PKG_VERSION"),
                    ALL_COMMANDS.len()
                ),
            )
            .await;
    }

    async fn shutdown(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn execute_command(&self, params: ExecuteCommandParams) -> RpcResult<Option<Value>> {
        let cmd = params.command.as_str();

        let arg = params
            .arguments
            .into_iter()
            .next()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        let result = match cmd {
            COMMAND_FINGERPRINT => exec_fingerprint(&arg),
            COMMAND_SAMPLE => exec_sample(&arg),
            COMMAND_ANALYZE => exec_analyze(&arg),
            COMMAND_CLASSIFY => exec_classify(&arg),
            COMMAND_TAGS => exec_tags(&arg),
            COMMAND_DEDUP => exec_dedup(&arg),
            COMMAND_SIMILARITY => exec_similarity(&arg),
            COMMAND_NORMALIZE_URL => exec_normalize_url(&arg),
            COMMAND_GENERATE => exec_generate(&arg),
            COMMAND_TOPICS => exec_topics(&arg),
            COMMAND_ORGANIZE => exec_organize(&arg),
            COMMAND_CACHE_INFO => exec_cache_info(&arg),
            COMMAND_CACHE_CLEAR => exec_cache_clear(&arg),
            _ => Err(format!("Unknown command: {cmd}")),
        };

        match result {
            Ok(value) => Ok(Some(value)),
            Err(msg) => {
                self.client.log_message(MessageType::ERROR, &msg).await;
                Err(RpcError::invalid_params(msg))
            }
        }
    }
}

// ── Entry point ─────────────────────────────────────────────────────────────

/// Start the LSP server on stdio. Called from `cli.rs` when `--lsp` is passed.
pub async fn serve_stdio() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();

    let (service, socket) = LspService::new(TopologyLsp::new);
    Server::new(stdin, stdout, socket).serve(service).await;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
//  Thin command dispatchers — extract params, delegate to ops::op_*
// ═══════════════════════════════════════════════════════════════════════════

fn get_records(arg: &Value) -> Result<Vec<Value>, String> {
    arg.get("records")
        .and_then(|v| v.as_array())
        .cloned()
        .ok_or_else(|| "Missing required field 'records' (JSON array)".into())
}

fn get_str<'a>(arg: &'a Value, key: &str, default: &'a str) -> &'a str {
    arg.get(key).and_then(|v| v.as_str()).unwrap_or(default)
}

fn get_u64(arg: &Value, key: &str, default: u64) -> u64 {
    arg.get(key).and_then(|v| v.as_u64()).unwrap_or(default)
}

fn get_usize(arg: &Value, key: &str, default: usize) -> usize {
    arg.get(key)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(default)
}

fn get_f64(arg: &Value, key: &str, default: f64) -> f64 {
    arg.get(key).and_then(|v| v.as_f64()).unwrap_or(default)
}

fn get_bool(arg: &Value, key: &str, default: bool) -> bool {
    arg.get(key).and_then(|v| v.as_bool()).unwrap_or(default)
}

fn exec_fingerprint(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let weighted = get_bool(arg, "weighted", false);
    let cache = arg.get("cache").and_then(|v| v.as_str());
    Ok(ops::op_fingerprint_cached(&rows, field, weighted, cache))
}

fn exec_sample(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let size = get_usize(arg, "size", 100);
    let strategy = get_str(arg, "strategy", "random");
    let field = arg.get("field").and_then(|v| v.as_str());
    let seed = get_u64(arg, "seed", 42);
    ops::op_sample(&rows, size, strategy, field, seed)
}

fn exec_analyze(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = arg.get("field").and_then(|v| v.as_str());
    let cache = arg.get("cache").and_then(|v| v.as_str());
    Ok(ops::op_analyze_cached(&rows, field, cache))
}

fn exec_classify(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let clusters = get_usize(arg, "clusters", 15);
    let sample_size = get_usize(arg, "sample", 500);
    let threshold = get_f64(arg, "threshold", 0.5);
    let seed = get_u64(arg, "seed", 42);
    let taxonomy = arg.get("taxonomy").filter(|v| !v.is_null());
    let cache = arg.get("cache").and_then(|v| v.as_str());
    ops::op_classify_cached(&rows, field, taxonomy, clusters, sample_size, threshold, seed, cache)
}

fn exec_tags(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let count = get_usize(arg, "count", 5);
    let cache = arg.get("cache").and_then(|v| v.as_str());
    Ok(ops::op_tags_cached(&rows, field, count, cache))
}

fn exec_dedup(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let url_field = get_str(arg, "url_field", "url");
    let strategy = get_str(arg, "strategy", "combined");
    let threshold = arg
        .get("threshold")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(3);
    let cache = arg.get("cache").and_then(|v| v.as_str());
    Ok(ops::op_dedup_cached(&rows, field, url_field, strategy, threshold, cache))
}

fn exec_similarity(arg: &Value) -> Result<Value, String> {
    let a = arg
        .get("a")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'a'")?;
    let b = arg
        .get("b")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'b'")?;
    let metric = get_str(arg, "metric", "levenshtein");
    let all = get_bool(arg, "all", false);
    ops::op_similarity(a, b, metric, all)
}

fn exec_normalize_url(arg: &Value) -> Result<Value, String> {
    let url = arg
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'url'")?;
    ops::op_normalize_url(url)
}

fn exec_generate(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let depth = get_usize(arg, "depth", 10);
    let linkage = get_str(arg, "linkage", "ward");
    let top_terms = get_usize(arg, "top_terms", 5);
    let cache = arg.get("cache").and_then(|v| v.as_str());
    ops::op_generate_cached(&rows, field, depth, linkage, top_terms, cache)
}

fn exec_topics(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let k = get_usize(arg, "topics", 5);
    let top_n = get_usize(arg, "terms", 10);
    let max_iter = get_usize(arg, "iterations", 200);
    let vocab = get_usize(arg, "vocab", 5000);
    ops::op_topics(&rows, field, k, top_n, max_iter, vocab)
}

fn exec_organize(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let format = get_str(arg, "format", "folders");
    let output_dir = get_str(arg, "output_dir", "./organized");
    let category_field = get_str(arg, "category_field", "_category");
    let name_field = get_str(arg, "name_field", "id");
    Ok(ops::op_organize(&rows, format, output_dir, category_field, name_field))
}

fn exec_cache_info(arg: &Value) -> Result<Value, String> {
    let path = arg
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'path'")?;
    ops::op_cache_info(path)
}

fn exec_cache_clear(arg: &Value) -> Result<Value, String> {
    let path = arg
        .get("path")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'path'")?;
    let kind = arg.get("kind").and_then(|v| v.as_str());
    ops::op_cache_clear(path, kind)
}
