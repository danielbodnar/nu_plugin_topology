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

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use tower_lsp::jsonrpc::{Error as RpcError, Result as RpcResult};
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer, LspService, Server};

use crate::algo::{
    clustering, discover, lsh, nmf, sampling, simhash, string_distance, taxonomy, tfidf, tokenizer,
    url_normalize,
};

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
                // Register all topology commands for workspace/executeCommand.
                execute_command_provider: Some(ExecuteCommandOptions {
                    commands: ALL_COMMANDS.iter().map(|s| s.to_string()).collect(),
                    work_done_progress_options: WorkDoneProgressOptions {
                        work_done_progress: Some(false),
                    },
                }),
                // We support text sync so that editors can send us open documents,
                // but we don't require it — commands accept inline data.
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

        // Each command expects a single JSON object argument with operation-
        // specific fields (records, field, etc.). Extract it once.
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
//  Command dispatchers
// ═══════════════════════════════════════════════════════════════════════════
//
// Each function extracts parameters from the JSON object, runs the
// corresponding algo pipeline, and returns a JSON value. These are pure
// synchronous functions (the algo crate is sync); they are called from
// the async execute_command handler on the tokio runtime.

// ── Helpers ─────────────────────────────────────────────────────────────────

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

fn get_text(row: &Value, field: &str) -> String {
    row.get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

// ── fingerprint ─────────────────────────────────────────────────────────────

fn exec_fingerprint(arg: &Value) -> Result<Value, String> {
    use rayon::prelude::*;

    let rows = get_records(arg)?;
    let field = get_str(arg, "field", "content");
    let weighted = get_bool(arg, "weighted", false);

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();
    let token_lists: Vec<Vec<String>> = texts.par_iter().map(|t| tokenizer::tokenize(t)).collect();

    let fingerprints: Vec<u64> = if weighted {
        let mut corpus = tfidf::Corpus::new();
        for tokens in &token_lists {
            corpus.add_document(tokens);
        }
        token_lists
            .par_iter()
            .map(|tokens| {
                let weights = corpus.token_weights(tokens);
                simhash::simhash(tokens, &weights)
            })
            .collect()
    } else {
        token_lists
            .par_iter()
            .map(|tokens| simhash::simhash_uniform(tokens))
            .collect()
    };

    let output: Vec<Value> = rows
        .into_iter()
        .zip(fingerprints)
        .map(|(mut row, fp)| {
            if let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "_fingerprint".into(),
                    Value::String(simhash::fingerprint_to_hex(fp)),
                );
            }
            row
        })
        .collect();

    Ok(Value::Array(output))
}

// ── sample ──────────────────────────────────────────────────────────────────

fn exec_sample(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let size = get_usize(arg, "size", 100);
    let strategy = get_str(arg, "strategy", "random");
    let field = arg.get("field").and_then(|v| v.as_str());
    let seed = get_u64(arg, "seed", 42);

    let total = rows.len();
    if total == 0 {
        return Ok(Value::Array(vec![]));
    }

    let strat = sampling::Strategy::from_str(strategy).ok_or_else(|| {
        format!("Unknown strategy '{strategy}'. Use: random, stratified, systematic, reservoir")
    })?;

    let indices = match strat {
        sampling::Strategy::Random => sampling::random_sample(total, size, seed),
        sampling::Strategy::Systematic => sampling::systematic_sample(total, size, seed),
        sampling::Strategy::Reservoir => sampling::reservoir_sample(total, size, seed),
        sampling::Strategy::Stratified => {
            let field_name = field.ok_or("Stratified sampling requires a 'field' parameter")?;
            let mut strata: HashMap<String, Vec<usize>> = HashMap::new();
            for (i, row) in rows.iter().enumerate() {
                let key = get_text(row, field_name);
                let key = if key.is_empty() {
                    "unknown".into()
                } else {
                    key
                };
                strata.entry(key).or_default().push(i);
            }
            sampling::stratified_sample(&strata, size, seed)
        }
    };

    let sampled: Vec<Value> = indices
        .iter()
        .filter(|&&i| i < total)
        .map(|&i| rows[i].clone())
        .collect();

    Ok(Value::Array(sampled))
}

// ── analyze ─────────────────────────────────────────────────────────────────

fn exec_analyze(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let field = arg.get("field").and_then(|v| v.as_str());

    let total = rows.len();
    if total == 0 {
        return Ok(serde_json::json!({"total_rows": 0, "columns": [], "fields": {}}));
    }

    let columns: Vec<String> = match field {
        Some(f) => vec![f.to_string()],
        None => match &rows[0] {
            Value::Object(map) => map.keys().cloned().collect(),
            _ => vec!["value".into()],
        },
    };

    let mut fields = serde_json::Map::new();
    for col in &columns {
        let mut null_count: usize = 0;
        let mut unique_vals: HashSet<String> = HashSet::new();
        let mut type_counts: HashMap<String, usize> = HashMap::new();
        let mut total_len: usize = 0;
        let mut min_len: usize = usize::MAX;
        let mut max_len: usize = 0;
        let mut values: Vec<String> = Vec::new();

        for row in &rows {
            match row.get(col.as_str()) {
                Some(Value::Null) | None => null_count += 1,
                Some(v) => {
                    let type_name = match v {
                        Value::String(_) => "string",
                        Value::Number(_) => "number",
                        Value::Bool(_) => "bool",
                        Value::Array(_) => "array",
                        Value::Object(_) => "object",
                        Value::Null => "null",
                    };
                    *type_counts.entry(type_name.into()).or_insert(0) += 1;
                    let s = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    total_len += s.len();
                    min_len = min_len.min(s.len());
                    max_len = max_len.max(s.len());
                    unique_vals.insert(s.clone());
                    values.push(s);
                }
            }
        }

        let non_null = total - null_count;
        if min_len == usize::MAX {
            min_len = 0;
        }

        let mut freq: HashMap<&str, usize> = HashMap::new();
        for v in &values {
            *freq.entry(v.as_str()).or_insert(0) += 1;
        }
        let mut freq_vec: Vec<(&str, usize)> = freq.into_iter().collect();
        freq_vec.sort_by(|a, b| b.1.cmp(&a.1));
        freq_vec.truncate(5);

        fields.insert(
            col.clone(),
            serde_json::json!({
                "non_null": non_null,
                "null_count": null_count,
                "cardinality": unique_vals.len(),
                "uniqueness": if non_null > 0 { unique_vals.len() as f64 / non_null as f64 } else { 0.0 },
                "avg_length": if non_null > 0 { total_len as f64 / non_null as f64 } else { 0.0 },
                "min_length": min_len,
                "max_length": max_len,
                "types": type_counts.iter().map(|(t, c)| serde_json::json!({"type": t, "count": c})).collect::<Vec<_>>(),
                "top_values": freq_vec.iter().map(|(v, c)| serde_json::json!({"value": v, "count": c})).collect::<Vec<_>>(),
            }),
        );
    }

    Ok(serde_json::json!({
        "total_rows": total,
        "columns": columns,
        "num_columns": columns.len(),
        "fields": Value::Object(fields),
    }))
}

// ── classify ────────────────────────────────────────────────────────────────

fn exec_classify(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    if rows.is_empty() {
        return Ok(Value::Array(vec![]));
    }

    let field = get_str(arg, "field", "content");
    let clusters = get_usize(arg, "clusters", 15);
    let sample_size = get_usize(arg, "sample", 500);
    let threshold = get_f64(arg, "threshold", 0.5);
    let seed = get_u64(arg, "seed", 42);

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();

    let tax = match arg.get("taxonomy") {
        Some(v) if !v.is_null() => {
            let json_str = serde_json::to_string(v)
                .map_err(|e| format!("Failed to serialize taxonomy: {e}"))?;
            taxonomy::parse_taxonomy(&json_str)?
        }
        _ => {
            let config = discover::DiscoverConfig {
                k: clusters,
                sample_size,
                label_terms: 3,
                keywords_per_cluster: 20,
                linkage: clustering::Linkage::Ward,
                seed,
            };
            discover::discover_taxonomy(&texts, &config)
        }
    };

    let classifications = discover::classify_against_taxonomy(&texts, &tax, threshold);

    let output: Vec<Value> = rows
        .into_iter()
        .zip(classifications)
        .map(|(mut row, (cat, hier, conf))| {
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_category".into(), Value::String(cat));
                obj.insert("_hierarchy".into(), Value::String(hier));
                obj.insert("_confidence".into(), serde_json::json!(conf));
            }
            row
        })
        .collect();

    Ok(Value::Array(output))
}

// ── tags ────────────────────────────────────────────────────────────────────

fn exec_tags(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    if rows.is_empty() {
        return Ok(Value::Array(vec![]));
    }

    let field = get_str(arg, "field", "content");
    let count = get_usize(arg, "count", 5);

    let mut corpus = tfidf::Corpus::new();
    let token_lists: Vec<Vec<String>> = rows
        .iter()
        .map(|r| tokenizer::tokenize(&get_text(r, field)))
        .collect();
    for tokens in &token_lists {
        corpus.add_document(tokens);
    }

    let output: Vec<Value> = rows
        .into_iter()
        .enumerate()
        .map(|(i, mut row)| {
            let top = corpus.top_terms(i, count);
            let tags: Vec<Value> = top.iter().map(|(t, _)| Value::String(t.clone())).collect();
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_tags".into(), Value::Array(tags));
            }
            row
        })
        .collect();

    Ok(Value::Array(output))
}

// ── dedup ───────────────────────────────────────────────────────────────────

fn exec_dedup(arg: &Value) -> Result<Value, String> {
    use rayon::prelude::*;

    let rows = get_records(arg)?;
    if rows.is_empty() {
        return Ok(Value::Array(vec![]));
    }

    let field = get_str(arg, "field", "content");
    let url_field = get_str(arg, "url_field", "url");
    let strategy = get_str(arg, "strategy", "combined");
    let threshold = arg
        .get("threshold")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .unwrap_or(3);
    let n = rows.len();

    // URL dedup
    let mut url_groups: HashMap<String, Vec<usize>> = HashMap::new();
    if strategy == "url" || strategy == "combined" {
        for (i, row) in rows.iter().enumerate() {
            if let Some(url_str) = row.get(url_field).and_then(|v| v.as_str()) {
                if let Some(key) = url_normalize::canonical_key(url_str) {
                    url_groups.entry(key).or_default().push(i);
                }
            }
        }
    }

    // Content dedup
    let mut content_pairs: HashSet<(usize, usize)> = HashSet::new();
    if strategy == "fuzzy" || strategy == "combined" {
        let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();
        let token_lists: Vec<Vec<String>> =
            texts.par_iter().map(|t| tokenizer::tokenize(t)).collect();
        let fingerprints: Vec<u64> = token_lists
            .par_iter()
            .map(|t| simhash::simhash_uniform(t))
            .collect();

        let mut lsh_index = lsh::SimHashLshIndex::default_64();
        for (i, &fp) in fingerprints.iter().enumerate() {
            lsh_index.insert(i, fp);
        }

        for (i, j) in lsh_index.candidate_pairs() {
            if simhash::hamming_distance(fingerprints[i], fingerprints[j]) <= threshold {
                content_pairs.insert((i, j));
            }
        }
    }

    // Union-find
    let mut parent: Vec<usize> = (0..n).collect();
    let find = |parent: &mut Vec<usize>, mut x: usize| -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    };
    let union = |parent: &mut Vec<usize>, a: usize, b: usize| {
        let ra = {
            let mut x = a;
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        };
        let rb = {
            let mut x = b;
            while parent[x] != x {
                parent[x] = parent[parent[x]];
                x = parent[x];
            }
            x
        };
        if ra != rb {
            parent[rb] = ra;
        }
    };

    for members in url_groups.values() {
        for i in 1..members.len() {
            union(&mut parent, members[0], members[i]);
        }
    }
    for &(i, j) in &content_pairs {
        union(&mut parent, i, j);
    }

    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        let root = find(&mut parent, i);
        groups.entry(root).or_default().push(i);
    }

    let mut group_ids = vec![0usize; n];
    let mut is_primary = vec![true; n];
    let mut gid = 0;
    for members in groups.values() {
        for (idx, &member) in members.iter().enumerate() {
            group_ids[member] = gid;
            is_primary[member] = idx == 0;
        }
        gid += 1;
    }

    let output: Vec<Value> = rows
        .into_iter()
        .enumerate()
        .map(|(i, mut row)| {
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_dup_group".into(), serde_json::json!(group_ids[i]));
                obj.insert("_is_primary".into(), serde_json::json!(is_primary[i]));
            }
            row
        })
        .collect();

    Ok(Value::Array(output))
}

// ── similarity ──────────────────────────────────────────────────────────────

fn exec_similarity(arg: &Value) -> Result<Value, String> {
    let a = arg
        .get("a")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'a'")?;
    let b = arg
        .get("b")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'b'")?;
    let metric_name = get_str(arg, "metric", "levenshtein");
    let all = get_bool(arg, "all", false);

    if all {
        let mut results = serde_json::Map::new();
        results.insert("a".into(), Value::String(a.into()));
        results.insert("b".into(), Value::String(b.into()));
        for name in string_distance::Metric::all_names() {
            let metric = string_distance::Metric::from_str(name).unwrap();
            let score = string_distance::similarity(a, b, metric);
            results.insert((*name).into(), serde_json::json!(score));
        }
        Ok(Value::Object(results))
    } else {
        let metric = string_distance::Metric::from_str(metric_name).ok_or_else(|| {
            format!(
                "Unknown metric '{metric_name}'. Use: {}",
                string_distance::Metric::all_names().join(", ")
            )
        })?;
        let score = string_distance::similarity(a, b, metric);
        Ok(serde_json::json!({
            "a": a, "b": b, "metric": metric_name, "similarity": score
        }))
    }
}

// ── normalize_url ───────────────────────────────────────────────────────────

fn exec_normalize_url(arg: &Value) -> Result<Value, String> {
    let url = arg
        .get("url")
        .and_then(|v| v.as_str())
        .ok_or("Missing required string field 'url'")?;

    match url_normalize::normalize(url) {
        Some(normalized) => {
            let canonical = url_normalize::canonical_key(url).unwrap_or_default();
            Ok(serde_json::json!({
                "original": url,
                "normalized": normalized,
                "canonical_key": canonical,
            }))
        }
        None => Err(format!("Could not parse URL: {url}")),
    }
}

// ── generate ────────────────────────────────────────────────────────────────

fn exec_generate(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    let n = rows.len();
    if n < 2 {
        return Err("Need at least 2 items to generate a taxonomy".into());
    }

    let field = get_str(arg, "field", "content");
    let depth = get_usize(arg, "depth", 10);
    let linkage_str = get_str(arg, "linkage", "ward");
    let top_n = get_usize(arg, "top_terms", 5);

    let linkage = clustering::Linkage::from_str(linkage_str).ok_or_else(|| {
        format!("Unknown linkage '{linkage_str}'. Use: ward, complete, average, single")
    })?;

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();
    let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();

    let mut corpus = tfidf::Corpus::new();
    for tokens in &token_lists {
        corpus.add_document(tokens);
    }

    let vectors: Vec<HashMap<String, f64>> = (0..n).map(|i| corpus.tfidf_vector(i)).collect();
    let distances = clustering::cosine_distance_matrix(&vectors);
    let k = depth.min(n);
    let dendrogram = clustering::hac(&distances, n, linkage);
    let labels = clustering::cut_tree(&dendrogram, k);

    let actual_k = labels.iter().max().map(|m| m + 1).unwrap_or(0);
    let mut categories: Vec<Value> = Vec::with_capacity(actual_k);

    for cluster_idx in 0..actual_k {
        let member_indices: Vec<usize> = labels
            .iter()
            .enumerate()
            .filter(|(_, &l)| l == cluster_idx)
            .map(|(i, _)| i)
            .collect();

        if member_indices.is_empty() {
            continue;
        }

        let mut merged: HashMap<String, f64> = HashMap::new();
        for &i in &member_indices {
            for (term, weight) in &vectors[i] {
                *merged.entry(term.clone()).or_insert(0.0) += weight;
            }
        }

        let mut sorted_terms: Vec<(String, f64)> = merged.into_iter().collect();
        sorted_terms
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        sorted_terms.truncate(top_n);

        let label = sorted_terms
            .iter()
            .take(3)
            .map(|(t, _)| t.as_str())
            .collect::<Vec<&str>>()
            .join(", ");

        let keywords: Vec<Value> = sorted_terms
            .iter()
            .map(|(t, w)| serde_json::json!({"term": t, "weight": w}))
            .collect();

        let members: Vec<Value> = member_indices.iter().map(|&i| serde_json::json!(i)).collect();

        categories.push(serde_json::json!({
            "id": cluster_idx,
            "label": label,
            "size": member_indices.len(),
            "keywords": keywords,
            "members": members,
        }));
    }

    Ok(serde_json::json!({
        "name": "generated",
        "num_clusters": actual_k,
        "num_items": n,
        "linkage": linkage_str,
        "categories": categories,
    }))
}

// ── topics ──────────────────────────────────────────────────────────────────

fn exec_topics(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    if rows.is_empty() {
        return Err("Need at least 1 item for topic modeling".into());
    }

    let field = get_str(arg, "field", "content");
    let k = get_usize(arg, "topics", 5);
    let top_n = get_usize(arg, "terms", 10);
    let max_iter = get_usize(arg, "iterations", 200);
    let vocab_limit = get_usize(arg, "vocab", 5000);

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();
    let token_lists: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();

    let mut corpus = tfidf::Corpus::new();
    for tokens in &token_lists {
        corpus.add_document(tokens);
    }

    let vectors: Vec<HashMap<String, f64>> = (0..rows.len())
        .map(|i| corpus.tfidf_vector(i))
        .collect();

    let result = nmf::nmf(&vectors, k, max_iter, vocab_limit);
    let dominant = result.dominant_topics();

    let topics: Vec<Value> = (0..k)
        .map(|t| {
            let top = result.top_terms(t, top_n);
            let terms: Vec<Value> = top
                .iter()
                .map(|(term, weight)| serde_json::json!({"term": term, "weight": weight}))
                .collect();

            let members: Vec<Value> = dominant
                .iter()
                .enumerate()
                .filter(|(_, &topic)| topic == t)
                .map(|(i, _)| serde_json::json!(i))
                .collect();

            let label = top
                .iter()
                .take(3)
                .map(|(t, _)| t.as_str())
                .collect::<Vec<&str>>()
                .join(", ");

            serde_json::json!({
                "id": t,
                "label": label,
                "size": members.len(),
                "terms": terms,
                "members": members,
            })
        })
        .collect();

    let assignments: Vec<Value> = dominant
        .iter()
        .enumerate()
        .map(|(i, &topic)| serde_json::json!({"item": i, "topic": topic}))
        .collect();

    Ok(serde_json::json!({
        "num_topics": k,
        "num_items": rows.len(),
        "topics": topics,
        "assignments": assignments,
    }))
}

// ── organize ────────────────────────────────────────────────────────────────

fn exec_organize(arg: &Value) -> Result<Value, String> {
    let rows = get_records(arg)?;
    if rows.is_empty() {
        return Ok(Value::Array(vec![]));
    }

    let format = get_str(arg, "format", "folders");
    let output_dir = get_str(arg, "output_dir", "./organized");
    let category_field = get_str(arg, "category_field", "_category");
    let name_field = get_str(arg, "name_field", "id");

    let output: Vec<Value> = rows
        .into_iter()
        .map(|mut row| {
            let category = row
                .get(category_field)
                .and_then(|v| v.as_str())
                .unwrap_or("Uncategorized")
                .to_string();

            let name = row
                .get(name_field)
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let slug_cat = url_normalize::slugify(&category);
            let slug_name = url_normalize::slugify(&name);

            let output_path = match format {
                "flat" => std::format!("{output_dir}/{slug_cat}--{slug_name}"),
                "nested" => {
                    let hierarchy = row
                        .get("_hierarchy")
                        .and_then(|v| v.as_str())
                        .unwrap_or(&category)
                        .to_string();
                    let path = hierarchy
                        .split(" > ")
                        .map(|p| url_normalize::slugify(p))
                        .collect::<Vec<String>>()
                        .join("/");
                    std::format!("{output_dir}/{path}/{slug_name}")
                }
                _ => std::format!("{output_dir}/{slug_cat}/{slug_name}"),
            };

            if let Some(obj) = row.as_object_mut() {
                obj.insert("_output_path".into(), Value::String(output_path));
            }
            row
        })
        .collect();

    Ok(Value::Array(output))
}
