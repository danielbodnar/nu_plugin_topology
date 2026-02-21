//! MCP (Model Context Protocol) server for topology.
//!
//! Exposes every algorithm in `algo/` as an MCP tool so that AI assistants
//! (Claude Desktop, Cursor, etc.) can invoke them over stdio JSON-RPC.
//!
//! Start with: `topology --mcp`

use std::collections::{HashMap, HashSet};

use rmcp::{
    handler::server::tool::{ToolCallContext, ToolRouter},
    handler::server::wrapper::Parameters,
    model::*,
    service::RequestContext,
    tool, tool_router, ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::algo::{
    clustering, discover, lsh, nmf, sampling, simhash, string_distance, taxonomy, tfidf, tokenizer,
    url_normalize,
};

// ── Parameter structs ───────────────────────────────────────────────────────
// Each struct maps 1:1 with a CLI subcommand. `JsonSchema` is required by
// rmcp so that the tool's input schema is auto-generated for the AI client.

#[derive(Debug, Deserialize, JsonSchema)]
pub struct FingerprintParams {
    /// JSON array of objects to fingerprint.
    pub records: Vec<serde_json::Value>,
    /// Field name containing text to fingerprint (default: "content").
    #[serde(default = "default_field")]
    pub field: String,
    /// Use TF-IDF weighted SimHash instead of uniform.
    #[serde(default)]
    pub weighted: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SampleParams {
    /// JSON array of objects to sample from.
    pub records: Vec<serde_json::Value>,
    /// Number of rows to sample.
    #[serde(default = "default_sample_size")]
    pub size: usize,
    /// Strategy: "random", "stratified", "systematic", "reservoir".
    #[serde(default = "default_strategy")]
    pub strategy: String,
    /// Field to stratify by (required when strategy is "stratified").
    pub field: Option<String>,
    /// Random seed.
    #[serde(default = "default_seed")]
    pub seed: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct AnalyzeParams {
    /// JSON array of objects to analyze.
    pub records: Vec<serde_json::Value>,
    /// Restrict analysis to a single field.
    pub field: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ClassifyParams {
    /// JSON array of objects to classify.
    pub records: Vec<serde_json::Value>,
    /// Field name containing text (default: "content").
    #[serde(default = "default_field")]
    pub field: String,
    /// Inline taxonomy JSON. If omitted, categories are auto-discovered.
    pub taxonomy: Option<serde_json::Value>,
    /// Number of categories to discover (default: 15).
    #[serde(default = "default_clusters")]
    pub clusters: usize,
    /// Max items to sample during discovery (default: 500).
    #[serde(default = "default_sample_limit")]
    pub sample: usize,
    /// Minimum BM25 score threshold (default: 0.5).
    #[serde(default = "default_threshold")]
    pub threshold: f64,
    /// Random seed.
    #[serde(default = "default_seed")]
    pub seed: u64,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TagsParams {
    /// JSON array of objects.
    pub records: Vec<serde_json::Value>,
    /// Field containing text (default: "content").
    #[serde(default = "default_field")]
    pub field: String,
    /// Number of tags per item (default: 5).
    #[serde(default = "default_tag_count")]
    pub count: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct DedupParams {
    /// JSON array of objects.
    pub records: Vec<serde_json::Value>,
    /// Field containing text (default: "content").
    #[serde(default = "default_field")]
    pub field: String,
    /// Field containing URL (default: "url").
    #[serde(default = "default_url_field")]
    pub url_field: String,
    /// Strategy: "url", "fuzzy", "combined" (default: "combined").
    #[serde(default = "default_dedup_strategy")]
    pub strategy: String,
    /// SimHash hamming-distance threshold (default: 3).
    #[serde(default = "default_dedup_threshold")]
    pub threshold: u32,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SimilarityParams {
    /// First string.
    pub a: String,
    /// Second string.
    pub b: String,
    /// Metric: "levenshtein", "jaro-winkler", "cosine".
    #[serde(default = "default_metric")]
    pub metric: String,
    /// Compute all metrics at once.
    #[serde(default)]
    pub all: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct NormalizeUrlParams {
    /// URL to normalize.
    pub url: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct GenerateParams {
    /// JSON array of objects to generate taxonomy from.
    pub records: Vec<serde_json::Value>,
    /// Field containing text (default: "content").
    #[serde(default = "default_field")]
    pub field: String,
    /// Number of clusters / taxonomy depth (default: 10).
    #[serde(default = "default_depth")]
    pub depth: usize,
    /// Linkage method: "ward", "complete", "average", "single" (default: "ward").
    #[serde(default = "default_linkage")]
    pub linkage: String,
    /// Number of top terms per cluster label (default: 5).
    #[serde(default = "default_top_terms")]
    pub top_terms: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct TopicsParams {
    /// JSON array of objects for topic modeling.
    pub records: Vec<serde_json::Value>,
    /// Field containing text (default: "content").
    #[serde(default = "default_field")]
    pub field: String,
    /// Number of topics to discover (default: 5).
    #[serde(default = "default_topics")]
    pub topics: usize,
    /// Number of top terms per topic (default: 10).
    #[serde(default = "default_terms")]
    pub terms: usize,
    /// NMF iterations (default: 200).
    #[serde(default = "default_iterations")]
    pub iterations: usize,
    /// Max vocabulary size (default: 5000).
    #[serde(default = "default_vocab")]
    pub vocab: usize,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct OrganizeParams {
    /// JSON array of classified objects (must have a category field).
    pub records: Vec<serde_json::Value>,
    /// Output format: "folders", "flat", "nested" (default: "folders").
    #[serde(default = "default_format")]
    pub format: String,
    /// Base output directory path (default: "./organized").
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    /// Field containing category (default: "_category").
    #[serde(default = "default_category_field")]
    pub category_field: String,
    /// Field to use for filename (default: "id").
    #[serde(default = "default_name_field")]
    pub name_field: String,
}

// ── Default helpers ─────────────────────────────────────────────────────────

fn default_field() -> String {
    "content".into()
}
fn default_sample_size() -> usize {
    100
}
fn default_strategy() -> String {
    "random".into()
}
fn default_seed() -> u64 {
    42
}
fn default_clusters() -> usize {
    15
}
fn default_sample_limit() -> usize {
    500
}
fn default_threshold() -> f64 {
    0.5
}
fn default_tag_count() -> usize {
    5
}
fn default_url_field() -> String {
    "url".into()
}
fn default_dedup_strategy() -> String {
    "combined".into()
}
fn default_dedup_threshold() -> u32 {
    3
}
fn default_metric() -> String {
    "levenshtein".into()
}
fn default_depth() -> usize {
    10
}
fn default_linkage() -> String {
    "ward".into()
}
fn default_top_terms() -> usize {
    5
}
fn default_topics() -> usize {
    5
}
fn default_terms() -> usize {
    10
}
fn default_iterations() -> usize {
    200
}
fn default_vocab() -> usize {
    5000
}
fn default_format() -> String {
    "folders".into()
}
fn default_output_dir() -> String {
    "./organized".into()
}
fn default_category_field() -> String {
    "_category".into()
}
fn default_name_field() -> String {
    "id".into()
}

// ── Shared helpers ──────────────────────────────────────────────────────────

fn get_text(row: &serde_json::Value, field: &str) -> String {
    row.get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn json_text(v: &serde_json::Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

// ── MCP Server ──────────────────────────────────────────────────────────────

/// The MCP server struct. Holds only the auto-generated tool router.
#[derive(Clone)]
pub struct TopologyMcp {
    #[allow(dead_code)] // accessed at runtime by the #[tool_router] macro
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl TopologyMcp {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    // ── fingerprint ─────────────────────────────────────────────────────

    #[tool(
        name = "fingerprint",
        description = "Compute 64-bit SimHash fingerprints for JSON records. Returns the input records with an added `_fingerprint` hex column. Use `weighted: true` to apply TF-IDF weighting for more accurate fingerprints on longer text."
    )]
    async fn fingerprint(
        &self,
        params: Parameters<FingerprintParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result =
            tokio::task::spawn_blocking(move || do_fingerprint(&p.records, &p.field, p.weighted))
                .await
                .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── sample ──────────────────────────────────────────────────────────

    #[tool(
        name = "sample",
        description = "Sample rows from a JSON array. Strategies: random (Fisher-Yates), stratified (proportional groups), systematic (every k-th), reservoir (single-pass streaming). Returns the sampled subset."
    )]
    async fn sample(&self, params: Parameters<SampleParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            do_sample(&p.records, p.size, &p.strategy, p.field.as_deref(), p.seed)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── analyze ─────────────────────────────────────────────────────────

    #[tool(
        name = "analyze",
        description = "Analyze the structure and field statistics of a JSON array. Returns total rows, column names, cardinality, type distribution, average/min/max lengths, and top-5 most frequent values per field."
    )]
    async fn analyze(&self, params: Parameters<AnalyzeParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result =
            tokio::task::spawn_blocking(move || do_analyze(&p.records, p.field.as_deref()))
                .await
                .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── classify ────────────────────────────────────────────────────────

    #[tool(
        name = "classify",
        description = "Classify items into categories. When no taxonomy is provided, categories are auto-discovered via TF-IDF + HAC clustering + BM25 scoring. Returns records with `_category`, `_hierarchy`, and `_confidence` columns."
    )]
    async fn classify(
        &self,
        params: Parameters<ClassifyParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            do_classify(
                &p.records,
                &p.field,
                p.taxonomy.as_ref(),
                p.clusters,
                p.sample,
                p.threshold,
                p.seed,
            )
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── tags ────────────────────────────────────────────────────────────

    #[tool(
        name = "tags",
        description = "Extract the top TF-IDF keywords from each record. Returns the input with an added `_tags` array column per row."
    )]
    async fn tags(&self, params: Parameters<TagsParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || do_tags(&p.records, &p.field, p.count))
            .await
            .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── dedup ───────────────────────────────────────────────────────────

    #[tool(
        name = "dedup",
        description = "Find duplicates using SimHash + LSH + URL normalization. Strategies: url (URL canonicalization only), fuzzy (SimHash content fingerprinting), combined (both). Returns records with `_dup_group` and `_is_primary` columns."
    )]
    async fn dedup(&self, params: Parameters<DedupParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            do_dedup(&p.records, &p.field, &p.url_field, &p.strategy, p.threshold)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── similarity ──────────────────────────────────────────────────────

    #[tool(
        name = "similarity",
        description = "Compute string similarity between two strings. Metrics: levenshtein (edit distance), jaro-winkler (prefix-weighted), cosine (character bigrams). Set `all: true` to compute every metric at once. Returns a score between 0.0 (no match) and 1.0 (identical)."
    )]
    async fn similarity(
        &self,
        params: Parameters<SimilarityParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = do_similarity(&p.a, &p.b, &p.metric, p.all)
            .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── normalize_url ───────────────────────────────────────────────────

    #[tool(
        name = "normalize_url",
        description = "Normalize a URL for deduplication. Strips tracking parameters (utm_*, fbclid, etc.), removes fragments, lowercases host, strips www prefix, removes default ports, and sorts query parameters. Returns the normalized URL and its canonical key."
    )]
    async fn normalize_url(
        &self,
        params: Parameters<NormalizeUrlParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = do_normalize_url(&p.url).map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── generate ────────────────────────────────────────────────────────

    #[tool(
        name = "generate",
        description = "Auto-generate a taxonomy from content using TF-IDF + hierarchical agglomerative clustering. Returns a record with cluster labels, keywords, and member indices. Useful for discovering natural groupings in unstructured text data."
    )]
    async fn generate(
        &self,
        params: Parameters<GenerateParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            do_generate(&p.records, &p.field, p.depth, &p.linkage, p.top_terms)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── topics ──────────────────────────────────────────────────────────

    #[tool(
        name = "topics",
        description = "Discover topics using Non-negative Matrix Factorization (NMF) on TF-IDF vectors. Returns topic labels with top terms, member assignments, and topic sizes. Good for finding latent themes across a collection of documents."
    )]
    async fn topics(&self, params: Parameters<TopicsParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            do_topics(&p.records, &p.field, p.topics, p.terms, p.iterations, p.vocab)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }

    // ── organize ────────────────────────────────────────────────────────

    #[tool(
        name = "organize",
        description = "Generate output paths and structure from classified items. Reads a category field from each record and produces a `_output_path` column. Supports formats: folders (category/name), flat (category--name), nested (hierarchy path)."
    )]
    async fn organize(
        &self,
        params: Parameters<OrganizeParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            do_organize(
                &p.records,
                &p.format,
                &p.output_dir,
                &p.category_field,
                &p.name_field,
            )
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(
            &result,
        ))]))
    }
}

// ── ServerHandler glue ──────────────────────────────────────────────────────

impl ServerHandler for TopologyMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Content topology engine. Tools: fingerprint, sample, analyze, classify, \
                 tags, dedup, similarity, normalize_url, generate, topics, organize. \
                 Pass JSON records for bulk operations or simple strings for \
                 similarity/normalize_url."
                    .into(),
            ),
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability { list_changed: None }),
                ..Default::default()
            },
            server_info: Implementation {
                name: "topology".into(),
                version: env!("CARGO_PKG_VERSION").into(),
                title: Some("Topology Engine".into()),
                description: Some(
                    "Content topology, classification, and deduplication engine".into(),
                ),
                icons: None,
                website_url: None,
            },
            ..Default::default()
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, McpError>> + Send + '_ {
        std::future::ready(Ok(ListToolsResult {
            tools: self.tool_router.list_all(),
            next_cursor: None,
            meta: Default::default(),
        }))
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, McpError>> + Send + '_ {
        let tool_context = ToolCallContext::new(self, request, context);
        async move { self.tool_router.call(tool_context).await }
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.tool_router.get(name).cloned()
    }
}

// ── Entry point ─────────────────────────────────────────────────────────────

/// Start the MCP server on stdio. Called from `cli.rs` when `--mcp` is passed.
pub async fn serve_stdio() -> Result<(), Box<dyn std::error::Error>> {
    let server = TopologyMcp::new();
    let transport = rmcp::transport::io::stdio();
    let service = server.serve(transport).await.inspect_err(|e| {
        eprintln!("MCP serve error: {e}");
    })?;
    service.waiting().await?;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════════
//  Pure-algorithm wrappers (no async, no rmcp types)
// ═══════════════════════════════════════════════════════════════════════════

fn do_fingerprint(rows: &[serde_json::Value], field: &str, weighted: bool) -> serde_json::Value {
    use rayon::prelude::*;

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

    let output: Vec<serde_json::Value> = rows
        .iter()
        .cloned()
        .zip(fingerprints)
        .map(|(mut row, fp)| {
            if let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "_fingerprint".into(),
                    serde_json::Value::String(simhash::fingerprint_to_hex(fp)),
                );
            }
            row
        })
        .collect();

    serde_json::Value::Array(output)
}

fn do_sample(
    rows: &[serde_json::Value],
    size: usize,
    strategy: &str,
    field: Option<&str>,
    seed: u64,
) -> Result<serde_json::Value, String> {
    let total = rows.len();
    if total == 0 {
        return Ok(serde_json::Value::Array(vec![]));
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

    let sampled: Vec<serde_json::Value> = indices
        .iter()
        .filter(|&&i| i < total)
        .map(|&i| rows[i].clone())
        .collect();

    Ok(serde_json::Value::Array(sampled))
}

fn do_analyze(rows: &[serde_json::Value], field: Option<&str>) -> serde_json::Value {
    let total = rows.len();
    if total == 0 {
        return serde_json::json!({"total_rows": 0, "columns": [], "fields": {}});
    }

    let columns: Vec<String> = match field {
        Some(f) => vec![f.to_string()],
        None => match &rows[0] {
            serde_json::Value::Object(map) => map.keys().cloned().collect(),
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

        for row in rows {
            match row.get(col.as_str()) {
                Some(serde_json::Value::Null) | None => null_count += 1,
                Some(v) => {
                    let type_name = match v {
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::Bool(_) => "bool",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                        serde_json::Value::Null => "null",
                    };
                    *type_counts.entry(type_name.into()).or_insert(0) += 1;
                    let s = match v {
                        serde_json::Value::String(s) => s.clone(),
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

    serde_json::json!({
        "total_rows": total,
        "columns": columns,
        "num_columns": columns.len(),
        "fields": serde_json::Value::Object(fields),
    })
}

fn do_classify(
    rows: &[serde_json::Value],
    field: &str,
    taxonomy_json: Option<&serde_json::Value>,
    clusters: usize,
    sample_size: usize,
    threshold: f64,
    seed: u64,
) -> Result<serde_json::Value, String> {
    if rows.is_empty() {
        return Ok(serde_json::Value::Array(vec![]));
    }

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();

    let tax = match taxonomy_json {
        Some(v) => {
            let json_str = serde_json::to_string(v)
                .map_err(|e| format!("Failed to serialize taxonomy: {e}"))?;
            taxonomy::parse_taxonomy(&json_str)?
        }
        None => {
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

    let output: Vec<serde_json::Value> = rows
        .iter()
        .cloned()
        .zip(classifications)
        .map(|(mut row, (cat, hier, conf))| {
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_category".into(), serde_json::Value::String(cat));
                obj.insert("_hierarchy".into(), serde_json::Value::String(hier));
                obj.insert("_confidence".into(), serde_json::json!(conf));
            }
            row
        })
        .collect();

    Ok(serde_json::Value::Array(output))
}

fn do_tags(rows: &[serde_json::Value], field: &str, count: usize) -> serde_json::Value {
    if rows.is_empty() {
        return serde_json::Value::Array(vec![]);
    }

    let mut corpus = tfidf::Corpus::new();
    let token_lists: Vec<Vec<String>> = rows
        .iter()
        .map(|r| tokenizer::tokenize(&get_text(r, field)))
        .collect();
    for tokens in &token_lists {
        corpus.add_document(tokens);
    }

    let output: Vec<serde_json::Value> = rows
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, mut row)| {
            let top = corpus.top_terms(i, count);
            let tags: Vec<serde_json::Value> = top
                .iter()
                .map(|(t, _)| serde_json::Value::String(t.clone()))
                .collect();
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_tags".into(), serde_json::Value::Array(tags));
            }
            row
        })
        .collect();

    serde_json::Value::Array(output)
}

fn do_dedup(
    rows: &[serde_json::Value],
    field: &str,
    url_field: &str,
    strategy: &str,
    threshold: u32,
) -> serde_json::Value {
    use rayon::prelude::*;

    if rows.is_empty() {
        return serde_json::Value::Array(vec![]);
    }
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

    let output: Vec<serde_json::Value> = rows
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, mut row)| {
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_dup_group".into(), serde_json::json!(group_ids[i]));
                obj.insert("_is_primary".into(), serde_json::json!(is_primary[i]));
            }
            row
        })
        .collect();

    serde_json::Value::Array(output)
}

fn do_similarity(
    a: &str,
    b: &str,
    metric_name: &str,
    all: bool,
) -> Result<serde_json::Value, String> {
    if all {
        let mut results = serde_json::Map::new();
        results.insert("a".into(), serde_json::Value::String(a.into()));
        results.insert("b".into(), serde_json::Value::String(b.into()));
        for name in string_distance::Metric::all_names() {
            let metric = string_distance::Metric::from_str(name).unwrap();
            let score = string_distance::similarity(a, b, metric);
            results.insert((*name).into(), serde_json::json!(score));
        }
        Ok(serde_json::Value::Object(results))
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

fn do_normalize_url(url: &str) -> Result<serde_json::Value, String> {
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

fn do_generate(
    rows: &[serde_json::Value],
    field: &str,
    depth: usize,
    linkage_str: &str,
    top_n: usize,
) -> Result<serde_json::Value, String> {
    let n = rows.len();
    if n < 2 {
        return Err("Need at least 2 items to generate a taxonomy".into());
    }

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
    let mut categories: Vec<serde_json::Value> = Vec::with_capacity(actual_k);

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

        let keywords: Vec<serde_json::Value> = sorted_terms
            .iter()
            .map(|(t, w)| serde_json::json!({"term": t, "weight": w}))
            .collect();

        let members: Vec<serde_json::Value> =
            member_indices.iter().map(|&i| serde_json::json!(i)).collect();

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

fn do_topics(
    rows: &[serde_json::Value],
    field: &str,
    k: usize,
    top_n: usize,
    max_iter: usize,
    vocab_limit: usize,
) -> Result<serde_json::Value, String> {
    if rows.is_empty() {
        return Err("Need at least 1 item for topic modeling".into());
    }

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

    let topics: Vec<serde_json::Value> = (0..k)
        .map(|t| {
            let top = result.top_terms(t, top_n);
            let terms: Vec<serde_json::Value> = top
                .iter()
                .map(|(term, weight)| serde_json::json!({"term": term, "weight": weight}))
                .collect();

            let members: Vec<serde_json::Value> = dominant
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

    let assignments: Vec<serde_json::Value> = dominant
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

fn do_organize(
    rows: &[serde_json::Value],
    format: &str,
    output_dir: &str,
    category_field: &str,
    name_field: &str,
) -> serde_json::Value {
    if rows.is_empty() {
        return serde_json::Value::Array(vec![]);
    }

    let output: Vec<serde_json::Value> = rows
        .iter()
        .cloned()
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
                "flat" => format!("{output_dir}/{slug_cat}--{slug_name}"),
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
                    format!("{output_dir}/{path}/{slug_name}")
                }
                _ => format!("{output_dir}/{slug_cat}/{slug_name}"),
            };

            if let Some(obj) = row.as_object_mut() {
                obj.insert(
                    "_output_path".into(),
                    serde_json::Value::String(output_path),
                );
            }
            row
        })
        .collect();

    serde_json::Value::Array(output)
}
