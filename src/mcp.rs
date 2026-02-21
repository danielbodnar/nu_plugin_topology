//! MCP (Model Context Protocol) server for topology.
//!
//! Exposes every algorithm in `algo/` as an MCP tool so that AI assistants
//! (Claude Desktop, Cursor, etc.) can invoke them over stdio JSON-RPC.
//!
//! Start with: `topology --mcp`

use rmcp::{
    handler::server::tool::{ToolCallContext, ToolRouter},
    handler::server::wrapper::Parameters,
    model::*,
    service::RequestContext,
    tool, tool_router, ErrorData as McpError, RoleServer, ServerHandler, ServiceExt,
};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::ops;

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
    /// Optional path to SQLite cache database for persistent artifact caching.
    pub cache: Option<String>,
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
    /// Optional path to SQLite cache database for persistent artifact caching.
    pub cache: Option<String>,
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
    /// Optional path to SQLite cache database for persistent artifact caching.
    pub cache: Option<String>,
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
    /// Optional path to SQLite cache database for persistent artifact caching.
    pub cache: Option<String>,
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
    /// Optional path to SQLite cache database for persistent artifact caching.
    pub cache: Option<String>,
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
    /// Optional path to SQLite cache database for persistent artifact caching.
    pub cache: Option<String>,
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

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CacheInfoParams {
    /// Path to the SQLite cache database.
    pub path: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CacheClearParams {
    /// Path to the SQLite cache database.
    pub path: String,
    /// Artifact kind to clear: "corpus", "dendrogram", "taxonomy", "fingerprints". If omitted, clears all.
    pub kind: Option<String>,
}

// ── Default helpers ─────────────────────────────────────────────────────────

fn default_field() -> String { "content".into() }
fn default_sample_size() -> usize { 100 }
fn default_strategy() -> String { "random".into() }
fn default_seed() -> u64 { 42 }
fn default_clusters() -> usize { 15 }
fn default_sample_limit() -> usize { 500 }
fn default_threshold() -> f64 { 0.5 }
fn default_tag_count() -> usize { 5 }
fn default_url_field() -> String { "url".into() }
fn default_dedup_strategy() -> String { "combined".into() }
fn default_dedup_threshold() -> u32 { 3 }
fn default_metric() -> String { "levenshtein".into() }
fn default_depth() -> usize { 10 }
fn default_linkage() -> String { "ward".into() }
fn default_top_terms() -> usize { 5 }
fn default_topics() -> usize { 5 }
fn default_terms() -> usize { 10 }
fn default_iterations() -> usize { 200 }
fn default_vocab() -> usize { 5000 }
fn default_format() -> String { "folders".into() }
fn default_output_dir() -> String { "./organized".into() }
fn default_category_field() -> String { "_category".into() }
fn default_name_field() -> String { "id".into() }

// ── Shared helpers ──────────────────────────────────────────────────────────

fn json_text(v: &serde_json::Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

// ── MCP Server ──────────────────────────────────────────────────────────────

/// The MCP server struct. Holds only the auto-generated tool router.
#[derive(Clone)]
pub struct TopologyMcp {
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

#[tool_router]
impl TopologyMcp {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

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
            tokio::task::spawn_blocking(move || ops::op_fingerprint_cached(&p.records, &p.field, p.weighted, p.cache.as_deref()))
                .await
                .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "sample",
        description = "Sample rows from a JSON array. Strategies: random (Fisher-Yates), stratified (proportional groups), systematic (every k-th), reservoir (single-pass streaming). Returns the sampled subset."
    )]
    async fn sample(&self, params: Parameters<SampleParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            ops::op_sample(&p.records, p.size, &p.strategy, p.field.as_deref(), p.seed)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "analyze",
        description = "Analyze the structure and field statistics of a JSON array. Returns total rows, column names, cardinality, type distribution, average/min/max lengths, and top-5 most frequent values per field."
    )]
    async fn analyze(&self, params: Parameters<AnalyzeParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result =
            tokio::task::spawn_blocking(move || ops::op_analyze_cached(&p.records, p.field.as_deref(), p.cache.as_deref()))
                .await
                .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

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
            ops::op_classify_cached(
                &p.records,
                &p.field,
                p.taxonomy.as_ref(),
                p.clusters,
                p.sample,
                p.threshold,
                p.seed,
                p.cache.as_deref(),
            )
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "tags",
        description = "Extract the top TF-IDF keywords from each record. Returns the input with an added `_tags` array column per row."
    )]
    async fn tags(&self, params: Parameters<TagsParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || ops::op_tags_cached(&p.records, &p.field, p.count, p.cache.as_deref()))
            .await
            .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "dedup",
        description = "Find duplicates using SimHash + LSH + URL normalization. Strategies: url (URL canonicalization only), fuzzy (SimHash content fingerprinting), combined (both). Returns records with `_dup_group` and `_is_primary` columns."
    )]
    async fn dedup(&self, params: Parameters<DedupParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            ops::op_dedup_cached(&p.records, &p.field, &p.url_field, &p.strategy, p.threshold, p.cache.as_deref())
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "similarity",
        description = "Compute string similarity between two strings. Metrics: levenshtein (edit distance), jaro-winkler (prefix-weighted), cosine (character bigrams). Set `all: true` to compute every metric at once. Returns a score between 0.0 (no match) and 1.0 (identical)."
    )]
    async fn similarity(
        &self,
        params: Parameters<SimilarityParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = ops::op_similarity(&p.a, &p.b, &p.metric, p.all)
            .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "normalize_url",
        description = "Normalize a URL for deduplication. Strips tracking parameters (utm_*, fbclid, etc.), removes fragments, lowercases host, strips www prefix, removes default ports, and sorts query parameters. Returns the normalized URL and its canonical key."
    )]
    async fn normalize_url(
        &self,
        params: Parameters<NormalizeUrlParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = ops::op_normalize_url(&p.url)
            .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

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
            ops::op_generate_cached(&p.records, &p.field, p.depth, &p.linkage, p.top_terms, p.cache.as_deref())
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "topics",
        description = "Discover topics using Non-negative Matrix Factorization (NMF) on TF-IDF vectors. Returns topic labels with top terms, member assignments, and topic sizes. Good for finding latent themes across a collection of documents."
    )]
    async fn topics(&self, params: Parameters<TopicsParams>) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = tokio::task::spawn_blocking(move || {
            ops::op_topics(&p.records, &p.field, p.topics, p.terms, p.iterations, p.vocab)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?
        .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

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
            ops::op_organize(&p.records, &p.format, &p.output_dir, &p.category_field, &p.name_field)
        })
        .await
        .map_err(|e| McpError::internal_error(format!("task join error: {e}"), None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "cache_info",
        description = "Show information about a topology cache database: artifact types, sizes, counts, and total database size."
    )]
    async fn cache_info(
        &self,
        params: Parameters<CacheInfoParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = ops::op_cache_info(&p.path)
            .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }

    #[tool(
        name = "cache_clear",
        description = "Clear cached artifacts from a topology cache database. Optionally clear only a specific kind: corpus, dendrogram, taxonomy, or fingerprints."
    )]
    async fn cache_clear(
        &self,
        params: Parameters<CacheClearParams>,
    ) -> Result<CallToolResult, McpError> {
        let p = params.0;
        let result = ops::op_cache_clear(&p.path, p.kind.as_deref())
            .map_err(|e| McpError::invalid_params(e, None))?;
        Ok(CallToolResult::success(vec![Content::text(json_text(&result))]))
    }
}

// ── ServerHandler glue ──────────────────────────────────────────────────────

impl ServerHandler for TopologyMcp {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            instructions: Some(
                "Content topology engine. Tools: fingerprint, sample, analyze, classify, \
                 tags, dedup, similarity, normalize_url, generate, topics, organize, \
                 cache_info, cache_clear. \
                 Pass JSON records for bulk operations or simple strings for \
                 similarity/normalize_url. Use the `cache` parameter on supported tools \
                 to enable persistent SQLite caching."
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
