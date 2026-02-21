use clap::{Parser, Subcommand};
use nu_plugin_topology::ops;
use serde_json::Value;
use std::io::{self, Read};

#[derive(Parser)]
#[command(
    name = "topology",
    version,
    about = "Content topology, classification, and deduplication engine"
)]
struct Cli {
    /// Start as an MCP (Model Context Protocol) server on stdio.
    /// AI assistants (Claude Desktop, Cursor, etc.) connect via JSON-RPC.
    #[cfg(feature = "mcp")]
    #[arg(long, exclusive = true)]
    mcp: bool,

    /// Start as an LSP (Language Server Protocol) server on stdio.
    /// Editors (VS Code, Zed, Neovim, etc.) connect via JSON-RPC.
    #[cfg(feature = "lsp")]
    #[arg(long, exclusive = true)]
    lsp: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Compute SimHash fingerprints for JSON records
    Fingerprint {
        /// JSON field containing text to fingerprint
        #[arg(short, long, default_value = "content")]
        field: String,
        /// Use TF-IDF weighted SimHash
        #[arg(short, long)]
        weighted: bool,
    },
    /// Sample rows from a JSON array
    Sample {
        /// Number of rows to sample
        #[arg(short = 'n', long, default_value_t = 100)]
        size: usize,
        /// Strategy: random, stratified, systematic, reservoir
        #[arg(short, long, default_value = "random")]
        strategy: String,
        /// Field to stratify by (required for stratified)
        #[arg(short, long)]
        field: Option<String>,
        /// Random seed
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// Analyze table structure and field statistics
    Analyze {
        /// JSON field containing text (if single-field analysis)
        #[arg(short, long)]
        field: Option<String>,
    },
    /// Classify items into auto-discovered categories (or user-provided taxonomy)
    Classify {
        /// JSON field containing text to classify
        #[arg(short, long, default_value = "content")]
        field: String,
        /// Path to taxonomy JSON file. If omitted, categories are discovered from the data
        #[arg(short, long)]
        taxonomy: Option<String>,
        /// Number of categories to discover (default: 15)
        #[arg(short, long, default_value_t = 15)]
        clusters: usize,
        /// Max items to sample for discovery
        #[arg(long, default_value_t = 500)]
        sample: usize,
        /// Minimum BM25 score threshold
        #[arg(long, default_value_t = 0.5)]
        threshold: f64,
        /// Random seed
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// Extract top TF-IDF tags from content
    Tags {
        /// JSON field containing text
        #[arg(short, long, default_value = "content")]
        field: String,
        /// Number of tags per item
        #[arg(short = 'n', long, default_value_t = 5)]
        count: usize,
    },
    /// Find duplicates using SimHash + LSH + URL normalization
    Dedup {
        /// JSON field containing text
        #[arg(short, long, default_value = "content")]
        field: String,
        /// JSON field containing URL
        #[arg(long, default_value = "url")]
        url_field: String,
        /// Strategy: url, fuzzy, combined
        #[arg(short, long, default_value = "combined")]
        strategy: String,
        /// SimHash hamming distance threshold
        #[arg(long, default_value_t = 3)]
        threshold: u32,
    },
    /// Compute string similarity between two strings
    Similarity {
        /// First string
        a: String,
        /// Second string
        b: String,
        /// Metric: levenshtein, jaro-winkler, cosine
        #[arg(short, long, default_value = "levenshtein")]
        metric: String,
        /// Compute all metrics at once
        #[arg(short, long)]
        all: bool,
    },
    /// Normalize a URL for deduplication
    #[command(name = "normalize-url")]
    NormalizeUrl {
        /// URL to normalize
        url: String,
    },
    /// Auto-generate a taxonomy from content using hierarchical clustering
    Generate {
        /// JSON field containing text
        #[arg(short, long, default_value = "content")]
        field: String,
        /// Number of clusters / taxonomy depth
        #[arg(short = 'k', long, default_value_t = 10)]
        depth: usize,
        /// Linkage method: ward, complete, average, single
        #[arg(short, long, default_value = "ward")]
        linkage: String,
        /// Number of top terms per cluster label
        #[arg(long, default_value_t = 5)]
        top_terms: usize,
    },
    /// Discover topics using NMF (Non-negative Matrix Factorization)
    Topics {
        /// JSON field containing text
        #[arg(short, long, default_value = "content")]
        field: String,
        /// Number of topics to discover
        #[arg(short = 'k', long, default_value_t = 5)]
        topics: usize,
        /// Number of top terms per topic
        #[arg(short = 'n', long, default_value_t = 10)]
        terms: usize,
        /// NMF iterations
        #[arg(long, default_value_t = 200)]
        iterations: usize,
        /// Max vocabulary size
        #[arg(long, default_value_t = 5000)]
        vocab: usize,
    },
    /// Generate output paths and structure from classified items
    Organize {
        /// Output format: folders, flat, nested
        #[arg(long, default_value = "folders")]
        format: String,
        /// Base output directory path
        #[arg(short, long, default_value = "./organized")]
        output_dir: String,
        /// Field containing category
        #[arg(long, default_value = "_category")]
        category_field: String,
        /// Field to use for filename
        #[arg(long, default_value = "id")]
        name_field: String,
    },
}

fn main() {
    let cli = Cli::parse();

    // ── MCP server mode ─────────────────────────────────────────────────
    #[cfg(feature = "mcp")]
    if cli.mcp {
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async {
            if let Err(e) = nu_plugin_topology::mcp::serve_stdio().await {
                eprintln!("MCP server error: {e}");
                std::process::exit(1);
            }
        });
        return;
    }

    // ── LSP server mode ─────────────────────────────────────────────────
    #[cfg(feature = "lsp")]
    if cli.lsp {
        let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
        rt.block_on(async {
            if let Err(e) = nu_plugin_topology::lsp::serve_stdio().await {
                eprintln!("LSP server error: {e}");
                std::process::exit(1);
            }
        });
        return;
    }

    // ── Normal subcommand dispatch ──────────────────────────────────────
    let command = cli.command.unwrap_or_else(|| {
        eprintln!("No subcommand provided. Run `topology --help` for usage.");
        std::process::exit(1);
    });

    match command {
        Commands::Fingerprint { field, weighted } => {
            let rows = read_stdin_json();
            print_json(&ops::op_fingerprint(&rows, &field, weighted));
        }
        Commands::Sample {
            size,
            strategy,
            field,
            seed,
        } => {
            let rows = read_stdin_json();
            match ops::op_sample(&rows, size, &strategy, field.as_deref(), seed) {
                Ok(result) => print_json(&result),
                Err(e) => die(&e),
            }
        }
        Commands::Analyze { field } => {
            let rows = read_stdin_json();
            print_json(&ops::op_analyze(&rows, field.as_deref()));
        }
        Commands::Classify {
            field,
            taxonomy: tax,
            clusters,
            sample,
            threshold,
            seed,
        } => {
            let rows = read_stdin_json();
            match ops::op_classify_from_file(&rows, &field, tax.as_deref(), clusters, sample, threshold, seed) {
                Ok(result) => print_json(&result),
                Err(e) => die(&e),
            }
        }
        Commands::Tags { field, count } => {
            let rows = read_stdin_json();
            print_json(&ops::op_tags(&rows, &field, count));
        }
        Commands::Dedup {
            field,
            url_field,
            strategy,
            threshold,
        } => {
            let rows = read_stdin_json();
            print_json(&ops::op_dedup(&rows, &field, &url_field, &strategy, threshold));
        }
        Commands::Similarity { a, b, metric, all } => {
            match ops::op_similarity(&a, &b, &metric, all) {
                Ok(result) => print_json(&result),
                Err(e) => die(&e),
            }
        }
        Commands::NormalizeUrl { url } => {
            match ops::op_normalize_url(&url) {
                Ok(result) => print_json(&result),
                Err(e) => die(&e),
            }
        }
        Commands::Generate {
            field,
            depth,
            linkage,
            top_terms,
        } => {
            let rows = read_stdin_json();
            match ops::op_generate(&rows, &field, depth, &linkage, top_terms) {
                Ok(result) => print_json(&result),
                Err(e) => die(&e),
            }
        }
        Commands::Topics {
            field,
            topics,
            terms,
            iterations,
            vocab,
        } => {
            let rows = read_stdin_json();
            match ops::op_topics(&rows, &field, topics, terms, iterations, vocab) {
                Ok(result) => print_json(&result),
                Err(e) => die(&e),
            }
        }
        Commands::Organize {
            format,
            output_dir,
            category_field,
            name_field,
        } => {
            let rows = read_stdin_json();
            print_json(&ops::op_organize(&rows, &format, &output_dir, &category_field, &name_field));
        }
    }
}

fn read_stdin_json() -> Vec<Value> {
    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .expect("failed to read stdin");
    let parsed: Value = serde_json::from_str(&buf).expect("invalid JSON on stdin");
    match parsed {
        Value::Array(arr) => arr,
        single => vec![single],
    }
}

fn print_json(value: &Value) {
    println!("{}", serde_json::to_string_pretty(value).unwrap());
}

fn die(msg: &str) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}
