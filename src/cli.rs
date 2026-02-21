use clap::{Parser, Subcommand};
use nu_plugin_topology::algo::{
    clustering, discover, lsh, sampling, simhash, string_distance, taxonomy, tfidf, tokenizer,
    url_normalize,
};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
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
        Commands::Fingerprint { field, weighted } => cmd_fingerprint(&field, weighted),
        Commands::Sample {
            size,
            strategy,
            field,
            seed,
        } => cmd_sample(size, &strategy, field.as_deref(), seed),
        Commands::Analyze { field } => cmd_analyze(field.as_deref()),
        Commands::Classify {
            field,
            taxonomy: tax,
            clusters,
            sample,
            threshold,
            seed,
        } => cmd_classify(&field, tax.as_deref(), clusters, sample, threshold, seed),
        Commands::Tags { field, count } => cmd_tags(&field, count),
        Commands::Dedup {
            field,
            url_field,
            strategy,
            threshold,
        } => cmd_dedup(&field, &url_field, &strategy, threshold),
        Commands::Similarity { a, b, metric, all } => cmd_similarity(&a, &b, &metric, all),
        Commands::NormalizeUrl { url } => cmd_normalize_url(&url),
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

fn get_text(row: &Value, field: &str) -> String {
    row.get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

fn cmd_fingerprint(field: &str, weighted: bool) {
    let rows = read_stdin_json();
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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_sample(size: usize, strategy: &str, field: Option<&str>, seed: u64) {
    let rows = read_stdin_json();
    let total = rows.len();
    if total == 0 {
        println!("[]");
        return;
    }

    let strat = sampling::Strategy::from_str(strategy).unwrap_or_else(|| {
        eprintln!("Unknown strategy '{strategy}'. Use: random, stratified, systematic, reservoir");
        std::process::exit(1);
    });

    let indices = match strat {
        sampling::Strategy::Random => sampling::random_sample(total, size, seed),
        sampling::Strategy::Systematic => sampling::systematic_sample(total, size, seed),
        sampling::Strategy::Reservoir => sampling::reservoir_sample(total, size, seed),
        sampling::Strategy::Stratified => {
            let field_name = field.unwrap_or_else(|| {
                eprintln!("Stratified sampling requires --field");
                std::process::exit(1);
            });
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

    let sampled: Vec<&Value> = indices
        .iter()
        .filter(|&&i| i < total)
        .map(|&i| &rows[i])
        .collect();
    println!("{}", serde_json::to_string_pretty(&sampled).unwrap());
}

fn cmd_analyze(field: Option<&str>) {
    let rows = read_stdin_json();
    let total = rows.len();
    if total == 0 {
        println!(r#"{{"total_rows":0,"columns":[],"fields":{{}}}}"#);
        return;
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

        fields.insert(col.clone(), serde_json::json!({
            "non_null": non_null, "null_count": null_count,
            "cardinality": unique_vals.len(),
            "uniqueness": if non_null > 0 { unique_vals.len() as f64 / non_null as f64 } else { 0.0 },
            "avg_length": if non_null > 0 { total_len as f64 / non_null as f64 } else { 0.0 },
            "min_length": min_len, "max_length": max_len,
            "types": type_counts.iter().map(|(t, c)| serde_json::json!({"type": t, "count": c})).collect::<Vec<_>>(),
            "top_values": freq_vec.iter().map(|(v, c)| serde_json::json!({"value": v, "count": c})).collect::<Vec<_>>(),
        }));
    }

    let output = serde_json::json!({ "total_rows": total, "columns": columns, "num_columns": columns.len(), "fields": fields });
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_classify(
    field: &str,
    taxonomy_path: Option<&str>,
    clusters: usize,
    sample_size: usize,
    threshold: f64,
    seed: u64,
) {
    let rows = read_stdin_json();
    if rows.is_empty() {
        println!("[]");
        return;
    }

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();

    let tax = match taxonomy_path {
        Some(path) => taxonomy::load_taxonomy(path).unwrap_or_else(|e| {
            eprintln!("{e}");
            std::process::exit(1);
        }),
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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_tags(field: &str, count: usize) {
    let rows = read_stdin_json();
    if rows.is_empty() {
        println!("[]");
        return;
    }

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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_dedup(field: &str, url_field: &str, strategy: &str, threshold: u32) {
    let rows = read_stdin_json();
    if rows.is_empty() {
        println!("[]");
        return;
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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_similarity(a: &str, b: &str, metric_name: &str, all: bool) {
    if all {
        let mut results = serde_json::Map::new();
        results.insert("a".into(), Value::String(a.into()));
        results.insert("b".into(), Value::String(b.into()));
        for name in string_distance::Metric::all_names() {
            let metric = string_distance::Metric::from_str(name).unwrap();
            let score = string_distance::similarity(a, b, metric);
            results.insert((*name).into(), serde_json::json!(score));
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&Value::Object(results)).unwrap()
        );
    } else {
        let metric = string_distance::Metric::from_str(metric_name).unwrap_or_else(|| {
            eprintln!(
                "Unknown metric '{metric_name}'. Use: {}",
                string_distance::Metric::all_names().join(", ")
            );
            std::process::exit(1);
        });
        let score = string_distance::similarity(a, b, metric);
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "a": a, "b": b, "metric": metric_name, "similarity": score
            }))
            .unwrap()
        );
    }
}

fn cmd_normalize_url(url: &str) {
    match url_normalize::normalize(url) {
        Some(normalized) => {
            let canonical = url_normalize::canonical_key(url).unwrap_or_default();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "original": url, "normalized": normalized, "canonical_key": canonical
                }))
                .unwrap()
            );
        }
        None => {
            eprintln!("Could not parse URL: {url}");
            std::process::exit(1);
        }
    }
}
