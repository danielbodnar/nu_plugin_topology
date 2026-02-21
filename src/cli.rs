use clap::{Parser, Subcommand};
use nu_plugin_topology::algo::{
    sampling, simhash, string_distance, tfidf, tokenizer, url_normalize,
};
use rayon::prelude::*;
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, Read};

#[derive(Parser)]
#[command(name = "topology", version, about = "Content topology, classification, and deduplication engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
    Analyze,

    /// Compute string similarity between two strings
    Similarity {
        /// First string
        a: String,

        /// Second string
        b: String,

        /// Metric: levenshtein, jaro-winkler, cosine
        #[arg(short, long, default_value = "levenshtein")]
        metric: String,
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

    match cli.command {
        Commands::Fingerprint { field, weighted } => cmd_fingerprint(&field, weighted),
        Commands::Sample {
            size,
            strategy,
            field,
            seed,
        } => cmd_sample(size, &strategy, field.as_deref(), seed),
        Commands::Analyze => cmd_analyze(),
        Commands::Similarity { a, b, metric } => cmd_similarity(&a, &b, &metric),
        Commands::NormalizeUrl { url } => cmd_normalize_url(&url),
    }
}

fn read_stdin_json() -> Vec<Value> {
    let mut buf = String::new();
    io::stdin().read_to_string(&mut buf).expect("failed to read stdin");
    let parsed: Value = serde_json::from_str(&buf).expect("invalid JSON on stdin");
    match parsed {
        Value::Array(arr) => arr,
        single => vec![single],
    }
}

fn cmd_fingerprint(field: &str, weighted: bool) {
    let rows = read_stdin_json();

    let texts: Vec<String> = rows
        .iter()
        .map(|row| {
            row.get(field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        })
        .collect();

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

    let strat = sampling::Strategy::from_str(strategy)
        .unwrap_or_else(|| {
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
                let key = row
                    .get(field_name)
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                strata.entry(key).or_default().push(i);
            }
            sampling::stratified_sample(&strata, size, seed)
        }
    };

    let sampled: Vec<&Value> = indices.iter().filter(|&&i| i < total).map(|&i| &rows[i]).collect();
    println!("{}", serde_json::to_string_pretty(&sampled).unwrap());
}

fn cmd_analyze() {
    let rows = read_stdin_json();
    let total = rows.len();

    if total == 0 {
        println!(r#"{{"total_rows":0,"columns":[],"fields":{{}}}}"#);
        return;
    }

    // Discover columns from first row
    let columns: Vec<String> = match &rows[0] {
        Value::Object(map) => map.keys().cloned().collect(),
        _ => vec!["value".into()],
    };

    let mut fields = serde_json::Map::new();

    for col in &columns {
        let mut null_count: usize = 0;
        let mut unique_vals: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut type_counts: HashMap<String, usize> = HashMap::new();
        let mut total_len: usize = 0;
        let mut min_len: usize = usize::MAX;
        let mut max_len: usize = 0;
        let mut values: Vec<String> = Vec::new();

        for row in &rows {
            match row.get(col) {
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
                    let len = s.len();
                    total_len += len;
                    min_len = min_len.min(len);
                    max_len = max_len.max(len);
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

        let report = serde_json::json!({
            "non_null": non_null,
            "null_count": null_count,
            "cardinality": unique_vals.len(),
            "uniqueness": if non_null > 0 { unique_vals.len() as f64 / non_null as f64 } else { 0.0 },
            "avg_length": if non_null > 0 { total_len as f64 / non_null as f64 } else { 0.0 },
            "min_length": min_len,
            "max_length": max_len,
            "types": type_counts.iter().map(|(t, c)| serde_json::json!({"type": t, "count": c})).collect::<Vec<_>>(),
            "top_values": freq_vec.iter().map(|(v, c)| serde_json::json!({"value": v, "count": c})).collect::<Vec<_>>(),
        });

        fields.insert(col.clone(), report);
    }

    let output = serde_json::json!({
        "total_rows": total,
        "columns": columns,
        "num_columns": columns.len(),
        "fields": fields,
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_similarity(a: &str, b: &str, metric_name: &str) {
    let metric = string_distance::Metric::from_str(metric_name).unwrap_or_else(|| {
        eprintln!(
            "Unknown metric '{metric_name}'. Use: {}",
            string_distance::Metric::all_names().join(", ")
        );
        std::process::exit(1);
    });

    let score = string_distance::similarity(a, b, metric);

    let output = serde_json::json!({
        "a": a,
        "b": b,
        "metric": metric_name,
        "similarity": score,
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn cmd_normalize_url(url: &str) {
    match url_normalize::normalize(url) {
        Some(normalized) => {
            let canonical = url_normalize::canonical_key(url).unwrap_or_default();
            let output = serde_json::json!({
                "original": url,
                "normalized": normalized,
                "canonical_key": canonical,
            });
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
        }
        None => {
            eprintln!("Could not parse URL: {url}");
            std::process::exit(1);
        }
    }
}
