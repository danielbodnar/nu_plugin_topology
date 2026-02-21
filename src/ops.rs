//! Shared operation wrappers for all interfaces (CLI, MCP, LSP, plugin).
//!
//! Each `op_*` function is a pure, synchronous wrapper around one or more
//! `algo` modules. Input and output are `serde_json::Value` — no dependency
//! on rmcp, tower-lsp, clap, or nu-plugin.

use std::collections::{HashMap, HashSet};

use rayon::prelude::*;
use serde_json::Value;

use crate::algo::{
    clustering, discover, lsh, nmf, sampling, simhash, string_distance, taxonomy, tfidf, tokenizer,
    url_normalize,
};

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Extract a text field from a JSON object, returning "" if missing.
pub fn get_text(row: &Value, field: &str) -> String {
    row.get(field)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string()
}

/// Union-find with path compression (halving).
struct UnionFind {
    parent: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        Self {
            parent: (0..n).collect(),
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]];
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra != rb {
            self.parent[rb] = ra;
        }
    }
}

// ── Operations ───────────────────────────────────────────────────────────────

pub fn op_fingerprint(rows: &[Value], field: &str, weighted: bool) -> Value {
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
        .iter()
        .cloned()
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

    Value::Array(output)
}

pub fn op_sample(
    rows: &[Value],
    size: usize,
    strategy: &str,
    field: Option<&str>,
    seed: u64,
) -> Result<Value, String> {
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

pub fn op_analyze(rows: &[Value], field: Option<&str>) -> Value {
    let total = rows.len();
    if total == 0 {
        return serde_json::json!({"total_rows": 0, "columns": [], "fields": {}});
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

        for row in rows {
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

    serde_json::json!({
        "total_rows": total,
        "columns": columns,
        "num_columns": columns.len(),
        "fields": Value::Object(fields),
    })
}

pub fn op_classify(
    rows: &[Value],
    field: &str,
    taxonomy_json: Option<&Value>,
    clusters: usize,
    sample_size: usize,
    threshold: f64,
    seed: u64,
) -> Result<Value, String> {
    if rows.is_empty() {
        return Ok(Value::Array(vec![]));
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

    let output: Vec<Value> = rows
        .iter()
        .cloned()
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

/// Classify using a taxonomy loaded from a file path (for CLI usage).
pub fn op_classify_from_file(
    rows: &[Value],
    field: &str,
    taxonomy_path: Option<&str>,
    clusters: usize,
    sample_size: usize,
    threshold: f64,
    seed: u64,
) -> Result<Value, String> {
    if rows.is_empty() {
        return Ok(Value::Array(vec![]));
    }

    let texts: Vec<String> = rows.iter().map(|r| get_text(r, field)).collect();

    let tax = match taxonomy_path {
        Some(path) => taxonomy::load_taxonomy(path)?,
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
        .iter()
        .cloned()
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

pub fn op_tags(rows: &[Value], field: &str, count: usize) -> Value {
    if rows.is_empty() {
        return Value::Array(vec![]);
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
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, mut row)| {
            let top = corpus.top_terms(i, count);
            let tags: Vec<Value> = top
                .iter()
                .map(|(t, _)| Value::String(t.clone()))
                .collect();
            if let Some(obj) = row.as_object_mut() {
                obj.insert("_tags".into(), Value::Array(tags));
            }
            row
        })
        .collect();

    Value::Array(output)
}

pub fn op_dedup(
    rows: &[Value],
    field: &str,
    url_field: &str,
    strategy: &str,
    threshold: u32,
) -> Value {
    if rows.is_empty() {
        return Value::Array(vec![]);
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
    let mut uf = UnionFind::new(n);
    for members in url_groups.values() {
        for i in 1..members.len() {
            uf.union(members[0], members[i]);
        }
    }
    for &(i, j) in &content_pairs {
        uf.union(i, j);
    }

    let mut groups: HashMap<usize, Vec<usize>> = HashMap::new();
    for i in 0..n {
        let root = uf.find(i);
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

    Value::Array(output)
}

pub fn op_similarity(
    a: &str,
    b: &str,
    metric_name: &str,
    all: bool,
) -> Result<Value, String> {
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

pub fn op_normalize_url(url: &str) -> Result<Value, String> {
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

pub fn op_generate(
    rows: &[Value],
    field: &str,
    depth: usize,
    linkage_str: &str,
    top_n: usize,
) -> Result<Value, String> {
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

pub fn op_topics(
    rows: &[Value],
    field: &str,
    k: usize,
    top_n: usize,
    max_iter: usize,
    vocab_limit: usize,
) -> Result<Value, String> {
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

pub fn op_organize(
    rows: &[Value],
    format: &str,
    output_dir: &str,
    category_field: &str,
    name_field: &str,
) -> Value {
    if rows.is_empty() {
        return Value::Array(vec![]);
    }

    let output: Vec<Value> = rows
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

    Value::Array(output)
}
