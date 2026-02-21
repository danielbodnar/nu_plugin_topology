use std::collections::HashMap;

use crate::algo::{clustering, sampling, taxonomy, tfidf, tokenizer};

/// Configuration for automatic taxonomy discovery.
pub struct DiscoverConfig {
    /// Number of clusters to discover.
    pub k: usize,
    /// Max sample size for clustering (HAC is O(n^2), so we cap it).
    pub sample_size: usize,
    /// Number of top TF-IDF terms per cluster label.
    pub label_terms: usize,
    /// Number of keywords per cluster (used for BM25 classification).
    pub keywords_per_cluster: usize,
    /// HAC linkage method.
    pub linkage: clustering::Linkage,
    /// Random seed for sampling.
    pub seed: u64,
}

impl Default for DiscoverConfig {
    fn default() -> Self {
        Self {
            k: 15,
            sample_size: 500,
            label_terms: 3,
            keywords_per_cluster: 20,
            linkage: clustering::Linkage::Ward,
            seed: 42,
        }
    }
}

/// Discover a taxonomy from raw text content.
///
/// Pipeline:
/// 1. Tokenize all items
/// 2. Build TF-IDF corpus
/// 3. Sample if dataset > sample_size (HAC is O(n^2))
/// 4. Compute cosine distance matrix on sample
/// 5. Run HAC clustering
/// 6. Cut dendrogram at k clusters
/// 7. Label each cluster by its top TF-IDF terms
/// 8. Return taxonomy with keyword lists per cluster
pub fn discover_taxonomy(texts: &[String], config: &DiscoverConfig) -> taxonomy::Taxonomy {
    let n = texts.len();
    if n == 0 {
        return empty_taxonomy();
    }

    // Tokenize everything (needed for corpus IDF)
    let all_tokens: Vec<Vec<String>> = texts.iter().map(|t| tokenizer::tokenize(t)).collect();

    // Build full corpus for IDF computation
    let mut corpus = tfidf::Corpus::new();
    for tokens in &all_tokens {
        corpus.add_document(tokens);
    }

    // Sample if too large for HAC
    let (_sample_indices, sample_tokens) = if n > config.sample_size {
        let indices =
            sampling::random_sample(n, config.sample_size, config.seed);
        let tokens: Vec<Vec<String>> = indices.iter().map(|&i| all_tokens[i].clone()).collect();
        (indices, tokens)
    } else {
        ((0..n).collect(), all_tokens.clone())
    };

    let sample_n = sample_tokens.len();
    if sample_n < 2 {
        return single_cluster_taxonomy(&corpus, 0);
    }

    // Build TF-IDF vectors for sample
    // We need a sample-local corpus for proper TF-IDF vectors
    let mut sample_corpus = tfidf::Corpus::new();
    for tokens in &sample_tokens {
        sample_corpus.add_document(tokens);
    }

    let vectors: Vec<HashMap<String, f64>> = (0..sample_n)
        .map(|i| sample_corpus.tfidf_vector(i))
        .collect();

    // Compute distance matrix and run HAC
    let distances = clustering::cosine_distance_matrix(&vectors);
    let k = config.k.min(sample_n);
    let dendrogram = clustering::hac(&distances, sample_n, config.linkage);
    let labels = clustering::cut_tree(&dendrogram, k);

    // Group sample items by cluster
    let actual_k = labels.iter().max().map(|m| m + 1).unwrap_or(0);
    let mut categories: Vec<taxonomy::Category> = Vec::with_capacity(actual_k);

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

        // Merge TF-IDF vectors for this cluster
        let mut merged: HashMap<String, f64> = HashMap::new();
        for &i in &member_indices {
            for (term, weight) in &vectors[i] {
                *merged.entry(term.clone()).or_insert(0.0) += weight;
            }
        }

        // Sort by weight, take top keywords
        let mut sorted_terms: Vec<(String, f64)> = merged.into_iter().collect();
        sorted_terms
            .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let keywords: Vec<String> = sorted_terms
            .iter()
            .take(config.keywords_per_cluster)
            .map(|(t, _)| t.clone())
            .collect();

        let label = sorted_terms
            .iter()
            .take(config.label_terms)
            .map(|(t, _)| capitalize(t))
            .collect::<Vec<String>>()
            .join(", ");

        categories.push(taxonomy::Category {
            name: label,
            keywords,
            children: vec![],
        });
    }

    taxonomy::Taxonomy {
        name: "discovered".into(),
        version: "auto".into(),
        categories,
    }
}

/// Classify items against a discovered (or provided) taxonomy using BM25.
///
/// Returns a Vec of (category_name, hierarchy_path, confidence_score) per item.
pub fn classify_against_taxonomy(
    texts: &[String],
    taxonomy: &taxonomy::Taxonomy,
    threshold: f64,
) -> Vec<(String, String, f64)> {
    let flat = taxonomy.flatten();

    // Build BM25 corpus from taxonomy keywords
    let mut corpus = tfidf::Corpus::new();
    for (_, keywords) in &flat {
        corpus.add_document(keywords);
    }

    texts
        .iter()
        .map(|text| {
            let tokens = tokenizer::tokenize(text);

            let mut best_score = 0.0f64;
            let mut best_category = String::new();
            let mut best_path = String::new();

            for (doc_idx, (path, _)) in flat.iter().enumerate() {
                let score = corpus.bm25_score(doc_idx, &tokens);
                if score > best_score {
                    best_score = score;
                    best_category = path.split(" > ").last().unwrap_or(path).to_string();
                    best_path = path.clone();
                }
            }

            if best_score >= threshold {
                (best_category, best_path, best_score)
            } else {
                ("Uncategorized".into(), "Uncategorized".into(), 0.0)
            }
        })
        .collect()
}

fn empty_taxonomy() -> taxonomy::Taxonomy {
    taxonomy::Taxonomy {
        name: "discovered".into(),
        version: "auto".into(),
        categories: vec![],
    }
}

fn single_cluster_taxonomy(corpus: &tfidf::Corpus, doc_idx: usize) -> taxonomy::Taxonomy {
    let top = corpus.top_terms(doc_idx, 20);
    let keywords: Vec<String> = top.iter().map(|(t, _)| t.clone()).collect();
    let label = top
        .iter()
        .take(3)
        .map(|(t, _)| capitalize(t))
        .collect::<Vec<String>>()
        .join(", ");

    taxonomy::Taxonomy {
        name: "discovered".into(),
        version: "auto".into(),
        categories: vec![taxonomy::Category {
            name: label,
            keywords,
            children: vec![],
        }],
    }
}

fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_separates_distinct_topics() {
        // 3 clearly distinct groups
        let texts: Vec<String> = vec![
            "rust systems memory safety ownership borrow checker compiler".into(),
            "rust performance zero cost abstractions concurrent safe compile".into(),
            "cooking recipe pasta italian sauce ingredients kitchen chef".into(),
            "cooking baking bread flour dessert restaurant dinner menu".into(),
            "astronomy telescope star galaxy nebula planet cosmos universe".into(),
            "astronomy observatory comet asteroid space sky solar orbit".into(),
        ];

        let config = DiscoverConfig {
            k: 3,
            sample_size: 100,
            label_terms: 3,
            keywords_per_cluster: 15,
            ..Default::default()
        };

        let tax = discover_taxonomy(&texts, &config);
        assert!(
            tax.categories.len() >= 2,
            "Should discover at least 2 clusters, got {}",
            tax.categories.len()
        );

        // Each category should have keywords extracted from the data
        for cat in &tax.categories {
            assert!(!cat.keywords.is_empty(), "Category '{}' has no keywords", cat.name);
            assert!(!cat.name.is_empty(), "Category has empty name");
        }

        // The taxonomy should be serializable (required for --taxonomy roundtrip)
        let json = serde_json::to_string(&tax).unwrap();
        let reparsed = taxonomy::parse_taxonomy(&json).unwrap();
        assert_eq!(reparsed.categories.len(), tax.categories.len());
    }

    #[test]
    fn discover_empty_input() {
        let texts: Vec<String> = vec![];
        let config = DiscoverConfig::default();
        let tax = discover_taxonomy(&texts, &config);
        assert!(tax.categories.is_empty());
    }

    #[test]
    fn discover_single_item() {
        let texts = vec!["rust programming language".into()];
        let config = DiscoverConfig::default();
        let tax = discover_taxonomy(&texts, &config);
        assert_eq!(tax.categories.len(), 1);
    }

    #[test]
    fn discover_produces_categories() {
        let texts: Vec<String> = (0..30)
            .map(|i| {
                // 3 distinct groups of 10
                match i % 3 {
                    0 => format!("alpha bravo charlie delta echo {i}"),
                    1 => format!("foxtrot golf hotel india juliet {i}"),
                    _ => format!("kilo lima mike november oscar {i}"),
                }
            })
            .collect();
        let config = DiscoverConfig {
            k: 3,
            sample_size: 100,
            ..Default::default()
        };
        let tax = discover_taxonomy(&texts, &config);
        assert!(
            !tax.categories.is_empty(),
            "Should produce at least 1 category"
        );
        // Each category should have keywords
        for cat in &tax.categories {
            assert!(!cat.keywords.is_empty());
        }
    }

    #[test]
    fn classify_with_threshold() {
        let texts = vec![
            "rust systems programming memory safety".into(),
            "completely unrelated gibberish xyzzy plugh".into(),
        ];
        let tax = taxonomy::Taxonomy {
            name: "test".into(),
            version: "1.0".into(),
            categories: vec![taxonomy::Category {
                name: "Rust".into(),
                keywords: vec!["rust".into(), "systems".into(), "memory".into(), "safety".into()],
                children: vec![],
            }],
        };
        // With only 1 category and threshold=0, the matching text should get it
        let results = classify_against_taxonomy(&texts, &tax, 0.0);
        assert_eq!(results[0].0, "Rust");
        // The matching text should score higher than the gibberish
        assert!(results[0].2 > results[1].2);
    }
}
