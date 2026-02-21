use std::collections::HashMap;

/// Non-negative Matrix Factorization for topic modeling.
///
/// Given a term-document matrix V (n_docs × n_terms), decompose into:
///   V ≈ W × H
/// where W (n_docs × k) represents document-topic weights
/// and H (k × n_terms) represents topic-term weights.
///
/// Uses multiplicative update rules (Lee & Seung, 2001).
pub struct NmfResult {
    /// Document-topic matrix (n_docs × k). Each row sums roughly to describe topic mixture.
    pub doc_topics: Vec<Vec<f64>>,
    /// Topic-term matrix (k × n_terms). Each row's top entries are that topic's keywords.
    pub topic_terms: Vec<Vec<f64>>,
    /// Vocabulary mapping index → term.
    pub vocabulary: Vec<String>,
    /// Number of topics.
    pub k: usize,
}

impl NmfResult {
    /// Get top N terms for topic t.
    pub fn top_terms(&self, topic: usize, n: usize) -> Vec<(String, f64)> {
        if topic >= self.k {
            return vec![];
        }
        let row = &self.topic_terms[topic];
        let mut indexed: Vec<(usize, f64)> = row.iter().copied().enumerate().collect();
        indexed.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        indexed
            .into_iter()
            .take(n)
            .map(|(i, w)| (self.vocabulary[i].clone(), w))
            .collect()
    }

    /// Get dominant topic for each document.
    pub fn dominant_topics(&self) -> Vec<usize> {
        self.doc_topics
            .iter()
            .map(|row| {
                row.iter()
                    .enumerate()
                    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|(i, _)| i)
                    .unwrap_or(0)
            })
            .collect()
    }
}

/// Run NMF on TF-IDF vectors.
///
/// * `tfidf_vectors` - One HashMap per document, term → weight.
/// * `k` - Number of topics.
/// * `max_iter` - Maximum iterations (default: 200).
/// * `vocab_limit` - Maximum vocabulary size (top terms by doc frequency).
pub fn nmf(
    tfidf_vectors: &[HashMap<String, f64>],
    k: usize,
    max_iter: usize,
    vocab_limit: usize,
) -> NmfResult {
    let n_docs = tfidf_vectors.len();

    // Build vocabulary (sorted by doc frequency, limited)
    let mut doc_freq: HashMap<&str, usize> = HashMap::new();
    for vec in tfidf_vectors {
        for term in vec.keys() {
            *doc_freq.entry(term.as_str()).or_insert(0) += 1;
        }
    }
    let mut vocab: Vec<(String, usize)> = doc_freq
        .into_iter()
        .map(|(t, c)| (t.to_string(), c))
        .collect();
    vocab.sort_by(|a, b| b.1.cmp(&a.1));
    vocab.truncate(vocab_limit);
    let vocabulary: Vec<String> = vocab.into_iter().map(|(t, _)| t).collect();
    let n_terms = vocabulary.len();

    let term_idx: HashMap<&str, usize> = vocabulary
        .iter()
        .enumerate()
        .map(|(i, t)| (t.as_str(), i))
        .collect();

    // Build V matrix (n_docs × n_terms)
    let mut v = vec![vec![0.0f64; n_terms]; n_docs];
    for (d, vec) in tfidf_vectors.iter().enumerate() {
        for (term, &weight) in vec {
            if let Some(&idx) = term_idx.get(term.as_str()) {
                v[d][idx] = weight;
            }
        }
    }

    if n_docs == 0 || n_terms == 0 || k == 0 {
        return NmfResult {
            doc_topics: vec![vec![0.0; k]; n_docs],
            topic_terms: vec![vec![0.0; n_terms]; k],
            vocabulary,
            k,
        };
    }

    // Initialize W and H with small positive values (deterministic)
    let mut w = vec![vec![0.0f64; k]; n_docs];
    let mut h = vec![vec![0.0f64; n_terms]; k];

    for i in 0..n_docs {
        for j in 0..k {
            w[i][j] = 0.1 + 0.01 * ((i * k + j) % 100) as f64 / 100.0;
        }
    }
    for i in 0..k {
        for j in 0..n_terms {
            h[i][j] = 0.1 + 0.01 * ((i * n_terms + j) % 100) as f64 / 100.0;
        }
    }

    let eps = 1e-10;

    // Multiplicative update rules
    for _ in 0..max_iter {
        // Update H: H = H * (W^T V) / (W^T W H)
        let wt_v = mat_mul_transpose_a(&w, &v, n_docs, k, n_terms);
        let wtw = mat_mul_transpose_a(&w, &w, n_docs, k, k);
        let wtw_h = mat_mul(&wtw, &h, k, k, n_terms);

        for i in 0..k {
            for j in 0..n_terms {
                h[i][j] *= wt_v[i][j] / (wtw_h[i][j] + eps);
            }
        }

        // Update W: W = W * (V H^T) / (W H H^T)
        let v_ht = mat_mul_transpose_b(&v, &h, n_docs, n_terms, k);
        let wh = mat_mul(&w, &h, n_docs, k, n_terms);
        let wh_ht = mat_mul_transpose_b(&wh, &h, n_docs, n_terms, k);

        for i in 0..n_docs {
            for j in 0..k {
                w[i][j] *= v_ht[i][j] / (wh_ht[i][j] + eps);
            }
        }
    }

    NmfResult {
        doc_topics: w,
        topic_terms: h,
        vocabulary,
        k,
    }
}

// A^T × B where A is (m × n), result is (n × p)
fn mat_mul_transpose_a(a: &[Vec<f64>], b: &[Vec<f64>], m: usize, n: usize, p: usize) -> Vec<Vec<f64>> {
    let mut result = vec![vec![0.0; p]; n];
    for i in 0..n {
        for j in 0..p {
            let mut sum = 0.0;
            for k in 0..m {
                sum += a[k][i] * b[k][j];
            }
            result[i][j] = sum;
        }
    }
    result
}

// A × B where A is (m × n), B is (n × p)
fn mat_mul(a: &[Vec<f64>], b: &[Vec<f64>], m: usize, n: usize, p: usize) -> Vec<Vec<f64>> {
    let mut result = vec![vec![0.0; p]; m];
    for i in 0..m {
        for j in 0..p {
            let mut sum = 0.0;
            for k in 0..n {
                sum += a[i][k] * b[k][j];
            }
            result[i][j] = sum;
        }
    }
    result
}

// A × B^T where B is (p × n), result is (m × p)
fn mat_mul_transpose_b(a: &[Vec<f64>], b: &[Vec<f64>], m: usize, _n: usize, p: usize) -> Vec<Vec<f64>> {
    let mut result = vec![vec![0.0; p]; m];
    for i in 0..m {
        for j in 0..p {
            let mut sum = 0.0;
            for k in 0..a[i].len().min(b[j].len()) {
                sum += a[i][k] * b[j][k];
            }
            result[i][j] = sum;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nmf_basic() {
        let docs: Vec<HashMap<String, f64>> = vec![
            [("rust".into(), 3.0), ("systems".into(), 2.0), ("fast".into(), 1.0)].into(),
            [("rust".into(), 2.0), ("memory".into(), 3.0), ("safe".into(), 1.0)].into(),
            [("web".into(), 3.0), ("javascript".into(), 2.0), ("html".into(), 1.0)].into(),
            [("web".into(), 2.0), ("css".into(), 3.0), ("design".into(), 1.0)].into(),
        ];
        let result = nmf(&docs, 2, 100, 1000);
        assert_eq!(result.k, 2);
        assert_eq!(result.doc_topics.len(), 4);
        assert_eq!(result.topic_terms.len(), 2);

        // The two Rust docs should share a dominant topic, as should the two web docs
        let topics = result.dominant_topics();
        assert_eq!(topics[0], topics[1], "Rust docs should share topic");
        assert_eq!(topics[2], topics[3], "Web docs should share topic");
        assert_ne!(topics[0], topics[2], "Rust and web should differ");
    }

    #[test]
    fn nmf_top_terms() {
        let docs: Vec<HashMap<String, f64>> = vec![
            [("alpha".into(), 5.0), ("beta".into(), 1.0)].into(),
            [("gamma".into(), 5.0), ("beta".into(), 1.0)].into(),
        ];
        let result = nmf(&docs, 2, 50, 100);
        let top = result.top_terms(0, 2);
        assert_eq!(top.len(), 2);
        // Top term should have highest weight
        assert!(top[0].1 >= top[1].1);
    }

    #[test]
    fn nmf_empty() {
        let docs: Vec<HashMap<String, f64>> = vec![];
        let result = nmf(&docs, 3, 10, 100);
        assert_eq!(result.doc_topics.len(), 0);
    }
}
