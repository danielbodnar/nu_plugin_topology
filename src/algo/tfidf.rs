use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A corpus of documents for TF-IDF and BM25 scoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Corpus {
    /// document_id -> term -> count
    doc_terms: Vec<HashMap<String, u32>>,
    /// term -> number of documents containing it
    doc_freq: HashMap<String, u32>,
    /// total number of documents
    num_docs: u32,
    /// average document length (in tokens)
    avg_dl: f64,
}

impl Corpus {
    pub fn new() -> Self {
        Self {
            doc_terms: Vec::new(),
            doc_freq: HashMap::new(),
            num_docs: 0,
            avg_dl: 0.0,
        }
    }

    /// Add a document (as pre-tokenized terms) to the corpus.
    pub fn add_document(&mut self, tokens: &[String]) {
        let mut term_counts: HashMap<String, u32> = HashMap::new();
        for token in tokens {
            *term_counts.entry(token.clone()).or_insert(0) += 1;
        }
        for term in term_counts.keys() {
            *self.doc_freq.entry(term.clone()).or_insert(0) += 1;
        }
        self.doc_terms.push(term_counts);
        self.num_docs += 1;

        // Recompute average document length
        let total_len: usize = self.doc_terms.iter().map(|d| d.values().sum::<u32>() as usize).sum();
        self.avg_dl = total_len as f64 / self.num_docs as f64;
    }

    /// Compute IDF for a term: log((N - df + 0.5) / (df + 0.5) + 1)
    pub fn idf(&self, term: &str) -> f64 {
        let df = *self.doc_freq.get(term).unwrap_or(&0) as f64;
        let n = self.num_docs as f64;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    /// Compute TF-IDF vector for a document by index.
    pub fn tfidf_vector(&self, doc_idx: usize) -> HashMap<String, f64> {
        let doc = &self.doc_terms[doc_idx];
        let dl: u32 = doc.values().sum();
        let mut result = HashMap::new();
        for (term, &count) in doc {
            let tf = count as f64 / dl as f64;
            let idf = self.idf(term);
            result.insert(term.clone(), tf * idf);
        }
        result
    }

    /// BM25 score for a query (list of terms) against document at doc_idx.
    /// k1 = 1.2, b = 0.75 (standard parameters)
    pub fn bm25_score(&self, doc_idx: usize, query_terms: &[String]) -> f64 {
        self.bm25_score_params(doc_idx, query_terms, 1.2, 0.75)
    }

    /// BM25 score with custom k1 and b parameters.
    pub fn bm25_score_params(
        &self,
        doc_idx: usize,
        query_terms: &[String],
        k1: f64,
        b: f64,
    ) -> f64 {
        let doc = &self.doc_terms[doc_idx];
        let dl: f64 = doc.values().sum::<u32>() as f64;
        let mut score = 0.0;

        for term in query_terms {
            let tf = *doc.get(term).unwrap_or(&0) as f64;
            if tf == 0.0 {
                continue;
            }
            let idf = self.idf(term);
            let numerator = tf * (k1 + 1.0);
            let denominator = tf + k1 * (1.0 - b + b * dl / self.avg_dl);
            score += idf * numerator / denominator;
        }
        score
    }

    /// Extract top-N terms by TF-IDF weight for a document.
    pub fn top_terms(&self, doc_idx: usize, n: usize) -> Vec<(String, f64)> {
        let mut tfidf: Vec<(String, f64)> = self.tfidf_vector(doc_idx).into_iter().collect();
        tfidf.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        tfidf.truncate(n);
        tfidf
    }

    /// Compute TF-IDF weights for each token in a single document against this corpus.
    /// Used by SimHash to weight token contributions.
    pub fn token_weights(&self, tokens: &[String]) -> HashMap<String, f64> {
        let mut counts: HashMap<String, u32> = HashMap::new();
        for t in tokens {
            *counts.entry(t.clone()).or_insert(0) += 1;
        }
        let dl = tokens.len() as f64;
        let mut weights = HashMap::new();
        for (term, count) in counts {
            let tf = count as f64 / dl;
            let idf = self.idf(&term);
            weights.insert(term, tf * idf);
        }
        weights
    }

    pub fn num_docs(&self) -> u32 {
        self.num_docs
    }
}

impl Default for Corpus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_corpus() -> Corpus {
        let mut c = Corpus::new();
        c.add_document(&["rust".into(), "programming".into(), "language".into()]);
        c.add_document(&["rust".into(), "systems".into(), "performance".into()]);
        c.add_document(&["javascript".into(), "web".into(), "programming".into()]);
        c
    }

    #[test]
    fn idf_common_term() {
        let c = make_corpus();
        let idf_rust = c.idf("rust");
        let idf_programming = c.idf("programming");
        // "rust" appears in 2/3 docs, "programming" in 2/3 — same IDF
        assert!((idf_rust - idf_programming).abs() < 1e-10);
    }

    #[test]
    fn idf_rare_term() {
        let c = make_corpus();
        let idf_systems = c.idf("systems");
        let idf_rust = c.idf("rust");
        // "systems" appears in 1/3 docs, higher IDF than "rust" (2/3)
        assert!(idf_systems > idf_rust);
    }

    #[test]
    fn bm25_relevant_higher() {
        let c = make_corpus();
        let query = vec!["rust".into(), "systems".into()];
        let score_0 = c.bm25_score(0, &query); // has "rust"
        let score_1 = c.bm25_score(1, &query); // has "rust" + "systems"
        let score_2 = c.bm25_score(2, &query); // has neither
        assert!(score_1 > score_0);
        assert!(score_0 > score_2);
    }

    #[test]
    fn top_terms_returns_sorted() {
        let c = make_corpus();
        let top = c.top_terms(0, 2);
        assert_eq!(top.len(), 2);
        // First element should have highest weight
        assert!(top[0].1 >= top[1].1);
    }

    #[test]
    fn empty_corpus() {
        let c = Corpus::new();
        assert_eq!(c.num_docs(), 0);
        // IDF of unknown term in empty corpus should not panic
        let idf = c.idf("unknown");
        assert!(idf.is_finite());
    }

    #[test]
    fn single_document_corpus() {
        let mut c = Corpus::new();
        c.add_document(&["rust".into(), "rust".into(), "fast".into()]);
        assert_eq!(c.num_docs(), 1);
        let vec = c.tfidf_vector(0);
        assert!(vec.contains_key("rust"));
        assert!(vec.contains_key("fast"));
    }

    #[test]
    fn idf_unknown_term() {
        let c = make_corpus();
        let idf = c.idf("nonexistent");
        // df=0 → IDF = ln((3 - 0 + 0.5)/(0 + 0.5) + 1) = ln(8)
        assert!(idf > 0.0);
    }

    #[test]
    fn bm25_no_matching_terms() {
        let c = make_corpus();
        let query = vec!["completely".into(), "unrelated".into()];
        let score = c.bm25_score(0, &query);
        assert_eq!(score, 0.0);
    }

    #[test]
    fn bm25_custom_params() {
        // Need varying doc lengths so b parameter has effect
        let mut c = Corpus::new();
        c.add_document(&["rust".into(), "fast".into()]);
        c.add_document(&["rust".into(), "safe".into(), "memory".into(), "ownership".into(), "borrow".into()]);
        let query = vec!["rust".into()];
        let default_score = c.bm25_score(0, &query);
        let custom_score = c.bm25_score_params(0, &query, 2.0, 0.5);
        // Different k1/b with varying doc lengths should produce different scores
        assert!((default_score - custom_score).abs() > 1e-10);
    }

    #[test]
    fn token_weights_computation() {
        let c = make_corpus();
        let tokens = vec!["rust".into(), "systems".into()];
        let weights = c.token_weights(&tokens);
        assert!(weights.contains_key("rust"));
        assert!(weights.contains_key("systems"));
        // "systems" is rarer so should have higher weight
        assert!(weights["systems"] > weights["rust"]);
    }

    #[test]
    fn top_terms_exceeding_doc_size() {
        let c = make_corpus();
        // Doc 0 has 3 terms, ask for 100
        let top = c.top_terms(0, 100);
        assert_eq!(top.len(), 3);
    }

    #[test]
    fn corpus_serde_roundtrip() {
        let c = make_corpus();
        let json = serde_json::to_string(&c).unwrap();
        let c2: Corpus = serde_json::from_str(&json).unwrap();
        assert_eq!(c.num_docs(), c2.num_docs());
        // Verify IDF values are preserved
        assert!((c.idf("rust") - c2.idf("rust")).abs() < 1e-10);
        assert!((c.idf("systems") - c2.idf("systems")).abs() < 1e-10);
        // Verify BM25 scores match
        let query = vec!["rust".into(), "systems".into()];
        assert!((c.bm25_score(1, &query) - c2.bm25_score(1, &query)).abs() < 1e-10);
    }

    #[test]
    fn tfidf_vector_sums_positive() {
        let c = make_corpus();
        let vec = c.tfidf_vector(0);
        for &v in vec.values() {
            assert!(v >= 0.0, "TF-IDF values should be non-negative");
        }
    }
}
