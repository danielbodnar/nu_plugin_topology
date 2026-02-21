use strsim::{jaro_winkler, normalized_levenshtein};

/// All supported distance metrics.
#[derive(Debug, Clone, Copy)]
pub enum Metric {
    Levenshtein,
    JaroWinkler,
    Cosine,
}

/// Compute string similarity (0.0 = no match, 1.0 = identical) using the specified metric.
pub fn similarity(a: &str, b: &str, metric: Metric) -> f64 {
    match metric {
        Metric::Levenshtein => normalized_levenshtein(a, b),
        Metric::JaroWinkler => jaro_winkler(a, b),
        Metric::Cosine => cosine_similarity(a, b),
    }
}

/// Cosine similarity on character bigrams.
fn cosine_similarity(a: &str, b: &str) -> f64 {
    let bigrams_a = char_bigrams(a);
    let bigrams_b = char_bigrams(b);

    if bigrams_a.is_empty() || bigrams_b.is_empty() {
        return if a == b { 1.0 } else { 0.0 };
    }

    let mut dot = 0.0;
    let mut norm_a = 0.0;
    let mut norm_b = 0.0;

    let all_keys: std::collections::HashSet<&String> =
        bigrams_a.keys().chain(bigrams_b.keys()).collect();

    for key in all_keys {
        let va = *bigrams_a.get(key).unwrap_or(&0) as f64;
        let vb = *bigrams_b.get(key).unwrap_or(&0) as f64;
        dot += va * vb;
        norm_a += va * va;
        norm_b += vb * vb;
    }

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a.sqrt() * norm_b.sqrt())
}

fn char_bigrams(s: &str) -> std::collections::HashMap<String, u32> {
    let chars: Vec<char> = s.to_lowercase().chars().collect();
    let mut map = std::collections::HashMap::new();
    for pair in chars.windows(2) {
        let bigram: String = pair.iter().collect();
        *map.entry(bigram).or_insert(0) += 1;
    }
    map
}

impl Metric {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "levenshtein" | "lev" => Some(Self::Levenshtein),
            "jaro-winkler" | "jaro_winkler" | "jw" => Some(Self::JaroWinkler),
            "cosine" | "cos" => Some(Self::Cosine),
            _ => None,
        }
    }

    pub fn all_names() -> &'static [&'static str] {
        &["levenshtein", "jaro-winkler", "cosine"]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_strings() {
        assert_eq!(similarity("hello", "hello", Metric::Levenshtein), 1.0);
        assert_eq!(similarity("hello", "hello", Metric::JaroWinkler), 1.0);
        assert_eq!(similarity("hello", "hello", Metric::Cosine), 1.0);
    }

    #[test]
    fn completely_different() {
        let sim = similarity("abc", "xyz", Metric::Levenshtein);
        assert!(sim < 0.1);
    }

    #[test]
    fn similar_strings() {
        let sim = similarity("kitten", "sitting", Metric::Levenshtein);
        assert!(sim > 0.4 && sim < 0.8);
    }

    #[test]
    fn jaro_winkler_prefix_bonus() {
        let jw = similarity("martha", "marhta", Metric::JaroWinkler);
        assert!(jw > 0.9);
    }

    #[test]
    fn cosine_similar() {
        let sim = similarity("night", "nacht", Metric::Cosine);
        assert!(sim > 0.0);
    }

    #[test]
    fn metric_from_str() {
        assert!(Metric::from_str("levenshtein").is_some());
        assert!(Metric::from_str("jw").is_some());
        assert!(Metric::from_str("cosine").is_some());
        assert!(Metric::from_str("unknown").is_none());
    }

    #[test]
    fn metric_from_str_aliases() {
        assert!(Metric::from_str("lev").is_some());
        assert!(Metric::from_str("jaro_winkler").is_some());
        assert!(Metric::from_str("jaro-winkler").is_some());
        assert!(Metric::from_str("cos").is_some());
    }

    #[test]
    fn metric_from_str_case_insensitive() {
        assert!(Metric::from_str("LEVENSHTEIN").is_some());
        assert!(Metric::from_str("Cosine").is_some());
        assert!(Metric::from_str("JW").is_some());
    }

    #[test]
    fn empty_strings_all_metrics() {
        assert_eq!(similarity("", "", Metric::Levenshtein), 1.0);
        assert_eq!(similarity("", "", Metric::JaroWinkler), 1.0);
        assert_eq!(similarity("", "", Metric::Cosine), 1.0);
    }

    #[test]
    fn one_empty_string() {
        let sim = similarity("hello", "", Metric::Levenshtein);
        assert_eq!(sim, 0.0);
    }

    #[test]
    fn similarity_bounds() {
        // All metrics should return values in [0, 1]
        let pairs = [("abc", "xyz"), ("hello", "world"), ("a", "b"), ("test", "testing")];
        for (a, b) in &pairs {
            for metric in [Metric::Levenshtein, Metric::JaroWinkler, Metric::Cosine] {
                let s = similarity(a, b, metric);
                assert!(s >= 0.0 && s <= 1.0, "{a} vs {b} with {metric:?} = {s}");
            }
        }
    }

    #[test]
    fn similarity_symmetry() {
        let a = "kitten";
        let b = "sitting";
        for metric in [Metric::Levenshtein, Metric::JaroWinkler, Metric::Cosine] {
            let ab = similarity(a, b, metric);
            let ba = similarity(b, a, metric);
            assert!((ab - ba).abs() < 1e-10, "{metric:?} not symmetric: {ab} vs {ba}");
        }
    }

    #[test]
    fn cosine_single_char() {
        // Single char strings produce no bigrams → falls back to exact match
        let sim = similarity("a", "a", Metric::Cosine);
        assert_eq!(sim, 1.0);
        let sim2 = similarity("a", "b", Metric::Cosine);
        assert_eq!(sim2, 0.0);
    }

    #[test]
    fn all_names_contains_expected() {
        let names = Metric::all_names();
        assert!(names.contains(&"levenshtein"));
        assert!(names.contains(&"jaro-winkler"));
        assert!(names.contains(&"cosine"));
        assert_eq!(names.len(), 3);
    }
}
