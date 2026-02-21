use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};

const DEFAULT_NUM_PERM: usize = 128;

/// MinHash signature: a vector of minimum hash values across permutations.
///
/// Two signatures can be compared with Jaccard estimation:
/// J(A,B) ≈ (number of matching positions) / num_perm
pub struct MinHasher {
    num_perm: usize,
    /// Seeds for each permutation (hash function family).
    seeds: Vec<(u64, u64)>,
}

impl MinHasher {
    pub fn new(num_perm: usize) -> Self {
        // Generate deterministic seeds from sequential values
        let seeds: Vec<(u64, u64)> = (0..num_perm)
            .map(|i| {
                let a = (i as u64).wrapping_mul(6364136223846793005).wrapping_add(1);
                let b = (i as u64).wrapping_mul(1442695040888963407).wrapping_add(7);
                (a, b)
            })
            .collect();

        Self { num_perm, seeds }
    }

    pub fn with_default_perm() -> Self {
        Self::new(DEFAULT_NUM_PERM)
    }

    /// Compute MinHash signature for a set of tokens.
    pub fn signature(&self, tokens: &[String]) -> Vec<u64> {
        let mut sig = vec![u64::MAX; self.num_perm];

        for token in tokens {
            for (i, &(key0, key1)) in self.seeds.iter().enumerate() {
                let mut hasher = SipHasher13::new_with_keys(key0, key1);
                token.hash(&mut hasher);
                let h = hasher.finish();
                if h < sig[i] {
                    sig[i] = h;
                }
            }
        }

        sig
    }

    /// Estimate Jaccard similarity between two signatures.
    pub fn jaccard(&self, sig_a: &[u64], sig_b: &[u64]) -> f64 {
        assert_eq!(sig_a.len(), sig_b.len());
        let matches = sig_a
            .iter()
            .zip(sig_b.iter())
            .filter(|(a, b)| a == b)
            .count();
        matches as f64 / sig_a.len() as f64
    }

    pub fn num_perm(&self) -> usize {
        self.num_perm
    }
}

impl Default for MinHasher {
    fn default() -> Self {
        Self::with_default_perm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_sets_jaccard_one() {
        let mh = MinHasher::new(128);
        let tokens: Vec<String> = vec!["a".into(), "b".into(), "c".into()];
        let sig = mh.signature(&tokens);
        let j = mh.jaccard(&sig, &sig);
        assert!((j - 1.0).abs() < 1e-10);
    }

    #[test]
    fn overlapping_sets_high_jaccard() {
        let mh = MinHasher::new(256);
        let a: Vec<String> = (0..100).map(|i| format!("token_{i}")).collect();
        let mut b = a.clone();
        // Change 10% of tokens
        for i in 0..10 {
            b[i] = format!("different_{i}");
        }
        let sig_a = mh.signature(&a);
        let sig_b = mh.signature(&b);
        let j = mh.jaccard(&sig_a, &sig_b);
        // 90 shared out of 110 unique ≈ 0.818 Jaccard
        assert!(j > 0.7, "Expected high Jaccard, got {j}");
    }

    #[test]
    fn disjoint_sets_low_jaccard() {
        let mh = MinHasher::new(128);
        let a: Vec<String> = vec!["a".into(), "b".into(), "c".into()];
        let b: Vec<String> = vec!["x".into(), "y".into(), "z".into()];
        let sig_a = mh.signature(&a);
        let sig_b = mh.signature(&b);
        let j = mh.jaccard(&sig_a, &sig_b);
        assert!(j < 0.2, "Expected low Jaccard, got {j}");
    }

    #[test]
    fn deterministic() {
        let mh = MinHasher::new(64);
        let tokens: Vec<String> = vec!["hello".into(), "world".into()];
        let s1 = mh.signature(&tokens);
        let s2 = mh.signature(&tokens);
        assert_eq!(s1, s2);
    }
}
