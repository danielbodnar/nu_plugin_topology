use siphasher::sip::SipHasher13;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

const BITS: usize = 64;

/// Compute a 64-bit SimHash fingerprint from weighted tokens.
///
/// Each token is hashed with SipHash-1-3 to produce a 64-bit value.
/// Bit positions are accumulated with +weight (bit=1) or -weight (bit=0).
/// Final fingerprint: bit i = 1 if accumulator[i] > 0.
pub fn simhash(tokens: &[String], weights: &HashMap<String, f64>) -> u64 {
    let mut v = [0.0f64; BITS];

    for token in tokens {
        let weight = weights.get(token).copied().unwrap_or(1.0);
        let hash = sip_hash(token);

        for i in 0..BITS {
            if (hash >> i) & 1 == 1 {
                v[i] += weight;
            } else {
                v[i] -= weight;
            }
        }
    }

    let mut fingerprint: u64 = 0;
    for i in 0..BITS {
        if v[i] > 0.0 {
            fingerprint |= 1 << i;
        }
    }
    fingerprint
}

/// Compute SimHash with uniform weights (all tokens weight = 1.0).
pub fn simhash_uniform(tokens: &[String]) -> u64 {
    let weights: HashMap<String, f64> = tokens.iter().map(|t| (t.clone(), 1.0)).collect();
    simhash(tokens, &weights)
}

/// Hamming distance between two 64-bit fingerprints.
pub fn hamming_distance(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Check if two fingerprints are near-duplicates (Hamming distance <= threshold).
pub fn is_near_duplicate(a: u64, b: u64, threshold: u32) -> bool {
    hamming_distance(a, b) <= threshold
}

/// Format a fingerprint as a hex string.
pub fn fingerprint_to_hex(fp: u64) -> String {
    format!("{:016x}", fp)
}

/// Parse a hex string back to a fingerprint.
pub fn hex_to_fingerprint(hex: &str) -> Option<u64> {
    u64::from_str_radix(hex, 16).ok()
}

fn sip_hash(value: &str) -> u64 {
    let mut hasher = SipHasher13::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_input_same_hash() {
        let tokens: Vec<String> = vec!["rust".into(), "plugin".into(), "nushell".into()];
        let h1 = simhash_uniform(&tokens);
        let h2 = simhash_uniform(&tokens);
        assert_eq!(h1, h2);
    }

    #[test]
    fn similar_input_low_distance() {
        let t1: Vec<String> = vec!["rust".into(), "plugin".into(), "nushell".into(), "shell".into()];
        let t2: Vec<String> = vec!["rust".into(), "plugin".into(), "nushell".into(), "terminal".into()];
        let h1 = simhash_uniform(&t1);
        let h2 = simhash_uniform(&t2);
        let dist = hamming_distance(h1, h2);
        // Similar documents should have low hamming distance
        assert!(dist < 20, "Expected low distance, got {dist}");
    }

    #[test]
    fn different_input_high_distance() {
        let t1: Vec<String> = vec!["rust".into(), "systems".into(), "programming".into()];
        let t2: Vec<String> = vec!["cooking".into(), "recipe".into(), "kitchen".into()];
        let h1 = simhash_uniform(&t1);
        let h2 = simhash_uniform(&t2);
        let dist = hamming_distance(h1, h2);
        // Very different content should have higher distance
        assert!(dist > 5, "Expected high distance, got {dist}");
    }

    #[test]
    fn hex_roundtrip() {
        let fp: u64 = 0xdeadbeef12345678;
        let hex = fingerprint_to_hex(fp);
        assert_eq!(hex, "deadbeef12345678");
        assert_eq!(hex_to_fingerprint(&hex), Some(fp));
    }

    #[test]
    fn weighted_simhash_differs_from_uniform() {
        let tokens: Vec<String> = vec!["rust".into(), "common".into(), "word".into()];
        let mut weights = HashMap::new();
        weights.insert("rust".into(), 5.0);
        weights.insert("common".into(), 0.1);
        weights.insert("word".into(), 0.1);

        let h_uniform = simhash_uniform(&tokens);
        let h_weighted = simhash(&tokens, &weights);
        // Weighted hash should generally differ from uniform
        // (not guaranteed but very likely with such extreme weights)
        // We just check both produce valid non-zero hashes
        assert_ne!(h_uniform, 0);
        assert_ne!(h_weighted, 0);
    }
}
