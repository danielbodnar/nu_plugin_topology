use siphasher::sip::SipHasher13;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

/// Locality-Sensitive Hashing index for fast near-neighbor search.
///
/// Splits MinHash signatures into `bands` bands of `rows` rows each.
/// Items sharing a band hash are candidate pairs.
///
/// Default: bands=16, rows=8 (for 128-perm MinHash).
/// Threshold ≈ (1/bands)^(1/rows) ≈ 0.54 Jaccard at 50% recall.
pub struct LshIndex {
    bands: usize,
    rows: usize,
    /// band_idx → band_hash → set of item IDs
    buckets: Vec<HashMap<u64, Vec<usize>>>,
}

impl LshIndex {
    /// Create a new LSH index.
    ///
    /// * `bands` - Number of bands to split signature into.
    /// * `rows` - Number of rows per band.
    ///
    /// `bands * rows` should equal the MinHash signature length.
    pub fn new(bands: usize, rows: usize) -> Self {
        Self {
            bands,
            rows,
            buckets: (0..bands).map(|_| HashMap::new()).collect(),
        }
    }

    /// Default configuration for 128-perm MinHash.
    /// bands=16, rows=8 → threshold ≈ 0.54
    pub fn default_128() -> Self {
        Self::new(16, 8)
    }

    /// Insert an item's MinHash signature into the index.
    pub fn insert(&mut self, item_id: usize, signature: &[u64]) {
        assert!(
            signature.len() >= self.bands * self.rows,
            "Signature length {} < bands*rows {}",
            signature.len(),
            self.bands * self.rows
        );

        for band_idx in 0..self.bands {
            let start = band_idx * self.rows;
            let band_slice = &signature[start..start + self.rows];
            let band_hash = hash_band(band_idx, band_slice);
            self.buckets[band_idx]
                .entry(band_hash)
                .or_default()
                .push(item_id);
        }
    }

    /// Query for candidate near-neighbors of a signature.
    /// Returns item IDs that share at least one band hash.
    pub fn query(&self, signature: &[u64]) -> HashSet<usize> {
        let mut candidates = HashSet::new();

        for band_idx in 0..self.bands {
            let start = band_idx * self.rows;
            let band_slice = &signature[start..start + self.rows];
            let band_hash = hash_band(band_idx, band_slice);

            if let Some(items) = self.buckets[band_idx].get(&band_hash) {
                for &id in items {
                    candidates.insert(id);
                }
            }
        }

        candidates
    }

    /// Find all candidate pairs (items that share at least one band).
    /// Returns deduplicated pairs (i, j) where i < j.
    pub fn candidate_pairs(&self) -> Vec<(usize, usize)> {
        let mut pairs: HashSet<(usize, usize)> = HashSet::new();

        for bucket_map in &self.buckets {
            for items in bucket_map.values() {
                if items.len() < 2 {
                    continue;
                }
                for i in 0..items.len() {
                    for j in (i + 1)..items.len() {
                        let a = items[i].min(items[j]);
                        let b = items[i].max(items[j]);
                        pairs.insert((a, b));
                    }
                }
            }
        }

        let mut result: Vec<(usize, usize)> = pairs.into_iter().collect();
        result.sort();
        result
    }

    pub fn bands(&self) -> usize {
        self.bands
    }

    pub fn rows(&self) -> usize {
        self.rows
    }
}

fn hash_band(band_idx: usize, values: &[u64]) -> u64 {
    let mut hasher = SipHasher13::new_with_keys(band_idx as u64, 0xCAFEBABE);
    for &v in values {
        v.hash(&mut hasher);
    }
    hasher.finish()
}

/// SimHash-based LSH index using bit banding.
///
/// Splits 64-bit fingerprints into bands of bits.
/// Items matching on any band are candidates.
pub struct SimHashLshIndex {
    bands: usize,
    bits_per_band: usize,
    /// band_idx → band_value → set of item IDs
    buckets: Vec<HashMap<u64, Vec<usize>>>,
}

impl SimHashLshIndex {
    /// Create a SimHash LSH index.
    /// Default: bands=16, bits_per_band=4 (64 bits / 16 bands).
    pub fn new(bands: usize, bits_per_band: usize) -> Self {
        Self {
            bands,
            bits_per_band,
            buckets: (0..bands).map(|_| HashMap::new()).collect(),
        }
    }

    /// Default: 16 bands × 4 bits = 64 bits.
    pub fn default_64() -> Self {
        Self::new(16, 4)
    }

    /// Insert a SimHash fingerprint.
    pub fn insert(&mut self, item_id: usize, fingerprint: u64) {
        for band_idx in 0..self.bands {
            let band_val = extract_band(fingerprint, band_idx, self.bits_per_band);
            self.buckets[band_idx]
                .entry(band_val)
                .or_default()
                .push(item_id);
        }
    }

    /// Query for candidate near-neighbors.
    pub fn query(&self, fingerprint: u64) -> HashSet<usize> {
        let mut candidates = HashSet::new();
        for band_idx in 0..self.bands {
            let band_val = extract_band(fingerprint, band_idx, self.bits_per_band);
            if let Some(items) = self.buckets[band_idx].get(&band_val) {
                for &id in items {
                    candidates.insert(id);
                }
            }
        }
        candidates
    }

    /// Find all candidate pairs.
    pub fn candidate_pairs(&self) -> Vec<(usize, usize)> {
        let mut pairs: HashSet<(usize, usize)> = HashSet::new();
        for bucket_map in &self.buckets {
            for items in bucket_map.values() {
                if items.len() < 2 {
                    continue;
                }
                for i in 0..items.len() {
                    for j in (i + 1)..items.len() {
                        let a = items[i].min(items[j]);
                        let b = items[i].max(items[j]);
                        pairs.insert((a, b));
                    }
                }
            }
        }
        let mut result: Vec<(usize, usize)> = pairs.into_iter().collect();
        result.sort();
        result
    }
}

fn extract_band(fingerprint: u64, band_idx: usize, bits_per_band: usize) -> u64 {
    let shift = band_idx * bits_per_band;
    let mask = (1u64 << bits_per_band) - 1;
    (fingerprint >> shift) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsh_identical_signatures_are_candidates() {
        let mut idx = LshIndex::new(4, 2);
        let sig = vec![1u64, 2, 3, 4, 5, 6, 7, 8];
        idx.insert(0, &sig);
        idx.insert(1, &sig);
        let pairs = idx.candidate_pairs();
        assert!(pairs.contains(&(0, 1)));
    }

    #[test]
    fn lsh_different_signatures_not_candidates() {
        let mut idx = LshIndex::new(4, 2);
        let sig_a = vec![1u64, 2, 3, 4, 5, 6, 7, 8];
        let sig_b = vec![100, 200, 300, 400, 500, 600, 700, 800];
        idx.insert(0, &sig_a);
        idx.insert(1, &sig_b);
        let pairs = idx.candidate_pairs();
        assert!(pairs.is_empty() || !pairs.contains(&(0, 1)));
    }

    #[test]
    fn lsh_query_returns_self() {
        let mut idx = LshIndex::new(4, 2);
        let sig = vec![1u64, 2, 3, 4, 5, 6, 7, 8];
        idx.insert(0, &sig);
        let results = idx.query(&sig);
        assert!(results.contains(&0));
    }

    #[test]
    fn simhash_lsh_identical() {
        let mut idx = SimHashLshIndex::default_64();
        idx.insert(0, 0xDEADBEEF12345678);
        idx.insert(1, 0xDEADBEEF12345678);
        let pairs = idx.candidate_pairs();
        assert!(pairs.contains(&(0, 1)));
    }

    #[test]
    fn simhash_lsh_near_duplicate() {
        let mut idx = SimHashLshIndex::default_64();
        let fp1: u64 = 0xDEADBEEF12345678;
        let fp2: u64 = fp1 ^ 0x3; // Hamming distance 2 — flip 2 low bits
        idx.insert(0, fp1);
        idx.insert(1, fp2);
        let pairs = idx.candidate_pairs();
        // With 16 bands of 4 bits, 2-bit difference should still share most bands
        assert!(!pairs.is_empty(), "Near-duplicates should be candidates");
    }
}
