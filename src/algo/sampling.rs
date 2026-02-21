use std::collections::HashMap;

/// Sampling strategy.
#[derive(Debug, Clone, Copy)]
pub enum Strategy {
    /// Simple random sampling (Fisher-Yates shuffle, take first N).
    Random,
    /// Stratified: proportional representation of each group.
    Stratified,
    /// Systematic: every k-th element.
    Systematic,
    /// Reservoir: single-pass O(n) for streams (Vitter's Algorithm R).
    Reservoir,
}

impl Strategy {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "random" => Some(Self::Random),
            "stratified" => Some(Self::Stratified),
            "systematic" => Some(Self::Systematic),
            "reservoir" => Some(Self::Reservoir),
            _ => None,
        }
    }
}

/// Random sample of `size` indices from `total` using a seeded PRNG.
pub fn random_sample(total: usize, size: usize, seed: u64) -> Vec<usize> {
    if size >= total {
        return (0..total).collect();
    }
    let mut indices: Vec<usize> = (0..total).collect();
    // Fisher-Yates shuffle (partial) using simple LCG
    let mut rng = LcgRng::new(seed);
    for i in 0..size {
        let j = i + (rng.next() as usize % (total - i));
        indices.swap(i, j);
    }
    indices.truncate(size);
    indices.sort_unstable();
    indices
}

/// Systematic sample: every k-th element starting from a random offset.
pub fn systematic_sample(total: usize, size: usize, seed: u64) -> Vec<usize> {
    if size >= total {
        return (0..total).collect();
    }
    let k = total as f64 / size as f64;
    let mut rng = LcgRng::new(seed);
    let start = (rng.next() as f64 / u64::MAX as f64) * k;
    let mut indices = Vec::with_capacity(size);
    for i in 0..size {
        let idx = (start + i as f64 * k) as usize;
        if idx < total {
            indices.push(idx);
        }
    }
    indices
}

/// Stratified sample: proportional representation per stratum.
/// `strata` maps item index to stratum key.
pub fn stratified_sample(
    strata: &HashMap<String, Vec<usize>>,
    size: usize,
    seed: u64,
) -> Vec<usize> {
    let total: usize = strata.values().map(|v| v.len()).sum();
    if size >= total {
        let mut all: Vec<usize> = strata.values().flat_map(|v| v.iter().copied()).collect();
        all.sort_unstable();
        return all;
    }

    let mut result = Vec::with_capacity(size);
    let mut remaining = size;
    let mut rng_seed = seed;

    // Sort strata for deterministic ordering
    let mut sorted_strata: Vec<(&String, &Vec<usize>)> = strata.iter().collect();
    sorted_strata.sort_by_key(|(k, _)| *k);

    for (i, (_, indices)) in sorted_strata.iter().enumerate() {
        let proportion = indices.len() as f64 / total as f64;
        let stratum_size = if i == sorted_strata.len() - 1 {
            remaining // Last stratum gets all remaining slots
        } else {
            let s = (proportion * size as f64).round() as usize;
            s.min(remaining).min(indices.len())
        };

        let sampled = random_sample(indices.len(), stratum_size, rng_seed);
        for &idx in &sampled {
            result.push(indices[idx]);
        }
        remaining = remaining.saturating_sub(stratum_size);
        rng_seed = rng_seed.wrapping_add(1);
    }

    result.sort_unstable();
    result
}

/// Reservoir sampling (Algorithm R): select `size` items from a stream of unknown length.
/// Returns indices of selected items.
pub fn reservoir_sample(total: usize, size: usize, seed: u64) -> Vec<usize> {
    if size >= total {
        return (0..total).collect();
    }
    let mut reservoir: Vec<usize> = (0..size).collect();
    let mut rng = LcgRng::new(seed);

    for i in size..total {
        let j = rng.next() as usize % (i + 1);
        if j < size {
            reservoir[j] = i;
        }
    }

    reservoir.sort_unstable();
    reservoir
}

/// Simple Linear Congruential Generator for deterministic sampling.
struct LcgRng {
    state: u64,
}

impl LcgRng {
    fn new(seed: u64) -> Self {
        Self { state: seed.wrapping_add(1) }
    }

    fn next(&mut self) -> u64 {
        // LCG constants from Numerical Recipes
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_sample_correct_size() {
        let s = random_sample(100, 10, 42);
        assert_eq!(s.len(), 10);
        assert!(s.iter().all(|&i| i < 100));
    }

    #[test]
    fn random_sample_deterministic() {
        let s1 = random_sample(100, 10, 42);
        let s2 = random_sample(100, 10, 42);
        assert_eq!(s1, s2);
    }

    #[test]
    fn random_sample_oversized() {
        let s = random_sample(5, 10, 42);
        assert_eq!(s, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn systematic_sample_correct_size() {
        let s = systematic_sample(100, 10, 42);
        assert!(s.len() <= 10);
        assert!(s.iter().all(|&i| i < 100));
    }

    #[test]
    fn stratified_proportional() {
        let mut strata = HashMap::new();
        strata.insert("a".into(), (0..70).collect());
        strata.insert("b".into(), (70..100).collect());
        let s = stratified_sample(&strata, 10, 42);
        // Should get roughly 7 from "a" and 3 from "b"
        let a_count = s.iter().filter(|&&i| i < 70).count();
        let b_count = s.iter().filter(|&&i| i >= 70).count();
        assert!(a_count >= 5 && a_count <= 9, "a_count={a_count}");
        assert!(b_count >= 1 && b_count <= 5, "b_count={b_count}");
    }

    #[test]
    fn reservoir_sample_correct_size() {
        let s = reservoir_sample(1000, 50, 42);
        assert_eq!(s.len(), 50);
        assert!(s.iter().all(|&i| i < 1000));
    }

    #[test]
    fn reservoir_deterministic() {
        let s1 = reservoir_sample(1000, 50, 42);
        let s2 = reservoir_sample(1000, 50, 42);
        assert_eq!(s1, s2);
    }

    #[test]
    fn random_sample_no_duplicates() {
        let s = random_sample(100, 50, 123);
        let unique: std::collections::HashSet<usize> = s.iter().copied().collect();
        assert_eq!(unique.len(), s.len(), "Sample should have no duplicates");
    }

    #[test]
    fn random_sample_different_seeds() {
        let s1 = random_sample(100, 10, 1);
        let s2 = random_sample(100, 10, 2);
        assert_ne!(s1, s2, "Different seeds should produce different samples");
    }

    #[test]
    fn systematic_sample_evenly_spaced() {
        let s = systematic_sample(100, 10, 42);
        // Elements should be roughly evenly spaced (step ~10)
        for w in s.windows(2) {
            let gap = w[1] - w[0];
            assert!(gap >= 5 && gap <= 15, "Gap should be near 10, got {gap}");
        }
    }

    #[test]
    fn systematic_sample_oversized() {
        let s = systematic_sample(5, 20, 42);
        assert_eq!(s, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn stratified_sample_oversized() {
        let mut strata = HashMap::new();
        strata.insert("a".into(), vec![0, 1, 2]);
        strata.insert("b".into(), vec![3, 4]);
        let s = stratified_sample(&strata, 100, 42);
        assert_eq!(s.len(), 5, "Oversized sample returns all items");
    }

    #[test]
    fn stratified_sample_single_stratum() {
        let mut strata = HashMap::new();
        strata.insert("only".into(), (0..20).collect());
        let s = stratified_sample(&strata, 5, 42);
        assert_eq!(s.len(), 5);
    }

    #[test]
    fn reservoir_sample_oversized() {
        let s = reservoir_sample(3, 10, 42);
        assert_eq!(s, vec![0, 1, 2]);
    }

    #[test]
    fn reservoir_sample_no_duplicates() {
        let s = reservoir_sample(1000, 100, 99);
        let unique: std::collections::HashSet<usize> = s.iter().copied().collect();
        assert_eq!(unique.len(), s.len());
    }

    #[test]
    fn strategy_from_str_all() {
        assert!(Strategy::from_str("random").is_some());
        assert!(Strategy::from_str("stratified").is_some());
        assert!(Strategy::from_str("systematic").is_some());
        assert!(Strategy::from_str("reservoir").is_some());
        assert!(Strategy::from_str("RANDOM").is_some());
        assert!(Strategy::from_str("unknown").is_none());
    }

    #[test]
    fn random_sample_sorted() {
        let s = random_sample(100, 20, 42);
        for w in s.windows(2) {
            assert!(w[0] <= w[1], "Sample should be sorted");
        }
    }
}
