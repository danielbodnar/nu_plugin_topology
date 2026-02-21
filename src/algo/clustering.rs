use ordered_float::OrderedFloat;
use std::collections::HashMap;

/// Linkage method for HAC.
#[derive(Debug, Clone, Copy)]
pub enum Linkage {
    /// Min distance between any pair of points in two clusters.
    Single,
    /// Max distance between any pair of points in two clusters.
    Complete,
    /// Average distance between all pairs of points.
    Average,
    /// Ward's method: minimizes total within-cluster variance.
    Ward,
}

impl Linkage {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "single" => Some(Self::Single),
            "complete" => Some(Self::Complete),
            "average" => Some(Self::Average),
            "ward" => Some(Self::Ward),
            _ => None,
        }
    }
}

/// Result of HAC: a dendrogram represented as merge steps.
#[derive(Debug, Clone)]
pub struct Dendrogram {
    pub merges: Vec<Merge>,
    pub n: usize,
}

/// A single merge step in the dendrogram.
#[derive(Debug, Clone)]
pub struct Merge {
    pub cluster_a: usize,
    pub cluster_b: usize,
    pub distance: f64,
    pub size: usize,
}

/// Perform Hierarchical Agglomerative Clustering on a distance matrix.
///
/// `distances` is a flat upper-triangular distance matrix of size n*(n-1)/2.
/// Index for pair (i,j) where i < j: i*n - i*(i+1)/2 + j - i - 1
pub fn hac(distances: &[f64], n: usize, linkage: Linkage) -> Dendrogram {
    // Work with a mutable distance matrix (condensed form → full for easier updates)
    let mut dist = vec![vec![0.0f64; n]; n];
    for i in 0..n {
        for j in (i + 1)..n {
            let idx = condensed_index(i, j, n);
            dist[i][j] = distances[idx];
            dist[j][i] = distances[idx];
        }
    }

    let mut active: Vec<bool> = vec![true; n];
    let mut sizes: Vec<usize> = vec![1; n];
    let mut merges: Vec<Merge> = Vec::with_capacity(n - 1);
    // Map from original indices to new cluster IDs (>= n means merged cluster)
    let mut cluster_id: Vec<usize> = (0..n).collect();
    let mut next_id = n;

    for _ in 0..(n - 1) {
        // Find closest pair among active clusters
        let mut best_i = 0;
        let mut best_j = 0;
        let mut best_dist = f64::INFINITY;

        for i in 0..n {
            if !active[i] {
                continue;
            }
            for j in (i + 1)..n {
                if !active[j] {
                    continue;
                }
                if dist[i][j] < best_dist {
                    best_dist = dist[i][j];
                    best_i = i;
                    best_j = j;
                }
            }
        }

        let new_size = sizes[best_i] + sizes[best_j];
        merges.push(Merge {
            cluster_a: cluster_id[best_i],
            cluster_b: cluster_id[best_j],
            distance: best_dist,
            size: new_size,
        });

        // Update distances: merge best_j into best_i
        for k in 0..n {
            if !active[k] || k == best_i || k == best_j {
                continue;
            }
            let new_dist = match linkage {
                Linkage::Single => dist[best_i][k].min(dist[best_j][k]),
                Linkage::Complete => dist[best_i][k].max(dist[best_j][k]),
                Linkage::Average => {
                    let ni = sizes[best_i] as f64;
                    let nj = sizes[best_j] as f64;
                    (ni * dist[best_i][k] + nj * dist[best_j][k]) / (ni + nj)
                }
                Linkage::Ward => {
                    let ni = sizes[best_i] as f64;
                    let nj = sizes[best_j] as f64;
                    let nk = sizes[k] as f64;
                    let total = ni + nj + nk;
                    ((ni + nk) * dist[best_i][k] + (nj + nk) * dist[best_j][k]
                        - nk * best_dist)
                        / total
                }
            };
            dist[best_i][k] = new_dist;
            dist[k][best_i] = new_dist;
        }

        active[best_j] = false;
        sizes[best_i] = new_size;
        cluster_id[best_i] = next_id;
        next_id += 1;
    }

    Dendrogram { merges, n }
}

/// Cut the dendrogram at a given number of clusters.
/// Returns a vector mapping each original item to a cluster label 0..k-1.
pub fn cut_tree(dendrogram: &Dendrogram, k: usize) -> Vec<usize> {
    let n = dendrogram.n;
    if k >= n {
        return (0..n).collect();
    }

    // Apply first (n - k) merges, then the remaining items form k clusters
    let num_merges = n.saturating_sub(k);
    let mut parent: HashMap<usize, usize> = HashMap::new();

    for merge in dendrogram.merges.iter().take(num_merges) {
        let new_id = parent.len() + n;
        parent.insert(merge.cluster_a, new_id);
        parent.insert(merge.cluster_b, new_id);
    }

    // Find root for each original item
    let find_root = |mut id: usize| -> usize {
        while let Some(&p) = parent.get(&id) {
            id = p;
        }
        id
    };

    let roots: Vec<usize> = (0..n).map(find_root).collect();

    // Map unique roots to sequential labels
    let mut label_map: HashMap<usize, usize> = HashMap::new();
    let mut next_label = 0;
    roots
        .iter()
        .map(|&r| {
            *label_map.entry(r).or_insert_with(|| {
                let l = next_label;
                next_label += 1;
                l
            })
        })
        .collect()
}

/// Compute cosine distance matrix (condensed form) from TF-IDF vectors.
pub fn cosine_distance_matrix(vectors: &[HashMap<String, f64>]) -> Vec<f64> {
    let n = vectors.len();
    let mut distances = vec![0.0; n * (n - 1) / 2];

    // Precompute norms
    let norms: Vec<f64> = vectors
        .iter()
        .map(|v| v.values().map(|x| x * x).sum::<f64>().sqrt())
        .collect();

    for i in 0..n {
        for j in (i + 1)..n {
            let dot: f64 = vectors[i]
                .iter()
                .filter_map(|(k, vi)| vectors[j].get(k).map(|vj| vi * vj))
                .sum();
            let sim = if norms[i] > 0.0 && norms[j] > 0.0 {
                dot / (norms[i] * norms[j])
            } else {
                0.0
            };
            let dist = 1.0 - sim;
            distances[condensed_index(i, j, n)] = dist;
        }
    }

    distances
}

/// Index into a condensed distance matrix for pair (i, j) where i < j.
fn condensed_index(i: usize, j: usize, n: usize) -> usize {
    debug_assert!(i < j);
    i * n - i * (i + 1) / 2 + j - i - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    fn simple_distances() -> (Vec<f64>, usize) {
        // 4 points: (0,1)=1.0, (0,2)=4.0, (0,3)=5.0, (1,2)=2.0, (1,3)=6.0, (2,3)=3.0
        let distances = vec![1.0, 4.0, 5.0, 2.0, 6.0, 3.0];
        (distances, 4)
    }

    #[test]
    fn hac_single_linkage() {
        let (d, n) = simple_distances();
        let dend = hac(&d, n, Linkage::Single);
        assert_eq!(dend.merges.len(), 3);
        // First merge should be the closest pair (distance 1.0)
        assert!((dend.merges[0].distance - 1.0).abs() < 1e-10);
    }

    #[test]
    fn cut_tree_gives_correct_k() {
        let (d, n) = simple_distances();
        let dend = hac(&d, n, Linkage::Complete);
        let labels = cut_tree(&dend, 2);
        assert_eq!(labels.len(), 4);
        let unique: std::collections::HashSet<usize> = labels.iter().copied().collect();
        assert_eq!(unique.len(), 2);
    }

    #[test]
    fn cut_tree_all_separate() {
        let (d, n) = simple_distances();
        let dend = hac(&d, n, Linkage::Single);
        let labels = cut_tree(&dend, 4);
        assert_eq!(labels, vec![0, 1, 2, 3]);
    }

    #[test]
    fn cosine_distance_identical_vectors() {
        let v1: HashMap<String, f64> = [("a".into(), 1.0), ("b".into(), 2.0)].into();
        let v2 = v1.clone();
        let distances = cosine_distance_matrix(&[v1, v2]);
        assert!(distances[0].abs() < 1e-10); // identical = distance 0
    }

    #[test]
    fn cosine_distance_orthogonal_vectors() {
        let v1: HashMap<String, f64> = [("a".into(), 1.0)].into();
        let v2: HashMap<String, f64> = [("b".into(), 1.0)].into();
        let distances = cosine_distance_matrix(&[v1, v2]);
        assert!((distances[0] - 1.0).abs() < 1e-10); // orthogonal = distance 1
    }
}
