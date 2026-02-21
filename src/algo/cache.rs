//! Cache types, content hashing, and argument hashing for topology artifacts.
//!
//! This module is always compiled (no feature gate). Only the storage backend
//! (`storage.rs`) requires the `cache` feature with rusqlite.

use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use super::simhash;
use super::tokenizer;

/// Artifact types that can be cached in the topology cache database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ArtifactKind {
    Corpus,
    Dendrogram,
    Taxonomy,
    Fingerprints,
}

impl ArtifactKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Corpus => "corpus",
            Self::Dendrogram => "dendrogram",
            Self::Taxonomy => "taxonomy",
            Self::Fingerprints => "fingerprints",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "corpus" => Some(Self::Corpus),
            "dendrogram" => Some(Self::Dendrogram),
            "taxonomy" => Some(Self::Taxonomy),
            "fingerprints" => Some(Self::Fingerprints),
            _ => None,
        }
    }
}

/// Metadata for a cached artifact, stored alongside the payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMeta {
    /// SimHash of concatenated input texts (detects data changes).
    pub content_hash: u64,
    /// Number of input rows when the artifact was cached.
    pub row_count: usize,
    /// SipHash of serialized command arguments (detects config changes).
    pub args_hash: u64,
    /// Crate version when artifact was created (detects upgrades).
    pub version: String,
    /// Unix timestamp when artifact was created.
    pub created_at: u64,
}

impl CacheMeta {
    /// Create a new CacheMeta with the current version and timestamp.
    pub fn new(content_hash: u64, row_count: usize, args_hash: u64) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            content_hash,
            row_count,
            args_hash,
            version: env!("CARGO_PKG_VERSION").to_string(),
            created_at,
        }
    }
}

/// Compute a content hash for cache invalidation.
///
/// Tokenizes and SimHash-es the concatenation of all input texts.
/// This produces a stable 64-bit fingerprint that changes when data changes.
pub fn content_hash(texts: &[String]) -> u64 {
    if texts.is_empty() {
        return 0;
    }
    // Concatenate all texts and compute SimHash with uniform weights
    let all_tokens: Vec<String> = texts
        .iter()
        .flat_map(|t| tokenizer::tokenize(t))
        .collect();
    simhash::simhash_uniform(&all_tokens)
}

/// Hash command arguments for cache keying.
///
/// Serializes the argument struct to JSON, then SipHash-es the bytes.
/// Any change to command parameters (clusters, linkage, threshold, etc.)
/// produces a different hash, invalidating the cache.
pub fn args_hash(args: &impl Serialize) -> u64 {
    let json = serde_json::to_string(args).unwrap_or_default();
    let mut hasher = SipHasher13::new();
    json.hash(&mut hasher);
    hasher.finish()
}

/// Check if a CacheMeta is valid against current inputs.
///
/// A cached artifact is valid only if all three signals match:
/// - Content hash (same data)
/// - Args hash (same parameters)
/// - Version (same crate version)
pub fn is_valid(meta: &CacheMeta, content_hash: u64, args_hash: u64) -> bool {
    meta.content_hash == content_hash
        && meta.args_hash == args_hash
        && meta.version == env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_kind_roundtrip() {
        for kind in [
            ArtifactKind::Corpus,
            ArtifactKind::Dendrogram,
            ArtifactKind::Taxonomy,
            ArtifactKind::Fingerprints,
        ] {
            let s = kind.as_str();
            assert_eq!(ArtifactKind::from_str(s), Some(kind));
        }
    }

    #[test]
    fn artifact_kind_unknown() {
        assert_eq!(ArtifactKind::from_str("unknown"), None);
    }

    #[test]
    fn content_hash_deterministic() {
        let texts = vec!["hello world".into(), "foo bar".into()];
        let h1 = content_hash(&texts);
        let h2 = content_hash(&texts);
        assert_eq!(h1, h2);
    }

    #[test]
    fn content_hash_changes_with_data() {
        let t1 = vec!["rust programming".into()];
        let t2 = vec!["cooking recipes".into()];
        let h1 = content_hash(&t1);
        let h2 = content_hash(&t2);
        assert_ne!(h1, h2);
    }

    #[test]
    fn content_hash_empty() {
        assert_eq!(content_hash(&[]), 0);
    }

    #[test]
    fn args_hash_deterministic() {
        #[derive(Serialize)]
        struct Args {
            clusters: usize,
            linkage: String,
        }
        let a = Args { clusters: 15, linkage: "ward".into() };
        let h1 = args_hash(&a);
        let h2 = args_hash(&a);
        assert_eq!(h1, h2);
    }

    #[test]
    fn args_hash_changes_with_params() {
        #[derive(Serialize)]
        struct Args {
            clusters: usize,
        }
        let h1 = args_hash(&Args { clusters: 15 });
        let h2 = args_hash(&Args { clusters: 20 });
        assert_ne!(h1, h2);
    }

    #[test]
    fn cache_meta_new_has_current_version() {
        let meta = CacheMeta::new(42, 100, 99);
        assert_eq!(meta.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(meta.content_hash, 42);
        assert_eq!(meta.row_count, 100);
        assert_eq!(meta.args_hash, 99);
        assert!(meta.created_at > 0);
    }

    #[test]
    fn is_valid_matching() {
        let meta = CacheMeta::new(42, 100, 99);
        assert!(is_valid(&meta, 42, 99));
    }

    #[test]
    fn is_valid_wrong_content_hash() {
        let meta = CacheMeta::new(42, 100, 99);
        assert!(!is_valid(&meta, 43, 99));
    }

    #[test]
    fn is_valid_wrong_args_hash() {
        let meta = CacheMeta::new(42, 100, 99);
        assert!(!is_valid(&meta, 42, 100));
    }

    #[test]
    fn is_valid_wrong_version() {
        let mut meta = CacheMeta::new(42, 100, 99);
        meta.version = "0.0.0".into();
        assert!(!is_valid(&meta, 42, 99));
    }

    #[test]
    fn cache_meta_serde_roundtrip() {
        let meta = CacheMeta::new(12345, 500, 67890);
        let json = serde_json::to_string(&meta).unwrap();
        let parsed: CacheMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.content_hash, meta.content_hash);
        assert_eq!(parsed.row_count, meta.row_count);
        assert_eq!(parsed.args_hash, meta.args_hash);
        assert_eq!(parsed.version, meta.version);
        assert_eq!(parsed.created_at, meta.created_at);
    }
}
