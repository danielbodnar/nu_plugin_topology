use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Embedded default taxonomy — compiled from `taxonomies/default.json`.
/// Users override by placing a file at `$XDG_DATA_HOME/topology/taxonomy.json`
/// or `$TOPOLOGY_TAXONOMY` env var, or passing `--taxonomy <path>`.
const EMBEDDED_DEFAULT: &str = include_str!("../../taxonomies/default.json");

/// A taxonomy category with keywords for BM25 matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Category {
    pub name: String,
    pub keywords: Vec<String>,
    #[serde(default)]
    pub children: Vec<Category>,
}

/// A full taxonomy tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Taxonomy {
    pub name: String,
    pub version: String,
    pub categories: Vec<Category>,
}

impl Taxonomy {
    /// Flatten into a list of (path, keywords) for matching.
    /// Path is "Parent > Child > Grandchild" style.
    pub fn flatten(&self) -> Vec<(String, Vec<String>)> {
        let mut result = Vec::new();
        for cat in &self.categories {
            flatten_recursive(cat, "", &mut result);
        }
        result
    }

    /// Get category names as a flat list.
    pub fn category_names(&self) -> Vec<String> {
        self.categories.iter().map(|c| c.name.clone()).collect()
    }
}

fn flatten_recursive(cat: &Category, prefix: &str, out: &mut Vec<(String, Vec<String>)>) {
    let path = if prefix.is_empty() {
        cat.name.clone()
    } else {
        format!("{prefix} > {}", cat.name)
    };

    out.push((path.clone(), cat.keywords.clone()));

    for child in &cat.children {
        flatten_recursive(child, &path, out);
    }
}

/// Load the default taxonomy using this resolution order:
///
/// 1. `$TOPOLOGY_TAXONOMY` env var (path to JSON file)
/// 2. `$XDG_DATA_HOME/topology/taxonomy.json` (user override)
/// 3. `~/.local/share/topology/taxonomy.json` (fallback XDG path)
/// 4. Embedded compile-time default from `taxonomies/default.json`
///
/// Any resolution step that fails silently falls through to the next.
pub fn default_taxonomy() -> Taxonomy {
    // 1. Env var override
    if let Ok(path) = std::env::var("TOPOLOGY_TAXONOMY") {
        if let Ok(json) = std::fs::read_to_string(&path) {
            if let Ok(tax) = parse_taxonomy(&json) {
                return tax;
            }
        }
    }

    // 2. XDG_DATA_HOME
    if let Some(path) = xdg_taxonomy_path() {
        if path.exists() {
            if let Ok(json) = std::fs::read_to_string(&path) {
                if let Ok(tax) = parse_taxonomy(&json) {
                    return tax;
                }
            }
        }
    }

    // 3. Embedded fallback (always succeeds — it's compiled in)
    parse_taxonomy(EMBEDDED_DEFAULT).expect("embedded default taxonomy is invalid JSON")
}

/// Parse a taxonomy from a JSON string.
pub fn parse_taxonomy(json: &str) -> Result<Taxonomy, String> {
    serde_json::from_str(json).map_err(|e| format!("Failed to parse taxonomy: {e}"))
}

/// Load taxonomy from a file path.
pub fn load_taxonomy(path: &str) -> Result<Taxonomy, String> {
    let json =
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read '{path}': {e}"))?;
    parse_taxonomy(&json)
}

/// Return the XDG data path for topology taxonomy.
fn xdg_taxonomy_path() -> Option<PathBuf> {
    let data_home = std::env::var("XDG_DATA_HOME")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| PathBuf::from(h).join(".local/share"))
        })?;
    Some(data_home.join("topology/taxonomy.json"))
}

/// Return the embedded default taxonomy as a JSON string.
/// Useful for exporting/seeding user-customizable files.
pub fn embedded_default_json() -> &'static str {
    EMBEDDED_DEFAULT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_taxonomy_has_17_categories() {
        let t = default_taxonomy();
        assert_eq!(t.categories.len(), 17);
    }

    #[test]
    fn flatten_produces_paths() {
        let t = default_taxonomy();
        let flat = t.flatten();
        assert_eq!(flat.len(), 17);
        assert!(flat.iter().any(|(path, _)| path == "AI & Machine Learning"));
    }

    #[test]
    fn taxonomy_roundtrip() {
        let t = default_taxonomy();
        let json = serde_json::to_string(&t).unwrap();
        let parsed = parse_taxonomy(&json).unwrap();
        assert_eq!(parsed.categories.len(), 17);
    }

    #[test]
    fn each_category_has_keywords() {
        let t = default_taxonomy();
        for cat in &t.categories {
            assert!(
                !cat.keywords.is_empty(),
                "Category '{}' has no keywords",
                cat.name
            );
        }
    }

    #[test]
    fn embedded_json_is_valid() {
        let json = embedded_default_json();
        let tax: Taxonomy = serde_json::from_str(json).unwrap();
        assert!(!tax.categories.is_empty());
    }

    #[test]
    fn category_names_list() {
        let t = default_taxonomy();
        let names = t.category_names();
        assert_eq!(names.len(), 17);
        assert!(names.contains(&"Web Development".to_string()));
    }
}
