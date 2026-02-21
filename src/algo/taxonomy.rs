use serde::{Deserialize, Serialize};

/// A taxonomy category discovered from data or loaded from user file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Category {
    pub name: String,
    pub keywords: Vec<String>,
    #[serde(default)]
    pub children: Vec<Category>,
}

/// A full taxonomy tree — always discovered from data or user-provided.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Taxonomy {
    pub name: String,
    pub version: String,
    pub categories: Vec<Category>,
}

impl Taxonomy {
    /// Flatten into a list of (path, keywords) for BM25 matching.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn taxonomy_roundtrip() {
        let tax = Taxonomy {
            name: "test".into(),
            version: "1.0".into(),
            categories: vec![
                Category {
                    name: "Alpha".into(),
                    keywords: vec!["foo".into(), "bar".into()],
                    children: vec![],
                },
                Category {
                    name: "Beta".into(),
                    keywords: vec!["baz".into()],
                    children: vec![Category {
                        name: "Gamma".into(),
                        keywords: vec!["qux".into()],
                        children: vec![],
                    }],
                },
            ],
        };
        let json = serde_json::to_string(&tax).unwrap();
        let parsed = parse_taxonomy(&json).unwrap();
        assert_eq!(parsed.categories.len(), 2);
        let flat = parsed.flatten();
        assert_eq!(flat.len(), 3); // Alpha, Beta, Beta > Gamma
        assert!(flat.iter().any(|(p, _)| p == "Beta > Gamma"));
    }
}
