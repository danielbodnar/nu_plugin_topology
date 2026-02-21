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

    #[test]
    fn empty_taxonomy() {
        let tax = Taxonomy {
            name: "empty".into(),
            version: "1.0".into(),
            categories: vec![],
        };
        let flat = tax.flatten();
        assert!(flat.is_empty());
        assert!(tax.category_names().is_empty());
    }

    #[test]
    fn single_category_no_children() {
        let tax = Taxonomy {
            name: "test".into(),
            version: "1.0".into(),
            categories: vec![Category {
                name: "Solo".into(),
                keywords: vec!["one".into()],
                children: vec![],
            }],
        };
        let flat = tax.flatten();
        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, "Solo");
        assert_eq!(flat[0].1, vec!["one".to_string()]);
    }

    #[test]
    fn deeply_nested_children() {
        let tax = Taxonomy {
            name: "deep".into(),
            version: "1.0".into(),
            categories: vec![Category {
                name: "L1".into(),
                keywords: vec![],
                children: vec![Category {
                    name: "L2".into(),
                    keywords: vec![],
                    children: vec![Category {
                        name: "L3".into(),
                        keywords: vec!["deep".into()],
                        children: vec![],
                    }],
                }],
            }],
        };
        let flat = tax.flatten();
        assert_eq!(flat.len(), 3);
        assert_eq!(flat[2].0, "L1 > L2 > L3");
    }

    #[test]
    fn category_names_returns_top_level_only() {
        let tax = Taxonomy {
            name: "test".into(),
            version: "1.0".into(),
            categories: vec![
                Category { name: "A".into(), keywords: vec![], children: vec![
                    Category { name: "A1".into(), keywords: vec![], children: vec![] },
                ]},
                Category { name: "B".into(), keywords: vec![], children: vec![] },
            ],
        };
        let names = tax.category_names();
        assert_eq!(names, vec!["A", "B"]);
    }

    #[test]
    fn parse_taxonomy_invalid_json() {
        let result = parse_taxonomy("not json");
        assert!(result.is_err());
    }

    #[test]
    fn parse_taxonomy_missing_fields() {
        let result = parse_taxonomy(r#"{"name": "test"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn load_taxonomy_nonexistent_file() {
        let result = load_taxonomy("/nonexistent/path/taxonomy.json");
        assert!(result.is_err());
    }

    #[test]
    fn taxonomy_serde_preserves_keywords() {
        let tax = Taxonomy {
            name: "test".into(),
            version: "2.0".into(),
            categories: vec![Category {
                name: "Cat".into(),
                keywords: vec!["a".into(), "b".into(), "c".into()],
                children: vec![],
            }],
        };
        let json = serde_json::to_string(&tax).unwrap();
        let parsed = parse_taxonomy(&json).unwrap();
        assert_eq!(parsed.categories[0].keywords, vec!["a", "b", "c"]);
        assert_eq!(parsed.version, "2.0");
    }
}
