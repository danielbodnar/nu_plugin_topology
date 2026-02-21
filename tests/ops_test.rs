use nu_plugin_topology::ops;
use serde_json::{json, Value};

fn sample_records() -> Vec<Value> {
    vec![
        json!({"content": "Rust is a systems programming language focused on safety", "url": "https://rust-lang.org", "id": "rust"}),
        json!({"content": "Python is a high-level general-purpose programming language", "url": "https://python.org", "id": "python"}),
        json!({"content": "JavaScript runs in the browser and on the server with Node.js", "url": "https://developer.mozilla.org/en-US/docs/Web/JavaScript", "id": "javascript"}),
        json!({"content": "TypeScript adds static typing to JavaScript for better tooling", "url": "https://www.typescriptlang.org", "id": "typescript"}),
        json!({"content": "Go is a statically typed compiled language designed at Google", "url": "https://go.dev", "id": "go"}),
    ]
}

#[test]
fn ops_fingerprint_adds_column() {
    let rows = sample_records();
    let result = ops::op_fingerprint(&rows, "content", false);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    for row in arr {
        assert!(row.get("_fingerprint").is_some());
        let fp = row["_fingerprint"].as_str().unwrap();
        assert_eq!(fp.len(), 16); // 64-bit hex = 16 chars
    }
}

#[test]
fn ops_fingerprint_weighted() {
    let rows = sample_records();
    let result = ops::op_fingerprint(&rows, "content", true);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    for row in arr {
        assert!(row.get("_fingerprint").is_some());
    }
}

#[test]
fn ops_sample_random() {
    let rows = sample_records();
    let result = ops::op_sample(&rows, 3, "random", None, 42).unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
}

#[test]
fn ops_sample_empty() {
    let result = ops::op_sample(&[], 3, "random", None, 42).unwrap();
    assert_eq!(result, json!([]));
}

#[test]
fn ops_sample_invalid_strategy() {
    let rows = sample_records();
    let result = ops::op_sample(&rows, 3, "invalid", None, 42);
    assert!(result.is_err());
}

#[test]
fn ops_analyze_returns_stats() {
    let rows = sample_records();
    let result = ops::op_analyze(&rows, None);
    assert_eq!(result["total_rows"], 5);
    assert!(result["columns"].as_array().unwrap().len() > 0);
    assert!(result["fields"].is_object());
}

#[test]
fn ops_analyze_single_field() {
    let rows = sample_records();
    let result = ops::op_analyze(&rows, Some("content"));
    assert_eq!(result["total_rows"], 5);
    let cols = result["columns"].as_array().unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0], "content");
}

#[test]
fn ops_classify_auto_discovers() {
    let rows = sample_records();
    let result = ops::op_classify(&rows, "content", None, 2, 500, 0.1, 42).unwrap();
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    for row in arr {
        assert!(row.get("_category").is_some());
        assert!(row.get("_hierarchy").is_some());
        assert!(row.get("_confidence").is_some());
    }
}

#[test]
fn ops_tags_extracts_keywords() {
    let rows = sample_records();
    let result = ops::op_tags(&rows, "content", 3);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 5);
    for row in arr {
        let tags = row["_tags"].as_array().unwrap();
        assert!(tags.len() <= 3);
    }
}

#[test]
fn ops_dedup_finds_url_duplicates() {
    let rows = vec![
        json!({"content": "Hello world", "url": "https://example.com/page?utm_source=twitter"}),
        json!({"content": "Different text", "url": "https://example.com/page"}),
        json!({"content": "Unique content", "url": "https://other.com"}),
    ];
    let result = ops::op_dedup(&rows, "content", "url", "url", 3);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // First two should share a group (same canonical URL)
    assert_eq!(arr[0]["_dup_group"], arr[1]["_dup_group"]);
    // Third should be different
    assert_ne!(arr[0]["_dup_group"], arr[2]["_dup_group"]);
}

#[test]
fn ops_dedup_fuzzy_identical_content() {
    let rows = vec![
        json!({"content": "The quick brown fox jumps over the lazy dog"}),
        json!({"content": "The quick brown fox jumps over the lazy dog"}),
        json!({"content": "Something completely different from the rest"}),
    ];
    let result = ops::op_dedup(&rows, "content", "url", "fuzzy", 3);
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 3);
    // First two identical should share a group
    assert_eq!(arr[0]["_dup_group"], arr[1]["_dup_group"]);
}

#[test]
fn ops_similarity_levenshtein() {
    let result = ops::op_similarity("kitten", "sitting", "levenshtein", false).unwrap();
    let score = result["similarity"].as_f64().unwrap();
    assert!(score > 0.0);
    assert!(score < 1.0);
}

#[test]
fn ops_similarity_all_metrics() {
    let result = ops::op_similarity("hello", "world", "levenshtein", true).unwrap();
    assert!(result.get("levenshtein").is_some());
    assert!(result.get("jaro-winkler").is_some());
    assert!(result.get("cosine").is_some());
}

#[test]
fn ops_similarity_invalid_metric() {
    let result = ops::op_similarity("a", "b", "invalid", false);
    assert!(result.is_err());
}

#[test]
fn ops_normalize_url_strips_tracking() {
    let result =
        ops::op_normalize_url("https://example.com/page?utm_source=twitter&id=42").unwrap();
    let normalized = result["normalized"].as_str().unwrap();
    assert!(!normalized.contains("utm_source"));
    assert!(normalized.contains("id=42"));
}

#[test]
fn ops_normalize_url_strips_www() {
    let result = ops::op_normalize_url("https://www.example.com/path").unwrap();
    let normalized = result["normalized"].as_str().unwrap();
    assert!(!normalized.contains("www."));
}

#[test]
fn ops_normalize_url_invalid() {
    let result = ops::op_normalize_url("not a url at all");
    assert!(result.is_err());
}

#[test]
fn ops_generate_taxonomy_returns_clusters() {
    let rows = sample_records();
    let result = ops::op_generate(&rows, "content", 2, "ward", 5).unwrap();
    assert_eq!(result["name"], "generated");
    assert!(result["num_clusters"].as_u64().unwrap() > 0);
    assert_eq!(result["num_items"], 5);
    let categories = result["categories"].as_array().unwrap();
    assert!(!categories.is_empty());
    for cat in categories {
        assert!(cat.get("label").is_some());
        assert!(cat.get("keywords").is_some());
        assert!(cat.get("members").is_some());
    }
}

#[test]
fn ops_generate_taxonomy_too_few() {
    let rows = vec![json!({"content": "only one"})];
    let result = ops::op_generate(&rows, "content", 2, "ward", 5);
    assert!(result.is_err());
}

#[test]
fn ops_topics_nmf_returns_terms() {
    let rows = sample_records();
    let result = ops::op_topics(&rows, "content", 2, 5, 50, 1000).unwrap();
    assert_eq!(result["num_topics"], 2);
    assert_eq!(result["num_items"], 5);
    let topics = result["topics"].as_array().unwrap();
    assert_eq!(topics.len(), 2);
    for topic in topics {
        assert!(topic.get("label").is_some());
        assert!(topic.get("terms").is_some());
    }
    let assignments = result["assignments"].as_array().unwrap();
    assert_eq!(assignments.len(), 5);
}

#[test]
fn ops_topics_empty() {
    let result = ops::op_topics(&[], "content", 2, 5, 50, 1000);
    assert!(result.is_err());
}

#[test]
fn ops_organize_generates_paths() {
    let rows = vec![
        json!({"_category": "Web Development", "id": "react-tutorial"}),
        json!({"_category": "Data Science", "id": "pandas-guide"}),
    ];
    let result = ops::op_organize(&rows, "folders", "./out", "_category", "id");
    let arr = result.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    let path0 = arr[0]["_output_path"].as_str().unwrap();
    assert!(path0.contains("web-development"));
    assert!(path0.contains("react-tutorial"));
    let path1 = arr[1]["_output_path"].as_str().unwrap();
    assert!(path1.contains("data-science"));
}

#[test]
fn ops_organize_flat_format() {
    let rows = vec![json!({"_category": "Tools", "id": "my-tool"})];
    let result = ops::op_organize(&rows, "flat", "./out", "_category", "id");
    let arr = result.as_array().unwrap();
    let path = arr[0]["_output_path"].as_str().unwrap();
    assert!(path.contains("--")); // flat uses -- separator
}

#[test]
fn ops_organize_empty() {
    let result = ops::op_organize(&[], "folders", "./out", "_category", "id");
    assert_eq!(result, json!([]));
}
