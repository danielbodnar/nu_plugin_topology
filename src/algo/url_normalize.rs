use url::Url;

/// Normalize a URL for deduplication:
/// - Lowercase scheme and host
/// - Remove default ports (80, 443)
/// - Remove trailing slashes
/// - Sort query parameters
/// - Remove tracking parameters (utm_*, fbclid, etc.)
/// - Remove fragment
/// - Normalize www prefix (strip www.)
pub fn normalize(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Add scheme if missing
    let with_scheme = if !trimmed.contains("://") {
        format!("https://{trimmed}")
    } else {
        trimmed.to_string()
    };

    let mut parsed = Url::parse(&with_scheme).ok()?;

    // Remove fragment
    parsed.set_fragment(None);

    // Remove default ports
    if parsed.port() == Some(80) || parsed.port() == Some(443) {
        let _ = parsed.set_port(None);
    }

    // Sort and filter query parameters
    let query_pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .filter(|(k, _)| !is_tracking_param(k))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    if query_pairs.is_empty() {
        parsed.set_query(None);
    } else {
        let mut sorted = query_pairs;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let qs: String = sorted
            .iter()
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    format!("{k}={v}")
                }
            })
            .collect::<Vec<_>>()
            .join("&");
        parsed.set_query(Some(&qs));
    }

    let mut result = parsed.to_string();

    // Remove trailing slash (but keep root "/" for bare domains)
    if result.ends_with('/') {
        let path = parsed.path();
        if path == "/" {
            result = result.trim_end_matches('/').to_string();
        } else {
            result = result.trim_end_matches('/').to_string();
        }
    }

    // Strip www. prefix from host
    result = result.replacen("://www.", "://", 1);

    Some(result)
}

/// Generate a canonical key for grouping duplicate URLs.
/// Strips scheme entirely and produces a bare host+path+query key.
pub fn canonical_key(raw: &str) -> Option<String> {
    let normalized = normalize(raw)?;
    let stripped = normalized
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    Some(stripped.to_string())
}

/// Convert a string to a URL-safe slug (lowercase, alphanumeric, hyphens).
pub fn slugify(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|s| !s.is_empty())
        .collect::<Vec<&str>>()
        .join("-")
}

fn is_tracking_param(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.starts_with("utm_")
        || matches!(
            lower.as_str(),
            "fbclid" | "gclid" | "dclid" | "msclkid" | "mc_cid" | "mc_eid"
            | "ref" | "_ga" | "_gl" | "yclid" | "twclid" | "igshid"
            | "s" | "source" | "si"
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_normalization() {
        let n = normalize("https://Example.COM/path/").unwrap();
        assert_eq!(n, "https://example.com/path");
    }

    #[test]
    fn removes_tracking_params() {
        let n = normalize("https://example.com/page?utm_source=google&id=123").unwrap();
        assert_eq!(n, "https://example.com/page?id=123");
    }

    #[test]
    fn sorts_query_params() {
        let n = normalize("https://example.com?z=1&a=2&m=3").unwrap();
        // URL parser adds trailing / for bare domain with query
        assert_eq!(n, "https://example.com/?a=2&m=3&z=1");
    }

    #[test]
    fn strips_www() {
        let n = normalize("https://www.example.com/path").unwrap();
        assert_eq!(n, "https://example.com/path");
    }

    #[test]
    fn removes_fragment() {
        let n = normalize("https://example.com/path#section").unwrap();
        assert_eq!(n, "https://example.com/path");
    }

    #[test]
    fn adds_scheme() {
        let n = normalize("example.com/path").unwrap();
        assert_eq!(n, "https://example.com/path");
    }

    #[test]
    fn removes_default_port() {
        let n = normalize("https://example.com:443/path").unwrap();
        assert_eq!(n, "https://example.com/path");
    }

    #[test]
    fn canonical_key_strips_scheme() {
        let k = canonical_key("https://www.example.com/path?a=1").unwrap();
        assert_eq!(k, "example.com/path?a=1");
    }

    #[test]
    fn empty_returns_none() {
        assert!(normalize("").is_none());
    }

    #[test]
    fn removes_all_tracking_params() {
        let n = normalize("https://example.com/page?fbclid=abc&gclid=def&real=yes").unwrap();
        assert_eq!(n, "https://example.com/page?real=yes");
    }

    #[test]
    fn removes_utm_variants() {
        let n = normalize("https://example.com?utm_source=a&utm_medium=b&utm_campaign=c&keep=1").unwrap();
        assert_eq!(n, "https://example.com/?keep=1");
    }

    #[test]
    fn all_params_removed_leaves_clean_url() {
        let n = normalize("https://example.com/page?utm_source=google&fbclid=abc").unwrap();
        assert_eq!(n, "https://example.com/page");
    }

    #[test]
    fn preserves_non_tracking_params() {
        let n = normalize("https://example.com/search?q=rust&page=2").unwrap();
        assert_eq!(n, "https://example.com/search?page=2&q=rust");
    }

    #[test]
    fn http_non_default_port_preserved() {
        let n = normalize("https://example.com:8080/path").unwrap();
        assert_eq!(n, "https://example.com:8080/path");
    }

    #[test]
    fn normalize_with_multiple_trailing_slashes() {
        let n = normalize("https://example.com/path///").unwrap();
        assert_eq!(n, "https://example.com/path");
    }

    #[test]
    fn canonical_key_different_schemes_same_key() {
        let k1 = canonical_key("https://example.com/path").unwrap();
        let k2 = canonical_key("http://example.com/path").unwrap();
        assert_eq!(k1, k2);
    }

    #[test]
    fn canonical_key_empty_returns_none() {
        assert!(canonical_key("").is_none());
    }

    #[test]
    fn normalize_idempotent() {
        let url = "https://www.Example.COM/Path?z=1&a=2&utm_source=x#frag";
        let first = normalize(url).unwrap();
        let second = normalize(&first).unwrap();
        assert_eq!(first, second, "normalize should be idempotent");
    }

    #[test]
    fn is_tracking_param_cases() {
        assert!(is_tracking_param("utm_source"));
        assert!(is_tracking_param("utm_medium"));
        assert!(is_tracking_param("fbclid"));
        assert!(is_tracking_param("gclid"));
        assert!(is_tracking_param("FBCLID")); // case insensitive
        assert!(!is_tracking_param("id"));
        assert!(!is_tracking_param("page"));
        assert!(!is_tracking_param("q"));
    }

    #[test]
    fn slugify_basic() {
        assert_eq!(slugify("Web Dev"), "web-dev");
        assert_eq!(slugify("AI & ML"), "ai-ml");
        assert_eq!(slugify("  spaces  "), "spaces");
        assert_eq!(slugify("Hello World!"), "hello-world");
        assert_eq!(slugify("rust/systems"), "rust-systems");
    }

    #[test]
    fn slugify_empty() {
        assert_eq!(slugify(""), "");
        assert_eq!(slugify("---"), "");
    }
}
