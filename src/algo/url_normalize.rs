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
}
