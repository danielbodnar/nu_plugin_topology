use unicode_segmentation::UnicodeSegmentation;

/// Tokenize text into lowercase word tokens, filtering stopwords and short tokens.
pub fn tokenize(text: &str) -> Vec<String> {
    text.unicode_words()
        .map(|w| w.to_lowercase())
        .filter(|w| w.len() >= 2 && !is_stopword(w))
        .collect()
}

/// Generate character n-grams (shingles) from text.
pub fn shingles(text: &str, n: usize) -> Vec<String> {
    let lower = text.to_lowercase();
    let chars: Vec<char> = lower.chars().collect();
    if chars.len() < n {
        return vec![lower];
    }
    chars.windows(n).map(|w| w.iter().collect()).collect()
}

/// Generate word n-grams from a token list.
pub fn word_ngrams(tokens: &[String], n: usize) -> Vec<String> {
    if tokens.len() < n {
        return vec![tokens.join(" ")];
    }
    tokens
        .windows(n)
        .map(|w| w.join(" "))
        .collect()
}

fn is_stopword(word: &str) -> bool {
    matches!(
        word,
        "a" | "an" | "the" | "is" | "it" | "of" | "to" | "in" | "for" | "on" | "with"
        | "at" | "by" | "from" | "as" | "or" | "and" | "but" | "not" | "be" | "are"
        | "was" | "were" | "been" | "being" | "have" | "has" | "had" | "do" | "does"
        | "did" | "will" | "would" | "could" | "should" | "may" | "might" | "shall"
        | "can" | "this" | "that" | "these" | "those" | "there" | "here" | "where"
        | "when" | "what" | "which" | "who" | "whom" | "how" | "all" | "each" | "every"
        | "both" | "few" | "more" | "most" | "other" | "some" | "such" | "no" | "nor"
        | "only" | "own" | "same" | "so" | "than" | "too" | "very" | "just" | "because"
        | "about" | "into" | "through" | "during" | "before" | "after" | "above" | "below"
        | "between" | "under" | "again" | "further" | "then" | "once" | "any" | "its"
        | "your" | "our" | "their" | "his" | "her" | "my" | "if" | "up" | "out" | "also"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_basic() {
        let tokens = tokenize("Hello World! This is a test.");
        assert_eq!(tokens, vec!["hello", "world", "test"]);
    }

    #[test]
    fn tokenize_filters_short() {
        let tokens = tokenize("I am a x y z developer");
        assert_eq!(tokens, vec!["am", "developer"]);
    }

    #[test]
    fn shingles_basic() {
        let s = shingles("hello", 3);
        assert_eq!(s, vec!["hel", "ell", "llo"]);
    }

    #[test]
    fn shingles_short_text() {
        let s = shingles("hi", 3);
        assert_eq!(s, vec!["hi"]);
    }

    #[test]
    fn word_ngrams_basic() {
        let tokens = vec!["rust".into(), "plugin".into(), "system".into()];
        let ng = word_ngrams(&tokens, 2);
        assert_eq!(ng, vec!["rust plugin", "plugin system"]);
    }
}
