#!/usr/bin/env nu
# pipeline.nu — Composable topology pipeline steps
#
# Individual steps and a combined `run` command for the full pipeline.
# All commands expect input from stdin (piped data).

# Run the full topology pipeline: classify → tags → dedup → organize
export def run [
    --clusters: int = 15          # Number of categories to discover
    --tags-count: int = 5         # Tags per item
    --dedup-strategy: string = "combined"  # Dedup strategy: url, fuzzy, combined
    --format: string = "folders"  # Organization format: folders, flat, nested
    --output-dir: path = "./output"  # Base output directory
    --skip-dedup                  # Skip deduplication step
]: list<record> -> list<record> {
    let classified = ($in | topology classify --clusters $clusters)
    let tagged = ($classified | topology tags --count $tags_count)
    let result = if $skip_dedup {
        $tagged
    } else {
        $tagged | topology dedup --strategy $dedup_strategy
    }
    $result | topology organize --format $format --output-dir ($output_dir | path expand)
}

# Classify input records into auto-discovered categories
export def classify [
    --clusters: int = 15
    --field: string = "content"
]: list<record> -> list<record> {
    topology classify --clusters $clusters --field $field
}

# Extract top TF-IDF tags from each record
export def tags [
    --count: int = 5
    --field: string = "content"
]: list<record> -> list<record> {
    topology tags --count $count --field $field
}

# Find and mark duplicate records
export def dedup [
    --strategy: string = "combined"
    --field: string = "content"
]: list<record> -> list<record> {
    topology dedup --strategy $strategy --field $field
}

# Generate output paths from classified records
export def organize [
    --format: string = "folders"
    --output-dir: path = "./organized"
]: list<record> -> list<record> {
    topology organize --format $format --output-dir ($output_dir | path expand)
}
