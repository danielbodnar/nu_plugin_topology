#!/usr/bin/env nu
# stars.nu — Topology pipeline for GitHub stars
#
# Prerequisites:
#   plugin add target/release/nu_plugin_topology
#   plugin use topology
#
# Usage:
#   nu scripts/stars.nu --dry-run --clusters 5
#   nu scripts/stars.nu --min-stars 100 --clusters 20

use std/log

# Load and filter GitHub stars JSON
def load-stars [source_path: path, min_stars: int]: nothing -> list<record> {
    let raw = try { open --raw $source_path | from json } catch {|e|
        error make {msg: $"Failed to parse stars JSON: ($e.msg)"}
    }
    log info $"Loaded ($raw | length) repositories"
    if $min_stars > 0 {
        let filtered = ($raw | where { ($in.stargazers_count? | default 0) >= $min_stars })
        log info $"Filtered to ($filtered | length) repos with >= ($min_stars) stars"
        $filtered
    } else {
        $raw
    }
}

# Normalize GitHub API records into uniform topology input
def normalize-stars []: list<record> -> list<record> {
    each {|r|
        let topics = ($r.topics? | default [] | str join " ")
        {
            content: $"($r.full_name? | default '') ($r.description? | default '') ($topics) ($r.language? | default '')"
            url: ($r.html_url? | default "")
            id: ($r.full_name? | default "")
            language: ($r.language? | default unknown)
            stars: ($r.stargazers_count? | default 0)
        }
    }
}

# Run the full topology pipeline
def run-pipeline [
    --clusters: int = 15
    --tags-count: int = 5
    --dedup-strategy: string = combined
    --format: string = folders
    --output-dir: path = ./output/stars
    --cache: string = ""
]: list<record> -> list<record> {
    log info $"Classifying into ($clusters) categories..."
    let classified = if $cache != "" {
        ($in | topology classify --clusters $clusters --cache $cache)
    } else {
        ($in | topology classify --clusters $clusters)
    }
    log info $"Extracting top ($tags_count) tags per item..."
    let tagged = if $cache != "" {
        ($classified | topology tags --count $tags_count --cache $cache)
    } else {
        ($classified | topology tags --count $tags_count)
    }
    log info $"Deduplicating with strategy: ($dedup_strategy)..."
    let deduped = if $cache != "" {
        ($tagged | topology dedup --strategy $dedup_strategy --cache $cache)
    } else {
        ($tagged | topology dedup --strategy $dedup_strategy)
    }
    log info "Generating output paths..."
    $deduped | topology organize --format $format --output-dir ($output_dir | path expand)
}

# Save reports and print summary
def save-and-summarize [
    output_dir: path
    --dry-run
]: list<record> -> nothing {
    let organized = $in
    try { mkdir $output_dir } catch {|e| error make {msg: $"Failed to create output dir ($output_dir): ($e.msg)"} }

    try { $organized | to json | save --force $"($output_dir)/report.json" } catch {|e|
        error make {msg: $"Failed to save report: ($e.msg)"}
    }
    log info $"Saved full report to ($output_dir)/report.json"

    let categories = ($organized
        | group-by _category
        | items {|cat rows| {category: $cat, count: ($rows | length)} }
        | sort-by count --reverse)
    try { $categories | to json | save --force $"($output_dir)/categories.json" } catch {|e|
        error make {msg: $"Failed to save categories: ($e.msg)"}
    }

    let dups = ($organized | where { not $in._is_primary })
    try { $dups | to json | save --force $"($output_dir)/duplicates.json" } catch {|e|
        error make {msg: $"Failed to save duplicates: ($e.msg)"}
    }
    log info $"Saved ($dups | length) duplicates to ($output_dir)/duplicates.json"

    let languages = ($organized
        | group-by language
        | items {|lang rows| {language: $lang, count: ($rows | length)} }
        | sort-by count --reverse
        | first 15)

    print $"\n═══ Topology Summary ═══\n  Total items:  ($organized | length)\n  Categories:   ($categories | length)\n  Duplicates:   ($dups | length)"
    print "\n── Category Distribution ──"
    $categories | first 15 | print
    print "\n── Language Distribution ──"
    $languages | print

    if not $dry_run {
        log info "Creating organized directory tree..."
        for r in ($organized | where _is_primary) {
            let dir = ($r._output_path | path dirname)
            if not ($dir | path exists) {
                try { mkdir $dir } catch {|e|
                    log warning $"Failed to create ($dir): ($e.msg)"
                }
            }
        }
        print $"Directory tree created under ($output_dir)"
    } else {
        print "\n(dry-run: skipping directory creation)"
    }
}

def main [
    --source: path = ~/.config/bookmarks/gh-stars.raw.json  # Path to GitHub stars JSON
    --clusters: int = 15                                     # Number of categories to discover
    --tags-count: int = 5                                    # Tags per item
    --output-dir: path = ./output/stars                      # Output directory
    --format: string = folders                               # Organization format: folders, flat, nested
    --dry-run                                                # Skip directory creation
    --dedup-strategy: string = combined                      # Dedup strategy: url, fuzzy, combined
    --min-stars: int = 0                                     # Filter repos with fewer stars
    --cache: string = ""                                     # Path to SQLite cache database
] {
    let source_path = ($source | path expand)
    if not ($source_path | path exists) {
        error make {
            msg: $"Source file not found: ($source_path)"
            label: {text: "file does not exist", span: (metadata $source).span}
        }
    }

    log info $"Loading stars from ($source_path)..."
    let items = (load-stars $source_path $min_stars | normalize-stars)
    log info $"Normalized ($items | length) items"

    $items
        | run-pipeline --clusters $clusters --tags-count $tags_count --dedup-strategy $dedup_strategy --format $format --output-dir $output_dir --cache $cache
        | save-and-summarize $output_dir --dry-run=$dry_run
}
