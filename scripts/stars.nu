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

# Load and filter GitHub stars JSON
def load-stars [source_path: string, min_stars: int] {
    let raw = (open --raw $source_path | from json)
    print $"Loaded ($raw | length) repositories"
    if $min_stars > 0 {
        let filtered = ($raw | where {|r| ($r.stargazers_count? | default 0) >= $min_stars})
        print $"Filtered to ($filtered | length) repos with >= ($min_stars) stars"
        $filtered
    } else {
        $raw
    }
}

# Normalize GitHub API records into uniform topology input
def normalize-stars [] {
    each {|r|
        let topics = ($r.topics? | default [] | str join " ")
        {content: $"($r.full_name? | default '') ($r.description? | default '') ($topics) ($r.language? | default '')",
         url: ($r.html_url? | default ""),
         id: ($r.full_name? | default ""),
         language: ($r.language? | default "unknown"),
         stars: ($r.stargazers_count? | default 0)}
    }
}

# Run the full topology pipeline
def run-pipeline [clusters: int, tags_count: int, dedup_strategy: string, format: string, output_dir: string] {
    print $"Classifying into ($clusters) categories..."
    let classified = ($in | topology classify --clusters $clusters)
    print $"Extracting top ($tags_count) tags per item..."
    let tagged = ($classified | topology tags --count $tags_count)
    print $"Deduplicating with strategy: ($dedup_strategy)..."
    let deduped = ($tagged | topology dedup --strategy $dedup_strategy)
    print "Generating output paths..."
    $deduped | topology organize --format $format --output-dir $output_dir
}

# Save reports and print summary
def save-and-summarize [output_dir: string, --dry-run] {
    let organized = $in
    mkdir $output_dir

    $organized | to json | save --force $"($output_dir)/report.json"
    print $"Saved full report to ($output_dir)/report.json"

    let categories = ($organized
        | group-by _category
        | items {|cat rows| {category: $cat, count: ($rows | length)}}
        | sort-by count --reverse)
    $categories | to json | save --force $"($output_dir)/categories.json"

    let dups = ($organized | where {|r| not $r._is_primary})
    $dups | to json | save --force $"($output_dir)/duplicates.json"
    print $"Saved ($dups | length) duplicates to ($output_dir)/duplicates.json"

    let languages = ($organized
        | group-by language
        | items {|lang rows| {language: $lang, count: ($rows | length)}}
        | sort-by count --reverse
        | first 15)

    print $"\n═══ Topology Summary ═══\n  Total items:  ($organized | length)\n  Categories:   ($categories | length)\n  Duplicates:   ($dups | length)"
    print "\n── Category Distribution ──"
    $categories | first 15 | print
    print "\n── Language Distribution ──"
    $languages | print

    if not $dry_run {
        print "\nCreating organized directory tree..."
        for r in ($organized | where _is_primary) {
            try { mkdir ($r._output_path | path dirname) }
        }
        print $"Directory tree created under ($output_dir)"
    } else {
        print "\n(dry-run: skipping directory creation)"
    }
}

def main [
    --source: string = "~/.config/bookmarks/gh-stars.raw.json"  # Path to GitHub stars JSON
    --clusters: int = 15                                         # Number of categories to discover
    --tags-count: int = 5                                        # Tags per item
    --output-dir: string = "./output/stars"                      # Output directory
    --format: string = "folders"                                 # Organization format: folders, flat, nested
    --dry-run                                                    # Skip directory creation
    --dedup-strategy: string = "combined"                        # Dedup strategy: url, fuzzy, combined
    --min-stars: int = 0                                         # Filter repos with fewer stars
] {
    let source_path = ($source | path expand)
    if not ($source_path | path exists) {
        print $"Error: Source file not found: ($source_path)"
        return
    }

    print $"Loading stars from ($source_path)..."
    let items = (load-stars $source_path $min_stars | normalize-stars)
    print $"Normalized ($items | length) items"

    $items
        | run-pipeline $clusters $tags_count $dedup_strategy $format $output_dir
        | save-and-summarize $output_dir --dry-run=$dry_run
}
