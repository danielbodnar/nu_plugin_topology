#!/usr/bin/env nu
# bookmarks.nu — Topology pipeline for Chrome bookmarks
#
# Prerequisites:
#   plugin add target/release/nu_plugin_topology
#   plugin use topology
#
# Usage:
#   nu scripts/bookmarks.nu --dry-run --clusters 5
#   nu scripts/bookmarks.nu --source ~/snap/chromium/common/chromium/Default/Bookmarks

# Recursively flatten Chrome bookmark tree into flat records
def flatten-bookmarks [nodes: list, folder_path: string, --include-bookmarklets] {
    $nodes | each {|node|
        if ($node.type? | default "") == "folder" {
            let child_path = if $folder_path == "" {
                ($node.name? | default "")
            } else {
                $"($folder_path)/($node.name? | default '')"
            }
            flatten-bookmarks ($node.children? | default []) $child_path --include-bookmarklets=$include_bookmarklets
        } else if ($node.type? | default "") == "url" {
            let url = ($node.url? | default "")
            if (not $include_bookmarklets) and ($url | str starts-with "javascript:") {
                []
            } else {
                [{name: ($node.name? | default ""), url: $url, folder_path: $folder_path, date_added: ($node.date_added? | default "")}]
            }
        } else {
            []
        }
    } | flatten
}

# Normalize Chrome bookmarks into uniform topology input
def normalize-bookmarks [] {
    each {|b|
        {content: $"($b.name) ($b.url) ($b.folder_path)",
         url: $b.url,
         id: $"($b.folder_path)/($b.name)",
         folder_path: $b.folder_path}
    }
}

# Run the full topology pipeline with dedup
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

# Save reports and print summary with Chrome folder comparison
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

    let chrome_folders = ($organized
        | group-by folder_path
        | items {|folder rows| {folder: $folder, count: ($rows | length)}}
        | sort-by count --reverse
        | first 15)

    print $"\n═══ Topology Summary ═══\n  Total items:  ($organized | length)\n  Categories:   ($categories | length)\n  Duplicates:   ($dups | length)"
    print "\n── Topology Categories ──"
    $categories | first 15 | print
    print "\n── Original Chrome Folders ──"
    $chrome_folders | print

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
    --source: string = "/workspaces/bookmarks/Bookmarks"  # Path to Chrome Bookmarks JSON
    --clusters: int = 15                                    # Number of categories to discover
    --tags-count: int = 5                                   # Tags per item
    --output-dir: string = "./output/bookmarks"             # Output directory
    --format: string = "folders"                            # Organization format: folders, flat, nested
    --dry-run                                               # Skip directory creation
    --dedup-strategy: string = "combined"                   # Dedup strategy: url, fuzzy, combined
    --include-bookmarklets                                  # Include javascript: bookmarklets
] {
    let source_path = ($source | path expand)
    if not ($source_path | path exists) {
        print $"Error: Source file not found: ($source_path)"
        return
    }

    print $"Loading bookmarks from ($source_path)..."
    let raw = (open --raw $source_path | from json)
    let roots = ($raw.roots? | default {})

    print "Flattening bookmark tree..."
    let bar = (flatten-bookmarks ($roots.bookmark_bar?.children? | default []) "Bookmark Bar" --include-bookmarklets=$include_bookmarklets)
    let other = (flatten-bookmarks ($roots.other?.children? | default []) "Other" --include-bookmarklets=$include_bookmarklets)
    let synced = (flatten-bookmarks ($roots.synced?.children? | default []) "Synced" --include-bookmarklets=$include_bookmarklets)
    let all_bookmarks = ($bar | append $other | append $synced)
    print $"Found ($all_bookmarks | length) bookmarks"

    if ($all_bookmarks | is-empty) {
        print "No bookmarks to process."
        return
    }

    print "Normalizing records..."
    let items = ($all_bookmarks | normalize-bookmarks)

    $items
        | run-pipeline $clusters $tags_count $dedup_strategy $format $output_dir
        | save-and-summarize $output_dir --dry-run=$dry_run
}
