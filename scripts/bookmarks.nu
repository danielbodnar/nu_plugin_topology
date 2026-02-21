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

use std/log

# Recursively flatten Chrome bookmark tree into flat records
def flatten-bookmarks [
    folder_path: string
    --include-bookmarklets
]: list<record> -> list<record> {
    each {|node|
        if ($node.type? | default "") == folder {
            let child_path = if $folder_path == "" {
                ($node.name? | default "")
            } else {
                $"($folder_path)/($node.name? | default '')"
            }
            $node.children?
                | default []
                | flatten-bookmarks $child_path --include-bookmarklets=$include_bookmarklets
        } else if ($node.type? | default "") == url {
            let url = ($node.url? | default "")
            if (not $include_bookmarklets) and ($url | str starts-with javascript:) {
                []
            } else {
                [{
                    name: ($node.name? | default "")
                    url: $url
                    folder_path: $folder_path
                    date_added: ($node.date_added? | default "")
                }]
            }
        } else {
            []
        }
    } | flatten
}

# Normalize Chrome bookmarks into uniform topology input
def normalize-bookmarks []: list<record> -> list<record> {
    each {|b|
        {
            content: $"($b.name) ($b.url) ($b.folder_path)"
            url: $b.url
            id: $"($b.folder_path)/($b.name)"
            folder_path: $b.folder_path
        }
    }
}

# Run the full topology pipeline with dedup
def run-pipeline [
    --clusters: int = 15
    --tags-count: int = 5
    --dedup-strategy: string = fuzzy
    --format: string = folders
    --output-dir: path = ./output/bookmarks
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

# Save reports and print summary with Chrome folder comparison
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

    let chrome_folders = ($organized
        | group-by folder_path
        | items {|folder rows| {folder: $folder, count: ($rows | length)} }
        | sort-by count --reverse
        | first 15)

    print $"\n═══ Topology Summary ═══\n  Total items:  ($organized | length)\n  Categories:   ($categories | length)\n  Duplicates:   ($dups | length)"
    print "\n── Topology Categories ──"
    $categories | first 15 | print
    print "\n── Original Chrome Folders ──"
    $chrome_folders | print

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
    --source: path = /workspaces/bookmarks/Bookmarks       # Path to Chrome Bookmarks JSON
    --clusters: int = 1000                                  # Number of categories to discover
    --tags-count: int = 10                                  # Tags per item
    --output-dir: path = ./output/bookmarks                 # Output directory
    --format: string = folders                              # Organization format: folders, flat, nested
    --dry-run                                               # Skip directory creation
    --dedup-strategy: string = fuzzy                        # Dedup strategy: url, fuzzy, combined
    --include-bookmarklets                                  # Include javascript: bookmarklets
    --cache: string = ""                                    # Path to SQLite cache database
] {
    let source_path = ($source | path expand)
    if not ($source_path | path exists) {
        error make {
            msg: $"Source file not found: ($source_path)"
            label: {text: "file does not exist", span: (metadata $source).span}
        }
    }

    log info $"Loading bookmarks from ($source_path)..."
    let raw = try { open --raw $source_path | from json } catch {|e|
        error make {msg: $"Failed to parse bookmarks JSON: ($e.msg)"}
    }
    let roots = ($raw.roots? | default {})

    log info "Flattening bookmark tree..."
    let bar = ($roots.bookmark_bar?.children?
        | default []
        | flatten-bookmarks "Bookmark Bar" --include-bookmarklets=$include_bookmarklets)
    let other = ($roots.other?.children?
        | default []
        | flatten-bookmarks Other --include-bookmarklets=$include_bookmarklets)
    let synced = ($roots.synced?.children?
        | default []
        | flatten-bookmarks Synced --include-bookmarklets=$include_bookmarklets)
    let all_bookmarks = [...$bar ...$other ...$synced]
    log info $"Found ($all_bookmarks | length) bookmarks"

    if ($all_bookmarks | is-empty) {
        print "No bookmarks to process."
        return
    }

    log info "Normalizing records..."
    let items = ($all_bookmarks | normalize-bookmarks)

    $items
        | run-pipeline --clusters $clusters --tags-count $tags_count --dedup-strategy $dedup_strategy --format $format --output-dir $output_dir --cache $cache
        | save-and-summarize $output_dir --dry-run=$dry_run
}
