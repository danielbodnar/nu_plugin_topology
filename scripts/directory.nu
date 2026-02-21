#!/usr/bin/env nu
# directory.nu — Topology pipeline for filesystem directories
#
# Prerequisites:
#   plugin add target/release/nu_plugin_topology
#   plugin use topology
#
# Usage:
#   nu scripts/directory.nu src/ --dry-run --clusters 5
#   nu scripts/directory.nu ~/projects --exclude-pattern "node_modules"

use std/log

# Normalize filesystem entries into uniform topology input
def normalize-files [dir_path: path]: list<record> -> list<record> {
    each {|f|
        let rel_path = ($f.name | str replace $"($dir_path)/" "")
        let ext = ($f.name | path parse | get extension | default "")
        let parent = ($rel_path | path dirname)
        let filename = ($f.name | path basename)
        {
            content: $"($filename) ($parent) ($ext)"
            url: $f.name
            id: $rel_path
            extension: $ext
            size: $f.size
            modified: $f.modified
        }
    }
}

# Run the topology pipeline (no dedup for unique file paths)
def run-pipeline [
    --clusters: int = 10
    --tags-count: int = 5
    --format: string = folders
    --output-dir: path = ./output/directory
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
    log info "Generating output paths..."
    $tagged | topology organize --format $format --output-dir ($output_dir | path expand)
}

# Save reports and print summary with extension/size breakdown
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

    let extensions = ($organized
        | group-by extension
        | items {|ext rows|
            {
                extension: (if $ext == "" { "(none)" } else { $ext })
                count: ($rows | length)
            }
        }
        | sort-by count --reverse
        | first 15)

    let size_by_cat = ($organized
        | group-by _category
        | items {|cat rows|
            let total_size = ($rows | get size | math sum)
            {category: $cat, total_size: $total_size, files: ($rows | length)}
        }
        | sort-by total_size --reverse
        | first 15)

    print $"\n═══ Topology Summary ═══\n  Total files:  ($organized | length)\n  Categories:   ($categories | length)"
    print "\n── Category Distribution ──"
    $categories | first 15 | print
    print "\n── Extension Distribution ──"
    $extensions | print
    print "\n── Size by Category ──"
    $size_by_cat | print

    if not $dry_run {
        log info "Creating organized directory tree..."
        for r in $organized {
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

# Scan directory and run topology pipeline
def scan-and-process [
    dir_path: path
    exclude_pattern: string
]: nothing -> list<record> {
    log info $"Scanning directory: ($dir_path)..."
    let all_files = try {
        glob $"($dir_path)/**/*" | each {|p|
            let info = (ls --long $p | first)
            if $info.type == file { $info } else { null }
        } | compact
    } catch {|e|
        error make {msg: $"Failed to scan directory ($dir_path): ($e.msg)"}
    }

    let files = if $exclude_pattern != "" {
        $all_files | where { $in.name !~ $exclude_pattern }
    } else {
        $all_files
    }
    log info $"Found ($files | length) files"
    $files
}

def main [
    directory: path = .                             # Directory to scan
    --clusters: int = 10                            # Number of categories to discover
    --tags-count: int = 5                           # Tags per item
    --output-dir: path = ./output/directory          # Output directory
    --format: string = folders                       # Organization format: folders, flat, nested
    --dry-run                                        # Skip directory creation
    --exclude-pattern: string = ""                   # Pattern to exclude from scan
    --cache: string = ""                             # Path to SQLite cache database
] {
    let dir_path = ($directory | path expand)
    if not ($dir_path | path exists) {
        error make {
            msg: $"Directory not found: ($dir_path)"
            label: {text: "directory does not exist", span: (metadata $directory).span}
        }
    }

    let files = (scan-and-process $dir_path $exclude_pattern)

    if ($files | is-empty) {
        print "No files to process."
        return
    }

    log info "Normalizing records..."
    let items = ($files | normalize-files $dir_path)

    $items
        | run-pipeline --clusters $clusters --tags-count $tags_count --format $format --output-dir $output_dir --cache $cache
        | save-and-summarize $output_dir --dry-run=$dry_run
}
