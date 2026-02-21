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

# Normalize filesystem entries into uniform topology input
def normalize-files [dir_path: string] {
    each {|f|
        let rel_path = ($f.name | str replace $"($dir_path)/" "")
        let ext = ($f.name | path parse | get extension | default "")
        let parent = ($rel_path | path dirname)
        let filename = ($f.name | path basename)
        {content: $"($filename) ($parent) ($ext)",
         url: $f.name,
         id: $rel_path,
         extension: $ext,
         size: $f.size,
         modified: $f.modified}
    }
}

# Run the topology pipeline (no dedup for unique file paths)
def run-pipeline [clusters: int, tags_count: int, format: string, output_dir: string] {
    print $"Classifying into ($clusters) categories..."
    let classified = ($in | topology classify --clusters $clusters)
    print $"Extracting top ($tags_count) tags per item..."
    let tagged = ($classified | topology tags --count $tags_count)
    print "Generating output paths..."
    $tagged | topology organize --format $format --output-dir $output_dir
}

# Save reports and print summary with extension/size breakdown
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

    let extensions = ($organized
        | group-by extension
        | items {|ext rows| {extension: (if $ext == "" { "(none)" } else { $ext }), count: ($rows | length)}}
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
        print "\nCreating organized directory tree..."
        for r in $organized {
            try { mkdir ($r._output_path | path dirname) }
        }
        print $"Directory tree created under ($output_dir)"
    } else {
        print "\n(dry-run: skipping directory creation)"
    }
}

def main [
    directory: string = "."                       # Directory to scan
    --clusters: int = 10                          # Number of categories to discover
    --tags-count: int = 5                         # Tags per item
    --output-dir: string = "./output/directory"   # Output directory
    --format: string = "folders"                  # Organization format: folders, flat, nested
    --dry-run                                     # Skip directory creation
    --exclude-pattern: string = ""                # Pattern to exclude from scan
] {
    let dir_path = ($directory | path expand)
    if not ($dir_path | path exists) {
        print $"Error: Directory not found: ($dir_path)"
        return
    }

    print $"Scanning directory: ($dir_path)..."
    let all_files = (glob $"($dir_path)/**/*" | each {|p|
        let info = (ls -l $p | first)
        if $info.type == "file" { $info } else { null }
    } | compact)

    let files = if $exclude_pattern != "" {
        $all_files | where {|f| not ($f.name | str contains $exclude_pattern)}
    } else {
        $all_files
    }
    print $"Found ($files | length) files"

    if ($files | is-empty) {
        print "No files to process."
        return
    }

    print "Normalizing records..."
    let items = ($files | normalize-files $dir_path)

    $items
        | run-pipeline $clusters $tags_count $format $output_dir
        | save-and-summarize $output_dir --dry-run=$dry_run
}
