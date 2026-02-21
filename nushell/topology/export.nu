#!/usr/bin/env nu
# export.nu — Output formatters for topology results
#
# Supports JSON, markdown, and Parquet (via Polars) export.

# Export results to JSON file
export def to-json [output: path]: list<record> -> nothing {
    let data = $in
    let dir = ($output | path dirname)
    if not ($dir | path exists) { try { mkdir $dir } }
    try { $data | to json | save --force $output } catch {|e|
        error make {msg: $"Failed to save JSON: ($e.msg)"}
    }
}

# Export results to Parquet file (requires Polars plugin)
export def to-parquet [output: path]: list<record> -> nothing {
    let data = $in
    let dir = ($output | path dirname)
    if not ($dir | path exists) { try { mkdir $dir } }
    try {
        $data | polars into-df | polars save $output
    } catch {|e|
        error make {msg: $"Parquet export requires the polars plugin: ($e.msg)"}
    }
}

# Export category distribution as a markdown table
export def to-markdown [output: path]: list<record> -> nothing {
    let data = $in
    let dir = ($output | path dirname)
    if not ($dir | path exists) { try { mkdir $dir } }

    let categories = ($data
        | group-by _category
        | items {|cat rows| {category: $cat, count: ($rows | length)} }
        | sort-by count --reverse)

    let total = ($data | length)
    mut lines = ["# Topology Report" ""]
    $lines ++= [$"Total items: **($total)**"]
    $lines ++= [$"Categories: **($categories | length)**"]
    $lines ++= [""]
    $lines ++= ["| Category | Count | % |"]
    $lines ++= ["|----------|------:|---:|"]

    for row in $categories {
        let pct = (($row.count / $total * 100) | math round --precision 1)
        $lines ++= [$"| ($row.category) | ($row.count) | ($pct)% |"]
    }

    try { $lines | str join "\n" | save --force $output } catch {|e|
        error make {msg: $"Failed to save markdown: ($e.msg)"}
    }
}

# Save full report + categories + duplicates to an output directory
export def save-all [
    output_dir: path
]: list<record> -> nothing {
    let data = $in
    if not ($output_dir | path exists) { try { mkdir $output_dir } }

    try { $data | to json | save --force $"($output_dir)/report.json" } catch {|e|
        error make {msg: $"Failed to save report: ($e.msg)"}
    }

    let categories = ($data
        | group-by _category
        | items {|cat rows| {category: $cat, count: ($rows | length)} }
        | sort-by count --reverse)
    try { $categories | to json | save --force $"($output_dir)/categories.json" } catch {|e|
        error make {msg: $"Failed to save categories: ($e.msg)"}
    }

    let dups = ($data | where { not ($in._is_primary? | default true) })
    try { $dups | to json | save --force $"($output_dir)/duplicates.json" } catch {|e|
        error make {msg: $"Failed to save duplicates: ($e.msg)"}
    }
}
