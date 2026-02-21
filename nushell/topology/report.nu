#!/usr/bin/env nu
# report.nu — Summary reports and statistics for topology results
#
# Generates category distributions, duplicate reports, and summaries.
# Supports Polars acceleration when available, with plain Nushell fallback.

# Generate category distribution stats (uses Polars if available)
export def stats []: list<record> -> table {
    let data = $in
    let has_polars = (try { [[a]; [1]] | polars into-df; true } catch { false })
    if $has_polars {
        $data | polars into-df
            | polars group-by _category
            | polars agg ...[( polars col _category | polars count | polars as count )]
            | polars sort-by count --reverse [true]
            | polars into-nu
    } else {
        $data
            | group-by _category
            | items {|cat rows| {category: $cat, count: ($rows | length)} }
            | sort-by count --reverse
    }
}

# Generate a full summary report
export def summary []: list<record> -> record {
    let data = $in
    let total = ($data | length)
    let categories = ($data | stats)
    let num_categories = ($categories | length)
    let dups = ($data | where { not ($in._is_primary? | default true) })
    let num_dups = ($dups | length)

    {
        total_items: $total
        num_categories: $num_categories
        num_duplicates: $num_dups
        num_unique: ($total - $num_dups)
        top_categories: ($categories | first 15)
    }
}

# Print a formatted summary to stdout
export def print-summary []: list<record> -> nothing {
    let data = $in
    let report = ($data | summary)

    print $"\n═══ Topology Summary ═══
  Total items:  ($report.total_items)
  Categories:   ($report.num_categories)
  Duplicates:   ($report.num_duplicates)
  Unique:       ($report.num_unique)"
    print "\n── Category Distribution ──"
    $report.top_categories | print
}

# List duplicate records
export def duplicates []: list<record> -> list<record> {
    where { not ($in._is_primary? | default true) }
}
