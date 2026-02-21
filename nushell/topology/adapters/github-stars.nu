#!/usr/bin/env nu
# github-stars.nu — Adapter for GitHub stars JSON data
#
# Loads and normalizes GitHub API star exports into the canonical
# topology schema: {content: string, url: string, id: string, ...}

const DEFAULT_SOURCE = "~/.config/bookmarks/gh-stars.raw.json"

# Load GitHub stars from a JSON file and optionally filter by star count
export def load [
    source?: path        # Path to stars JSON (default: ~/.config/bookmarks/gh-stars.raw.json)
    --min-stars: int = 0 # Filter repos with fewer stars
]: nothing -> list<record> {
    let source_path = ($source | default $DEFAULT_SOURCE | path expand)
    if not ($source_path | path exists) {
        error make {msg: $"Source file not found: ($source_path)"}
    }

    let raw = try { open --raw $source_path | from json } catch {|e|
        error make {msg: $"Failed to parse stars JSON: ($e.msg)"}
    }

    if $min_stars > 0 {
        $raw | where { ($in.stargazers_count? | default 0) >= $min_stars }
    } else {
        $raw
    }
}

# Normalize GitHub API records into the canonical topology schema
export def normalize []: list<record> -> list<record> {
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
