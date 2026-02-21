#!/usr/bin/env nu
# chrome-bookmarks.nu — Adapter for Chrome Bookmarks JSON
#
# Loads and normalizes Chrome bookmark exports into the canonical
# topology schema: {content: string, url: string, id: string, ...}

# Load Chrome bookmarks from JSON, flatten the tree, and return flat records
export def load [
    source?: path              # Path to Chrome Bookmarks JSON
    --include-bookmarklets     # Include javascript: bookmarklets
]: nothing -> list<record> {
    let source_path = ($source | default "/workspaces/bookmarks/Bookmarks" | path expand)
    if not ($source_path | path exists) {
        error make {msg: $"Source file not found: ($source_path)"}
    }

    let raw = try { open --raw $source_path | from json } catch {|e|
        error make {msg: $"Failed to parse bookmarks JSON: ($e.msg)"}
    }
    let roots = ($raw.roots? | default {})

    let bar = ($roots.bookmark_bar?.children?
        | default []
        | flatten-tree "Bookmark Bar" --include-bookmarklets=$include_bookmarklets)
    let other = ($roots.other?.children?
        | default []
        | flatten-tree Other --include-bookmarklets=$include_bookmarklets)
    let synced = ($roots.synced?.children?
        | default []
        | flatten-tree Synced --include-bookmarklets=$include_bookmarklets)

    [...$bar ...$other ...$synced]
}

# Recursively flatten Chrome bookmark tree into flat records
export def flatten-tree [
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
                | flatten-tree $child_path --include-bookmarklets=$include_bookmarklets
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

# Normalize Chrome bookmarks into the canonical topology schema
export def normalize []: list<record> -> list<record> {
    each {|b|
        {
            content: $"($b.name) ($b.url) ($b.folder_path)"
            url: $b.url
            id: $"($b.folder_path)/($b.name)"
            folder_path: $b.folder_path
        }
    }
}
