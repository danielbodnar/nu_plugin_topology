#!/usr/bin/env nu
# directory.nu — Adapter for filesystem directory scanning
#
# Scans a directory and normalizes file entries into the canonical
# topology schema: {content: string, url: string, id: string, ...}

# Scan a directory and return file records
export def load [
    directory?: path                # Directory to scan (default: current dir)
    --exclude-pattern: string = ""  # Pattern to exclude from scan
]: nothing -> list<record> {
    let dir_path = ($directory | default "." | path expand)
    if not ($dir_path | path exists) {
        error make {msg: $"Directory not found: ($dir_path)"}
    }

    let all_files = try {
        glob $"($dir_path)/**/*" | each {|p|
            let info = (ls --long $p | first)
            if $info.type == file { $info } else { null }
        } | compact
    } catch {|e|
        error make {msg: $"Failed to scan directory ($dir_path): ($e.msg)"}
    }

    if $exclude_pattern != "" {
        $all_files | where { $in.name !~ $exclude_pattern }
    } else {
        $all_files
    }
}

# Normalize filesystem entries into the canonical topology schema
export def normalize [dir_path: path]: list<record> -> list<record> {
    let dp = ($dir_path | path expand)
    each {|f|
        let rel_path = ($f.name | str replace $"($dp)/" "")
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
