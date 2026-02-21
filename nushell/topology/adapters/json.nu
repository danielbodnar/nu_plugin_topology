#!/usr/bin/env nu
# json.nu — Generic JSON passthrough adapter
#
# Loads pre-normalized JSON data and validates the required schema fields.

# Load a JSON file and validate it has the expected field
export def load [
    source: path               # Path to JSON file
    --field: string = "content" # Required field name to validate
]: nothing -> list<record> {
    let source_path = ($source | path expand)
    if not ($source_path | path exists) {
        error make {msg: $"Source file not found: ($source_path)"}
    }

    let data = try { open --raw $source_path | from json } catch {|e|
        error make {msg: $"Failed to parse JSON: ($e.msg)"}
    }

    let records = if ($data | describe | str starts-with "list") {
        $data
    } else {
        [$data]
    }

    let columns = ($records | first | columns)
    if $field not-in $columns {
        error make {msg: $"Required field '($field)' not found. Available columns: ($columns | str join ', ')"}
    }

    $records
}
