# nu_plugin_topology

Content topology, classification, and deduplication engine.

A Rust-based tool providing high-performance SimHash fingerprinting, stratified sampling, and data profiling — designed for organizing large collections of bookmarks, GitHub stars, and files.

**Two ways to use it:**

| Binary | Size | Requires Nushell | Input/Output |
|--------|------|-----------------|--------------|
| `nu_plugin_topology` | 8.9 MB | Yes (v0.110.0+) | Nushell tables via plugin protocol |
| `topology` | 1.3 MB | No | JSON via stdin/stdout |

Both binaries share the same core algorithms — the Nushell plugin adds typed column integration, while the standalone CLI works anywhere with JSON piping.

## Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (stable toolchain)
- [Nushell](https://www.nushell.sh/) v0.110.0+ _(only for the plugin binary)_

## Build

### Both binaries at once

```sh
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --features plugin,cli
```

### Plugin only (default)

```sh
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml
```

### Standalone CLI only (no Nushell dependency)

```sh
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --no-default-features --features cli
```

Binaries are written to:

```
target/release/nu_plugin_topology   # Nushell plugin (8.9 MB)
target/release/topology              # Standalone CLI (1.3 MB)
```

---

## Standalone CLI (`topology`)

Works without Nushell. Reads JSON from stdin, writes JSON to stdout.

### Install

Copy the binary somewhere on your `$PATH`:

```sh
cp /workspaces/bookmarks/packages/nu_plugin_topology/target/release/topology ~/.local/bin/
```

### Commands

```sh
topology --help
```

```
Commands:
  fingerprint    Compute SimHash fingerprints for JSON records
  sample         Sample rows from a JSON array
  analyze        Analyze table structure and field statistics
  similarity     Compute string similarity between two strings
  normalize-url  Normalize a URL for deduplication
```

### `topology fingerprint`

Pipe a JSON array of objects. Each object gets a `_fingerprint` field appended.

```sh
echo '[{"content": "rust systems programming"}, {"content": "cooking Italian recipes"}]' \
  | topology fingerprint
```

```json
[
  { "content": "rust systems programming", "_fingerprint": "f88eec8c521637f5" },
  { "content": "cooking Italian recipes", "_fingerprint": "042090a0b9281711" }
]
```

Specify a different text field with `--field`:

```sh
echo '[{"title": "My Project"}]' | topology fingerprint --field title
```

Use TF-IDF weighted mode for better accuracy on large datasets:

```sh
cat bookmarks.json | topology fingerprint --weighted
```

#### Detect duplicates

These produce the **same** fingerprint (word order doesn't matter):

```sh
echo '[{"content": "rust fast safe"}, {"content": "rust safe fast"}]' \
  | topology fingerprint
```

### `topology sample`

```sh
cat data.json | topology sample --size 10
cat data.json | topology sample --size 5 --strategy stratified --field lang
cat data.json | topology sample --size 50 --strategy reservoir --seed 12345
```

Strategies: `random` (default), `stratified`, `systematic`, `reservoir`.

### `topology analyze`

```sh
cat data.json | topology analyze
```

Returns field-level statistics: cardinality, null counts, type distribution, top values.

### `topology similarity`

Direct string comparison — no stdin needed:

```sh
topology similarity "kitten" "sitting" --metric levenshtein
```

```json
{ "a": "kitten", "b": "sitting", "metric": "levenshtein", "similarity": 0.571 }
```

Metrics: `levenshtein`, `jaro-winkler`, `cosine`.

### `topology normalize-url`

```sh
topology normalize-url "https://www.example.com:443/path?utm_source=google&id=123#section"
```

```json
{
  "original": "https://www.example.com:443/path?utm_source=google&id=123#section",
  "normalized": "https://example.com/path?id=123",
  "canonical_key": "example.com/path?id=123"
}
```

Strips: `www.`, default ports, tracking params (`utm_*`, `fbclid`, etc.), fragments. Sorts query params.

### Piping with other tools

```sh
# jq → topology → jq
cat stars.json | jq '[.[] | {content: .description}]' | topology fingerprint | jq '.[].\_fingerprint'

# curl → topology
curl -s https://api.example.com/items | topology analyze

# Find duplicates with jq
cat data.json | topology fingerprint | jq 'group_by(._fingerprint) | map(select(length > 1))'
```

---

## Nushell Plugin (`nu_plugin_topology`)

### Install

Two steps — register the binary, then load it.

#### 1. Register

Writes the plugin path into Nushell's registry (`~/.config/nushell/plugin.msgpackz`). Run once, or again after rebuilding.

```nushell
plugin add /workspaces/bookmarks/packages/nu_plugin_topology/target/release/nu_plugin_topology
```

#### 2. Load

Spawns the plugin process and makes commands available in the current session.

```nushell
plugin use topology
```

To auto-load in every session, add to `~/.config/nushell/config.nu`:

```nushell
plugin use topology
```

#### Verify

```nushell
plugin list | where name == topology
```

```
╭───┬──────────┬─────────┬────────┬─────╮
│ # │   name   │ version │ status │ pid │
├───┼──────────┼─────────┼────────┼─────┤
│ 0 │ topology │ 0.1.0   │ loaded │ ... │
╰───┴──────────┴─────────┴────────┴─────╯
```

### `topology fingerprint`

```nushell
# Fingerprint the "content" column (default)
[{content: "rust systems programming language"}] | topology fingerprint

# Specify a different field
[[title]; ["My Rust Project"]] | topology fingerprint --field title

# TF-IDF weighted mode
open data.json | topology fingerprint --weighted
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--field` | `-f` | `content` | Column containing text to fingerprint |
| `--weighted` | `-w` | off | Use TF-IDF weights (better for large corpora) |

Appends a `_fingerprint` column (16-char hex string) to each row.

### `topology sample`

```nushell
# Random sample of 5 rows
seq 1 100 | wrap id | topology sample --size 5

# Stratified by language
[[name lang]; [a rust] [b go] [c rust] [d py] [e go] [f rust]] | topology sample --size 3 --strategy stratified --field lang

# Reproducible with seed
open data.json | topology sample --size 50 --seed 12345
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--size` | `-n` | `100` | Number of rows to sample |
| `--strategy` | `-s` | `random` | `random`, `stratified`, `systematic`, `reservoir` |
| `--field` | `-f` | — | Column to stratify by (required for `stratified`) |
| `--seed` | — | `42` | Random seed for reproducibility |

### `topology analyze`

```nushell
[[name lang stars]; [foo rust 100] [bar go 50] [baz rust 200]] | topology analyze
```

Returns a record with `total_rows`, `columns`, `num_columns`, and `fields` (per-column stats).

```nushell
# Drill into a specific field
open data.json | topology analyze | get fields.lang
```

### Uninstall

```nushell
plugin rm topology
```

---

## Run tests

```sh
cargo test --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml
```

36 unit tests covering all algorithm modules.

## Rebuild after changes

```sh
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --features plugin,cli
```

Then re-register and reload the plugin:

```nushell
plugin add /workspaces/bookmarks/packages/nu_plugin_topology/target/release/nu_plugin_topology
plugin use topology
```

## Project structure

```
nu_plugin_topology/
├── Cargo.toml
├── README.md
├── src/
│   ├── main.rs                  # Nushell plugin entry: serve_plugin()
│   ├── cli.rs                   # Standalone CLI entry: clap
│   ├── lib.rs                   # TopologyPlugin + shared algo re-export
│   ├── commands/                # Nushell PluginCommand impls (feature = "plugin")
│   │   ├── mod.rs
│   │   ├── sample.rs
│   │   ├── fingerprint.rs
│   │   └── analyze.rs
│   └── algo/                    # Core algorithms (no Nushell dependency)
│       ├── mod.rs
│       ├── tokenizer.rs         # Unicode word/n-gram tokenization
│       ├── tfidf.rs             # TF-IDF + BM25 scoring
│       ├── simhash.rs           # 64-bit SimHash fingerprinting
│       ├── string_distance.rs   # Levenshtein, Jaro-Winkler, cosine
│       ├── url_normalize.rs     # URL canonicalization
│       └── sampling.rs          # Random, stratified, systematic, reservoir
├── tests/
└── benches/
```

## Roadmap

- **Phase 2:** `topology classify` (BM25 rule-based), `topology generate` (HAC taxonomy), `topology tags` (TF-IDF extraction), `topology topics` (NMF)
- **Phase 3:** `topology dedup` (LSH + MinHash), `topology organize`, `topology similarity`
- **Phase 4:** Nushell module with adapters (stars, bookmarks, filesystem), Polars integration, Cloudflare Workers AI embeddings
