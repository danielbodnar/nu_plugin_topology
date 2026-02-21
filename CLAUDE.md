# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Rust crate producing two binaries from shared algorithm modules:

- **`nu_plugin_topology`** — Nushell plugin (feature `plugin`). Communicates via MsgPack protocol. Requires Nushell 0.110.0+.
- **`topology`** — Standalone CLI (feature `cli`). Reads JSON stdin, writes JSON stdout. No Nushell dependency.

Both binaries call into `src/algo/` which contains all algorithms with zero Nushell or CLI dependency.

## Build Commands

```sh
# Both binaries (most common)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --features plugin,cli

# Plugin only (default feature)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml

# CLI only (no Nushell deps — 1.3MB vs 8.9MB)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --no-default-features --features cli
```

## Test Commands

```sh
# All unit tests (78 tests across algo modules)
cargo test --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --lib

# Single module
cargo test --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --lib algo::discover

# Single test
cargo test --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --lib algo::simhash::tests::identical_input_same_hash
```

## Register Plugin in Nushell

After building, register and load:

```nushell
plugin add /workspaces/bookmarks/packages/nu_plugin_topology/target/release/nu_plugin_topology
plugin use topology
```

Must re-run both commands after every rebuild.

## Architecture

### Feature-gated dual binary

`Cargo.toml` defines two `[[bin]]` targets with `required-features`. The `algo/` module is unconditional (always compiled). The `commands/` module is gated behind `#[cfg(feature = "plugin")]`. The CLI (`src/cli.rs`) is gated behind `required-features = ["cli"]`.

```
src/
├── main.rs          # plugin entry: serve_plugin(&TopologyPlugin, MsgPackSerializer)
├── cli.rs           # standalone entry: clap derives, reads JSON stdin
├── lib.rs           # TopologyPlugin struct (cfg plugin), re-exports algo/
├── algo/            # Pure algorithms — no nu-plugin or clap deps
│   ├── tokenizer    # Unicode word segmentation, stopword filter, n-grams
│   ├── tfidf        # Corpus with TF-IDF vectors + BM25 scoring
│   ├── simhash      # 64-bit SimHash fingerprinting (SipHash + weighted bits)
│   ├── minhash      # MinHash signatures (k=128 permutations, Jaccard estimation)
│   ├── lsh          # LSH banding index (MinHash bands + SimHash bit bands)
│   ├── clustering   # HAC (Ward/complete/average/single) + dendrogram cutting
│   ├── nmf          # Non-negative Matrix Factorization (multiplicative updates)
│   ├── discover     # Full auto-discovery pipeline: sample → cluster → label → classify
│   ├── taxonomy     # Taxonomy data types + JSON serialization (no hardcoded data)
│   ├── sampling     # Random, stratified, systematic, reservoir (all seeded LCG)
│   ├── string_distance  # Levenshtein, Jaro-Winkler, cosine bigram
│   └── url_normalize    # URL canonicalization (strip tracking, www, ports, fragments)
└── commands/        # Nushell PluginCommand impls (feature = "plugin")
    ├── util         # normalize_input(), append_column(), append_columns()
    ├── sample       # topology sample
    ├── fingerprint  # topology fingerprint
    ├── analyze      # topology analyze
    ├── classify     # topology classify (auto-discovers taxonomy from data)
    ├── generate_taxonomy  # topology generate
    ├── tags         # topology tags
    ├── topics       # topology topics
    ├── dedup        # topology dedup
    ├── organize     # topology organize
    └── similarity   # topology similarity
```

### Input normalization

All plugin commands accept `Type::Any` input. `commands/util.rs::normalize_input()` coerces any `PipelineData` into `Vec<Value>` of records:

- String → `{content: "..."}` (aligns with `--field content` default)
- List of strings → `[{content: s1}, {content: s2}, ...]`
- Single record → `[record]`
- List of non-records → `[{value: v1}, {value: v2}, ...]`
- Table → passthrough

Every command that processes text reads from the `--field` flag (default `content`). This convention means `"text" | topology fingerprint` works because the normalizer wraps the string as `{content: "text"}`.

### Dynamic taxonomy discovery

`topology classify` does NOT use hardcoded categories. The pipeline:

1. Extract text from `--field` on all rows
2. If no `--taxonomy` file provided, auto-discover via `algo::discover`:
   - Sample up to `--sample` items (default 500, because HAC is O(n²))
   - Build TF-IDF corpus, compute cosine distance matrix
   - Run HAC clustering with `--linkage` method (default Ward)
   - Cut dendrogram at `--clusters` (default 15)
   - Label each cluster by top TF-IDF terms
   - Produce `Taxonomy` struct with keyword lists per cluster
3. BM25-classify all items against discovered (or loaded) taxonomy
4. Append `_category`, `_hierarchy`, `_confidence` columns

### Dependency pinning

`interprocess` must be pinned to `=2.2.2` because `nu-plugin-core` 0.110.0 uses an import path (`ListenerNonblockingMode`) that moved in interprocess 2.3+. This pin is required until nu-plugin bumps its interprocess dependency.

## Plugin Command Reference

| Command | Input | Adds Columns | Key Flags |
|---------|-------|-------------|-----------|
| `topology sample` | any | — (subsets rows) | `--size`, `--strategy`, `--field`, `--seed` |
| `topology fingerprint` | any | `_fingerprint` | `--field`, `--weighted` |
| `topology analyze` | any | — (returns report record) | — |
| `topology classify` | any | `_category`, `_hierarchy`, `_confidence` | `--field`, `--clusters`, `--taxonomy`, `--threshold`, `--linkage` |
| `topology generate` | any | — (returns taxonomy record) | `--field`, `--depth`, `--linkage`, `--top-terms` |
| `topology tags` | any | `_tags` | `--field`, `--count` |
| `topology topics` | any | — (returns topics record) | `--field`, `--topics`, `--terms`, `--iterations` |
| `topology dedup` | any | `_dup_group`, `_is_primary` | `--field`, `--url-field`, `--strategy`, `--threshold` |
| `topology organize` | any | `_output_path` | `--format`, `--output-dir`, `--category-field` |
| `topology similarity` | positional args | — (returns record) | `--metric`, `--all` |

## Test Datasets

- `~/.config/bookmarks/gh-stars.raw.json` — 17,296 GitHub stars (JSON array of GitHub API repo objects). Fields: `full_name`, `description`, `topics`, `language`, `html_url`.
- `/workspaces/bookmarks/Bookmarks` — Chrome bookmarks export.

Example pipeline for stars:

```nushell
open --raw ~/.config/bookmarks/gh-stars.raw.json | from json | each {|r|
  let topics = ($r.topics? | default [] | str join " ")
  {content: $"($r.full_name? | default '') ($r.description? | default '') ($topics) ($r.language? | default '')",
   url: ($r.html_url? | default ''), id: ($r.full_name? | default '')}
} | topology classify --clusters 15
```

## Remaining Work (from original plan)

### Phase 4: Nushell Module
Scaffold `~/.config/nushell/modules/topology/` with:
- Adapters: `stars.nu`, `bookmarks.nu`, `filesystem.nu`, `csv.nu` — normalize source data into universal `{id, content, url?, path?, source, metadata}` records
- Polars LazyFrame integration in `formatters/dataframe.nu`
- Cloudflare Workers AI embeddings in `commands/cloud.nu` via `http post`
- SQLite persistence in `core/storage.nu` (XDG paths)
- Config management in `core/config.nu`

### Phase 5: Integration & Polish
- Integration tests in `tests/` using `nu-plugin-test-support`
- Benchmarks in `benches/` (fingerprint + dedup at 20k scale)
- SQLite persistent cache in `src/storage/` (rusqlite with FTS5, behind `sqlite` feature flag)
- Documentation site in `docs/` (Astro Starlight)
