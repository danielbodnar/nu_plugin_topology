# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository. For end-user docs, see `README.md`.

## What This Is

A Rust crate producing two binaries from shared algorithm modules. Four runtime modes:

| Binary | Feature | Mode | Protocol | Use case |
|--------|---------|------|----------|----------|
| `nu_plugin_topology` | `plugin` (default) | Nushell plugin | MsgPack over stdio | Interactive shell pipelines |
| `topology` | `cli` | CLI | JSON stdin → JSON stdout | Scripts, CI, non-Nushell users |
| `topology --mcp` | `mcp` | MCP server | JSON-RPC over stdio | AI assistants (Claude Desktop, Cursor, etc.) |
| `topology --lsp` | `lsp` | LSP server | JSON-RPC over stdio | Editors (VS Code, Zed, Neovim, etc.) |

All four modes call into `src/algo/` which contains all algorithms with zero Nushell, clap, MCP, or LSP dependency.

## Build Commands

```sh
# All four modes (most common during development)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --features plugin,cli,mcp,lsp

# Plugin only (default feature)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml

# CLI only (no Nushell deps — smallest binary)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --no-default-features --features cli

# CLI + MCP + LSP (no Nushell deps)
cargo build --release --manifest-path /workspaces/bookmarks/packages/nu_plugin_topology/Cargo.toml --no-default-features --features cli,mcp,lsp
```

## Test Commands

```sh
# All unit tests (163 tests across 12 algo modules)
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

### Feature flags

```
              ┌──────────┐
              │  algo/   │  ← always compiled, zero external deps
              └────┬─────┘
         ┌─────────┼─────────┐
         ▼         ▼         ▼
    ┌─────────┐ ┌─────┐ ┌─────────┐
    │ plugin  │ │ cli │ │commands/│
    └─────────┘ └──┬──┘ └─────────┘
                   │         ▲
              ┌────┼────┐    │
              ▼    ▼    │    │ cfg(feature = "plugin")
           ┌─────┐ ┌─────┐
           │ mcp │ │ lsp │
           └─────┘ └─────┘
```

| Feature | Dependencies | Extends | Notes |
|---------|-------------|---------|-------|
| `plugin` (default) | `nu-plugin`, `nu-protocol` | — | Nushell MsgPack protocol |
| `cli` | `clap` | — | Standalone JSON CLI |
| `mcp` | `rmcp`, `tokio`, `schemars` | `cli` | `--mcp` flag on the `topology` binary |
| `lsp` | `tower-lsp`, `tokio`, `schemars` | `cli` | `--lsp` flag on the `topology` binary |

Both `mcp` and `lsp` extend `cli` because they share the `topology` binary entry point (`src/cli.rs`).

### Source tree

```
src/
├── main.rs          # plugin entry: serve_plugin(&TopologyPlugin, MsgPackSerializer)
├── cli.rs           # standalone entry: clap CLI + --mcp/--lsp dispatch
├── lib.rs           # TopologyPlugin struct (cfg plugin), re-exports algo/, mcp, lsp
├── mcp.rs           # MCP server (cfg mcp): rmcp tool_router, 11 tools, stdio transport
├── lsp.rs           # LSP server (cfg lsp): tower-lsp, 11 workspace/executeCommand, stdio
├── algo/            # Pure algorithms — no nu-plugin, clap, rmcp, or tower-lsp deps
│   ├── tokenizer        # Unicode word segmentation, stopword filter, n-grams
│   ├── tfidf            # Corpus with TF-IDF vectors + BM25 scoring
│   ├── simhash          # 64-bit SimHash fingerprinting (SipHash + weighted bits)
│   ├── minhash          # MinHash signatures (k=128 permutations, Jaccard estimation)
│   ├── lsh              # LSH banding index (MinHash bands + SimHash bit bands)
│   ├── clustering       # HAC (Ward/complete/average/single) + dendrogram cutting
│   ├── nmf              # Non-negative Matrix Factorization (multiplicative updates)
│   ├── discover         # Full auto-discovery pipeline: sample → cluster → label → classify
│   ├── taxonomy         # Taxonomy data types + JSON serialization (no hardcoded data)
│   ├── sampling         # Random, stratified, systematic, reservoir (all seeded LCG)
│   ├── string_distance  # Levenshtein, Jaro-Winkler, cosine bigram
│   └── url_normalize    # URL canonicalization (strip tracking, www, ports, fragments) + slugify
└── commands/        # Nushell PluginCommand impls (feature = "plugin")
    ├── util             # normalize_input(), append_column(), append_columns()
    ├── sample           # topology sample
    ├── fingerprint      # topology fingerprint
    ├── analyze          # topology analyze
    ├── classify         # topology classify (auto-discovers taxonomy from data)
    ├── generate_taxonomy  # topology generate
    ├── tags             # topology tags
    ├── topics           # topology topics
    ├── dedup            # topology dedup
    ├── organize         # topology organize
    └── similarity       # topology similarity
```

### Command surface area

All commands are available across all interfaces. The plugin has 10 commands (no `normalize-url`); CLI, MCP, and LSP each expose 11 commands (no plugin-specific `topology` prefix needed).

| Command | Plugin | CLI | MCP | LSP |
|---------|--------|-----|-----|-----|
| `sample` | ✓ | ✓ | ✓ | ✓ |
| `fingerprint` | ✓ | ✓ | ✓ | ✓ |
| `analyze` | ✓ | ✓ | ✓ | ✓ |
| `classify` | ✓ | ✓ | ✓ | ✓ |
| `tags` | ✓ | ✓ | ✓ | ✓ |
| `dedup` | ✓ | ✓ | ✓ | ✓ |
| `similarity` | ✓ | ✓ | ✓ | ✓ |
| `normalize-url` | — | ✓ | ✓ | ✓ |
| `generate` | ✓ | ✓ | ✓ | ✓ |
| `topics` | ✓ | ✓ | ✓ | ✓ |
| `organize` | ✓ | ✓ | ✓ | ✓ |

### MCP server (`src/mcp.rs`)

Uses `rmcp` with `#[tool_router]` + `#[tool]` macros. Each tool maps 1:1 with a CLI subcommand:
- Parameter structs derive `JsonSchema` (via `schemars`) for auto-generated input schemas
- Heavy computation runs on `tokio::task::spawn_blocking` to avoid blocking the async transport
- `serve_stdio()` is the entry point, called from `cli.rs` when `--mcp` is passed

### LSP server (`src/lsp.rs`)

Uses `tower-lsp` with `workspace/executeCommand`. Each command is prefixed `topology.` (e.g., `topology.fingerprint`):
- Commands accept a single JSON object argument with operation-specific fields
- All 8 commands are registered via `ExecuteCommandOptions` in `initialize`
- `serve_stdio()` is the entry point, called from `cli.rs` when `--lsp` is passed

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

## Scripts

Three production Nushell scripts in `scripts/` demonstrate end-to-end topology pipelines. Each follows the same pattern: load → normalize → classify → tags → dedup → organize → save report.

| Script | Source data | Default clusters | Dedup strategy |
|--------|-----------|-----------------|----------------|
| `scripts/stars.nu` | GitHub stars JSON (`~/.config/bookmarks/gh-stars.raw.json`) | 15 | `combined` |
| `scripts/bookmarks.nu` | Chrome Bookmarks JSON (`/workspaces/bookmarks/Bookmarks`) | 1000 | `fuzzy` |
| `scripts/directory.nu` | Filesystem directory scan | 10 | (none — paths are unique) |

Usage:

```nushell
# GitHub stars
nu scripts/stars.nu --dry-run --clusters 5

# Chrome bookmarks
nu scripts/bookmarks.nu --dry-run --clusters 20

# Filesystem
nu scripts/directory.nu src/ --dry-run --clusters 5
```

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

## How to Add a New Algorithm

1. Create `src/algo/<name>.rs` with the algorithm implementation and `#[cfg(test)] mod tests`
2. Add `pub mod <name>;` to `src/algo/mod.rs`
3. Run `cargo test --lib algo::<name>` to verify
4. The new module is now available to all interfaces (plugin, CLI, MCP, LSP)

## How to Add a New Command

A new command must be wired into up to 4 places (depending on which interfaces should expose it):

1. **Plugin** (`src/commands/`): Create `src/commands/<name>.rs` implementing `PluginCommand`. Add `pub mod <name>;` + `pub use <name>::<Name>;` to `src/commands/mod.rs`. Register in `lib.rs::TopologyPlugin::commands()`.
2. **CLI** (`src/cli.rs`): Add a variant to the `Commands` enum with `#[derive(Subcommand)]` fields. Add a match arm in `main()` dispatching to a `cmd_<name>()` function.
3. **MCP** (`src/mcp.rs`): Add a `<Name>Params` struct deriving `JsonSchema`. Add a `#[tool]` method on `TopologyMcp`. Add a `do_<name>()` pure function.
4. **LSP** (`src/lsp.rs`): Add a `COMMAND_<NAME>` constant to `ALL_COMMANDS`. Add a match arm in `execute_command()` dispatching to an `exec_<name>()` function.

## Remaining Work

- Integration tests in `tests/` using `nu-plugin-test-support`
- Benchmarks in `benches/` (fingerprint + dedup at 20k scale)
- SQLite persistent cache in `src/storage/` (rusqlite with FTS5, behind `sqlite` feature flag)
- Documentation site in `docs/` (Astro Starlight)
- Cloudflare Workers AI embeddings endpoint
