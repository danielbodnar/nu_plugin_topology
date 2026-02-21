# Unified Development Plan — nu_plugin_topology

> **Last updated:** 2026-02-21
> **Purpose:** Coordination file for multiple concurrent Claude Code sessions working on this crate. Each session MUST read this file before starting work. If your session has a separate plan, merge it into this one.

## Active Sessions & File Ownership

**CRITICAL: Only one session may modify a file at a time.** Check this table before editing. If a file is claimed, do not touch it.

| File(s) | Claimed By | Status | Notes |
|---------|-----------|--------|-------|
| `src/ops.rs` | Session B (ops refactor) | IN PROGRESS | New shared operations layer |
| `src/mcp.rs` | Session B (ops refactor) | IN PROGRESS | Being simplified to use `ops::*` |
| `src/lsp.rs` | Session B (ops refactor) | IN PROGRESS | Being simplified to use `ops::*` |
| `src/cli.rs` | Session B (ops refactor) | IN PROGRESS | Being simplified to use `ops::*` |
| `src/lib.rs` | Session B (ops refactor) | IN PROGRESS | Added `pub mod ops` |
| `src/algo/cache.rs` | Session A (caching) | DONE | Cache types, hashing, validation |
| `src/algo/storage.rs` | Session A (caching) | NEXT | SQLite backend — not yet created |
| `src/algo/tfidf.rs` | Session A (caching) | DONE | Added Serialize/Deserialize |
| `src/algo/clustering.rs` | Session A (caching) | DONE | Added Serialize/Deserialize |
| `src/algo/discover.rs` | Session A (caching) | PENDING | Will add `discover_taxonomy_cached()` |
| `src/commands/cache_cmd.rs` | Session A (caching) | PENDING | New cache management command |
| `src/commands/normalize_url.rs` | Session C (normalize-url) | IN PROGRESS | New plugin command for normalize-url |
| `src/commands/mod.rs` | Session C (normalize-url) | IN PROGRESS | Added `normalize_url` + `NormalizeUrl` |
| `src/commands/*.rs` (others) | UNCLAIMED | — | Will need `--cache` flags eventually |
| `nushell/topology/**` | UNCLAIMED | DONE | Module with adapters, pipeline, reports, exports |
| `scripts/*.nu` | UNCLAIMED | DONE | Refactored; will need `--cache` later |
| `Cargo.toml` | Session A (caching) | PENDING | Will add `cache` feature + rusqlite |
| `CLAUDE.md` | UNCLAIMED | — | Update after each major feature lands |
| `tests/ops_test.rs` | Session B (ops refactor) | IN PROGRESS | Integration tests for ops module |
| `tests/mcp_test.rs` | Session B (ops refactor) | IN PROGRESS | MCP JSON-RPC integration tests |
| `benches/` | UNCLAIMED | — | Benchmarks |
| `docs/` | UNCLAIMED | — | Documentation site |

### How to Claim a File

Before editing a file not in your session's claim list:
1. Read this plan to check it's unclaimed
2. Add your session identifier and mark it IN PROGRESS
3. Proceed with edits

---

## Completed Work

### Phase A: Full command parity (DONE — committed)

All 11 commands now work across all 4 interfaces:

| Commit | Description |
|--------|-------------|
| `dbc9304` | feat: add MCP and LSP server support |
| `aacfd12` | feat: port generate, topics, organize to CLI/MCP/LSP |
| `3f2fc67` | feat(nushell): add topology module with adapters and pipeline |
| `e339199` | refactor(scripts): improve type safety and error handling |
| `66a3074` | feat(algo): add artifact caching and serialization support |

### Phase B: Serializable algo types (DONE — committed in `66a3074`)

- `Corpus` in `tfidf.rs` — `#[derive(Serialize, Deserialize)]`
- `Linkage`, `Dendrogram`, `Merge` in `clustering.rs` — `#[derive(Serialize, Deserialize)]`
- `cache.rs` — `ArtifactKind`, `CacheMeta`, `content_hash()`, `args_hash()`, `is_valid()`
- 16 new tests (3 serde roundtrips + 13 cache tests)
- Total tests: 181 passing

### Phase C: Nushell module (DONE — committed in `3f2fc67`)

- `nushell/topology/mod.nu` — module root re-exporting 7 submodules
- `nushell/topology/adapters/` — github-stars, chrome-bookmarks, directory, json adapters
- `nushell/topology/pipeline.nu` — composable run/classify/tags/dedup/organize
- `nushell/topology/report.nu` — stats (Polars-accelerated + fallback), summary, duplicates
- `nushell/topology/export.nu` — to-json, to-parquet, to-markdown, save-all

### Phase D: Script refactoring (DONE — committed in `e339199`)

- stars.nu, bookmarks.nu, directory.nu — type annotations, std/log, structured errors, Nushell idioms

---

## In-Progress Work

### Stream 1: Shared ops layer (Session B — UNCOMMITTED)

**Goal:** Extract duplicated algorithm-calling code from CLI, MCP, and LSP into a shared `src/ops.rs` module. Each `op_*` function takes `&[serde_json::Value]` and returns `serde_json::Value` — no framework dependencies.

**Current state:**
- `src/ops.rs` — CREATED, contains all 11 `op_*` functions + helpers (UnionFind, get_text)
- `src/mcp.rs` — MODIFIED, drastically simplified (~480 lines → thin wrappers calling `ops::*`)
- `src/lsp.rs` — MODIFIED, drastically simplified (~298 lines → thin dispatchers calling `ops::*`)
- `src/cli.rs` — MODIFIED, match arms simplified to call `ops::*`
- `src/lib.rs` — MODIFIED, added `pub mod ops`

**Remaining for this stream:**
- Verify the build compiles with all features: `cargo build --features plugin,cli,mcp,lsp`
- Run all tests: `cargo test --lib`
- Commit and push

**Impact on other streams:** The cache feature (Stream 2) should add `--cache` to `ops::op_classify()` rather than duplicating logic in CLI/MCP/LSP separately. This is actually _simpler_ — one `#[cfg(feature = "cache")]` path in `ops.rs` instead of three.

### Stream 1b: normalize-url plugin command (Session C — UNCOMMITTED)

**Goal:** Add `topology normalize-url` as a plugin command (it was previously CLI/MCP/LSP only).

**Current state:**
- `src/commands/normalize_url.rs` — CREATED, full `PluginCommand` impl
- `src/commands/mod.rs` — MODIFIED, added `mod normalize_url` + `pub use normalize_url::NormalizeUrl`

**Remaining for this stream:**
- Register `NormalizeUrl` in `src/lib.rs::TopologyPlugin::commands()` (but `lib.rs` is claimed by Session B)
- Verify build + test
- Commit and push

**Coordination note:** Sessions B and C both touch `src/lib.rs`. One must commit first, then the other rebases.

### Stream 1c: Integration tests (Session B — UNCOMMITTED)

**Current state:**
- `tests/ops_test.rs` — CREATED, tests for all `op_*` functions against sample JSON records
- `tests/mcp_test.rs` — CREATED, spawns `topology --mcp` and tests JSON-RPC protocol over stdio

**These depend on the ops refactor landing first.**

### Stream 2: Persistent caching (Session A — PARTIALLY STARTED)

**Goal:** SQLite-backed persistent cache for O(n²) artifacts (corpus, dendrogram, taxonomy, fingerprints). Opt-in via `--cache <path>`. Without `--cache`, behavior is identical to current.

**What's done:**
- Phase 1 (serde derives) — DONE
- Phase 2a (`src/algo/cache.rs`) — DONE
- Phase 2b (`src/algo/storage.rs`) — NOT STARTED (blocked: waiting to coordinate)

**Remaining phases:**
See detailed breakdown below in "Remaining Work" section.

---

## Remaining Work

### Priority 1: Merge ops refactor (Session B)

The `src/ops.rs` refactor must be committed first. It changes the "How to Add a New Command" pattern from "4 places" to "5 places" (ops.rs + 4 thin wrappers). The cache feature should be wired into `ops.rs` rather than the individual interfaces.

**Updated "How to Add a New Command" pattern after ops merge:**

1. **`src/ops.rs`**: Add `pub fn op_<name>(...)` — pure function, `Value` in/out
2. **Plugin** (`src/commands/<name>.rs`): Calls algo directly (Nushell `Value` ≠ JSON `Value`)
3. **CLI** (`src/cli.rs`): Calls `ops::op_<name>()`
4. **MCP** (`src/mcp.rs`): Calls `ops::op_<name>()` via `spawn_blocking`
5. **LSP** (`src/lsp.rs`): Calls `ops::op_<name>()` via `exec_<name>()`

### Priority 2: SQLite storage backend

**New file: `src/algo/storage.rs`** (feature-gated behind `cache`)

```rust
use rusqlite::{Connection, params};
use super::cache::{ArtifactKind, CacheMeta};

pub struct CacheDb {
    conn: Connection,
}
```

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS cache_artifacts (
    kind          TEXT NOT NULL,
    content_hash  INTEGER NOT NULL,
    args_hash     INTEGER NOT NULL,
    row_count     INTEGER NOT NULL,
    version       TEXT NOT NULL,
    created_at    INTEGER NOT NULL,
    payload       BLOB NOT NULL,
    UNIQUE(kind, content_hash, args_hash)
);
CREATE INDEX IF NOT EXISTS idx_cache_lookup ON cache_artifacts(kind, content_hash, args_hash);
```

**Methods:** `open_or_create`, `get`, `put`, `invalidate`, `invalidate_all`, `info`

**Cargo.toml changes:**
```toml
[features]
cache = ["dep:rusqlite"]

[dependencies]
rusqlite = { version = "0.33", features = ["bundled"], optional = true }
```

**Register in `src/algo/mod.rs`:**
```rust
#[cfg(feature = "cache")]
pub mod storage;
```

### Priority 3: Cached discover pipeline

**Modify: `src/algo/discover.rs`** — Add `discover_taxonomy_cached()`:

```rust
#[cfg(feature = "cache")]
pub fn discover_taxonomy_cached(
    texts: &[String],
    config: &DiscoverConfig,
    db: &storage::CacheDb,
) -> taxonomy::Taxonomy { ... }
```

Cache invalidation uses `cache::content_hash()` + `cache::args_hash()` + version check.

### Priority 4: Wire `--cache` into ops + commands

**After ops refactor lands**, add `--cache` parameter to:

| op function | Reads from cache | Writes to cache |
|-------------|-----------------|----------------|
| `op_classify` | taxonomy, corpus | taxonomy |
| `op_analyze` (extended) | — | corpus, fingerprints |
| `op_tags` | corpus | corpus |
| `op_dedup` | fingerprints | fingerprints |
| `op_fingerprint` | corpus (if weighted) | fingerprints |
| `op_generate` | corpus, dendrogram | corpus, dendrogram, taxonomy |
| `op_sample` | — | — (O(n), not worth caching) |
| `op_similarity` | — | — (single-pair, instant) |

Each `op_*` function gets an `Option<&CacheDb>` parameter (or `Option<&str>` cache path). When `cache` feature is disabled, these parameters don't exist (via `#[cfg]`).

**Interface wiring:**
- CLI: `--cache <path>` flag on relevant subcommands
- MCP: `cache` field on param structs
- LSP: `cache` field in JSON arg
- Plugin: `--cache` named flag on relevant commands
- Nushell scripts: `--cache` flag propagation

### Priority 5: Cache management command

**New: `src/commands/cache_cmd.rs`** + registration in ops/CLI/MCP/LSP

```nushell
topology cache info --path data.topology.db
topology cache clear --path data.topology.db --kind taxonomy
```

### Priority 6: Integration tests

**New: `tests/` directory**

Using `nu-plugin-test-support`:
- Roundtrip test: classify → tags → dedup pipeline
- Cache hit/miss test: two classify runs, second should be instant
- Each command: smoke test with small dataset

### Priority 7: Benchmarks

**New: `benches/` directory**

- `fingerprint` at 20k scale
- `classify` at 20k scale (with and without cache)
- `dedup` at 20k scale

### Priority 8: Documentation site

**New: `docs/` directory** (Astro Starlight)

- Getting started guide
- Command reference (auto-generated from plugin signatures)
- Architecture overview
- Cache system documentation

---

## Architecture After All Streams Merge

```
src/
├── main.rs              # Plugin entry
├── cli.rs               # CLI entry + --mcp/--lsp dispatch
├── lib.rs               # TopologyPlugin, pub mod {algo, ops, commands}
├── ops.rs               # Shared pure operations (Value → Value)
├── mcp.rs               # MCP server (thin wrappers → ops)
├── lsp.rs               # LSP server (thin dispatchers → ops)
├── algo/
│   ├── cache.rs         # Cache types + hashing (always compiled)
│   ├── storage.rs       # SQLite backend (feature = "cache")
│   ├── tfidf.rs         # Corpus (Serialize/Deserialize)
│   ├── clustering.rs    # HAC, Dendrogram (Serialize/Deserialize)
│   ├── discover.rs      # discover_taxonomy + discover_taxonomy_cached
│   ├── taxonomy.rs      # Taxonomy types + JSON serde
│   ├── simhash.rs       # 64-bit SimHash
│   ├── minhash.rs       # MinHash signatures
│   ├── lsh.rs           # LSH banding index
│   ├── nmf.rs           # NMF topic modeling
│   ├── sampling.rs      # Random/stratified/systematic/reservoir
│   ├── string_distance.rs
│   ├── tokenizer.rs
│   └── url_normalize.rs # + slugify()
├── commands/
│   ├── cache_cmd.rs     # topology cache (new)
│   └── ...              # existing plugin commands
nushell/
└── topology/            # Nushell module (adapters, pipeline, report, export)
scripts/                 # Production pipeline scripts
```

### Feature flags after cache feature

```toml
[features]
default = ["plugin"]
plugin = ["dep:nu-plugin", "dep:nu-protocol"]
cli = ["dep:clap"]
cache = ["dep:rusqlite"]
mcp = ["cli", "dep:rmcp", "dep:tokio", "dep:schemars"]
lsp = ["cli", "dep:tower-lsp", "dep:tokio", "dep:schemars"]
```

Build with everything: `cargo build --features plugin,cli,mcp,lsp,cache`

---

## Coordination Rules

- **One writer per file** — check the ownership table above
- **Always pull before starting** — `git pull` to get latest commits
- **Commit early** — small, focused commits with conventional commit messages
- **Update this plan** — mark items as DONE when complete, claim files when starting
- **Don't rewrite each other's code** — if you see something wrong, note it here rather than fixing it (unless it's your claimed file)
- **Cache goes through ops.rs** — after the ops refactor merges, all `--cache` logic lives in `ops::op_*` functions, not scattered across CLI/MCP/LSP
- **Test before committing** — `cargo test --lib` must pass. If adding cache feature: `cargo test --lib --features cache`
