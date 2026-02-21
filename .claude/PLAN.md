# nu_plugin_topology — Coordination Plan

> **Last updated:** 2026-02-21
> **Purpose:** Coordination document for multiple Claude Code sessions working on this crate. Each session should check this file before starting work and update it when completing tasks.

---

## Current State

### Completed (this session)

- **ops.rs extraction** — Created `src/ops.rs` shared operations module (721 lines) containing all algorithm wrappers as `pub fn op_*()` functions. No feature gate — always compiled.
- **MCP dedup** — Rewrote `src/mcp.rs` (1246→482 lines) to be a thin adapter calling `ops::op_*()`. Parameter structs and `#[tool]` macros retained; all `do_*` functions removed.
- **LSP dedup** — Rewrote `src/lsp.rs` (906→297 lines) to be a thin adapter calling `ops::op_*()`. All `exec_*` bodies replaced with 1-3 line delegations.
- **CLI dedup** — Rewrote `src/cli.rs` (906→326 lines) to be a thin adapter calling `ops::op_*()`. Added `op_classify_from_file()` variant for file-based taxonomy loading.
- **normalize-url plugin command** — Created `src/commands/normalize_url.rs`, registered in `commands/mod.rs` and `lib.rs`. The plugin now has 11 commands (was 10).
- **All 11 tools in MCP/LSP/CLI** — `generate`, `topics`, `organize` were already present in MCP/LSP (added in prior session). Now all share `ops.rs`.
- **ops integration tests** — `tests/ops_test.rs` with 24 tests covering all 11 operations.
- **MCP integration tests** — `tests/mcp_test.rs` with 4 tests (initialize, tools/list, tools/call similarity, tools/call normalize_url).
- **All tests passing** — 181 unit (algo), 24 ops integration, 4 MCP integration = **209 tests total**.

### Files Modified/Created

| File | Action | Lines |
|------|--------|-------|
| `src/ops.rs` | CREATED | 721 |
| `src/lib.rs` | MODIFIED | +2 lines (`pub mod ops`, `NormalizeUrl` registration) |
| `src/mcp.rs` | REWRITTEN | 482 (was 1246) |
| `src/lsp.rs` | REWRITTEN | 297 (was 906) |
| `src/cli.rs` | REWRITTEN | 326 (was 906) |
| `src/commands/normalize_url.rs` | CREATED | 67 |
| `src/commands/mod.rs` | MODIFIED | +2 lines |
| `tests/ops_test.rs` | CREATED | 233 |
| `tests/mcp_test.rs` | CREATED | 204 |

### Git Status

All changes are **unstaged**. Nothing committed yet. The changes are on the `main` branch.

---

## Remaining Work

### High Priority

- [ ] **Integration tests with nu-plugin-test-support** — Test plugin commands through the Nushell plugin protocol. Files: `tests/plugin_test.rs`. Uses `nu-plugin-test-support` (already in dev-deps).
- [ ] **Benchmarks** — `benches/` directory with criterion benchmarks for fingerprint + dedup at 20k scale. Useful for performance regression detection.
- [ ] **Update CLAUDE.md** — Reflect the new `ops.rs` module in the architecture docs. Update command count (now 11 across all interfaces).

### Medium Priority

- [ ] **SQLite persistent cache** — `src/storage/` module with rusqlite + FTS5, behind a `sqlite` feature flag. Cache fingerprints, taxonomy results, and dedup groups for large datasets.
- [ ] **Cloudflare Workers AI embeddings** — Add an `embeddings` command that calls Workers AI for semantic similarity (as an alternative to the local TF-IDF approach).
- [ ] **Documentation site** — Astro Starlight site in `docs/` with usage guides, API reference, and architecture diagrams.

### Low Priority

- [ ] **LSP integration tests** — Similar to `tests/mcp_test.rs` but for the LSP protocol.
- [ ] **CI/CD pipeline** — GitHub Actions workflow for build, test, lint, and release.
- [ ] **Nushell module wrapper** — `scripts/topology.nu` module that wraps the plugin commands with Nushell-native argument handling and pipeline composition.

---

## Architecture Reference

```
src/
├── main.rs          # Plugin binary entry point
├── cli.rs           # CLI binary entry point (thin adapter → ops.rs)
├── lib.rs           # TopologyPlugin + module declarations
├── ops.rs           # ★ NEW: Shared operations (all algorithm wrappers)
├── mcp.rs           # MCP server (thin adapter → ops.rs)
├── lsp.rs           # LSP server (thin adapter → ops.rs)
├── algo/            # Pure algorithms (zero deps on protocol crates)
│   ├── tokenizer, tfidf, simhash, minhash, lsh
│   ├── clustering, nmf, discover, taxonomy
│   ├── sampling, string_distance, url_normalize
└── commands/        # Nushell PluginCommand impls
    ├── sample, fingerprint, analyze, classify
    ├── generate_taxonomy, tags, topics, dedup
    ├── organize, similarity, normalize_url ← NEW
    └── util
```

### Key Design Decision

`ops.rs` operates exclusively on `serde_json::Value` — the universal JSON interchange format. This means:
- No `nu-protocol` dependency (so it compiles without the `plugin` feature)
- No `rmcp` or `tower-lsp` dependency (so it compiles without `mcp`/`lsp` features)
- No `clap` dependency (so it compiles without the `cli` feature)
- All 4 interfaces (plugin, CLI, MCP, LSP) call the same code path

### Command Surface Area (11 commands, all interfaces)

| Command | Plugin | CLI | MCP | LSP |
|---------|--------|-----|-----|-----|
| `sample` | ✓ | ✓ | ✓ | ✓ |
| `fingerprint` | ✓ | ✓ | ✓ | ✓ |
| `analyze` | ✓ | ✓ | ✓ | ✓ |
| `classify` | ✓ | ✓ | ✓ | ✓ |
| `tags` | ✓ | ✓ | ✓ | ✓ |
| `dedup` | ✓ | ✓ | ✓ | ✓ |
| `similarity` | ✓ | ✓ | ✓ | ✓ |
| `normalize-url` | ✓ | ✓ | ✓ | ✓ |
| `generate` | ✓ | ✓ | ✓ | ✓ |
| `topics` | ✓ | ✓ | ✓ | ✓ |
| `organize` | ✓ | ✓ | ✓ | ✓ |

---

## Session Coordination Rules

1. **Check this file first** — Before starting work, read `PLAN.md` to see what's been done and what's claimed.
2. **Claim work** — Add your session ID and mark tasks as `[in progress - session X]` before starting.
3. **Update on completion** — Move completed items to the "Completed" section with a brief description.
4. **Don't overlap on files** — If another session is modifying `src/ops.rs`, don't also modify it. Coordinate via this file.
5. **Feature branches** — Each session should work on a feature branch. Merge conflicts resolved by the session that merges second.
6. **Test before declaring done** — Run `cargo test --lib` + `cargo test --test ops_test` + `cargo build --features plugin,cli,mcp,lsp` before marking anything complete.

### Build Commands Quick Reference

```sh
# Full build (all features)
cargo build --manifest-path Cargo.toml --features plugin,cli,mcp,lsp

# All unit tests (algo modules)
cargo test --manifest-path Cargo.toml --lib

# Ops integration tests
cargo test --manifest-path Cargo.toml --test ops_test

# MCP integration tests
cargo test --manifest-path Cargo.toml --test mcp_test --features cli,mcp

# Release build
cargo build --release --manifest-path Cargo.toml --features plugin,cli,mcp,lsp
```

---

## Nushell Scripts Status

| Script | Status | Notes |
|--------|--------|-------|
| `scripts/stars.nu` | Works | Processes GitHub stars JSON |
| `scripts/bookmarks.nu` | Works | Processes Chrome bookmarks |
| `scripts/directory.nu` | Works | Filesystem directory scan |

All scripts need `plugin add` + `plugin use topology` after each rebuild.
