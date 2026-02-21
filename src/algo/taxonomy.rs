use serde::{Deserialize, Serialize};

/// A taxonomy category with keywords for BM25 matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Category {
    pub name: String,
    pub keywords: Vec<String>,
    pub children: Vec<Category>,
}

/// A full taxonomy tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Taxonomy {
    pub name: String,
    pub version: String,
    pub categories: Vec<Category>,
}

impl Taxonomy {
    /// Flatten into a list of (path, keywords) for matching.
    /// path is "Parent > Child > Grandchild" style.
    pub fn flatten(&self) -> Vec<(String, Vec<String>)> {
        let mut result = Vec::new();
        for cat in &self.categories {
            flatten_recursive(cat, "", &mut result);
        }
        result
    }
}

fn flatten_recursive(cat: &Category, prefix: &str, out: &mut Vec<(String, Vec<String>)>) {
    let path = if prefix.is_empty() {
        cat.name.clone()
    } else {
        format!("{prefix} > {}", cat.name)
    };

    out.push((path.clone(), cat.keywords.clone()));

    for child in &cat.children {
        flatten_recursive(child, &path, out);
    }
}

/// Built-in default taxonomy: 17 categories derived from bitmarks2 + keyword expansions.
pub fn default_taxonomy() -> Taxonomy {
    Taxonomy {
        name: "default".into(),
        version: "1.0.0".into(),
        categories: vec![
            cat("AI & Machine Learning", &[
                "ai", "artificial intelligence", "machine learning", "deep learning", "neural network",
                "llm", "large language model", "gpt", "transformer", "nlp", "natural language",
                "computer vision", "reinforcement learning", "diffusion", "embedding", "vector",
                "rag", "retrieval augmented", "fine-tuning", "inference", "training", "model",
                "pytorch", "tensorflow", "huggingface", "openai", "anthropic", "langchain",
            ]),
            cat("Web Development", &[
                "web", "frontend", "backend", "fullstack", "html", "css", "javascript",
                "typescript", "react", "vue", "svelte", "angular", "nextjs", "nuxt", "astro",
                "tailwind", "webpack", "vite", "bundler", "spa", "ssr", "pwa", "responsive",
                "browser", "dom", "http", "rest", "graphql", "api", "fetch", "ajax",
                "component", "framework", "ui", "ux", "design system",
            ]),
            cat("DevOps & Infrastructure", &[
                "devops", "infrastructure", "ci", "cd", "pipeline", "deploy", "deployment",
                "docker", "container", "kubernetes", "k8s", "terraform", "ansible", "pulumi",
                "aws", "azure", "gcp", "cloud", "serverless", "lambda", "cloudflare", "workers",
                "monitoring", "observability", "logging", "metrics", "grafana", "prometheus",
                "nginx", "caddy", "load balancer", "cdn", "dns", "ssl", "tls",
            ]),
            cat("Programming Languages", &[
                "programming language", "compiler", "interpreter", "runtime", "syntax",
                "rust", "go", "golang", "python", "ruby", "java", "kotlin", "swift",
                "zig", "nim", "elixir", "erlang", "haskell", "ocaml", "clojure", "lisp",
                "c++", "cpp", "carbon", "mojo", "gleam", "roc", "vale",
            ]),
            cat("Databases & Storage", &[
                "database", "sql", "nosql", "postgresql", "postgres", "mysql", "sqlite",
                "mongodb", "redis", "memcached", "elasticsearch", "clickhouse", "duckdb",
                "supabase", "neon", "planetscale", "turso", "libsql", "drizzle", "prisma",
                "orm", "migration", "schema", "query", "index", "replication", "sharding",
                "key-value", "kv", "graph database", "vector database", "time series",
            ]),
            cat("Security & Privacy", &[
                "security", "privacy", "encryption", "cryptography", "authentication", "auth",
                "oauth", "jwt", "certificate", "ssl", "tls", "vulnerability", "exploit",
                "penetration", "pentest", "firewall", "waf", "zero trust", "rbac", "acl",
                "password", "hash", "2fa", "mfa", "totp", "passkey", "webauthn", "fido",
                "audit", "compliance", "gdpr", "soc2",
            ]),
            cat("CLI & Terminal", &[
                "cli", "command line", "terminal", "shell", "bash", "zsh", "nushell", "fish",
                "tui", "ncurses", "prompt", "dotfiles", "tmux", "zellij", "multiplexer",
                "argument parser", "clap", "commander", "readline", "repl", "scripting",
                "automation", "cron", "task runner", "makefile", "justfile",
            ]),
            cat("Data Science & Analytics", &[
                "data science", "analytics", "statistics", "visualization", "chart", "graph",
                "dashboard", "bi", "business intelligence", "pandas", "polars", "dataframe",
                "jupyter", "notebook", "r language", "matplotlib", "plotly", "d3", "observable",
                "etl", "data pipeline", "data engineering", "spark", "hadoop", "airflow",
                "dbt", "warehouse", "data lake", "parquet", "arrow", "csv",
            ]),
            cat("Systems Programming", &[
                "systems", "operating system", "kernel", "driver", "firmware", "embedded",
                "bare metal", "rtos", "memory", "allocator", "garbage collection", "concurrency",
                "async", "parallel", "thread", "mutex", "lock-free", "atomic", "simd",
                "wasm", "webassembly", "assembly", "asm", "linker", "elf", "binary",
                "ffi", "binding", "interop", "syscall", "io_uring", "epoll",
            ]),
            cat("Networking & Protocols", &[
                "network", "protocol", "tcp", "udp", "http", "http2", "http3", "quic",
                "websocket", "grpc", "protobuf", "mqtt", "amqp", "zeromq", "nats",
                "proxy", "vpn", "wireguard", "tunnel", "socket", "dns", "dhcp",
                "peer-to-peer", "p2p", "torrent", "ipfs", "libp2p", "mesh",
            ]),
            cat("Mobile Development", &[
                "mobile", "ios", "android", "react native", "flutter", "dart", "swift",
                "kotlin", "objective-c", "xcode", "app store", "play store", "capacitor",
                "ionic", "expo", "native", "hybrid", "responsive", "pwa", "cordova",
            ]),
            cat("Game Development", &[
                "game", "gamedev", "game engine", "unity", "unreal", "godot", "bevy",
                "graphics", "opengl", "vulkan", "directx", "webgl", "webgpu", "shader",
                "rendering", "3d", "2d", "physics", "ecs", "entity component", "sprite",
                "pixel art", "procedural generation", "roguelike",
            ]),
            cat("Developer Tools", &[
                "developer tool", "devtool", "editor", "ide", "neovim", "vim", "emacs",
                "vscode", "zed", "helix", "lsp", "language server", "debugger", "profiler",
                "linter", "formatter", "prettier", "eslint", "biome", "oxlint",
                "git", "version control", "diff", "merge", "code review", "copilot",
                "documentation", "readme", "changelog", "package manager", "registry",
            ]),
            cat("Blockchain & Crypto", &[
                "blockchain", "cryptocurrency", "bitcoin", "ethereum", "solana", "web3",
                "smart contract", "solidity", "defi", "nft", "token", "wallet", "dapp",
                "consensus", "proof of work", "proof of stake", "layer 2", "rollup",
            ]),
            cat("Design & UI/UX", &[
                "design", "ui", "ux", "user interface", "user experience", "figma",
                "sketch", "prototype", "wireframe", "mockup", "color", "typography",
                "icon", "illustration", "animation", "motion", "accessibility", "a11y",
                "responsive design", "design system", "component library", "storybook",
            ]),
            cat("Media & Content", &[
                "media", "image", "video", "audio", "streaming", "ffmpeg", "codec",
                "compression", "transcoding", "podcast", "music", "photo", "camera",
                "pdf", "markdown", "latex", "publishing", "blog", "cms", "content",
                "rss", "feed", "newsletter", "email",
            ]),
            cat("Productivity & Knowledge", &[
                "productivity", "note", "knowledge", "wiki", "bookmark", "todo", "task",
                "calendar", "time tracking", "pomodoro", "project management", "kanban",
                "obsidian", "notion", "logseq", "roam", "zettelkasten", "pkm",
                "second brain", "spaced repetition", "flashcard", "learning",
                "search", "index", "catalog", "organize", "tag", "classify",
            ]),
        ],
    }
}

fn cat(name: &str, keywords: &[&str]) -> Category {
    Category {
        name: name.into(),
        keywords: keywords.iter().map(|s| s.to_string()).collect(),
        children: vec![],
    }
}

/// Parse a taxonomy from JSON/NUON string.
pub fn parse_taxonomy(json: &str) -> Result<Taxonomy, String> {
    serde_json::from_str(json).map_err(|e| format!("Failed to parse taxonomy: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_taxonomy_has_17_categories() {
        let t = default_taxonomy();
        assert_eq!(t.categories.len(), 17);
    }

    #[test]
    fn flatten_produces_paths() {
        let t = default_taxonomy();
        let flat = t.flatten();
        assert_eq!(flat.len(), 17);
        assert!(flat.iter().any(|(path, _)| path == "AI & Machine Learning"));
    }

    #[test]
    fn taxonomy_roundtrip() {
        let t = default_taxonomy();
        let json = serde_json::to_string(&t).unwrap();
        let parsed = parse_taxonomy(&json).unwrap();
        assert_eq!(parsed.categories.len(), 17);
    }

    #[test]
    fn each_category_has_keywords() {
        let t = default_taxonomy();
        for cat in &t.categories {
            assert!(!cat.keywords.is_empty(), "Category '{}' has no keywords", cat.name);
        }
    }
}
