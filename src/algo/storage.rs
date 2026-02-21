//! SQLite-backed persistent cache for topology artifacts.
//!
//! Each artifact is keyed by `(kind, content_hash, args_hash)` and stored as
//! a compressed JSON blob. The storage layer is opt-in via the `cache` feature.

use rusqlite::{params, Connection, OptionalExtension};

use super::cache::{ArtifactKind, CacheMeta};

/// SQLite-backed cache database for topology artifacts.
pub struct CacheDb {
    conn: Connection,
}

impl CacheDb {
    /// Open (or create) a cache database at the given path.
    pub fn open_or_create(path: &str) -> Result<Self, String> {
        let conn = Connection::open(path)
            .map_err(|e| format!("Failed to open cache DB at '{path}': {e}"))?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;",
        )
        .map_err(|e| format!("Failed to set PRAGMA: {e}"))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS cache_artifacts (
                kind          TEXT NOT NULL,
                content_hash  INTEGER NOT NULL,
                args_hash     INTEGER NOT NULL,
                row_count     INTEGER NOT NULL,
                version       TEXT NOT NULL,
                created_at    INTEGER NOT NULL,
                payload       BLOB NOT NULL,
                UNIQUE(kind, content_hash, args_hash)
            );
            CREATE INDEX IF NOT EXISTS idx_cache_lookup
                ON cache_artifacts(kind, content_hash, args_hash);",
        )
        .map_err(|e| format!("Failed to create cache schema: {e}"))?;

        Ok(Self { conn })
    }

    /// Retrieve a cached artifact. Returns `None` on cache miss.
    pub fn get(
        &self,
        kind: ArtifactKind,
        content_hash: u64,
        args_hash: u64,
    ) -> Result<Option<(CacheMeta, Vec<u8>)>, String> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT row_count, version, created_at, payload
                 FROM cache_artifacts
                 WHERE kind = ?1 AND content_hash = ?2 AND args_hash = ?3",
            )
            .map_err(|e| format!("Failed to prepare SELECT: {e}"))?;

        let result = stmt
            .query_row(
                params![kind.as_str(), content_hash as i64, args_hash as i64],
                |row| {
                    let row_count: i64 = row.get(0)?;
                    let version: String = row.get(1)?;
                    let created_at: i64 = row.get(2)?;
                    let payload: Vec<u8> = row.get(3)?;
                    Ok((row_count, version, created_at, payload))
                },
            )
            .optional()
            .map_err(|e| format!("Failed to query cache: {e}"))?;

        Ok(result.map(|(row_count, version, created_at, payload)| {
            let meta = CacheMeta {
                content_hash,
                row_count: row_count as usize,
                args_hash,
                version,
                created_at: created_at as u64,
            };
            (meta, payload)
        }))
    }

    /// Store (upsert) an artifact in the cache.
    pub fn put(
        &self,
        kind: ArtifactKind,
        meta: &CacheMeta,
        payload: &[u8],
    ) -> Result<(), String> {
        self.conn
            .execute(
                "INSERT INTO cache_artifacts (kind, content_hash, args_hash, row_count, version, created_at, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(kind, content_hash, args_hash)
                 DO UPDATE SET row_count = excluded.row_count,
                               version = excluded.version,
                               created_at = excluded.created_at,
                               payload = excluded.payload",
                params![
                    kind.as_str(),
                    meta.content_hash as i64,
                    meta.args_hash as i64,
                    meta.row_count as i64,
                    meta.version,
                    meta.created_at as i64,
                    payload,
                ],
            )
            .map_err(|e| format!("Failed to upsert cache artifact: {e}"))?;
        Ok(())
    }

    /// Invalidate a specific artifact kind, or all artifacts if `kind` is None.
    pub fn invalidate(&self, kind: Option<ArtifactKind>) -> Result<usize, String> {
        match kind {
            Some(k) => {
                let deleted = self
                    .conn
                    .execute(
                        "DELETE FROM cache_artifacts WHERE kind = ?1",
                        params![k.as_str()],
                    )
                    .map_err(|e| format!("Failed to invalidate: {e}"))?;
                Ok(deleted)
            }
            None => {
                let deleted = self
                    .conn
                    .execute("DELETE FROM cache_artifacts", [])
                    .map_err(|e| format!("Failed to invalidate all: {e}"))?;
                Ok(deleted)
            }
        }
    }

    /// Return metadata about all cached artifacts (for `topology cache info`).
    pub fn info(&self) -> Result<Vec<ArtifactInfo>, String> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT kind, content_hash, args_hash, row_count, version, created_at, length(payload)
                 FROM cache_artifacts
                 ORDER BY created_at DESC",
            )
            .map_err(|e| format!("Failed to prepare info query: {e}"))?;

        let rows = stmt
            .query_map([], |row| {
                Ok(ArtifactInfo {
                    kind: row.get::<_, String>(0)?,
                    content_hash: row.get::<_, i64>(1)? as u64,
                    args_hash: row.get::<_, i64>(2)? as u64,
                    row_count: row.get::<_, i64>(3)? as usize,
                    version: row.get(4)?,
                    created_at: row.get::<_, i64>(5)? as u64,
                    payload_bytes: row.get::<_, i64>(6)? as usize,
                })
            })
            .map_err(|e| format!("Failed to query info: {e}"))?;

        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(|e| format!("Failed to read row: {e}"))?);
        }
        Ok(result)
    }

    /// Total size of the database file in bytes (approximate).
    pub fn db_size_bytes(&self) -> Result<u64, String> {
        let page_count: i64 = self
            .conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .map_err(|e| format!("Failed to get page_count: {e}"))?;
        let page_size: i64 = self
            .conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .map_err(|e| format!("Failed to get page_size: {e}"))?;
        Ok((page_count * page_size) as u64)
    }
}

/// Info record for a cached artifact (returned by `CacheDb::info()`).
#[derive(Debug, Clone)]
pub struct ArtifactInfo {
    pub kind: String,
    pub content_hash: u64,
    pub args_hash: u64,
    pub row_count: usize,
    pub version: String,
    pub created_at: u64,
    pub payload_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> CacheDb {
        CacheDb::open_or_create(":memory:").unwrap()
    }

    #[test]
    fn open_or_create_in_memory() {
        let db = test_db();
        let info = db.info().unwrap();
        assert!(info.is_empty());
    }

    #[test]
    fn put_and_get_roundtrip() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        let payload = b"test payload data";

        db.put(ArtifactKind::Corpus, &meta, payload).unwrap();

        let result = db.get(ArtifactKind::Corpus, 111, 222).unwrap();
        assert!(result.is_some());
        let (got_meta, got_payload) = result.unwrap();
        assert_eq!(got_meta.content_hash, 111);
        assert_eq!(got_meta.args_hash, 222);
        assert_eq!(got_meta.row_count, 50);
        assert_eq!(got_payload, payload);
    }

    #[test]
    fn get_miss_returns_none() {
        let db = test_db();
        let result = db.get(ArtifactKind::Corpus, 999, 888).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn put_upsert_overwrites() {
        let db = test_db();
        let meta1 = CacheMeta::new(111, 50, 222);
        db.put(ArtifactKind::Corpus, &meta1, b"first").unwrap();

        let meta2 = CacheMeta::new(111, 100, 222);
        db.put(ArtifactKind::Corpus, &meta2, b"second").unwrap();

        let (got_meta, got_payload) = db.get(ArtifactKind::Corpus, 111, 222).unwrap().unwrap();
        assert_eq!(got_meta.row_count, 100);
        assert_eq!(got_payload, b"second");
    }

    #[test]
    fn different_kinds_are_independent() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        db.put(ArtifactKind::Corpus, &meta, b"corpus").unwrap();
        db.put(ArtifactKind::Taxonomy, &meta, b"taxonomy").unwrap();

        let corpus = db.get(ArtifactKind::Corpus, 111, 222).unwrap().unwrap();
        let taxonomy = db.get(ArtifactKind::Taxonomy, 111, 222).unwrap().unwrap();
        assert_eq!(corpus.1, b"corpus");
        assert_eq!(taxonomy.1, b"taxonomy");
    }

    #[test]
    fn different_args_hash_are_independent() {
        let db = test_db();
        let meta1 = CacheMeta::new(111, 50, 222);
        let meta2 = CacheMeta::new(111, 50, 333);
        db.put(ArtifactKind::Corpus, &meta1, b"args222").unwrap();
        db.put(ArtifactKind::Corpus, &meta2, b"args333").unwrap();

        let r1 = db.get(ArtifactKind::Corpus, 111, 222).unwrap().unwrap();
        let r2 = db.get(ArtifactKind::Corpus, 111, 333).unwrap().unwrap();
        assert_eq!(r1.1, b"args222");
        assert_eq!(r2.1, b"args333");
    }

    #[test]
    fn invalidate_by_kind() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        db.put(ArtifactKind::Corpus, &meta, b"corpus").unwrap();
        db.put(ArtifactKind::Taxonomy, &meta, b"taxonomy").unwrap();

        let deleted = db.invalidate(Some(ArtifactKind::Corpus)).unwrap();
        assert_eq!(deleted, 1);

        assert!(db.get(ArtifactKind::Corpus, 111, 222).unwrap().is_none());
        assert!(db.get(ArtifactKind::Taxonomy, 111, 222).unwrap().is_some());
    }

    #[test]
    fn invalidate_all() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        db.put(ArtifactKind::Corpus, &meta, b"corpus").unwrap();
        db.put(ArtifactKind::Taxonomy, &meta, b"taxonomy").unwrap();

        let deleted = db.invalidate(None).unwrap();
        assert_eq!(deleted, 2);

        let info = db.info().unwrap();
        assert!(info.is_empty());
    }

    #[test]
    fn info_returns_all_artifacts() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        db.put(ArtifactKind::Corpus, &meta, b"corpus data").unwrap();
        db.put(ArtifactKind::Fingerprints, &meta, b"fp data").unwrap();

        let info = db.info().unwrap();
        assert_eq!(info.len(), 2);
        assert!(info.iter().any(|i| i.kind == "corpus"));
        assert!(info.iter().any(|i| i.kind == "fingerprints"));
    }

    #[test]
    fn info_includes_payload_size() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        let payload = vec![0u8; 1024];
        db.put(ArtifactKind::Corpus, &meta, &payload).unwrap();

        let info = db.info().unwrap();
        assert_eq!(info[0].payload_bytes, 1024);
    }

    #[test]
    fn db_size_bytes_nonzero() {
        let db = test_db();
        let size = db.db_size_bytes().unwrap();
        assert!(size > 0);
    }

    #[test]
    fn version_stored_correctly() {
        let db = test_db();
        let meta = CacheMeta::new(111, 50, 222);
        db.put(ArtifactKind::Corpus, &meta, b"data").unwrap();

        let (got_meta, _) = db.get(ArtifactKind::Corpus, 111, 222).unwrap().unwrap();
        assert_eq!(got_meta.version, env!("CARGO_PKG_VERSION"));
    }
}
