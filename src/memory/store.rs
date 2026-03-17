use super::types::{ExtractedMemory, Memory, MemoryKind, RecalledSet};
use crate::config::MemoryConfig;
use rusqlite::Connection;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Sanitize a term for use inside an FTS5 double-quoted string.
/// Strips characters that are special in FTS5 query syntax (quotes, `*`, etc.)
/// and returns `None` if nothing useful remains.
fn escape_fts5_term(term: &str) -> Option<String> {
    let cleaned: String = term
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-')
        .collect();
    if cleaned.is_empty() {
        None
    } else {
        Some(format!("\"{cleaned}\""))
    }
}

#[derive(Clone)]
pub struct MemoryStore {
    conn: Arc<Mutex<Connection>>,
}

impl MemoryStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create memory DB directory: {e}"))?;
        }
        let conn = Connection::open(path).map_err(|e| format!("Failed to open memory DB: {e}"))?;

        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
            .map_err(|e| format!("Failed to set WAL mode: {e}"))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL,
                reasoning TEXT NOT NULL DEFAULT '',
                source_run TEXT NOT NULL DEFAULT '',
                source_agent TEXT NOT NULL DEFAULT '',
                evidence_count INTEGER NOT NULL DEFAULT 1,
                tags TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                expires_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
                content, reasoning, tags, content=memories, content_rowid=id
            );

            CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
                INSERT INTO memories_fts(rowid, content, reasoning, tags)
                VALUES (new.id, new.content, new.reasoning, new.tags);
            END;

            CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
                VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
                INSERT INTO memories_fts(rowid, content, reasoning, tags)
                VALUES (new.id, new.content, new.reasoning, new.tags);
            END;

            CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
                INSERT INTO memories_fts(memories_fts, rowid, content, reasoning, tags)
                VALUES ('delete', old.id, old.content, old.reasoning, old.tags);
            END;

            CREATE INDEX IF NOT EXISTS idx_memories_project ON memories(project_id);
            CREATE INDEX IF NOT EXISTS idx_memories_project_kind ON memories(project_id, kind);",
        )
        .map_err(|e| format!("Failed to create memory schema: {e}"))?;

        let store = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        let _ = store.cleanup_expired();
        Ok(store)
    }

    pub fn insert(
        &self,
        project_id: &str,
        mem: &ExtractedMemory,
        source_run: &str,
        source_agent: &str,
        config: &MemoryConfig,
    ) -> Result<i64, String> {
        // Reject empty or whitespace-only memories to prevent polluting recall
        if mem.content.trim().is_empty() {
            return Err("Empty memory content".into());
        }
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        let tags = mem.tags.join(",");
        let kind_str = mem.kind.as_str();

        // For principles, check for similar existing ones and reinforce
        if mem.kind == MemoryKind::Principle {
            if let Some(existing_id) =
                self.find_similar_principle(&conn, project_id, &mem.content)?
            {
                self.reinforce_principle_inner(&conn, existing_id, &now)?;
                return Ok(existing_id);
            }
        }

        let expires_at = compute_expires_at(&now, mem.kind, config);

        conn.execute(
            "INSERT INTO memories (project_id, kind, content, reasoning, source_run, source_agent, evidence_count, tags, created_at, expires_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?9, ?8)",
            rusqlite::params![
                project_id, kind_str, mem.content, mem.reasoning, source_run, source_agent, tags, now, expires_at
            ],
        )
        .map_err(|e| format!("Failed to insert memory: {e}"))?;

        Ok(conn.last_insert_rowid())
    }

    pub fn recall(
        &self,
        project_id: &str,
        terms: &[String],
        max: usize,
        max_bytes: usize,
    ) -> Result<RecalledSet, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        if terms.is_empty() {
            return Ok(RecalledSet {
                memories: vec![],
                total_bytes: 0,
            });
        }

        // Build FTS5 query: OR-join terms (sanitized for FTS5 safety)
        let escaped: Vec<String> = terms.iter().filter_map(|t| escape_fts5_term(t)).collect();
        if escaped.is_empty() {
            return Ok(RecalledSet {
                memories: vec![],
                total_bytes: 0,
            });
        }
        let fts_query = escaped.join(" OR ");

        // Apply kind-based boost inside SQL so the LIMIT operates on the
        // final ranking.  BM25 returns negative scores (more negative = better);
        // multiplying by a boost ≥ 1.0 makes higher-priority kinds even more
        // negative, so they sort first.  This avoids the previous two-phase
        // approach where the LIMIT could discard high-boost rows before the
        // Rust-side re-sort ever saw them.
        let mut stmt = conn
            .prepare(
                "SELECT m.id, m.project_id, m.kind, m.content, m.reasoning, m.source_run,
                        m.source_agent, m.evidence_count, m.tags, m.created_at,
                        m.expires_at, m.updated_at
                 FROM memories m
                 JOIN memories_fts ON memories_fts.rowid = m.id
                 WHERE memories_fts MATCH ?1 AND m.project_id = ?2
                   AND (m.expires_at IS NULL OR m.expires_at > ?4)
                 ORDER BY bm25(memories_fts) * CASE m.kind
                            WHEN 'principle'   THEN 1.5
                            WHEN 'decision'    THEN 1.3
                            WHEN 'observation' THEN 1.0
                            WHEN 'summary'     THEN 0.8
                            ELSE 1.0
                          END
                 LIMIT ?3",
            )
            .map_err(|e| format!("FTS query prepare failed: {e}"))?;

        let now = chrono::Utc::now().to_rfc3339();
        let rows = stmt
            .query_map(
                rusqlite::params![fts_query, project_id, max * 3, now],
                |row| {
                    let kind_str: String = row.get(2)?;
                    let kind = kind_str
                        .parse::<MemoryKind>()
                        .unwrap_or(MemoryKind::Observation);
                    Ok(Memory {
                        id: row.get(0)?,
                        project_id: row.get(1)?,
                        kind,
                        content: row.get(3)?,
                        reasoning: row.get(4)?,
                        source_run: row.get(5)?,
                        source_agent: row.get(6)?,
                        evidence_count: row.get(7)?,
                        tags: row.get(8)?,
                        created_at: row.get(9)?,
                        expires_at: row.get(10)?,
                        updated_at: row.get(11)?,
                    })
                },
            )
            .map_err(|e| format!("FTS query failed: {e}"))?;

        let scored: Vec<Memory> = rows.flatten().collect();

        // Truncate to max count and max_bytes, skipping oversized entries
        // so that one large memory doesn't suppress all smaller ones.
        // Per-entry overhead accounts for formatting in recall::format_memory_entry:
        // [KIND] prefix (~12), reasoning prefix (~15), newlines (~4), XML escaping (~10%).
        const PER_ENTRY_OVERHEAD: usize = 40;
        let mut memories = Vec::new();
        let mut total_bytes = 0usize;
        for mem in scored {
            let raw = mem.content.len() + mem.reasoning.len();
            let mem_bytes = raw + raw / 10 + PER_ENTRY_OVERHEAD;
            if total_bytes + mem_bytes > max_bytes {
                continue; // skip this one, try smaller ones
            }
            total_bytes += mem_bytes;
            memories.push(mem);
            if memories.len() >= max {
                break;
            }
        }

        Ok(RecalledSet {
            memories,
            total_bytes,
        })
    }

    pub fn list(
        &self,
        project_id: &str,
        kind_filter: Option<MemoryKind>,
    ) -> Result<Vec<Memory>, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = match kind_filter {
            Some(kind) => (
                "SELECT id, project_id, kind, content, reasoning, source_run, source_agent, evidence_count, tags, created_at, expires_at, updated_at FROM memories WHERE project_id = ?1 AND kind = ?2 AND (expires_at IS NULL OR expires_at > ?3) ORDER BY updated_at DESC LIMIT 500",
                vec![Box::new(project_id.to_string()), Box::new(kind.as_str().to_string()), Box::new(now.clone())],
            ),
            None => (
                "SELECT id, project_id, kind, content, reasoning, source_run, source_agent, evidence_count, tags, created_at, expires_at, updated_at FROM memories WHERE project_id = ?1 AND (expires_at IS NULL OR expires_at > ?2) ORDER BY updated_at DESC LIMIT 500",
                vec![Box::new(project_id.to_string()), Box::new(now.clone())],
            ),
        };

        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| format!("List query failed: {e}"))?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let kind_str: String = row.get(2)?;
                let kind = kind_str
                    .parse::<MemoryKind>()
                    .unwrap_or(MemoryKind::Observation);
                Ok(Memory {
                    id: row.get(0)?,
                    project_id: row.get(1)?,
                    kind,
                    content: row.get(3)?,
                    reasoning: row.get(4)?,
                    source_run: row.get(5)?,
                    source_agent: row.get(6)?,
                    evidence_count: row.get(7)?,
                    tags: row.get(8)?,
                    created_at: row.get(9)?,
                    expires_at: row.get(10)?,
                    updated_at: row.get(11)?,
                })
            })
            .map_err(|e| format!("List query failed: {e}"))?;

        let memories: Vec<Memory> = rows.flatten().collect();
        Ok(memories)
    }

    pub fn delete(&self, id: i64) -> Result<(), String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM memories WHERE id = ?1", rusqlite::params![id])
            .map_err(|e| format!("Failed to delete memory: {e}"))?;
        Ok(())
    }

    pub fn cleanup_expired(&self) -> Result<usize, String> {
        let conn = self.conn.lock().map_err(|e| e.to_string())?;
        let now = chrono::Utc::now().to_rfc3339();
        let count = conn
            .execute(
                "DELETE FROM memories WHERE expires_at IS NOT NULL AND expires_at < ?1",
                rusqlite::params![now],
            )
            .map_err(|e| format!("Failed to cleanup expired memories: {e}"))?;
        Ok(count)
    }

    fn reinforce_principle_inner(
        &self,
        conn: &Connection,
        id: i64,
        now: &str,
    ) -> Result<(), String> {
        conn.execute(
            "UPDATE memories SET evidence_count = evidence_count + 1, updated_at = ?1 WHERE id = ?2",
            rusqlite::params![now, id],
        )
        .map_err(|e| format!("Failed to reinforce principle: {e}"))?;
        Ok(())
    }

    fn find_similar_principle(
        &self,
        conn: &Connection,
        project_id: &str,
        content: &str,
    ) -> Result<Option<i64>, String> {
        // Use FTS5 to find similar principles, then check similarity
        let words: Vec<&str> = content.split_whitespace().take(10).collect();
        if words.is_empty() {
            return Ok(None);
        }
        let escaped: Vec<String> = words.iter().filter_map(|w| escape_fts5_term(w)).collect();
        if escaped.is_empty() {
            return Ok(None);
        }
        let fts_query = escaped.join(" OR ");

        let mut stmt = conn
            .prepare(
                "SELECT m.id, m.content
                 FROM memories m
                 JOIN memories_fts ON memories_fts.rowid = m.id
                 WHERE memories_fts MATCH ?1 AND m.project_id = ?2 AND m.kind = 'principle'
                 ORDER BY bm25(memories_fts)
                 LIMIT 5",
            )
            .map_err(|e| format!("Similarity search failed: {e}"))?;

        let rows = stmt
            .query_map(rusqlite::params![fts_query, project_id], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|e| format!("Similarity query failed: {e}"))?;

        for row in rows.flatten() {
            let (id, existing_content) = row;
            if text_similarity(content, &existing_content) > 0.6 {
                return Ok(Some(id));
            }
        }

        Ok(None)
    }
}

fn compute_expires_at(now: &str, kind: MemoryKind, config: &MemoryConfig) -> Option<String> {
    if kind.is_permanent() {
        return None;
    }
    let days = match kind {
        MemoryKind::Observation => config.observation_ttl_days,
        MemoryKind::Summary => config.summary_ttl_days,
        _ => return None,
    };
    chrono::DateTime::parse_from_rfc3339(now)
        .ok()
        .map(|dt| (dt + chrono::Duration::days(i64::from(days))).to_rfc3339())
}

/// Simple word-overlap similarity (Jaccard-like), case-insensitive.
fn text_similarity(a: &str, b: &str) -> f64 {
    let words_a: std::collections::HashSet<String> = a
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|w| !w.is_empty())
        .collect();
    let words_b: std::collections::HashSet<String> = b
        .split_whitespace()
        .map(|w| {
            w.trim_matches(|c: char| !c.is_alphanumeric())
                .to_lowercase()
        })
        .filter(|w| !w.is_empty())
        .collect();
    if words_a.is_empty() || words_b.is_empty() {
        return 0.0;
    }
    let intersection = words_a.intersection(&words_b).count();
    let union = words_a.union(&words_b).count();
    intersection as f64 / union as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config() -> MemoryConfig {
        MemoryConfig::default()
    }

    #[test]
    fn insert_and_list() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let mem = ExtractedMemory {
            kind: MemoryKind::Decision,
            content: "Use Rust for the backend".into(),
            reasoning: "Performance matters".into(),
            tags: vec!["arch".into()],
        };
        let id = store
            .insert("proj1", &mem, "run1", "Claude", &test_config())
            .unwrap();
        assert!(id > 0);

        let list = store.list("proj1", None).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].content, "Use Rust for the backend");
        assert_eq!(list[0].kind, MemoryKind::Decision);
    }

    #[test]
    fn ttl_cleanup() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let conn = store.conn.lock().unwrap();
        // Insert an already-expired observation
        let past = "2020-01-01T00:00:00+00:00";
        conn.execute(
            "INSERT INTO memories (project_id, kind, content, reasoning, tags, created_at, expires_at, updated_at)
             VALUES ('proj1', 'observation', 'old data', '', '', ?1, ?1, ?1)",
            rusqlite::params![past],
        )
        .unwrap();
        drop(conn);

        let count = store.cleanup_expired().unwrap();
        assert_eq!(count, 1);
        assert!(store.list("proj1", None).unwrap().is_empty());
    }

    #[test]
    fn principle_reinforcement() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let mem = ExtractedMemory {
            kind: MemoryKind::Principle,
            content: "Always validate user input at boundaries".into(),
            reasoning: "Security best practice".into(),
            tags: vec![],
        };
        store.insert("proj1", &mem, "run1", "Claude", &cfg).unwrap();

        // Insert similar principle — should reinforce
        let mem2 = ExtractedMemory {
            kind: MemoryKind::Principle,
            content: "Always validate user input at system boundaries".into(),
            reasoning: "Security".into(),
            tags: vec![],
        };
        store
            .insert("proj1", &mem2, "run2", "Claude", &cfg)
            .unwrap();

        let list = store.list("proj1", Some(MemoryKind::Principle)).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].evidence_count, 2);
    }

    #[test]
    fn fts_recall() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();

        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Decision,
                    content: "Use PostgreSQL for data storage".into(),
                    reasoning: "ACID compliance".into(),
                    tags: vec!["database".into()],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "The API response time is under 100ms".into(),
                    reasoning: "Benchmark results".into(),
                    tags: vec!["perf".into()],
                },
                "run1",
                "OpenAI",
                &cfg,
            )
            .unwrap();
        store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Summary,
                    content: "Completed the logging infrastructure".into(),
                    reasoning: "".into(),
                    tags: vec![],
                },
                "run1",
                "Gemini",
                &cfg,
            )
            .unwrap();

        let result = store
            .recall("proj1", &["PostgreSQL".into(), "database".into()], 10, 8192)
            .unwrap();
        assert!(!result.memories.is_empty());
        assert_eq!(
            result.memories[0].content,
            "Use PostgreSQL for data storage"
        );
    }

    #[test]
    fn delete_memory() {
        let dir = tempdir().unwrap();
        let store = MemoryStore::open(&dir.path().join("test.db")).unwrap();
        let cfg = test_config();
        let id = store
            .insert(
                "proj1",
                &ExtractedMemory {
                    kind: MemoryKind::Observation,
                    content: "temp".into(),
                    reasoning: "".into(),
                    tags: vec![],
                },
                "run1",
                "Claude",
                &cfg,
            )
            .unwrap();
        store.delete(id).unwrap();
        assert!(store.list("proj1", None).unwrap().is_empty());
    }

    #[test]
    fn open_bad_path() {
        let result = MemoryStore::open(std::path::Path::new("/nonexistent/deeply/nested/test.db"));
        assert!(result.is_err());
    }

    #[test]
    fn text_similarity_identical() {
        assert!((text_similarity("hello world", "hello world") - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn text_similarity_partial() {
        let sim = text_similarity("hello world foo", "hello world bar");
        assert!(sim > 0.4);
        assert!(sim < 1.0);
    }

    #[test]
    fn text_similarity_none() {
        assert!(text_similarity("alpha beta", "gamma delta").abs() < f64::EPSILON);
    }
}
