use regex::Regex;
use rusqlite::{params, Connection, Result};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};
use tantivy::{
    doc,
    schema::{Schema, STORED, TEXT},
    Index, IndexWriter, Term,
};

pub struct SearchableStringStore {
    conn: Arc<Mutex<Connection>>,
    cache: Arc<RwLock<HashMap<i64, String>>>,
    index: Index,
    index_writer: Arc<Mutex<IndexWriter>>,
    schema: Schema,
}

impl SearchableStringStore {
    pub fn new<P: AsRef<Path>>(db_path: P, index_path: P) -> Result<Self> {
        // Initialize SQLite connection with WAL mode
        let conn = Connection::open(&db_path)?;
        conn.execute("PRAGMA journal_mode=WAL", [])?;

        // Create main storage table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS strings (
                id INTEGER PRIMARY KEY,
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;

        // Create indexes
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_created_at ON strings(created_at)",
            [],
        )?;

        // Initialize Tantivy search index
        let mut schema_builder = Schema::builder();
        schema_builder.add_i64_field("id", STORED);
        schema_builder.add_text_field("content", TEXT | STORED);
        let schema = schema_builder.build();

        let index = Index::create_in(index_path, schema.clone())?;
        let index_writer = index.writer(50_000_000)?; // 50MB buffer

        Ok(SearchableStringStore {
            conn: Arc::new(Mutex::new(conn)),
            cache: Arc::new(RwLock::new(HashMap::new())),
            index,
            index_writer: Arc::new(Mutex::new(index_writer)),
            schema,
        })
    }

    pub fn insert(&self, content: &str) -> Result<i64> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Insert into SQLite
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO strings (content, created_at, updated_at) VALUES (?1, ?2, ?2)",
            params![content, timestamp],
        )?;

        let id = conn.last_insert_rowid();

        // Update cache
        self.cache.write().unwrap().insert(id, content.to_string());

        // Update search index
        let mut index_writer = self.index_writer.lock().unwrap();
        let content_field = self.schema.get_field("content").unwrap();
        let id_field = self.schema.get_field("id").unwrap();

        index_writer.add_document(doc!(
            id_field => id,
            content_field => content
        ))?;
        index_writer.commit()?;

        Ok(id)
    }

    pub fn search_regexp(&self, pattern: &str) -> Result<Vec<(i64, String)>> {
        let regex = Regex::new(pattern).map_err(|e| rusqlite::Error::InvalidQuery)?;

        // First, get all strings from SQLite
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, content FROM strings")?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;

        // Filter using regexp
        let mut matches = Vec::new();
        for row in rows {
            let (id, content): (i64, String) = row?;
            if regex.is_match(&content) {
                matches.push((id, content));
            }
        }

        Ok(matches)
    }

    pub fn search_text(&self, query: &str) -> tantivy::Result<Vec<(i64, String)>> {
        let reader = self.index.reader()?;
        let searcher = reader.searcher();

        let content_field = self.schema.get_field("content").unwrap();
        let id_field = self.schema.get_field("id").unwrap();

        let query_parser = tantivy::query::QueryParser::for_index(&self.index, vec![content_field]);
        let query = query_parser.parse_query(query)?;

        let top_docs = searcher.search(&query, &tantivy::collector::TopDocs::with_limit(100))?;

        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            let id = retrieved_doc.get_first(id_field).unwrap().as_i64().unwrap();
            let content = retrieved_doc
                .get_first(content_field)
                .unwrap()
                .as_text()
                .unwrap();
            results.push((id, content.to_string()));
        }

        Ok(results)
    }

    // Optimized regexp search using parallel processing
    pub fn search_regexp_parallel(&self, pattern: &str) -> Result<Vec<(i64, String)>> {
        use rayon::prelude::*;

        let regex = Regex::new(pattern).map_err(|e| rusqlite::Error::InvalidQuery)?;

        // Get all strings from SQLite
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id, content FROM strings")?;
        let rows: Vec<(i64, String)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>>>()?;

        // Process in parallel
        let matches: Vec<(i64, String)> = rows
            .into_par_iter()
            .filter(|(_, content)| regex.is_match(content))
            .collect();

        Ok(matches)
    }

    // Combined search using both full-text and regexp
    pub fn search_combined(
        &self,
        text_query: &str,
        regexp_pattern: Option<&str>,
    ) -> Result<Vec<(i64, String)>> {
        // First, perform full-text search
        let mut results = self
            .search_text(text_query)
            .map_err(|e| rusqlite::Error::InvalidQuery)?;

        // If regexp pattern is provided, filter results
        if let Some(pattern) = regexp_pattern {
            let regex = Regex::new(pattern).map_err(|e| rusqlite::Error::InvalidQuery)?;
            results.retain(|(_, content)| regex.is_match(content));
        }

        Ok(results)
    }

    // Batch search operations
    pub fn batch_regexp_search(
        &self,
        patterns: &[&str],
    ) -> Result<HashMap<String, Vec<(i64, String)>>> {
        let mut results = HashMap::new();

        for &pattern in patterns {
            results.insert(pattern.to_string(), self.search_regexp(pattern)?);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_search_operations() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().join("test.db");
        let index_path = dir.path().join("test_index");
        let store = SearchableStringStore::new(&db_path, &index_path)?;

        // Insert test data
        store.insert("Hello, World!")?;
        store.insert("Hello, Rust!")?;
        store.insert("Testing regexp search")?;

        // Test regexp search
        let results = store.search_regexp("Hello.*")?;
        assert_eq!(results.len(), 2);

        // Test full-text search
        let results = store.search_text("regexp").unwrap();
        assert_eq!(results.len(), 1);

        // Test combined search
        let results = store.search_combined("Hello", Some(".*Rust.*"))?;
        assert_eq!(results.len(), 1);

        Ok(())
    }
}
