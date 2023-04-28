use futures::future::join_all;
use async_trait::async_trait;
use core::fmt;
use std::{env, fs::File, collections::HashMap, sync::Arc};
use mysql_async::{prelude::*,Conn};
use serde_json::Value;
use tokio::sync::RwLock;
use crate::{error::*, element::Element, database_table::DatabaseTable, db_operation_cache::{DbOperationCacheValue, DbOperationCache}, query_triples::{QueryTriples, DatabaseQueryResult}, database_wrapper::DatabaseWrapper, app_state_mysql_stdout::AppStateStdoutMySQL};
use crate::app_state_mysql_live::AppStateLiveMySQL;

pub const MYSQL_CREATE_TEXTS_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS `texts` (
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `value` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `value` (`value`)
) ENGINE=InnoDB"# ;
pub const MYSQL_CREATE_TABLE_LIST_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS `table_list` (
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    `json` MEDIUMTEXT NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB"# ;

#[async_trait]
pub trait AppDB {
    async fn init_from_db(&self, app: &AppState) -> Result<(),WDSQErr> ;
    async fn db_conn(&self) -> Result<Conn, mysql_async::Error> ;
    async fn table(&self, table: &DatabaseTable) -> Result<(),WDSQErr> ;
    async fn prepare_text(&self, text_chunk: &[String]) -> Result<(),WDSQErr> ;
    async fn force_flush(&self, command: &str, value_chunk: &[Vec<DbOperationCacheValue>]) -> Result<Vec<(String, Vec<String>)>,WDSQErr> ;
    async fn run_query(&self, app: &AppState, query: &QueryTriples) -> Result<HashMap<String,DatabaseQueryResult>,WDSQErr> ;
}

impl std::fmt::Debug for dyn AppDB {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", "AppDB")
    }
}

#[derive(Clone)]
pub struct AppState {
    pub db_interface: Arc<Box<dyn AppDB + Sync + Send>>,
    pub tables: Arc<RwLock<HashMap<String,DatabaseTable>>>,
    pub parallel_parsing: usize,
    pub insert_batch_size: usize,
    pub insert_chunk_size: usize,
    prefixes: HashMap<String,String>,
}

unsafe impl Send for AppState {}
unsafe impl Sync for AppState {}

impl fmt::Debug for AppState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppState")
        //  .field("y", &self.y)
         .finish()
    }
}

impl AppState {
    /// Create an AppState object from a config JSON file
    pub fn from_config_file(filename: &str) -> Result<Self,WDSQErr> {
        let config = Self::get_config_from_file(filename)?;
        Ok(Self::from_config(&config))
    }

    pub fn get_config_from_file(filename: &str) -> Result<Value,WDSQErr> {
        let mut path = env::current_dir().expect("Can't get CWD");
        path.push(filename);
        let file = File::open(&path)?;
        Ok(serde_json::from_reader(file)?)
    }

    /// Creatre an AppState object from a config JSON object
    pub fn from_config(config: &Value) -> Self {
        let prefixes = config["prefixes"]
            .as_object()
            .expect("Prefixes JSON is not an object")
            .iter()
            .map(|(k,v)|(k.to_owned(),v.as_str().unwrap().to_string()))
            .collect();
        let db_type = config["db_type"].as_str().unwrap_or("mysql") as &str;
        let db_interface: Box<dyn AppDB+Send+Sync> = match db_type {
            "mysql" => Box::new(AppStateLiveMySQL::new(config)),
            "mysql_stdout" => Box::new(AppStateStdoutMySQL::new(config)),
            _ => panic!("Unknown db_type {db_type}"),
        };
        let ret = Self {
            db_interface: Arc::new(db_interface),
            tables: Arc::new(RwLock::new(HashMap::new())),
            parallel_parsing: config["parallel_parsing"].as_u64().unwrap_or(100) as usize,
            insert_batch_size: config["insert_batch_size"].as_u64().unwrap_or(100) as usize,
            insert_chunk_size: config["insert_chunk_size"].as_u64().unwrap_or(100) as usize,
            prefixes,
        };
        ret
    }

    pub fn replace_prefix(&self, s: &str) -> String {
        match s.split_once(":") {
            Some((before,after)) => {
                match self.prefixes.get(&before.trim().to_lowercase()) {
                    Some(path) => format!("{path}{}",after.trim()),
                    None => s.to_string(),
                }
            },
            None => s.to_string(),
        }
    }

    pub async fn init_from_db(&self) -> Result<(),WDSQErr> {
        self.db_interface.init_from_db(&self).await
    }

    pub async fn table(&self, s: &Element, p: &Element, o: &Element) -> Result<DatabaseTable,WDSQErr> {
        let table = DatabaseTable::new(s,p,o);
        if self.tables.read().await.contains_key(&table.name) {
            return Ok(table);
        }
        let mut tables = self.tables.write().await;
        let entry = tables.entry(table.name.to_owned()) ;
        if let std::collections::hash_map::Entry::Vacant(_) = entry {
            self.db_interface.table(&table).await?;
            entry.or_insert(table.clone());
        }
        Ok(table)
    }

    pub async fn prepare_text(&self, text_chunk: &[String]) -> Result<(),WDSQErr> {
        self.db_interface.prepare_text(text_chunk).await
    }

    pub async fn force_flush(&self, command: &str, value_chunk: &[Vec<DbOperationCacheValue>]) -> Result<Vec<(String, Vec<String>)>,WDSQErr> {
        self.db_interface.force_flush(command, value_chunk).await
    }

    pub async fn db_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.db_interface.db_conn().await
    }

    pub async fn force_flush_all(&self, oc: &DbOperationCache) -> Result<(),WDSQErr> {
        let mut futures = vec![];
        for value_chunk in oc.values.chunks(self.insert_chunk_size) {
            let to_the_future = self.force_flush(&oc.command, value_chunk).await?;
            for (sql,values) in to_the_future {
                let dbi = self.db_interface.clone();
                let future = tokio::spawn(async move {
                    dbi.db_conn().await?.exec_drop(sql, &values).await.map_err(|e|WDSQErr::MySQL(Arc::new(e)))
                });
                futures.push(future);
            }
        }
        DatabaseWrapper::first_err(join_all(futures).await, true)
    }

    pub async fn run_query(&self, query: &QueryTriples) -> Result<HashMap<String,DatabaseQueryResult>,WDSQErr> {
        self.db_interface.run_query(self, query).await
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replace_prefix() {
        let app = Arc::new(AppState::from_config_file("config.json").unwrap());
        assert_eq!(app.replace_prefix("wd:Q12345"),"http://www.wikidata.org/entity/Q12345");
        assert_eq!(app.replace_prefix("  wd  :  Q12345 "),"http://www.wikidata.org/entity/Q12345");
        assert_eq!(app.replace_prefix("wdt:P123"),"http://www.wikidata.org/prop/direct/P123");
        assert_eq!(app.replace_prefix("foo:bar"),"foo:bar");
        assert_eq!(app.replace_prefix("foo bar"),"foo bar");
    }
}