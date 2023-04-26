use futures::future::join_all;
use async_trait::async_trait;
use core::fmt;
use std::{env, fs::File, time::Duration, collections::HashMap, sync::Arc};
use mysql_async::{prelude::*,Conn,Opts,OptsBuilder,PoolConstraints,PoolOpts};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use crate::{error::*, element::Element, database_table::DatabaseTable, db_operation_cache::{DbOperationCacheValue, DbOperationCache}, query_triples::{QueryTriples, DatabaseQueryResult}, database_wrapper::DatabaseWrapper};

const MYSQL1: &str = r#"CREATE TABLE IF NOT EXISTS `texts` (
    `id` INT(11) NOT NULL AUTO_INCREMENT,
    `value` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `value` (`value`)
) ENGINE=InnoDB"# ;
const MYSQL2: &str = r#"CREATE TABLE IF NOT EXISTS `table_list` (
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



#[derive(Debug, Clone)]
struct AppStateLiveMySQL {
    db_pool: mysql_async::Pool,
}

impl AppStateLiveMySQL {
    fn new(config: &Value) -> Self {
        Self{
            db_pool: Self::create_pool(&config["tool_db"]),
        }
    }

    /// Helper function to create a DB pool from a JSON config object
    fn create_pool(config: &Value) -> mysql_async::Pool {
        let min_connections = config["min_connections"].as_u64().expect("No min_connections value") as usize;
        let max_connections = config["max_connections"].as_u64().expect("No max_connections value") as usize;
        let keep_sec = config["keep_sec"].as_u64().expect("No keep_sec value");
        let url = config["url"].as_str().expect("No url value");
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(min_connections, max_connections).unwrap())
            .with_inactive_connection_ttl(Duration::from_secs(keep_sec));
        let wd_url = url;
        let wd_opts = Opts::from_url(wd_url).expect(format!("Can not build options from db_wd URL {}",wd_url).as_str());
        mysql_async::Pool::new(OptsBuilder::from_opts(wd_opts).pool_opts(pool_opts.clone()))
    }

}

#[async_trait]
impl AppDB for AppStateLiveMySQL {
    async fn init_from_db(&self, app: &AppState) -> Result<(),WDSQErr> {
        let mut conn = self.db_conn().await?;
        conn.exec_drop(MYSQL1, ()).await?;
        conn.exec_drop(MYSQL2, ()).await?;
        let sql = r#"SELECT `name`,`json` FROM `table_list`"# ;
        let results = conn
            .exec_iter(sql, ()).await?
            .map_and_drop(|row| mysql_async::from_row::<(String,String)>(row)).await?;
        let mut tables = app.tables.write().await;
        for (name,json) in results {
            let table: DatabaseTable = serde_json::from_str(&json)?;
            tables.insert(name,table);
        }
        Ok(())
    }

    /// Returns a connection to the tool database
    async fn db_conn(&self) -> Result<Conn, mysql_async::Error> {
        println!("Getting DB connection...");
        self.db_pool.get_conn().await
    }

    async fn table(&self, table: &DatabaseTable) -> Result<(),WDSQErr> {
        let name = table.name.to_owned();
        let json = json!(table).to_string();
        let sql = table.create_statement();
        let mut conn = self.db_conn().await?;
        conn.exec_drop(sql, ()).await?;
        let sql = "INSERT IGNORE INTO `table_list` (`name`,`json`) VALUES(:name,:json)";
        conn.exec_drop(sql, params!{name,json}).await?;
        Ok(())
    }

    async fn prepare_text(&self, text_chunk: &[String]) -> Result<(),WDSQErr> {
        let question_marks = vec!["(?)"; text_chunk.len()].join(",");
        let sql = format!("INSERT IGNORE INTO `texts` (`value`) VALUES {question_marks}");
        self.db_conn().await?.exec_drop(sql, &text_chunk.to_owned()).await?;
        Ok(())
    }

    async fn force_flush(&self, command: &str, value_chunk: &[Vec<DbOperationCacheValue>]) -> Result<Vec<(String, Vec<String>)>,WDSQErr> {
        let mut ret = vec![];
        let question_marks: Vec<_> = value_chunk
            .iter()
            .map(|parts|{
                let ret: Vec<_> = parts.iter().map(|part|part.as_sql_placeholder()).collect();
                format!("({})",ret.join(","))
            })
            .collect();
        let question_marks = question_marks.join(",");
        let sql = format!("{} {question_marks}",command);
        let values: Vec<_> = value_chunk
                .iter()
                .map(|parts|{
                    let ret: Vec<_> = parts.iter().filter_map(|part|part.as_sql_variable()).collect();
                    ret
                })
                .flatten()
                .collect();
        ret.push((sql,values));
        Ok(ret)
    }

    async fn run_query(&self, _app: &AppState, query: &QueryTriples) -> Result<HashMap<String,DatabaseQueryResult>,WDSQErr> {
        let mut conn = self.db_conn().await?;
        let mut ret = HashMap::new();
        for (group_key,part) in &query.result {
            let mut dsr = DatabaseQueryResult::default();
            dsr.variables = part.variables.clone();
            let iter = conn.exec_iter(part.sql.to_owned(),part.values.to_owned()).await?;
            let results = iter.map_and_drop(|row| row).await?;
            for row in &results {
                let x = row.to_owned().unwrap();
                let res: Vec<Option<String>> = x.iter()
                    .enumerate()
                    .map(|(col_num,v)|{
                        part.variables[col_num].sql_value2string(v)
                    }).collect();
                dsr.rows.push(res);
            }
            ret.insert(group_key.to_owned(),dsr);
        }
        Ok(ret)

    }
}




#[derive(Debug, Clone)]
pub struct AppStateStdoutMySQL {
}

impl AppStateStdoutMySQL {
    fn new(_config: &Value) -> Self {
        Self{}
    }

    pub fn sql_escape(s: &str) -> String {
        s.to_string() // TODO
    }

    fn sql_group_escape(&self, vs: &[String]) -> String {
        let mut ret = String::new() ;
        for s in vs {
            if ret.is_empty() {
                ret = format!("(\"{}\")",Self::sql_escape(s)); 
            } else {
                ret += &format!(",(\"{}\")",Self::sql_escape(s));
            }
        }
        ret
    }
}

#[async_trait]
impl AppDB for AppStateStdoutMySQL {
    async fn init_from_db(&self, _app: &AppState) -> Result<(),WDSQErr> {
        println!("{MYSQL1};");
        println!("{MYSQL2};");
        Ok(())
    }

    async fn db_conn(&self) -> Result<Conn, mysql_async::Error> {
        let e = WDSQErr::String("AppStateStdoutMySQL::db_conn".into());
        Err(mysql_async::Error::Other(Box::new(e)))
    }

    async fn table(&self, table: &DatabaseTable) -> Result<(),WDSQErr> {
        let name = table.name.to_owned();
        let json = json!(table).to_string();
        let sql = table.create_statement();
        println!("{sql};");
        let sql = format!("INSERT IGNORE INTO `table_list` (`name`,`json`) VALUES(\"{name}\",\"{json}\")");
        println!("{sql};");
        Ok(())
    }

    async fn prepare_text(&self, text_chunk: &[String]) -> Result<(),WDSQErr> {
        let values = self.sql_group_escape(text_chunk);
        let sql = format!("INSERT IGNORE INTO `texts` (`value`) VALUES {values}");
        println!("{sql};");
        Ok(())
    }

    async fn force_flush(&self, command: &str, value_chunk: &[Vec<DbOperationCacheValue>]) -> Result<Vec<(String, Vec<String>)>,WDSQErr> {
        let question_marks: Vec<_> = value_chunk
            .iter()
            .map(|parts|{
                let ret: Vec<String> = parts.iter().map(|part|part.as_sql_stdout()).collect();
                let ret = ret.join(",");
                format!("({ret})")
            })
            .collect();
        let question_marks = question_marks.join(",");
        let sql = format!("{} {question_marks}",command);
        println!("{sql}");
        Ok(vec![])
    }

    async fn run_query(&self, _app: &AppState, _query: &QueryTriples) -> Result<HashMap<String,DatabaseQueryResult>,WDSQErr> {
        panic!("AppStateStdoutMySQL::run_query can never be implemented")
    }
}


#[derive(Clone)]
pub struct AppState {
    pub db_interface: Arc<Box<dyn AppDB + Sync + Send>>,
    pub tables: Arc<RwLock<HashMap<String,DatabaseTable>>>,
    pub parallel_parsing: usize,
    pub insert_batch_size: usize,
    pub insert_chunk_size: usize,
    pub to_stdout: bool,
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
        let mut path = env::current_dir().expect("Can't get CWD");
        path.push(filename);
        let file = File::open(&path)?;
        let config: Value = serde_json::from_reader(file)?;
        Ok(Self::from_config(&config))
    }

    /// Creatre an AppState object from a config JSON object
    pub fn from_config(config: &Value) -> Self {
        let prefixes = config["prefixes"]
            .as_object()
            .expect("Prefixes JSON is not an object")
            .iter()
            .map(|(k,v)|(k.to_owned(),v.as_str().unwrap().to_string()))
            .collect();
        let db_interface: Box<dyn AppDB+Send+Sync> = if config["to_stdout"].as_bool().unwrap_or(false) as bool {
            Box::new(AppStateStdoutMySQL::new(config))
        } else {
            Box::new(AppStateLiveMySQL::new(config))
        };
        let ret = Self {
            db_interface: Arc::new(db_interface),
            tables: Arc::new(RwLock::new(HashMap::new())),
            parallel_parsing: config["parallel_parsing"].as_u64().unwrap_or(100) as usize,
            insert_batch_size: config["insert_batch_size"].as_u64().unwrap_or(100) as usize,
            insert_chunk_size: config["insert_chunk_size"].as_u64().unwrap_or(100) as usize,
            to_stdout: config["to_stdout"].as_bool().unwrap_or(false) as bool,
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

    pub async fn table(&self, s: Element, p: Element, o: Element) -> Result<DatabaseTable,WDSQErr> {
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
        let the_values = oc.values.write().await;
        let command = oc.command.read().await;
        for value_chunk in the_values.chunks(self.insert_chunk_size) {
            let to_the_future = self.force_flush(&command, value_chunk).await?;
            for (sql,values) in to_the_future {
                let app = self.clone();
                let future = tokio::spawn(async move {
                    app.db_conn().await?.exec_drop(sql, &values).await.map_err(|e|WDSQErr::MySQL(Arc::new(e)))
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