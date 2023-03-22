use std::{env, fs::File, time::Duration, collections::HashMap, sync::Arc};
use mysql_async::{prelude::*,Conn,Opts,OptsBuilder,PoolConstraints,PoolOpts};
use serde_json::{Value, json};
use tokio::sync::RwLock;
use crate::{error::*, element::Element, database_table::DatabaseTable, db_operation_cache::DbOperationCache};


#[derive(Debug, Clone)]
pub struct AppState {
    db_pool: mysql_async::Pool,
    tables: Arc<RwLock<HashMap<String,DatabaseTable>>>,
    insert_cache: Arc<RwLock<HashMap<String,DbOperationCache>>>,
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
        let ret = Self {
            db_pool: Self::create_pool(&config["tool_db"]),
            tables: Arc::new(RwLock::new(HashMap::new())),
            insert_cache: Arc::new(RwLock::new(HashMap::new())),
        };
        ret
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

    /// Returns a connection to the tool database
    pub async fn db_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.db_pool.get_conn().await
    }

    pub async fn init_from_db(&self) -> Result<(),WDSQErr> {
        let sql = r#"CREATE TABLE IF NOT EXISTS `table_list` (
            `id` INT(11) NOT NULL AUTO_INCREMENT,
            `name` VARCHAR(255) NOT NULL,
            `json` MEDIUMTEXT NOT NULL,
            PRIMARY KEY (`id`)
        ) ENGINE=InnoDB"# ;
        let mut conn = self.db_conn().await?;
        conn.exec_drop(sql, ()).await?;

        let sql = r#"SELECT `name`,`json` FROM `table_list`"# ;
        let results = conn
            .exec_iter(sql, ()).await?
            .map_and_drop(|row| mysql_async::from_row::<(String,String)>(row)).await?;
        let mut tables = self.tables.write().await;
        for (name,json) in results {
            match serde_json::from_str(&json) {
                Ok(json) => {
                    let table: DatabaseTable = json;
                    tables.insert(name,table);
                },
                Err(_) => todo!(),
            }
        }
    Ok(())
    }

    async fn table(&self, s: Element, p: Element, o: Element) -> Result<DatabaseTable,WDSQErr> {
        let table = DatabaseTable::new(s,p,o);
        let mut tables = self.tables.write().await;
        let entry = tables.entry(table.name.to_owned()) ;
        if let std::collections::hash_map::Entry::Vacant(_) = entry {
            let sql = table.create_statement();
            let mut conn = self.db_conn().await?;
            conn.exec_drop(sql, ()).await?;

            let name = table.name.to_owned();
            let json = json!(table).to_string();
            let sql = "INSERT IGNORE INTO `table_list` (`name`,`json`) VALUES(:name,:json)";
            conn.exec_drop(sql, params!{name,json}).await?;
    
            entry.or_insert(table.clone());
        }
        Ok(table)
    }

    pub async fn add(&self, s: Element, p: Element, o: Element) -> Result<(),WDSQErr> {
        let table = self.table(s.clone(),p,o.clone()).await?;
        let mut values = s.values();
        values.append(&mut o.values());
        let mut cache = self.insert_cache.write().await;
        let entry = cache
            .entry(table.name.to_owned())
            .or_insert(DbOperationCache::new());
        entry.add(&s, &o, &table, values)?;
        entry.try_flush(&self).await?;
        Ok(())
    }

    pub async fn flush_insert_caches(&self) {
        let mut insert_cache = self.insert_cache.write().await;
        for (_table_name,cache) in insert_cache.iter_mut() {
            let _ = cache.force_flush(&self).await;
        }
        insert_cache.clear();
    }
}