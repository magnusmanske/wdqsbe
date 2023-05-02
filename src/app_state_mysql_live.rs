use async_trait::async_trait;
use std::{time::Duration, collections::HashMap};
use mysql_async::{prelude::*,Conn,Opts,OptsBuilder,PoolConstraints,PoolOpts};
use serde_json::{Value, json};
use crate::{error::*, database_table::DatabaseTable, db_operation_cache::DbOperationCacheValue, query_triples::{QueryTriples, DatabaseQueryResult}, app_state::*};

#[derive(Debug, Clone)]
pub struct AppStateLiveMySQL {
    db_pool: mysql_async::Pool,
}

impl AppStateLiveMySQL {
    pub fn new(config: &Value) -> Self {
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
    async fn init_from_db(&self, app: &AppState) -> Result<(),WDQSErr> {
        let mut conn = self.db_conn().await?;
        conn.exec_drop(MYSQL_CREATE_TEXTS_TABLE, ()).await?;
        conn.exec_drop(MYSQL_CREATE_TABLE_LIST_TABLE, ()).await?;
        let sql = r#"SELECT `name`,`json` FROM `table_list`"# ;
        let results = conn
            .exec_iter(sql, ()).await?
            .map_and_drop(|row| mysql_async::from_row::<(String,String)>(row)).await?;
        for (name,json) in results {
            let table: DatabaseTable = serde_json::from_str(&json)?;
            app.tables.insert(name,table);
        }
        Ok(())
    }

    /// Returns a connection to the tool database
    async fn db_conn(&self) -> Result<Conn, mysql_async::Error> {
        self.db_pool.get_conn().await
    }

    async fn add_to_table_list(&self, table: &DatabaseTable) -> Result<(),WDQSErr> {
        let name = table.name.to_owned();
        let json = json!(table).to_string();
        let sql = table.create_statement();
        let mut conn = self.db_conn().await?;
        conn.exec_drop(sql, ()).await?;
        let sql = "INSERT IGNORE INTO `table_list` (`name`,`json`) VALUES(:name,:json)";
        conn.exec_drop(sql, params!{name,json}).await?;
        Ok(())
    }

    async fn prepare_text(&self, text_chunk: &[String]) -> Result<(),WDQSErr> {
        let question_marks = vec!["(?)"; text_chunk.len()].join(",");
        let sql = format!("INSERT IGNORE INTO `texts` (`value`) VALUES {question_marks}");
        self.db_conn().await?.exec_drop(sql, &text_chunk.to_owned()).await?;
        Ok(())
    }

    async fn force_flush(&self, command: &str, value_chunk: &[Vec<DbOperationCacheValue>]) -> Result<Vec<(String, Vec<String>)>,WDQSErr> {
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

    async fn run_query(&self, _app: &AppState, query: &QueryTriples) -> Result<HashMap<String,DatabaseQueryResult>,WDQSErr> {
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
