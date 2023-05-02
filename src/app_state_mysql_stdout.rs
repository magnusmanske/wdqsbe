use async_trait::async_trait;
use std::collections::HashMap;
use mysql_async::Conn;
use serde_json::{Value, json};
use crate::{error::*, database_table::DatabaseTable, db_operation_cache::DbOperationCacheValue, query_triples::{QueryTriples, DatabaseQueryResult}, app_state::*};

#[derive(Debug, Clone)]
pub struct AppStateStdoutMySQL {
}

impl AppStateStdoutMySQL {
    pub fn new(_config: &Value) -> Self {
        Self{}
    }

    pub fn sql_escape(s: &str) -> String {
        s.replace('\\',"\\\\").replace('"', "\\\"")
    }

    fn sql_group_escape(&self, vs: &[String]) -> String {
        vs.iter().map(|s|Self::sql_escape(s)).map(|s|format!("(\"{s}\")")).collect::<Vec<String>>().join(",")
    }
}

#[async_trait]
impl AppDB for AppStateStdoutMySQL {
    async fn init_from_db(&self, _app: &AppState) -> Result<(),WDQSErr> {
        println!("{MYSQL_CREATE_TEXTS_TABLE};");
        println!("{MYSQL_CREATE_TABLE_LIST_TABLE};");
        Ok(())
    }

    async fn db_conn(&self) -> Result<Conn, mysql_async::Error> {
        let e = WDQSErr::String("AppStateStdoutMySQL::db_conn".into());
        Err(mysql_async::Error::Other(Box::new(e)))
    }

    async fn add_to_table_list(&self, table: &DatabaseTable) -> Result<(),WDQSErr> {
        let name = table.name.to_owned();
        let json = json!(table).to_string();
        let sql = table.create_statement();
        println!("{sql};");
        let sql = format!("INSERT IGNORE INTO `table_list` (`name`,`json`) VALUES(\"{name}\",\"{json}\")");
        println!("{sql};");
        Ok(())
    }

    async fn prepare_text(&self, text_chunk: &[String]) -> Result<(),WDQSErr> {
        let values = self.sql_group_escape(text_chunk);
        let sql = format!("INSERT IGNORE INTO `texts` (`value`) VALUES {values}");
        println!("{sql};");
        Ok(())
    }

    async fn force_flush(&self, command: &str, value_chunk: &[Vec<DbOperationCacheValue>]) -> Result<Vec<(String, Vec<String>)>,WDQSErr> {
        let question_marks: Vec<_> = value_chunk
            .iter()
            .map(|parts|{
                let ret = parts.iter().map(|part|part.as_sql_stdout()).collect::<Vec<String>>().join(",");
                format!("({ret})")
            })
            .collect();
        let question_marks = question_marks.join(",");
        let sql = format!("{} {question_marks}",command);
        println!("{sql}");
        Ok(vec![])
    }

    async fn run_query(&self, _app: &AppState, _query: &QueryTriples) -> Result<HashMap<String,DatabaseQueryResult>,WDQSErr> {
        panic!("AppStateStdoutMySQL::run_query can never be implemented")
    }
}