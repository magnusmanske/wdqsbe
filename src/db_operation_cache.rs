use std::sync::Arc;
use serde::{Serialize, Deserialize};
use mysql_async::{prelude::*, Conn};
use crate::{error::*, element::Element, database_table::DatabaseTable, app_state::AppState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DbOperationCacheValue {
    Quoted(String), // Some ID string
    Text(String), // To become a lookup in the text table
    Expression(String), // Some SQL expression
    Usize(usize), // A number
    I16(i16), // signed 16-bit integer
    U16(u16), // unsigned 16-bit integer
    I32(i32), // signed 16-bit integer
    U32(u32), // unsigned 16-bit integer
    U8(u8), // unsigned 8-bit integer
}

impl DbOperationCacheValue {
    fn as_sql_placeholder(&self) -> String {
        match self {
            DbOperationCacheValue::Quoted(_) => "?".to_string(),
            DbOperationCacheValue::Text(_) => format!("(SELECT `id` FROM `texts` WHERE `value`=?)"),
            DbOperationCacheValue::Expression(s) => s.to_string(),
            DbOperationCacheValue::Usize(u) => format!("{u}"),
            DbOperationCacheValue::I16(u) => format!("{u}"),
            DbOperationCacheValue::U16(u) => format!("{u}"),
            DbOperationCacheValue::I32(u) => format!("{u}"),
            DbOperationCacheValue::U32(u) => format!("{u}"),
            DbOperationCacheValue::U8(u) => format!("{u}"),
        }
    }

    fn as_sql_variable(&self) -> Option<String> {
        match self {
            DbOperationCacheValue::Quoted(s) => Some(s.to_string()),
            DbOperationCacheValue::Text(s) => Some(s.to_string()),
            DbOperationCacheValue::Expression(_) => None,
            DbOperationCacheValue::Usize(_) => None,
            DbOperationCacheValue::I16(_) => None,
            DbOperationCacheValue::U16(_) => None,
            DbOperationCacheValue::I32(_) => None,
            DbOperationCacheValue::U32(_) => None,
            DbOperationCacheValue::U8(_) => None,
        }
    }
}

impl From<String> for DbOperationCacheValue {
    fn from(s: String) -> Self {
        Self::Quoted(s)
    }
}

impl From<&String> for DbOperationCacheValue {
    fn from(s: &String) -> Self {
        Self::Quoted(s.to_owned())
    }
}

impl From<&str> for DbOperationCacheValue {
    fn from(s: &str) -> Self {
        Self::Quoted(s.to_string())
    }
}

impl ToString for DbOperationCacheValue {
    fn to_string(&self) -> String {
        match self {
            Self::Quoted(s) => s.to_owned(),
            Self::Text(s) => s.to_owned(),
            Self::Expression(s) => s.to_owned(),
            Self::Usize(u) => format!("{u}"),
            Self::I16(u) => format!("{u}"),
            Self::U16(u) => format!("{u}"),
            Self::I32(u) => format!("{u}"),
            Self::U32(u) => format!("{u}"),
            Self::U8(u) => format!("{u}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbOperationCache {
    command: String,
    values: Vec<Vec<DbOperationCacheValue>>,
    number_of_values: usize,
}

impl DbOperationCache {
    pub fn new() -> Self {
        Self {
            command: String::new(),
            values: vec![],
            number_of_values: 0,
        }
    }

    pub fn clear(&mut self) {
        self.values.clear();
    }

    pub async fn add(&mut self, k: &Element, v: &Element, table: &DatabaseTable, values: Vec<DbOperationCacheValue>, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        if values.is_empty() {
            return Err(format!("DbOperationCache::add: Nothing to do for {k:?} / {v:?}").into());
        }
        if self.number_of_values == 0 {
            self.number_of_values = values.len();
        }
        if self.number_of_values != values.len() {
            return Err(format!("DbOperationCache::add: [1] Expected {}, got {} values",self.number_of_values,values.len()).into());
        }
        self.values.push(values.to_owned());

        // Create command if necessary
        if !self.command.is_empty() {
            return Ok(());
        }
        let mut fields: Vec<String> = k.fields("k");
        fields.append(&mut v.fields("v"));
        if fields.len()!=values.len() {
            return Err(format!("DbOperationCache::add: [2] Expected {} fields, got {}",values.len(),fields.len()).into());
        }
        self.command = format!("INSERT IGNORE INTO `{}` (`{}`) VALUES ",&table.name,fields.join("`,`"));
        if self.values.len()>=app.insert_batch_size {
            self.force_flush(app).await?;
        }
        Ok(())
    }

    async fn prepare_text(&self, conn: &mut Conn) -> Result<(),WDSQErr> {
        let mut texts: Vec<_> = self.values
            .iter()
            .flatten()
            .filter_map(|part|{
                match part {
                    DbOperationCacheValue::Text(s) => Some(s.to_owned()),
                    _ => None
                }
            })
            .collect();
        if texts.is_empty() {
            return Ok(());
        }
        texts.sort();
        texts.dedup();
        for text_chunk in texts.chunks(100) { // chunks prevent "Packet too large" errors
            let question_marks = vec!["(?)"; text_chunk.len()].join(",");
            let sql = format!("INSERT IGNORE INTO `texts` (`value`) VALUES {question_marks}");
            conn.exec_drop(sql, &text_chunk.to_owned()).await?;
        }
        Ok(())
    }

    pub async fn force_flush(&mut self, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        if self.values.is_empty() {
            return Ok(());
        }

        let mut conn = app.db_conn().await?;
        self.prepare_text(&mut conn).await?;
        for value_chunk in self.values.chunks(1000) {
            let question_marks: Vec<_> = value_chunk
                .iter()
                .map(|parts|{
                    let ret: Vec<_> = parts.iter().map(|part|part.as_sql_placeholder()).collect();
                    format!("({})",ret.join(","))
                })
                .collect();
            let question_marks = question_marks.join(",");
            let values: Vec<_> = value_chunk
                .iter()
                .map(|parts|{
                    let ret: Vec<_> = parts.iter().filter_map(|part|part.as_sql_variable()).collect();
                    ret
                })
                .flatten()
                .collect();
            let sql = format!("{} {question_marks}",self.command);
            conn.exec_drop(sql, &values).await?;

        }
        self.values.clear();
        Ok(())
    }
}
