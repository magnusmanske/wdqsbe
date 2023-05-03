use serde::{Serialize, Deserialize};
use crate::{error::*, element::Element, database_table::DatabaseTable, app_state::AppState, app_state_mysql_stdout::AppStateStdoutMySQL};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DbOperationCacheValue {
    Quoted(String), // Some ID string
    Text(String), // To become a lookup in the text table
    Expression(String), // Some SQL expression
    Usize(usize), // A number
    I16(i16), // signed 16-bit integer
    U16(u16), // unsigned 16-bit integer
    I32(i32), // signed 32-bit integer
    U32(u32), // unsigned 32-bit integer
    I64(i64), // signed 64-bit integer
    U8(u8), // unsigned 8-bit integer
}

impl DbOperationCacheValue {
    pub fn as_sql_placeholder(&self) -> String {
        match self {
            DbOperationCacheValue::Quoted(_) => "?".to_string(),
            DbOperationCacheValue::Text(_) => format!("(SELECT `id` FROM `texts` WHERE `value`=?)"),
            DbOperationCacheValue::Expression(s) => s.to_string(),
            DbOperationCacheValue::Usize(u) => format!("{u}"),
            DbOperationCacheValue::I16(u) => format!("{u}"),
            DbOperationCacheValue::U16(u) => format!("{u}"),
            DbOperationCacheValue::I32(u) => format!("{u}"),
            DbOperationCacheValue::U32(u) => format!("{u}"),
            DbOperationCacheValue::I64(u) => format!("{u}"),
            DbOperationCacheValue::U8(u) => format!("{u}"),
        }
    }

    pub fn as_sql_stdout(&self) -> String {
        match self {
            DbOperationCacheValue::Quoted(s) => format!("\"{}\"",AppStateStdoutMySQL::sql_escape(s)),
            DbOperationCacheValue::Text(s) => format!("(SELECT `id` FROM `texts` WHERE `value`=\"{}\")",AppStateStdoutMySQL::sql_escape(s)),
            _ => self.as_sql_placeholder(),
        }
    }

    pub fn as_sql_variable(&self) -> Option<String> {
        match self {
            DbOperationCacheValue::Quoted(s) => Some(s.to_string()),
            DbOperationCacheValue::Text(s) => Some(s.to_string()),
            DbOperationCacheValue::Expression(_) => None,
            DbOperationCacheValue::Usize(_) => None,
            DbOperationCacheValue::I16(_) => None,
            DbOperationCacheValue::U16(_) => None,
            DbOperationCacheValue::I32(_) => None,
            DbOperationCacheValue::U32(_) => None,
            DbOperationCacheValue::I64(_) => None,
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
            Self::I64(u) => format!("{u}"),
            Self::U8(u) => format!("{u}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbOperationCache {
    pub command: String,
    pub values: Vec<Vec<DbOperationCacheValue>>,
}

impl DbOperationCache {
    pub fn new() -> Self {
        Self {
            command: String::new(),
            values: vec![],
        }
    }

    pub async fn clear(&mut self) {
        self.values = vec![];
    }

    pub async fn add(&mut self, k: &Element, v: &Element, table: &DatabaseTable, values: Vec<DbOperationCacheValue>, app: &AppState) -> Result<(),WDQSErr> {
        if values.is_empty() {
            return Err(format!("DbOperationCache::add: Nothing to do for {k:?} / {v:?}").into());
        }

        if self.command.is_empty() {
            let mut fields: Vec<String> = k.fields("k");
            fields.append(&mut v.fields("v"));
            if fields.len()!=values.len() {
                return Err(format!("DbOperationCache::add: [2] Expected {} fields, got {}",values.len(),fields.len()).into());
            }
            self.command = format!("INSERT IGNORE INTO `{}` (`{}`) VALUES ",&table.name,fields.join("`,`"));
        }

        self.values.push(values);
        if self.values.len()>=app.insert_batch_size {
            self.force_flush(app).await?;
        }
        Ok(())
    }

    async fn prepare_text(&self, app: &AppState) -> Result<(),WDQSErr> {
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
            app.prepare_text(text_chunk).await?;
        }
        Ok(())
    }

    pub async fn force_flush(&mut self, app: &AppState) -> Result<(),WDQSErr> {
        if self.values.is_empty() {
            return Ok(());
        }

        self.prepare_text(app).await?;
        app.force_flush_all(&self).await?;
        self.values = vec![];

        Ok(())
    }
}
