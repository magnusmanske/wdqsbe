use mysql_async::prelude::*;
use crate::{error::*, element::Element, database_table::DatabaseTable, app_state::AppState};

const INSERT_BATCH_SIZE: usize = 10;

#[derive(Debug, Clone)]
pub struct DbOperationCache {
    command: String,
    values: Vec<String>,
    number_of_values: usize,
    number_of_rows: usize,
}

impl DbOperationCache {
    pub fn new() -> Self {
        Self {
            command: String::new(),
            values: vec![],
            number_of_values: 0,
            number_of_rows: 0,
        }
    }

    pub fn add(&mut self, k: &Element, v: &Element, table: &DatabaseTable, values: Vec<String>) -> Result<(),WDSQErr> {
        if values.is_empty() { // Nothing to do
            return Err("Nothing to do".into());
        }
        let number_of_values = values.len();
        if self.number_of_values == 0 {
            self.number_of_values = number_of_values;
        }
        if self.number_of_values != number_of_values {
            return Err(format!("Expected {}, got {number_of_values} values",self.number_of_values).into());
        }
        let mut values = values.to_owned();
        self.values.append(&mut values);
        self.number_of_rows += 1;
        if !self.command.is_empty() {
            return Ok(());
        }
        let mut fields: Vec<String> = k.fields("k");
        fields.append(&mut v.fields("v"));
        if fields.len()!=number_of_values {
            return Err(format!("Expected {number_of_values} fields, got {}",fields.len()).into());
        }
        self.command = format!("INSERT IGNORE INTO `{}` (`{}`) VALUES ",&table.name,fields.join("`,`"));
        Ok(())
    }

    pub async fn try_flush(&mut self, app: &AppState) -> Result<(),WDSQErr> {
        if self.values.len()<INSERT_BATCH_SIZE {
            return Ok(())
        }
        self.force_flush(&app).await
    }

    pub async fn force_flush(&mut self, app: &AppState) -> Result<(),WDSQErr> {
        let question_marks = vec!["?"; self.number_of_values].join(",");
        let question_marks = format!("({question_marks})");
        let question_marks = vec![question_marks.as_str(); self.number_of_rows].join(",");
        let sql = format!("{} ({question_marks})",self.command);
        app.db_conn().await?.exec_drop(sql, &self.values).await?;
        self.values.clear();
        self.number_of_rows = 0;
        println!("Flushed {}",&self.command);
        Ok(())
    }
}
