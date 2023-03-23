use mysql_async::prelude::*;
use crate::{error::*, element::Element, database_table::DatabaseTable, app_state::AppState};

const INSERT_BATCH_SIZE: usize = 100;

#[derive(Debug, Clone)]
pub struct DbOperationCache {
    command: String,
    values: Vec<Vec<String>>,
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

    pub fn add(&mut self, k: &Element, v: &Element, table: &DatabaseTable, values: Vec<String>) -> Result<(),WDSQErr> {
        if values.is_empty() { // Nothing to do
            return Err("Nothing to do".into());
        }
        if self.number_of_values == 0 {
            self.number_of_values = values.len();
        }
        if self.number_of_values != values.len() {
            return Err(format!("Expected {}, got {} values",self.number_of_values,values.len()).into());
        }
        self.values.push(values.to_owned());

        // Create command if necessary
        if !self.command.is_empty() {
            return Ok(());
        }
        let mut fields: Vec<String> = k.fields("k");
        fields.append(&mut v.fields("v"));
        if fields.len()!=values.len() {
            return Err(format!("Expected {} fields, got {}",values.len(),fields.len()).into());
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
        let question_marks = vec![question_marks.as_str(); self.values.len()].join(",");
        let sql = format!("{} {question_marks}",self.command);
        let values: Vec<String> = self.values.clone().into_iter().flatten().collect();
        // println!("{sql}\n{:?}\n",&values);
        app.db_conn().await?.exec_drop(sql, &values).await?;
        self.values.clear();
        Ok(())
    }
}
