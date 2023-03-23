use std::{sync::Arc, collections::HashMap};

use crate::{query_part::QueryPart, app_state::AppState, database_table::DatabaseTable, error::WDSQErr, type_part::TypePart};

#[derive(Debug, Clone)]
struct SqlPart {
    pub sql: String,
    pub values: Vec<String>,
    pub table: Option<String>,
}

impl SqlPart {
    pub fn new(sql: String, value: String) -> Self {
        SqlPart {
            sql, 
            values: vec![value],
            table: None,
        }
    }
    pub fn and(&mut self, other: &mut SqlPart) {
        self.sql = format!("({}) AND ({})",self.sql,other.sql);
        self.values.append(&mut other.values);
    }

    pub fn union_all(&mut self, other: &mut SqlPart) {
        self.sql = format!("{} UNION ALL {}",self.sql,other.sql);
        self.values.append(&mut other.values);
    }
}

#[derive(Debug, Clone)]
pub struct QueryTriples {
    app: Arc<AppState>,
    s: QueryPart,
    p: QueryPart,
    o: QueryPart,
}

impl QueryTriples {
    pub fn new(app: &Arc<AppState>, s:QueryPart, p:QueryPart, o:QueryPart ) -> Self {
        Self {
            app: app.clone(),
            s,
            p,
            o,
        }
    }

    fn table_matches_property(&self, table: &DatabaseTable) -> bool {
        match &self.p {
            QueryPart::Element(element) => table.property() == element.get_table_name(),
            QueryPart::Unknown => true,
        }
    }

    fn table_matches_part(&self, part: &QueryPart, name: &str) -> bool {
        match part {
            QueryPart::Element(element) => name == element.name(),
            QueryPart::Unknown => true,
        }
    }


    fn table_matches_subject(&self, table: &DatabaseTable) -> bool {
        self.table_matches_part(&self.s, &table.names().0)
    }

    fn table_matches_object(&self, table: &DatabaseTable) -> bool {
        self.table_matches_part(&self.o, &table.names().2)
    }

    pub async fn filter_tables(&self) -> Vec<String> {
        self.app.tables.read().await
            .iter()
            .filter(|(_,table)|self.table_matches_property(table))
            .filter(|(_,table)|self.table_matches_subject(table))
            .filter(|(_,table)|self.table_matches_object(table))
            .map(|(table_name,_table)|table_name.to_string())
            .collect()
    }

    pub async fn group_tables(&self, tables: Vec<String>) -> HashMap<String,Vec<String>> {
        let mut ret = HashMap::new() ;
        for table_name in tables {
            if let Some(table) = self.app.tables.read().await.get(&table_name) {
                let names = table.names();
                let key = format!("{}__{}__{}",names.0,names.1,names.2);
                ret.entry(key).or_insert(vec![]).push(table_name);
            }
        }
        ret
    }

    fn get_sql_conditions(&self, part: &QueryPart, key: &str) -> Vec<SqlPart> {
        match part {
            QueryPart::Element(element) => {
                element.get_type_parts()
                    .iter()
                    .enumerate()
                    .filter(|(_num,part)|**part!=TypePart::Blank)
                    .map(|(num,_part)|num)
                    .zip(element.values().iter())
                    .map(|(num,value)|SqlPart::new(format!("`{key}{num}`=?"),value.to_owned()))
                    .collect()
            }
            QueryPart::Unknown => vec![],
        }
    }

    async fn sql_for_table(&self, table_name: &str) -> Result<Option<SqlPart>,WDSQErr> {
        let mut conditions = self.get_sql_conditions(&self.s,"k");
        conditions.append(&mut self.get_sql_conditions(&self.o,"v"));
        while conditions.len()>1 {
            let mut c = conditions.pop().unwrap(); // Safe
            conditions[0].and(&mut c);
        }
        let mut condition = match conditions.pop() {
            Some(condition) => condition,
            None => return Ok(None),
        };
        condition.table = Some(table_name.to_string());
        Ok(Some(condition))
    }

    // These tables must have the same columns, and the columns must have the the same meaning.
    async fn process_similar_tables(&self, table_names: &Vec<String>) -> Result<Option<SqlPart>,WDSQErr> {
        let mut ret = vec![] ;
        for table_name in table_names {
            match self.sql_for_table(table_name).await? {
                Some(sql) => ret.push(sql),
                None => {},
            }
        }
        for part in ret.iter_mut() {
            match &part.table {
                Some(table_name) => part.sql = format!("SELECT * FROM `{table_name}` WHERE {}",part.sql),
                None => {}, // TODO error
            }
            part.table = None;
        }
        while ret.len()>1 {
            let mut c = ret.pop().unwrap(); // Safe
            ret[0].union_all(&mut c);
        }
        Ok(ret.pop())
    }

    pub async fn process_grouped_tables(&self, grouped_tables: HashMap<String,Vec<String>>) -> Result<HashMap<String,Vec<String>>,WDSQErr> {
        let mut ret = HashMap::new();
        for (group_key,table_names) in grouped_tables {
            match self.process_similar_tables(&table_names).await? {
                Some(result) => {
                    ret.insert(group_key,result);
                }
                None => todo!(),
            }
        }
        println!("{ret:?}");

        Ok(HashMap::new()) // TODO FIXME
    }
}