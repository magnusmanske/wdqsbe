use std::{sync::Arc, collections::HashMap, fmt::{Display, self}};

use crate::{query_part::QueryPart, app_state::AppState, database_table::DatabaseTable, error::WDSQErr, type_part::TypePart, element::Element};

#[derive(Debug, Clone, Default)]
pub struct DatabaseQueryResult {
    pub variables: Vec<SqlVariable>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlVariable {
    name: String,
    kind: Option<String>,
}

impl Display for SqlVariable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl SqlVariable {
    pub fn sql_value2string(&self,v: &mysql_async::Value) -> Option<String> {
        let v = match v {
            mysql_async::Value::NULL => return None,
            mysql_async::Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
            mysql_async::Value::Int(i) => format!("{i}"),
            mysql_async::Value::UInt(i)=> format!("{i}"),
            mysql_async::Value::Float(_) => todo!(),
            mysql_async::Value::Double(_) => todo!(),
            mysql_async::Value::Date(_, _, _, _, _, _, _) => todo!(),
            mysql_async::Value::Time(_, _, _, _, _, _) => todo!(),
        };
        let element_name = self.kind.to_owned()?;
        let element = Element::from_sql_values(&element_name,&vec![v]);
        element.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct SqlPart {
    pub sql: String,
    pub values: Vec<String>,
    pub table: Option<String>,
    pub variables: Vec<SqlVariable>,
}

impl SqlPart {
    pub fn new(sql: String, value: String) -> Self {
        SqlPart {
            sql, 
            values: vec![value],
            table: None,
            variables: vec![],
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

#[derive(Debug, Clone, Default)]
struct QueryPartMeta {
    variable: Option<String>,
}

#[derive(Debug, Clone)]
pub struct QueryTriples {
    s: QueryPart,
    p: QueryPart,
    o: QueryPart,
    s_meta: QueryPartMeta,
    p_meta: QueryPartMeta,
    o_meta: QueryPartMeta,
    pub result: HashMap<String,SqlPart>,
}

impl QueryTriples {
    pub fn new(s:QueryPart, p:QueryPart, o:QueryPart ) -> Self {
        Self {
            s,
            p,
            o,
            s_meta: QueryPartMeta::default(),
            o_meta: QueryPartMeta::default(),
            p_meta: QueryPartMeta::default(),
            result: HashMap::new(),
        }
    }

    pub async fn from_str(app: &Arc<AppState>, s: &str, p: &str, o: &str ) -> Result<Self,WDSQErr> {
        let (s,s_meta) = Self::meta_part_from_string(s, &app)?;
        let (p,p_meta) = Self::meta_part_from_string(p, &app)?;
        let (o,o_meta) = Self::meta_part_from_string(o, &app)?;
        let mut ret = Self {
            s, 
            p, 
            o, 
            s_meta, 
            p_meta, 
            o_meta,
            result: HashMap::new(),
        };
        ret.process(app).await?;
        Ok(ret)
    }

    fn meta_part_from_string(s: &str, app: &Arc<AppState>) -> Result<(QueryPart,QueryPartMeta),WDSQErr> {
        if let Some((should_be_blank,var_name)) = s.split_once("?") {
            if should_be_blank.is_empty() {
                let part = QueryPart::Unknown;
                let meta = QueryPartMeta { variable:Some(var_name.to_string()) } ;
                return Ok((part,meta));
            }
        }
        let part = QueryPart::from_str(s, &app)?;
        let meta = QueryPartMeta::default();
        Ok((part,meta))
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

    async fn filter_tables(&self, app: &AppState) -> Vec<String> {
        app.tables.read().await
            .iter()
            .filter(|(_,table)|self.table_matches_property(table))
            .filter(|(_,table)|self.table_matches_subject(table))
            .filter(|(_,table)|self.table_matches_object(table))
            .map(|(table_name,_table)|table_name.to_string())
            .collect()
    }

    async fn group_tables(&self, tables: Vec<String>, app: &AppState) -> HashMap<String,Vec<String>> {
        let mut ret = HashMap::new() ;
        for table_name in tables {
            if let Some(table) = app.tables.read().await.get(&table_name) {
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
                    .map(|(num,value)|SqlPart::new(format!("`{key}{num}`=?"),value.to_string()))
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
    async fn process_similar_tables(&self, table_names: &Vec<String>, app: &AppState) -> Result<Option<SqlPart>,WDSQErr> {
        let mut ret = vec![] ;
        for table_name in table_names {
            match self.sql_for_table(table_name).await? {
                Some(sql) => ret.push(sql),
                None => {},
            }
        }
        for part in ret.iter_mut() {
            match &part.table {
                Some(table_name) => {
                    let (params,variables) = self.get_sql_return_params(&table_name,app).await?;
                    if params.is_empty() {
                        return Err("QueryTriples::process_similar_tables: Parameter list if empty".into());
                    }
                    let params = params.join(",");
                    part.sql = format!("SELECT {params} FROM `{table_name}` WHERE {}",part.sql);
                    part.variables = variables;
                }
                None => return Err("QueryTriples::process_similar_tables: Missing table name".into()),
            }
            part.table = None;
        }
        while ret.len()>1 {
            if let Some(mut c) = ret.pop() {
                ret[0].union_all(&mut c);
            }
        }
        Ok(ret.pop())
    }

    async fn get_sql_return_params(&self, table_name: &str, app: &AppState) -> Result<(Vec<String>,Vec<SqlVariable>),WDSQErr> {
        let mut params = vec![];
        let mut ret_variables = vec![];
        let tables = app.tables.read().await;
        let table = tables.get(table_name).ok_or_else(|| WDSQErr::String(format!("get_sql_return_params: Missing table '{table_name}'")))?;
        let names = table.names().to_owned();

        if let Some(variable) = &self.s_meta.variable {
            let sql_variables = Element::sql_var_from_name(&names.0,"k");
            match sql_variables.len() {
                0 => {}
                1 => {
                    let sql_variable = sql_variables[0].to_owned();
                    params.push(format!("{sql_variable} AS `{variable}`"));
                    ret_variables.push(SqlVariable{
                        name:variable.to_owned(),
                        kind: Some(table.names().0.to_owned()),
                    });
                }
                _ => return Err("get_sql_return_params: Too many variables from sql_var_from_name".into()),
            }
        }

        if let Some(_variable) = &self.p_meta.variable {
            // let fixed_value = table.values().join("_"); // TODO check
            // let fixed_value = "SOMETHING"; // TODO FIXME
            // params.push(format!("\"{fixed_value}\" AS `{variable}`"));
            // ret_variables.push(SqlVariable {
            //     name:variable.to_owned(),
            //     kind: Some(table.names().1.to_owned()),
            // });
            todo!();
        }

        if let Some(variable) = &self.o_meta.variable {
            let sql_variables = Element::sql_var_from_name(&names.2,"k");
            match sql_variables.len() {
                0 => {}
                1 => {
                    let sql_variable = sql_variables[0].to_owned();
                    params.push(format!("{sql_variable} AS `{variable}`"));
                    ret_variables.push(SqlVariable {
                        name:variable.to_owned(),
                        kind: Some(table.names().2.to_owned()),
                    });
                }
                _ => return Err("get_sql_return_params: Too many variables from sql_var_from_name".into()),
            }
        }

        Ok((params,ret_variables))
    }

    async fn process_grouped_tables(&self, grouped_tables: HashMap<String,Vec<String>>, app: &AppState) -> Result<HashMap<String,SqlPart>,WDSQErr> {
        let mut ret = HashMap::new();
        for (group_key,table_names) in grouped_tables {
            match self.process_similar_tables(&table_names, app).await? {
                Some(result) => {
                    ret.insert(group_key,result);
                }
                None => todo!(),
            }
        }
        Ok(ret) // TODO FIXME
    }

    pub async fn process(&mut self, app: &AppState) -> Result<(),WDSQErr> {
        let result = self.filter_tables(app).await;
        let result = self.group_tables(result, app).await;
        let result = self.process_grouped_tables(result, app).await?;
        self.result = result;
        Ok(())
    }

    pub fn and(&mut self, other: &Self) -> Result<(),WDSQErr> {
        self.join("INNER JOIN", other)
    }
    
    fn join(&mut self, join: &str, other: &Self) -> Result<(),WDSQErr> {
        let mut result = HashMap::new();
        for (group_key,part) in &self.result {
            if let Some(other_part) = other.result.get(group_key) {
                let variables_common: Vec<_> = part.variables.iter().filter(|v|other_part.variables.contains(v)).cloned().collect();
                if variables_common.is_empty() {
                    return Err(format!("QueryTriples::and: No common variables between {self:?} and {other:?}").into());
                }
                let variables_t1: Vec<_> = part.variables.iter().filter(|v|!other_part.variables.contains(v)).collect();
                let variables_t2: Vec<_> = other_part.variables.iter().filter(|v|!part.variables.contains(v)).collect();
                let mut variables: Vec<_> = variables_common.iter().map(|v|format!("t1.{v}")).collect();
                variables.append(&mut variables_t1.iter().map(|v|format!("t1.{v}")).collect());
                variables.append(&mut variables_t2.iter().map(|v|format!("t2.{v}")).collect());
                let join_key: Vec<_> = variables_common.iter().map(|v|format!("t1.{v}=t2.{v}")).collect();
                let sql = format!("SELECT {} FROM ({}) AS t1\n{join} ({}) AS t2\nON {}",variables.join(","),part.sql,other_part.sql,join_key.join(","));
                let mut values = part.values.clone();
                values.append(&mut other_part.values.clone());
                result.insert(group_key.to_owned(),SqlPart{ sql, values, table: None, variables: variables_common });
            }
        }
        self.result = result ;
        Ok(())
    }

    pub async fn run(&self, app: &AppState) -> Result<HashMap<String,DatabaseQueryResult>,WDSQErr> {
        app.run_query(&self).await
    }
}