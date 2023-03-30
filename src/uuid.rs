use regex::Regex;

use crate::{element_type::ElementType, type_part::TypePart, db_operation_cache::DbOperationCacheValue};

lazy_static! {
    static ref RE_UUID40_NO_DASHES: Regex = Regex::new(r#"^[a-zA-Z0-9]{40}$"#).expect("RE_UUID40_NO_DASHES does not parse");
    static ref RE_UUID32_NO_DASHES: Regex = Regex::new(r#"^[a-zA-Z0-9]{32}$"#).expect("RE_UUID32_NO_DASHES does not parse");
}

#[derive(Clone, Debug)]
pub struct UUID40 {
    uuid: String,
}

impl From<String> for UUID40 {
    fn from(s: String) -> Self {
        match UUID40::from_str(&s) {
            Some(ret) => *ret,
            None => panic!("Bad UUID: >{s}<"),
        }
    }
}

impl ElementType for UUID40 {
    fn from_str(s: &str) -> Option<Box<Self>> {
        if RE_UUID40_NO_DASHES.is_match(&s.replace('-',"")) {
            let uuid = s.replace('-',"").to_ascii_lowercase();
            Some(Box::new(UUID40 { uuid }))
        } else {
            None
        }
    }

    fn from_sql_values(name:&str, value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            "UUID" => UUID40::from_str(&value[0].parse::<String>().unwrap()),
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::UUID40]
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        let s = format!("UNHEX(\"{}\")", &self.uuid);
        // println!("{s}");
        vec![DbOperationCacheValue::Expression(s)]
    }

    fn to_string(&self) -> String  {
        self.uuid.to_owned() // TODO dashes?
    }

    fn name(&self) -> &str  {
        "UUID"
    }

    fn table_name(&self) -> String  {
        self.name().to_string()
    }

    fn to_url(&self) -> String  {
        self.to_string() // TODO CHECKME FIXME
    }

    fn sql_var_from_name(_name: &str, prefix: &str) -> Option<Vec<String>>  {
        Some(vec![format!("{prefix}0")])
    }
}




#[derive(Clone, Debug)]
pub struct UUID32 {
    uuid: String,
}

impl From<String> for UUID32 {
    fn from(s: String) -> Self {
        match UUID32::from_str(&s) {
            Some(ret) => *ret,
            None => panic!("Bad UUID32: >{s}<"),
        }
    }
}

impl ElementType for UUID32 {
    fn from_str(s: &str) -> Option<Box<Self>> {
        if RE_UUID32_NO_DASHES.is_match(&s.replace('-',"")) {
            let uuid = s.replace('-',"").to_ascii_lowercase();
            Some(Box::new(UUID32 { uuid }))
        } else {
            None
        }
    }

    fn from_sql_values(name:&str, value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            "UUID" => UUID32::from_str(&value[0].parse::<String>().unwrap()),
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::UUID32]
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        let s = format!("UNHEX(\"{}\")", &self.uuid);
        // println!("{s}");
        vec![DbOperationCacheValue::Expression(s)]
    }

    fn to_string(&self) -> String  {
        self.uuid.to_owned() // TODO dashes?
    }

    fn name(&self) -> &str  {
        "UUID32"
    }

    fn table_name(&self) -> String  {
        self.name().to_string()
    }

    fn to_url(&self) -> String  {
        self.to_string() // TODO CHECKME FIXME
    }

    fn sql_var_from_name(_name: &str, prefix: &str) -> Option<Vec<String>>  {
        Some(vec![format!("{prefix}0")])
    }
}