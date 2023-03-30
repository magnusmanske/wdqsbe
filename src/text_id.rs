use regex::Regex;

use crate::{element_type::ElementType, type_part::TypePart, db_operation_cache::DbOperationCacheValue};

lazy_static! {
    static ref RE_POINT: Regex = Regex::new(r#"^Point\(([+-]?[0-9.]+)\s+([+-]?[0-9.]+)\)$"#).expect("RE_POINT does not parse");
}

#[derive(Clone, Debug)]
pub struct TextId {
    s: String,
}

impl From<String> for TextId {
    fn from(s: String) -> Self {
        Self{s}
    }
}
impl ElementType for TextId {
    fn from_str(s: &str) -> Option<Box<Self>> {
        Some(Box::new(Self { s: s.to_string() }))
    }

    fn from_sql_values(name:&str, _value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            // "TextId" => TextId::from_str(&value[0].parse::<String>().unwrap()),
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::Int]
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        //let safe_s = self.s.replace('"',""); // TODO FIXME
        //format!("(SELECT `id` FROM `texts` WHERE `value`=\"{safe_s}\")")
        vec![DbOperationCacheValue::Text(self.s.to_owned())]
    }

    fn to_string(&self) -> String  {
        self.s.to_owned()
    }

    fn name(&self) -> &str  {
        "TextId"
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