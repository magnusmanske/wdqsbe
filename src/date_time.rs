use regex::Regex;

use crate::{element_type::ElementType, type_part::TypePart, db_operation_cache::DbOperationCacheValue};

lazy_static! {
    static ref RE_DATE_TIME: Regex = Regex::new(r#"^([+-]?\d+)-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$"#).expect("RE_DATE_TIME does not parse");
}

#[derive(Clone, Debug)]
pub struct DateTime {
    year: i32,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
}

impl ElementType for DateTime {
    fn from_str(s: &str) -> Option<Box<Self>> {
        if let Some(caps) = RE_DATE_TIME.captures(&s) {
            return Some(Box::new(Self {
                year: caps.get(1)?.as_str().parse::<i32>().ok()?,
                month: caps.get(2)?.as_str().parse::<u8>().ok()?,
                day: caps.get(3)?.as_str().parse::<u8>().ok()?,
                hour: caps.get(4)?.as_str().parse::<u8>().ok()?,
                minute: caps.get(5)?.as_str().parse::<u8>().ok()?,
                second: caps.get(6)?.as_str().parse::<u8>().ok()?,
            }));
        }
        None
    }

    fn from_sql_values(name:&str, _value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            "DateTime" => todo!(),//DateTime::from_str(&value[0].parse::<String>().unwrap()),
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::I16,TypePart::U8,TypePart::U8,TypePart::U8,TypePart::U8,TypePart::U8]
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        //let safe_s = self.s.replace('"',""); // TODO FIXME
        //format!("(SELECT `id` FROM `texts` WHERE `value`=\"{safe_s}\")")
        vec![
            DbOperationCacheValue::I32(self.year),
            DbOperationCacheValue::U8(self.month),
            DbOperationCacheValue::U8(self.day),
            DbOperationCacheValue::U8(self.hour),
            DbOperationCacheValue::U8(self.minute),
            DbOperationCacheValue::U8(self.second),
            ]
    }

    fn to_string(&self) -> String  {
        format!("{}-{:02}-{:02}T{:02}:{:02}:{:02}Z", self.year, self.month, self.day, self.hour, self.minute, self.second)
    }

    fn name(&self) -> &str  {
        "DateTime"
    }

    fn table_name(&self) -> String  {
        self.name().to_string()
    }

    fn to_url(&self) -> String  {
        self.to_string() // TODO CHECKME FIXME
    }

    fn sql_var_from_name(_name: &str, prefix: &str) -> Option<Vec<String>>  {
        Some(vec![format!("{prefix}0"),format!("{prefix}1"),format!("{prefix}2")])
    }
}