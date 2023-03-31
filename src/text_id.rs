use crate::{element_type::ElementType, type_part::TypePart, db_operation_cache::DbOperationCacheValue};

#[derive(Clone, Debug)]
pub struct TextId {
    s: String,
}

impl TextId {
    fn from_str_lossy(s: &str) -> Self {
        // Percent-decode if required
        let s = if s.contains('%') {
            percent_encoding::percent_decode(s.as_bytes()).decode_utf8_lossy().to_string()
        } else {
            s.to_string()
        };

        // Unicode backslash decode, eg "\u6BD4\u5229\u65F6"@zh
        // DOES NOT WORK TOTO FIXME
        // let s = if s.contains("\\u") {
        //     match serde_json::from_str(&s) {
        //         Ok(decoded) => decoded,
        //         Err(_) => s,
        //     }
        // } else {
        //     s
        // };
        Self{s}
    }
}

impl From<String> for TextId {
    fn from(s: String) -> Self {
        Self::from_str_lossy(&s)
    }
}

impl From<&str> for TextId {
    fn from(s: &str) -> Self {
        Self::from_str_lossy(&s)
    }
}

impl ElementType for TextId {
    fn from_str(s: &str) -> Option<Box<Self>> {
        Some(Box::new(Self::from_str_lossy(s)))
    }

    fn from_sql_values(name:&str, _value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            // "TextId" => TextId::from_str(&value[0].parse::<String>().unwrap()), // TODO CHECK
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::Int]
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
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