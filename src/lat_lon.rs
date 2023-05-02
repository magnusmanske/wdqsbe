use regex::Regex;

use crate::{element_type::ElementType, type_part::TypePart, db_operation_cache::DbOperationCacheValue};

lazy_static! {
    static ref RE_POINT: Regex = Regex::new(r#"^Point\(([+-]?[0-9.]+)\s+([+-]?[0-9.]+)\)$"#).expect("RE_POINT does not parse");
    static ref RE_POINT_GLOBE: Regex = Regex::new(r#"^<http://www.wikidata.org/entity/Q(\d+)> Point\(([+-]?[0-9.]+)\s+([+-]?[0-9.]+)\)$"#).expect("RE_POINT does not parse");
}

#[derive(Clone, Debug)]
pub struct LatLon {
    latitude: f64,
    longitude: f64,
    globe_q: u32,
}

impl ElementType for LatLon {
    fn from_str(s: &str) -> Option<Box<Self>> {
        if let Some(caps) = RE_POINT.captures(&s) {
            return Some(Box::new(LatLon {
                latitude: caps.get(1)?.as_str().parse::<f64>().ok()?,
                longitude: caps.get(2)?.as_str().parse::<f64>().ok()?,
                globe_q: 2,
            }));
        }
        if let Some(caps) = RE_POINT_GLOBE.captures(&s) {
            return Some(Box::new(LatLon {
                latitude: caps.get(2)?.as_str().parse::<f64>().ok()?,
                longitude: caps.get(3)?.as_str().parse::<f64>().ok()?,
                globe_q: caps.get(1)?.as_str().parse::<u32>().ok()?,
            }));
        }
        None
    }

    fn from_sql_values(name:&str, value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            "LatLon" => LatLon::from_str(&value[0].parse::<String>().ok()?), // TODO globe
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::Point, TypePart::U32]
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        vec![
            DbOperationCacheValue::Expression(format!("PointFromText(\"{}\")", self.to_string())),
            DbOperationCacheValue::U32(self.globe_q),
            ]
    }

    fn to_string(&self) -> String  {
        format!("Point({} {}) on http://www.wikidata.org/entity/Q{}", self.latitude, self.longitude, self.globe_q)
    }

    fn name(&self) -> &str  {
        "LatLon"
    }

    fn table_name(&self) -> String  {
        self.name().to_string()
    }

    fn to_url(&self) -> String  {
        self.to_string() // TODO CHECKME FIXME
    }

    fn sql_var_from_name(_name: &str, prefix: &str) -> Option<Vec<String>>  {
        Some(vec![format!("{prefix}0"), format!("{prefix}1")])
    }
}