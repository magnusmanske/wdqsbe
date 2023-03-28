use regex::Regex;

use crate::{element_type::ElementType, type_part::TypePart};

lazy_static! {
    static ref RE_POINT: Regex = Regex::new(r#"^Point\(([+-]?[0-9.]+)\s+([+-]?[0-9.]+)\)$"#).expect("RE_POINT does not parse");
}

#[derive(Clone, Debug)]
pub struct LatLon {
    latitude: f64,
    longitude: f64,
}

impl ElementType for LatLon {
    fn from_str(s: &str) -> LatLon {
        if let Some(caps) = RE_POINT.captures(&s) {
            return LatLon {
                latitude: caps.get(1).unwrap().as_str().parse::<f64>().unwrap(),
                longitude: caps.get(2).unwrap().as_str().parse::<f64>().unwrap(),
            };
        }
        panic!("Bad LatLon: {s}");
    }

    fn from_sql_values(name:&str, value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            "LatLon" => Some(Box::new(LatLon {
                latitude: value[0].parse::<f64>().unwrap(),
                longitude: value[1].parse::<f64>().unwrap()
            })),
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        vec![TypePart::Float,TypePart::Float]
    }

    fn values(&self) -> Vec<String>  {
        vec![format!("{}",self.latitude),format!("{}",self.longitude)]
    }

    fn to_string(&self) -> String  {
        format!("Point({} {})", self.latitude, self.longitude)
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
        Some(vec![format!("{prefix}0"),format!("{prefix}1")])
    }
}