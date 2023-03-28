use crate::type_part::TypePart;

pub trait ElementType {
    fn from_str(s: &str) -> Self ;
    fn get_type_parts(&self) -> Vec<TypePart> ;
    fn values(&self) -> Vec<String> ;
    fn to_string(&self) -> String ;
    fn name(&self) -> &str ;
    fn table_name(&self) -> String ;
    fn to_url(&self) -> String ;
    fn sql_var_from_name(name: &str, prefix: &str) -> Option<Vec<String>> ;
    fn from_sql_values(name:&str, value: &Vec<String>) -> Option<Box<Self>> ;
}
