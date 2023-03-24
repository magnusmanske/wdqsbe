use std::sync::Arc;

use crate::{element::Element, app_state::AppState, error::WDSQErr};

#[derive(Debug, Clone)]
pub enum QueryPart {
    Element(Element),
    Unknown,
}

impl QueryPart {
    pub fn from_str(s: &str, app: &Arc<AppState>) -> Result<Self,WDSQErr> {
        Ok(QueryPart::Element(Element::from_str(app.replace_prefix(s)).ok_or_else(||format!("QueryPart::from_str: Can not parse '{s}'"))?))
    }
}