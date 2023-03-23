use crate::element::Element;

#[derive(Debug, Clone)]
pub enum QueryPart {
    Element(Element),
    Unknown,
}