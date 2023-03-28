use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TypePart {
    Text,
    ShortText,
    Int,
    Float,
    Blank,
}

impl TypePart {
    pub fn create_sql(&self) -> Option<&str> {
        match self {
            TypePart::Text => Some("VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL"),
            TypePart::ShortText => Some("VARCHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL"),
            TypePart::Int => Some("INT(11) UNSIGNED NOT NULL"),
            TypePart::Float => Some("DOUBLE(11,4) UNSIGNED NOT NULL"),
            TypePart::Blank => None,
        }
    }
}