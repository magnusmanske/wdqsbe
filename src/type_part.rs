use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TypePart {
    Text,
    ShortText,
    Int,
    Float,
    Point,
    UUID40,
    UUID32,
    I16,
    I32,
    U8,
    Blank,
}

impl TypePart {
    pub fn create_sql(&self) -> Option<&str> {
        match self {
            TypePart::Text => Some("VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL"),
            TypePart::ShortText => Some("VARCHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL"),
            TypePart::Int => Some("INT(11) UNSIGNED NOT NULL"),
            TypePart::Float => Some("DOUBLE(11,4) UNSIGNED NOT NULL"),
            TypePart::Point => Some("POINT NOT NULL"),
            TypePart::UUID40 => Some("BINARY(20)"),
            TypePart::UUID32 => Some("BINARY(16)"),
            TypePart::I16 => Some("SMALLINT(6) SIGNED NOT NULL"),
            TypePart::I32 => Some("INT(6) SIGNED NOT NULL"),
            TypePart::U8 => Some("TINYINT(3) UNSIGNED NOT NULL"),
            TypePart::Blank => None,
        }
    }
}