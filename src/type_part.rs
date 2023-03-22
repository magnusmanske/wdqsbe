use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum TypePart {
    Text,
    ShortText,
    Int,
    Blank,
}

impl TypePart {
    pub fn create_sql(&self) -> Option<&str> {
        match self {
            TypePart::Text => Some("VARCHAR(255) NOT NULL CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"),
            TypePart::ShortText => Some("VARCHAR(64) NOT NULL CHARACTER SET ascii COLLATE ascii_general_ci"),
            TypePart::Int => Some("INT(11) UNSIGNED NOT NULL"),
            TypePart::Blank => None,
        }
    }
}