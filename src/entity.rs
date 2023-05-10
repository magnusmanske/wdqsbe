use regex::Regex;

use crate::{type_part::TypePart, element_type::ElementType, db_operation_cache::DbOperationCacheValue};

lazy_static! {
    static ref RE_ENTITY_ITEM: Regex = Regex::new(r#"^[qQ](\d+)$"#).expect("RE_ENTITY_ITEM does not parse");
    static ref RE_ENTITY_PROPERTY: Regex = Regex::new(r#"^[pP](\d+)$"#).expect("RE_ENTITY_PROPERTY does not parse");
    static ref RE_ENTITY_MEDIA: Regex = Regex::new(r#"^[mM](\d+)$"#).expect("RE_ENTITY_MEDIA does not parse");
    static ref RE_ENTITY_LEXEME: Regex = Regex::new(r#"^[lL](\d+)$"#).expect("RE_ENTITY_LEXEME does not parse");
    static ref RE_ENTITY_LEXEME_FORM: Regex = Regex::new(r#"^[lL](\d+)-[fF](\d+)$"#).expect("RE_ENTITY_LEXEME_FORM does not parse");
    static ref RE_ENTITY_LEXEME_SENSE: Regex = Regex::new(r#"^[lL](\d+)-[sS](\d+)$"#).expect("RE_ENTITY_LEXEME_SENSE does not parse");
}

#[derive(Clone, Debug)]
pub enum Entity {
    Item(u32),
    Property(u16),
    Media(u32),
    Lexeme(u32),
    LexemeForm((u32,u8)),
    LexemeSense((u32,u8)),
    Unknown(String),
}

impl ElementType for Entity {
    fn from_str(s: &str) -> Option<Box<Self>> {
        if let Some(caps) = RE_ENTITY_ITEM.captures(&s) {
            Some(Box::new(Entity::Item(caps.get(1).unwrap().as_str().parse().unwrap_or(0))))
        } else if let Some(caps) = RE_ENTITY_PROPERTY.captures(&s) {
            Some(Box::new(Entity::Property(caps.get(1).unwrap().as_str().parse().unwrap_or(0))))
        } else if let Some(caps) = RE_ENTITY_MEDIA.captures(&s) {
            Some(Box::new(Entity::Media(caps.get(1).unwrap().as_str().parse().unwrap_or(0))))
        } else if let Some(caps) = RE_ENTITY_LEXEME.captures(&s) {
            Some(Box::new(Entity::Lexeme(caps.get(1).unwrap().as_str().parse().unwrap_or(0))))
        } else if let Some(caps) = RE_ENTITY_LEXEME_FORM.captures(&s) {
            Some(Box::new(Entity::LexemeForm((
                caps.get(1).unwrap().as_str().parse().unwrap_or(0),
                caps.get(2).unwrap().as_str().parse().unwrap_or(0)
            ))))
        } else if let Some(caps) = RE_ENTITY_LEXEME_SENSE.captures(&s) {
            Some(Box::new(Entity::LexemeSense((
                caps.get(1).unwrap().as_str().parse().unwrap_or(0),
                caps.get(2).unwrap().as_str().parse().unwrap_or(0)
            ))))
        } else {
            eprintln!("Unknown entity pattern: '{s}'");
            Some(Box::new(Entity::Unknown(s.to_string())))
        }
    }

    fn get_type_parts(&self) -> Vec<TypePart> {
        match self {
            Entity::Unknown(_) => vec![TypePart::ShortText],
            Entity::Item(_) => vec![TypePart::U32],
            Entity::Property(_) => vec![TypePart::U16],
            Entity::Media(_) => vec![TypePart::U32],
            Entity::Lexeme(_) => vec![TypePart::U32],
            Entity::LexemeForm(_) => vec![TypePart::U32,TypePart::U8],
            Entity::LexemeSense(_) => vec![TypePart::U32,TypePart::U8],
        }
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        match self {
            Entity::Item(q) => vec![DbOperationCacheValue::U32(*q)],
            Entity::Property(p) => vec![DbOperationCacheValue::U16(*p)],
            Entity::Media(m) => vec![DbOperationCacheValue::U32(*m)],
            Entity::Lexeme(l) => vec![DbOperationCacheValue::U32(*l)],
            Entity::LexemeForm((l,f)) => vec![DbOperationCacheValue::U32(*l),DbOperationCacheValue::U8(*f)],
            Entity::LexemeSense((l,s)) => vec![DbOperationCacheValue::U32(*l),DbOperationCacheValue::U8(*s)],
            Entity::Unknown(u) => vec![u.into()],
        }
    }

    fn to_string(&self) -> String {
        match self {
            Entity::Item(q) => format!("Q{q}"),
            Entity::Property(p) => format!("P{p}"),
            Entity::Media(m) => format!("M{m}"),
            Entity::Lexeme(l) => format!("L{l}"),
            Entity::LexemeForm((l,f)) => format!("L{l}-F{f}"),
            Entity::LexemeSense((l,s)) => format!("L{l}-S{s}"),
            Entity::Unknown(u) => u.to_string(),
        }
    }

    fn name(&self) -> &str {
        match self {
            Entity::Item(_) => "EntityItem",
            Entity::Property(_) => "EntityProp",
            Entity::Media(_) => "EntityMedia",
            Entity::Lexeme(_) => "EntityLexeme",
            Entity::LexemeForm(_) => "EntityLexemeF",
            Entity::LexemeSense(_) => "EntityLexemeS",
            Entity::Unknown(_) => "EntityUnknown",
        }
    }

    fn table_name(&self) -> String {
        match self {
            Entity::Property(p) => format!("P{p}"),
            _ => self.name().to_string(),
        }
    }

    fn to_url(&self) -> String {
        match self {
            Entity::Item(q) => format!("http://www.wikidata.org/entity/Q{q}"),
            Entity::Property(p) => format!("http://www.wikidata.org/entity/P{p}"),
            Entity::Media(m) => format!("http://commons.wikimedia.org/entity/M{m}"),
            Entity::Lexeme(l) => format!("http://www.wikidata.org/entity/L{l}"),
            Entity::LexemeForm((l,f)) => format!("http://www.wikidata.org/entity/L{l}-F{f}"),
            Entity::LexemeSense((l,s)) => format!("http://www.wikidata.org/entity/L{l}-S{s}"),
            Entity::Unknown(s) => s.to_owned(),
        }
    }

    fn sql_var_from_name(name: &str, prefix: &str) -> Option<Vec<String>> {
        Some(match name {
            "EntityItem" => vec![format!("{prefix}0")],
            "EntityProp" => vec![format!("{prefix}0")],
            "EntityMedia" => vec![format!("{prefix}0")],
            "EntityLexeme" => vec![format!("{prefix}0")],
            "EntityLexemeForm" => vec![format!("{prefix}0"),format!("{prefix}1")],
            "EntityLexemeS" => vec![format!("{prefix}0"),format!("{prefix}1")],
            "EntityUnknown" => vec![format!("{prefix}0")],
            _ => return None,
        })
    }

    fn from_sql_values(name:&str, value: &Vec<String>) -> Option<Box<Entity>> {
        Some(Box::new(match name {
            "EntityItem" => Entity::Item(value[0].parse::<u32>().unwrap()),
            "EntityProp" => Entity::Property(value[0].parse::<u16>().unwrap()),
            "EntityMedia" => Entity::Media(value[0].parse::<u32>().unwrap()),
            "EntityLexeme" => Entity::Lexeme(value[0].parse::<u32>().unwrap()),
            "EntityLexemeForm" => Entity::LexemeForm((
                value[0].parse::<u32>().unwrap(),
                value[1].parse::<u8>().unwrap()
            )),
            "EntityLexemeS" => Entity::LexemeSense((
                value[0].parse::<u32>().unwrap(),
                value[1].parse::<u8>().unwrap()
            )),
            "EntityUnknown" => Entity::Unknown(value[0].to_string()),
            _ => return None
        }))
    }
}
