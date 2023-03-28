use regex::Regex;

use crate::{type_part::TypePart, element_type::ElementType};

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
    Item(usize),
    Property(usize),
    Media(usize),
    Lexeme(usize),
    LexemeForm((usize,usize)),
    LexemeSense((usize,usize)),
    Unknown(String),
}

impl ElementType for Entity {
    fn from_str(s: &str) -> Entity {
        if let Some(caps) = RE_ENTITY_ITEM.captures(&s) {
            Entity::Item(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_PROPERTY.captures(&s) {
            Entity::Property(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_MEDIA.captures(&s) {
            Entity::Media(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_LEXEME.captures(&s) {
            Entity::Lexeme(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_LEXEME_FORM.captures(&s) {
            Entity::LexemeForm((
                caps.get(1).unwrap().as_str().parse().unwrap_or(0),
                caps.get(2).unwrap().as_str().parse().unwrap_or(0)
            ))
        } else if let Some(caps) = RE_ENTITY_LEXEME_SENSE.captures(&s) {
            Entity::LexemeSense((
                caps.get(1).unwrap().as_str().parse().unwrap_or(0),
                caps.get(2).unwrap().as_str().parse().unwrap_or(0)
            ))
        } else {
            println!("Unknown entity pattern: '{s}'");
            Entity::Unknown(s.to_string())
        }
    }

    fn get_type_parts(&self) -> Vec<TypePart> {
        match self {
            Entity::LexemeForm(_) => vec![TypePart::Int,TypePart::Int],
            Entity::LexemeSense(_) => vec![TypePart::Int,TypePart::Int],
            Entity::Unknown(_) => vec![TypePart::ShortText],
            _ => vec![TypePart::Int],
        }
    }

    fn values(&self) -> Vec<String> {
        match self {
            Entity::Item(q) => vec![format!("{q}")],
            Entity::Property(p) => vec![format!("{p}")],
            Entity::Media(m) => vec![format!("{m}")],
            Entity::Lexeme(l) => vec![format!("{l}")],
            Entity::LexemeForm((l,f)) => vec![format!("{l}"),format!("{f}")],
            Entity::LexemeSense((l,s)) => vec![format!("{l}"),format!("{s}")],
            Entity::Unknown(u) => vec![u.to_string()],
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
            Entity::Property(_) => "EntityProperty",
            Entity::Media(_) => "EntityMedia",
            Entity::Lexeme(_) => "EntityLexeme",
            Entity::LexemeForm(_) => "EntityLexemeF",
            Entity::LexemeSense(_) => "EntityLexemeS",
            Entity::Unknown(_) => "EntityUnknown",
        }
    }

    fn table_name(&self) -> String {
        self.name().to_string()
    }

    fn to_url(&self) -> String {
        match self {
            Entity::Item(q) => format!("http://www.wikidata.org/entity/Q{q}"),
            Entity::Property(p) => format!("http://www.wikidata.org/entity/P{p}"),
            Entity::Media(m) => format!("http://www.wikidata.org/entity/M{m}"), // TODO FIXME commons?
            Entity::Lexeme(l) => format!("http://www.wikidata.org/entity/L{l}"),
            Entity::LexemeForm((l,f)) => format!("http://www.wikidata.org/entity/L{l}-F{f}"),
            Entity::LexemeSense((l,s)) => format!("http://www.wikidata.org/entity/L{l}-S{s}"),
            Entity::Unknown(s) => s.to_owned(),
        }
    }

    fn sql_var_from_name(name: &str, prefix: &str) -> Option<Vec<String>> {
        Some(match name {
            "EntityItem" => vec![format!("{prefix}0")],
            "EntityProperty" => vec![format!("{prefix}0")],
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
            "EntityItem" => Entity::Item(value[0].parse::<usize>().unwrap()),
            "EntityProperty" => Entity::Property(value[0].parse::<usize>().unwrap()),
            "EntityMedia" => Entity::Media(value[0].parse::<usize>().unwrap()),
            "EntityLexeme" => Entity::Lexeme(value[0].parse::<usize>().unwrap()),
            "EntityLexemeForm" => Entity::LexemeForm((
                value[0].parse::<usize>().unwrap(),
                value[1].parse::<usize>().unwrap()
            )),
            "EntityLexemeS" => Entity::LexemeSense((
                value[0].parse::<usize>().unwrap(),
                value[1].parse::<usize>().unwrap()
            )),
            "EntityUnknown" => Entity::Unknown(value[0].to_string()),
            _ => return None
        }))
    }
}
