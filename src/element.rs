use regex::Regex;

use crate::type_part::TypePart;

lazy_static! {
    static ref RE_WIKI_URL: Regex = Regex::new(r#"^https?://(.+?)/wiki/(.+)$"#).expect("RE_WIKI_URL does not parse");
    static ref RE_ENTITY_ITEM: Regex = Regex::new(r#"^[qQ](\d+)$"#).expect("RE_ENTITY_ITEM does not parse");
    static ref RE_ENTITY_PROPERTY: Regex = Regex::new(r#"^[pP](\d+)$"#).expect("RE_ENTITY_PROPERTY does not parse");
    static ref RE_ENTITY_MEDIA: Regex = Regex::new(r#"^[mM](\d+)$"#).expect("RE_ENTITY_MEDIA does not parse");
    static ref RE_ENTITY_LEXEME: Regex = Regex::new(r#"^[lL](\d+)$"#).expect("RE_ENTITY_LEXEME does not parse");
}

#[derive(Clone, Debug)]
pub enum Entity {
    Item(usize),
    Property(usize),
    Media(usize),
    Lexeme(usize),
    Unknown(String),
}

impl Entity {
    pub fn from_str(s: &str) -> Entity {
        if let Some(caps) = RE_ENTITY_ITEM.captures(&s) {
            Entity::Item(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_PROPERTY.captures(&s) {
            Entity::Property(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_MEDIA.captures(&s) {
            Entity::Media(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else if let Some(caps) = RE_ENTITY_LEXEME.captures(&s) {
            Entity::Lexeme(caps.get(1).unwrap().as_str().parse().unwrap_or(0))
        } else {
            println!("Unknown entity pattern: '{s}'");
            Entity::Unknown(s.to_string())
        }
    }

    pub fn get_type_parts(&self) -> Vec<TypePart> {
        match self {
            Entity::Unknown(_) => vec![TypePart::ShortText],
            _ => vec![TypePart::Int],
        }
    }
    pub fn values(&self) -> Vec<String> {
        match self {
            Entity::Item(q) => vec![format!("{q}")],
            Entity::Property(p) => vec![format!("{p}")],
            Entity::Media(m) => vec![format!("{m}")],
            Entity::Lexeme(l) => vec![format!("{l}")],
            Entity::Unknown(u) => vec![u.to_string()],
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Entity::Item(q) => format!("Q{q}"),
            Entity::Property(p) => format!("P{p}"),
            Entity::Media(m) => format!("M{m}"),
            Entity::Lexeme(l) => format!("L{l}"),
            Entity::Unknown(u) => u.to_string(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Entity::Item(_) => "EntityItem",
            Entity::Property(_) => "EntityProperty",
            Entity::Media(_) => "EntityMedia",
            Entity::Lexeme(_) => "EntityLexeme",
            Entity::Unknown(_) => "EntityUnknown",
        }
    }

    pub fn table_name(&self) -> String {
        self.name().to_string()
    }

    pub fn to_url(&self) -> String {
        match self {
            Entity::Item(q) => format!("http://www.wikidata.org/entity/Q{q}"),
            Entity::Property(p) => format!("http://www.wikidata.org/entity/P{p}"),
            Entity::Media(m) => format!("http://www.wikidata.org/entity/M{m}"), // TODO FIXME
            Entity::Lexeme(l) => format!("http://www.wikidata.org/entity/L{l}"),
            Entity::Unknown(s) => s.to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Element {
    Text(String),
    TextInLanguage((String,String)), // (text,language)
    WikiPage((String,String)), // (server,page)
    Entity(Entity),
    EntityStatement(String),
    Property(String),
    PropertyDirect(String),
    PropertyDirectNormalized(String),
    PropertyStatement(String),
    PropertyStatementValue(String),
    PropertyStatementValueNormalized(String),
    PropertyReference(String),
    PropertyReferenceValue(String),
    PropertyQualifier(String),
    PropertyQualifierValue(String),
    Reference(String),
    Value(String),

    Latitude,
    Longitude,
    RdfSchemaLabel,
    WasDerivedFrom,
    PurlLanguage,
    W3RdfSyntaxNsType,
    W3SkosCoreAltLabel,
    W3OntolexLexicalForm,
    W3OntolexRepresentation,
    SchemaOrgInLanguage,
    SchemaOrgIsPartOf,
    SchemaOrgAbout,
    SchemaOrgDescription,
    SchemaOrgName,
    SchemaOrgArticle,
    SchemaOrgDateModified,
    SchemaOrgVersion,
    OntologyBadge,
    OntologyRank,
    OntologyBestRank,
    OntologyNormalRank,
    OntologyIdentifiers,
    OntologyStatementProperty,
    OntologyLemma,
    OntologyStatements,
    OntologySitelinks,
    OntologyPropertyType,
    OntologyExternalId,
    OntologyClaim,
    OntologyDirectClaim,

    Other(String),
}

impl Element {
    pub fn from_str(element: String) -> Option<Self> {
        if let Some(caps) = RE_WIKI_URL.captures(&element) {
            let server = caps.get(1).map_or("", |m| m.as_str()).to_string();
            let page = caps.get(2).map_or("", |m| m.as_str()).to_string();
            return Some(Element::WikiPage((server,page)))
        }
        let mut parts: Vec<_> = element.split("/").collect();
        let key = parts.pop().unwrap().to_string();
        let root = parts.join("/");
        match root.as_str() {
            "http://www.wikidata.org/entity" => Some(Element::Entity(Entity::from_str(&key))),
            "http://www.wikidata.org/entity/statement" => Some(Element::EntityStatement(key)),
            "http://www.wikidata.org/prop" => Some(Element::Property(key)),
            "http://www.wikidata.org/prop/direct" => Some(Element::PropertyDirect(key)),
            "http://www.wikidata.org/prop/direct-normalized" => Some(Element::PropertyDirectNormalized(key)),
            "http://www.wikidata.org/prop/statement" => Some(Element::PropertyStatement(key)),
            "http://www.wikidata.org/prop/statement/value" => Some(Element::PropertyStatementValue(key)),
            "http://www.wikidata.org/prop/statement/value-normalized" => Some(Element::PropertyStatementValueNormalized(key)),
            "http://www.wikidata.org/prop/reference" => Some(Element::PropertyReference(key)),
            "http://www.wikidata.org/prop/reference/value" => Some(Element::PropertyReferenceValue(key)),
            "http://www.wikidata.org/prop/qualifier" => Some(Element::PropertyQualifier(key)),
            "http://www.wikidata.org/prop/qualifier/value" => Some(Element::PropertyQualifierValue(key)),
            "http://www.wikidata.org/reference" => Some(Element::Reference(key)),
            "http://www.wikidata.org/value" => Some(Element::Value(key)),
            "http://wikiba.se" => {
                match key.as_str() {
                    "ontology#geoLongitude" => Some(Element::Longitude),
                    "ontology#geoLatitude" => Some(Element::Latitude),
                    "ontology#badge" => Some(Element::OntologyBadge),
                    "ontology#rank" => Some(Element::OntologyRank),
                    "ontology#NormalRank" => Some(Element::OntologyNormalRank),
                    "ontology#BestRank" => Some(Element::OntologyBestRank),
                    "ontology#identifiers" => Some(Element::OntologyIdentifiers),
                    "ontology#statementProperty" => Some(Element::OntologyStatementProperty),
                    "ontology#lemma" => Some(Element::OntologyLemma),
                    "ontology#statements" => Some(Element::OntologyStatements),
                    "ontology#sitelinks" => Some(Element::OntologySitelinks),
                    "ontology#propertyType" => Some(Element::OntologyPropertyType),
                    "ontology#ExternalId" => Some(Element::OntologyExternalId),
                    "ontology#claim" => Some(Element::OntologyClaim),
                    "ontology#directClaim" => Some(Element::OntologyDirectClaim),
                    _ => Some(Element::Other(element)),
                }
            }
            "http://purl.org/dc/terms" => {
                match key.as_str() {
                    "language" => Some(Element::PurlLanguage),
                    _ => Some(Element::Other(element)),
                }
            }
            "http://www.w3.org/2000/01" => {
                match key.as_str() {
                    "rdf-schema#label" => Some(Element::RdfSchemaLabel),
                    _ => Some(Element::Other(element)),
                }
            }
            "http://www.w3.org/ns" => {
                match key.as_str() {
                    "prov#wasDerivedFrom" => Some(Element::WasDerivedFrom),
                    _ => Some(Element::Other(element)),
                }
            }
            "http://www.w3.org/1999/02" => {
                match key.as_str() {
                    "22-rdf-syntax-ns#type" => Some(Element::W3RdfSyntaxNsType),
                    _ => Some(Element::Other(element)),
                }
            }
            "http://www.w3.org/ns/lemon" => {
                match key.as_str() {
                    "ontolex#lexicalForm" => Some(Element::W3OntolexLexicalForm),
                    "ontolex#representation" => Some(Element::W3OntolexRepresentation),
                    _ => Some(Element::Other(element)),
                }
            }
            "http://www.w3.org/2004/02/skos" => {
                match key.as_str() {
                    "core#altLabel" => Some(Element::W3SkosCoreAltLabel),
                    _ => Some(Element::Other(element)),
                }
        }
            "http://schema.org" => {
                match key.as_str() {
                    "inLanguage" => Some(Element::SchemaOrgInLanguage),
                    "isPartOf" => Some(Element::SchemaOrgIsPartOf),
                    "about" => Some(Element::SchemaOrgAbout),
                    "name" => Some(Element::SchemaOrgName),
                    "version" => Some(Element::SchemaOrgVersion),
                    "dateModified" => Some(Element::SchemaOrgDateModified),
                    "Article" => Some(Element::SchemaOrgArticle),
                    "description" => Some(Element::SchemaOrgDescription),
                    _ => Some(Element::Other(element)),
                }
            }
            _ => Some(Element::Other(element)),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Element::Text(_) => "Text",
            Element::TextInLanguage(_) => "TextInLanguage",
            Element::WikiPage(_) => "WikiPage",
            Element::Entity(e) => e.name(),
            Element::EntityStatement(_) => "EntityStatement",
            Element::Property(_) => "Property",
            Element::PropertyDirect(_) => "PropertyDirect",
            Element::PropertyDirectNormalized(_) => "PropDirectNormalized",
            Element::PropertyStatement(_) => "PropStatement",
            Element::PropertyStatementValue(_) => "PropStatementValue",
            Element::PropertyStatementValueNormalized(_) => "PropStatementValueNormalized",
            Element::PropertyReference(_) => "PropertyReference",
            Element::PropertyReferenceValue(_) => "PropReferenceValue",
            Element::PropertyQualifier(_) => "PropQualifier",
            Element::PropertyQualifierValue(_) => "PropQualifierValue",
            Element::Reference(_) => "Reference",
            Element::Value(_) => "Value",
            Element::Latitude => "Latitude",
            Element::Longitude => "Longitude",
            Element::RdfSchemaLabel => "RdfSchemaLabel",
            Element::WasDerivedFrom => "WasDerivedFrom",
            Element::PurlLanguage => "PurlLanguage",
            Element::W3RdfSyntaxNsType => "W3RdfSyntaxNsType",
            Element::W3SkosCoreAltLabel => "W3SkosCoreAltLabel",
            Element::W3OntolexLexicalForm => "W3OntolexLexicalForm",
            Element::W3OntolexRepresentation => "W3OntolexRepresentation",
            Element::SchemaOrgInLanguage => "SchemaOrgInLanguage",
            Element::SchemaOrgIsPartOf => "SchemaOrgIsPartOf",
            Element::SchemaOrgAbout => "SchemaOrgAbout",
            Element::SchemaOrgDescription => "SchemaOrgDescription",
            Element::SchemaOrgName => "SchemaOrgName",
            Element::SchemaOrgArticle => "SchemaOrgArticle",
            Element::SchemaOrgDateModified => "SchemaOrgDateModified",
            Element::SchemaOrgVersion => "SchemaOrgVersion",
            Element::OntologyBadge => "OntologyBadge",
            Element::OntologyRank => "OntologyRank",
            Element::OntologyBestRank => "OntologyBestRank",
            Element::OntologyNormalRank => "OntologyNormalRank",
            Element::OntologyIdentifiers => "OntologyIdentifiers",
            Element::OntologyStatementProperty => "OntStatementProp",
            Element::OntologyLemma => "OntologyLemma",
            Element::OntologyStatements => "OntologyStatements",
            Element::OntologySitelinks => "OntologySitelinks",
            Element::OntologyPropertyType => "OntologyPropertyType",
            Element::OntologyExternalId => "OntologyExternalId",
            Element::OntologyClaim => "OntologyClaim",
            Element::OntologyDirectClaim => "OntologyDirectClaim",
            Element::Other(_) => "Other",
        }
    }

    pub fn from_sql_values(name:&str, value: Vec<String>) -> Self {
        match name {
            "EntityItem" => Element::Entity(Entity::Item(value[0].parse::<usize>().unwrap())),
            "EntityProperty" => Element::Entity(Entity::Property(value[0].parse::<usize>().unwrap())),
            "EntityMedia" => Element::Entity(Entity::Media(value[0].parse::<usize>().unwrap())),
            "EntityLexeme" => Element::Entity(Entity::Lexeme(value[0].parse::<usize>().unwrap())),
            "EntityUnknown" => Element::Entity(Entity::Unknown(value[0].to_string())),
            _ => Element::Other(value[0].to_owned()),
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            Element::Entity(e) => Some(e.to_url()),
            Element::Text(t) => Some(t.to_string()),
            _ => None
        }
    }

    pub fn sql_var_from_name(name: &str, prefix: &str) -> Vec<String> {
        match name {
            "Text" => vec![format!("{prefix}0")],
            "TextInLanguage" => vec![format!("{prefix}0"),format!("{prefix}1")],
            "WikiPage" => vec![format!("{prefix}0"),format!("{prefix}1")],

            "EntityItem" => vec![format!("{prefix}0")],
            "EntityProperty" => vec![format!("{prefix}0")],
            "EntityMedia" => vec![format!("{prefix}0")],
            "EntityLexeme" => vec![format!("{prefix}0")],
            "EntityUnknown" => vec![format!("{prefix}0")],

            "EntityStatement" => vec![format!("{prefix}0")],
            "Property" => vec![format!("{prefix}0")],
            "PropertyDirect" => vec![format!("{prefix}0")],
            "PropertyDirectNormalized" => vec![format!("{prefix}0")],
            "PropertyStatement" => vec![format!("{prefix}0")],
            "PropertyStatementValue" => vec![format!("{prefix}0")],
            "PropertyStatementValueNormalized" => vec![format!("{prefix}0")],
            "PropertyReference" => vec![format!("{prefix}0")],
            "PropertyReferenceValue" => vec![format!("{prefix}0")],
            "PropertyQualifier" => vec![format!("{prefix}0")],
            "PropertyQualifierValue" => vec![format!("{prefix}0")],
            "Reference" => vec![format!("{prefix}0")],
            "Value" => vec![format!("{prefix}0")],
            "Other" => vec![format!("{prefix}0")],
            _ => vec![],
        }
    }

    pub fn get_table_name(&self) -> String {
        match self {
            Element::Entity(e) => e.table_name(),
            Element::Property(s) => format!("Property_{s}"),
            Element::PropertyDirect(s) => format!("PropertyDirect_{s}"),
            Element::PropertyDirectNormalized(s) => format!("PropertyDirectNormalized_{s}"),
            Element::PropertyStatement(s) => format!("PropertyStatement_{s}"),
            Element::PropertyStatementValue(s) => format!("PropertyStatementValue_{s}"),
            Element::PropertyStatementValueNormalized(s) => format!("PSVN_{s}"), // Otherwise the table name can get too long
            Element::PropertyReference(s) => format!("PropertyReference_{s}"),
            Element::PropertyReferenceValue(s) => format!("PropertyReferenceValue_{s}"),
            Element::PropertyQualifier(s) => format!("PropertyQualifier_{s}"),
            Element::PropertyQualifierValue(s) => format!("PropertyQualifierValue_{s}"),
            _ => self.name().to_string(),
        }
    }

    pub fn get_type_parts(&self) -> Vec<TypePart> {
        match self {
            Element::Text(_) => vec![TypePart::Text],
            Element::TextInLanguage(_) => vec![TypePart::Text,TypePart::ShortText],
            Element::WikiPage(_) => vec![TypePart::ShortText,TypePart::Text],
            Element::Entity(e) => e.get_type_parts(),
            Element::EntityStatement(_) => vec![TypePart::ShortText],
            Element::Property(_) => vec![TypePart::ShortText],
            Element::PropertyDirect(_) => vec![TypePart::ShortText],
            Element::PropertyDirectNormalized(_) => vec![TypePart::ShortText],
            Element::PropertyStatement(_) => vec![TypePart::ShortText],
            Element::PropertyStatementValue(_) => vec![TypePart::ShortText],
            Element::PropertyStatementValueNormalized(_) => vec![TypePart::ShortText],
            Element::PropertyReference(_) => vec![TypePart::ShortText],
            Element::PropertyReferenceValue(_) => vec![TypePart::ShortText],
            Element::PropertyQualifier(_) => vec![TypePart::ShortText],
            Element::PropertyQualifierValue(_) => vec![TypePart::ShortText],
            Element::Reference(_) => vec![TypePart::ShortText],
            Element::Value(_) => vec![TypePart::Text],
            Element::Other(_) => vec![TypePart::Text],
            _ => vec![TypePart::Blank],
        }
    }

    pub fn fields(&self, prefix: &str) -> Vec<String> {
        self.get_type_parts()
            .iter()
            .enumerate()
            .filter(|(_num,part)|**part!=TypePart::Blank)
            .map(|(num,_part)|format!("{prefix}{num}"))
            .collect()
    }

    pub fn values(&self) -> Vec<String> {
        match self {
            Element::TextInLanguage(til) => vec![til.0.to_owned(),til.1.to_owned()],
            Element::WikiPage(wp) => vec![wp.0.to_owned(),wp.1.to_owned()],
            Element::Text(s) => vec![s.to_owned()],
            Element::Entity(e) => e.values(),
            Element::EntityStatement(s) => vec![s.to_owned()],
            Element::Property(s) => vec![s.to_owned()],
            Element::PropertyDirect(s) => vec![s.to_owned()],
            Element::PropertyDirectNormalized(s) => vec![s.to_owned()],
            Element::PropertyStatement(s) => vec![s.to_owned()],
            Element::PropertyStatementValue(s) => vec![s.to_owned()],
            Element::PropertyStatementValueNormalized(s) => vec![s.to_owned()],
            Element::PropertyReference(s) => vec![s.to_owned()],
            Element::PropertyReferenceValue(s) => vec![s.to_owned()],
            Element::PropertyQualifier(s) => vec![s.to_owned()],
            Element::PropertyQualifierValue(s) => vec![s.to_owned()],
            Element::Reference(s) => vec![s.to_owned()],
            Element::Value(s) => vec![s.to_owned()],
            Element::Other(s) => vec![s.to_owned()],
            _ => vec![]
        }
    }
}
