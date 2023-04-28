use regex::Regex;

/* TODO
- Coordinates on other planets (moon, Mars etc), encoded like:
<http://www.wikidata.org/entity/statement/Q45326-DEEEF3C4-21CA-4250-960F-62BE491BFA2C> <http://www.wikidata.org/prop/statement/P625> \"<http://www.wikidata.org/entity/Q2565> Point(336 73)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .
 */

use crate::date_time::DateTime;
use crate::db_operation_cache::DbOperationCacheValue;
use crate::element_type::ElementType;
use crate::entity_statement::EntityStatement;
use crate::lat_lon::LatLon;
use crate::text_id::TextId;
use crate::type_part::TypePart;
use crate::entity::Entity;
use crate::uuid::{UUID40, UUID32};

lazy_static! {
    static ref RE_WIKI_URL: Regex = Regex::new(r#"^https?://(.+?)/wiki/(.+)$"#).expect("RE_WIKI_URL does not parse");
}

#[derive(Clone, Debug)]
pub enum Element {
    Text(TextId),
    TextInLanguage((TextId,TextId)), // (text,language)
    WikiPage((TextId,TextId)), // (server,page)
    Entity(Entity),
    EntityStatement(EntityStatement),
    Property(Entity),
    PropertyDirect(String),
    PropertyDirectNormalized(String),
    PropertyStatement(String),
    PropertyStatementValue(String),
    PropertyStatementValueNormalized(String),
    PropertyReference(String),
    PropertyReferenceValue(String),
    PropertyReferenceValueNormalized(String),
    PropertyQualifier(String),
    PropertyQualifierValue(String),
    PropertyQualifierValueNormalized(String),
    Reference(UUID40),
    Value(UUID32),
    DateTime(DateTime),
    LatLon(LatLon),
    Int(i64),
    Float(f64),
    Url(TextId),
    WikibaseOntology(String),
    SchemaOrg(String),
    W3Owl(String),

    RdfSchemaLabel,
    WasDerivedFrom,
    PurlLanguage,
    W3RdfSyntaxNsType,
    W3SkosCoreAltLabel,
    W3SkosCorePrefLabel,
    W3OntolexLexicalForm,
    W3OntolexRepresentation,
    CreativeCommonsLicense,
}

impl Element {
    pub fn from_str(element: &str) -> Option<Self> {
        let (root,key) = match element.rsplit_once('/') {
            Some((root,key)) => (root,key),
            None => ("",element),
        };
        match root {
            "http://www.wikidata.org/entity" => Some(Element::Entity(*Entity::from_str(key)?)),
            "http://www.wikidata.org/entity/statement" => Some(Element::EntityStatement(*EntityStatement::from_str(key)?)),
            "http://www.wikidata.org/prop" => Some(Element::Property(*Entity::from_str(key)?)),
            "http://www.wikidata.org/prop/direct" => Some(Element::PropertyDirect(key.to_string())),
            "http://www.wikidata.org/prop/direct-normalized" => Some(Element::PropertyDirectNormalized(key.to_string())),
            "http://www.wikidata.org/prop/statement" => Some(Element::PropertyStatement(key.to_string())),
            "http://www.wikidata.org/prop/statement/value" => Some(Element::PropertyStatementValue(key.to_string())),
            "http://www.wikidata.org/prop/statement/value-normalized" => Some(Element::PropertyStatementValueNormalized(key.to_string())),
            "http://www.wikidata.org/prop/reference" => Some(Element::PropertyReference(key.to_string())),
            "http://www.wikidata.org/prop/reference/value" => Some(Element::PropertyReferenceValue(key.to_string())),
            "http://www.wikidata.org/prop/reference/value-normalized" => Some(Element::PropertyReferenceValueNormalized(key.to_string())),
            "http://www.wikidata.org/prop/qualifier" => Some(Element::PropertyQualifier(key.to_string())),
            "http://www.wikidata.org/prop/qualifier/value" => Some(Element::PropertyQualifierValue(key.to_string())),
            "http://www.wikidata.org/prop/qualifier/value-normalized" => Some(Element::PropertyQualifierValueNormalized(key.to_string())),
            "http://www.wikidata.org/reference" => Some(Element::Reference(*UUID40::from_str(key)?)),
            "http://www.wikidata.org/value" => Some(Element::Value(key.to_string().into())),
            "http://wikiba.se" => {
                match key.split_once('#') {
                    Some(("ontology",s)) => Some(Element::WikibaseOntology(s.to_string())),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://purl.org/dc/terms" => {
                match key {
                    "language" => Some(Element::PurlLanguage),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://www.w3.org/2000/01" => {
                match key {
                    "rdf-schema#label" => Some(Element::RdfSchemaLabel),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://creativecommons.org" => {
                match key {
                    "ns#license" => Some(Element::CreativeCommonsLicense),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://www.w3.org/ns" => {
                match key {
                    "prov#wasDerivedFrom" => Some(Element::WasDerivedFrom),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://www.w3.org/1999/02" => {
                match key {
                    "22-rdf-syntax-ns#type" => Some(Element::W3RdfSyntaxNsType),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://www.w3.org/2002/07" => {
                match key.split_once('#') {
                    Some((k1,k2)) => {
                        match k1 {
                            "owl" => Some(Element::W3Owl(k2.into())),
                            _ => Some(Element::Url(element.into())),
                        }
                    },
                    None => Some(Element::Url(element.into())),
                }
            }
            "http://www.w3.org/ns/lemon" => {
                match key {
                    "ontolex#lexicalForm" => Some(Element::W3OntolexLexicalForm),
                    "ontolex#representation" => Some(Element::W3OntolexRepresentation),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://www.w3.org/2004/02/skos" => {
                match key {
                    "core#altLabel" => Some(Element::W3SkosCoreAltLabel),
                    "core#prefLabel" => Some(Element::W3SkosCorePrefLabel),
                    _ => Some(Element::Url(element.into())),
                }
            }
            "http://schema.org" => Some(Element::SchemaOrg(key.to_string())),
            _ => {
                if let Some(caps) = RE_WIKI_URL.captures(&element) {
                    let server = caps.get(1).map_or("", |m| m.as_str()).to_string();
                    let page = caps.get(2).map_or("", |m| m.as_str()).to_string();
                    Some(Element::WikiPage((server.into(),page.into())))
                } else {
                    Some(Element::Url(element.into()))
                }
            }
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
            Element::PropertyDirectNormalized(_) => "PropDirectNorm",
            Element::PropertyStatement(_) => "PropStatement",
            Element::PropertyStatementValue(_) => "PropStatementValue",
            Element::PropertyStatementValueNormalized(_) => "PropStatementValueNorm",
            Element::PropertyReference(_) => "PropRef",
            Element::PropertyReferenceValue(_) => "PropReferenceValue",
            Element::PropertyReferenceValueNormalized(_) => "PropReferenceValueNorm",
            Element::PropertyQualifier(_) => "PropQual",
            Element::PropertyQualifierValue(_) => "PropQualValue",
            Element::PropertyQualifierValueNormalized(_) => "PropQualValueNorm",
            Element::Reference(_) => "Reference",
            Element::Value(_) => "Value",
            Element::DateTime(_) => "DateTime",
            Element::LatLon(_) => "LatLon",
            Element::Int(_) => "Integer",
            Element::Float(_) => "Decimal",
            Element::Url(_) => "Url",
            Element::W3Owl(_) => "W3Owl",
            Element::WikibaseOntology(_) => "WikibaseOntology",
            Element::SchemaOrg(_) => "SchemaOrg",
            Element::RdfSchemaLabel => "RdfSchemaLabel",
            Element::WasDerivedFrom => "WasDerivedFrom",
            Element::PurlLanguage => "PurlLanguage",
            Element::W3RdfSyntaxNsType => "W3RdfSyntaxNsType",
            Element::W3SkosCoreAltLabel => "W3SkosCoreAltLabel",
            Element::W3OntolexLexicalForm => "W3OntolexLexicalForm",
            Element::W3OntolexRepresentation => "W3OntolexRepresentation",
            Element::W3SkosCorePrefLabel => "W3SkosCorePrefLabel",
            Element::CreativeCommonsLicense => "CreativeCommonsLicense",
        }
    }

    pub fn from_sql_values(name:&str, value: &Vec<String>) -> Self {
        if let Some(entity) = Entity::from_sql_values(name, &value) {
            return Element::Entity(*entity);
        }
        if let Some(lat_lon) = LatLon::from_sql_values(name, &value) {
            return Element::LatLon(*lat_lon);
        }
        // TODO TextId?
        Element::Url(value[0].to_owned().into())
    }

    pub fn to_string(&self) -> Option<String> {
        match self { // TODO more types?
            Element::Entity(e) => Some(e.to_url()),
            Element::Text(t) => Some(t.to_string()),
            _ => None
        }
    }

    pub fn sql_var_from_name(name: &str, prefix: &str) -> Vec<String> {
        // check Entity
        if let Some(ret) = Entity::sql_var_from_name(name, prefix) {
            return ret;
        }
        match name {
            "Text" => vec![format!("{prefix}0")],
            "TextInLanguage" => vec![format!("{prefix}0"),format!("{prefix}1")],
            "WikiPage" => vec![format!("{prefix}0"),format!("{prefix}1")],
            "EntityStatement" => EntityStatement::sql_var_from_name(name, prefix).unwrap(),
            "Property" => vec![format!("{prefix}0")],
            "PropertyDirect" => vec![format!("{prefix}0")],
            "PropertyDirectNorm" => vec![format!("{prefix}0")],
            "PropertyStatement" => vec![format!("{prefix}0")],
            "PropertyStatementValue" => vec![format!("{prefix}0")],
            "PropertyStatementValueNorm" => vec![format!("{prefix}0")],
            "PropRef" => vec![format!("{prefix}0")],
            "PropertyReferenceValue" => vec![format!("{prefix}0")],
            "PropertyReferenceValueNorm" => vec![format!("{prefix}0")],
            "PropQual" => vec![format!("{prefix}0")],
            "PropQualValue" => vec![format!("{prefix}0")],
            "PropQualValueNorm" => vec![format!("{prefix}0")],
            "DateTime" => DateTime::sql_var_from_name(name, prefix).unwrap(),
            "Reference" => vec![format!("{prefix}0")],
            "Value" => vec![format!("{prefix}0")],
            "Url" => vec![format!("{prefix}0")],
            other => {
                println!("Element::sql_var_from_name: for '{other}'");
                vec![]
            }
        }
    }

    pub fn get_table_name(&self) -> String {
        match self {
            Element::Entity(e) => e.table_name(),
            Element::Property(p) => p.table_name(),//format!("Property_{s}"),
            Element::PropertyDirect(s) => format!("PropertyDirect_{s}"),
            Element::PropertyDirectNormalized(s) => format!("PropertyDirectNormalized_{s}"),
            Element::PropertyStatement(s) => format!("PropertyStatement_{s}"),
            Element::PropertyStatementValue(s) => format!("PropertyStatementValue_{s}"),
            Element::PropertyStatementValueNormalized(s) => format!("PSVN_{s}"), // Otherwise the table name can get too long
            Element::PropertyReference(s) => format!("PropertyReference_{s}"),
            Element::PropertyReferenceValue(s) => format!("PropertyReferenceValue_{s}"),
            Element::PropertyReferenceValueNormalized(s) => format!("PropertyReferenceValueNormalized_{s}"),
            Element::PropertyQualifier(s) => format!("PropQual_{s}"),
            Element::PropertyQualifierValue(s) => format!("PropQualValue_{s}"),
            Element::PropertyQualifierValueNormalized(s) => format!("PropQualValueNormalized_{s}"),
            Element::Url(_) => self.name().to_string(),
            Element::Text(_) => self.name().to_string(),
            Element::TextInLanguage(_) => self.name().to_string(),
            Element::WikiPage(_) => self.name().to_string(),
            Element::EntityStatement(es) => es.table_name(),
            Element::Reference(_) => self.name().to_string(),
            Element::Value(_) => self.name().to_string(),
            Element::DateTime(_) => self.name().to_string(),
            Element::LatLon(_) => self.name().to_string(),
            Element::Int(_) => self.name().to_string(),
            Element::Float(_) => self.name().to_string(),
            Element::WikibaseOntology(s) => format!("WO{s}"),
            Element::SchemaOrg(s) => format!("SchemaOrg{s}"),
            Element::W3Owl(s) => format!("W3Owl_{s}"),
            Element::RdfSchemaLabel => self.name().to_string(),
            Element::WasDerivedFrom => self.name().to_string(),
            Element::PurlLanguage => self.name().to_string(),
            Element::W3RdfSyntaxNsType => self.name().to_string(),
            Element::W3SkosCoreAltLabel => self.name().to_string(),
            Element::W3OntolexLexicalForm => self.name().to_string(),
            Element::W3OntolexRepresentation => self.name().to_string(),
            Element::W3SkosCorePrefLabel => self.name().to_string(),
            Element::CreativeCommonsLicense => self.name().to_string(),
        }
    }

    pub fn get_type_parts(&self) -> Vec<TypePart> {
        match self {
            Element::Text(_) => vec![TypePart::Int],
            Element::TextInLanguage(_) => vec![TypePart::Int,TypePart::Int], // TODO use get_type_parts
            Element::WikiPage(_) => vec![TypePart::Int,TypePart::Int], // TODO use get_type_parts
            Element::Entity(e) => e.get_type_parts(),
            Element::LatLon(l) => l.get_type_parts(),
            Element::DateTime(dt) => dt.get_type_parts(),
            Element::Reference(r) => r.get_type_parts(),
            Element::EntityStatement(es) => es.get_type_parts(),
            Element::Property(_) => vec![TypePart::ShortText],
            Element::PropertyDirect(_) => vec![TypePart::ShortText],
            Element::PropertyDirectNormalized(_) => vec![TypePart::ShortText],
            Element::PropertyStatement(_) => vec![TypePart::ShortText],
            Element::PropertyStatementValue(_) => vec![TypePart::ShortText],
            Element::PropertyStatementValueNormalized(_) => vec![TypePart::ShortText],
            Element::PropertyReference(_) => vec![TypePart::ShortText],
            Element::PropertyReferenceValue(_) => vec![TypePart::ShortText],
            Element::PropertyReferenceValueNormalized(_) => vec![TypePart::ShortText],
            Element::PropertyQualifier(_) => vec![TypePart::ShortText],
            Element::PropertyQualifierValue(_) => vec![TypePart::ShortText],
            Element::PropertyQualifierValueNormalized(_) => vec![TypePart::ShortText],
            Element::Int(_) => vec![TypePart::Int],
            Element::Float(_) => vec![TypePart::Float],
            Element::Value(_) => vec![TypePart::Text],
            Element::Url(_) => vec![TypePart::Int],
            Element::WikibaseOntology(_) => vec![TypePart::ShortText],
            Element::SchemaOrg(_) => vec![TypePart::ShortText],
            Element::W3Owl(_) => vec![TypePart::ShortText],
            Element::RdfSchemaLabel => vec![TypePart::Blank],
            Element::WasDerivedFrom => vec![TypePart::Blank],
            Element::PurlLanguage => vec![TypePart::Blank],
            Element::W3RdfSyntaxNsType => vec![TypePart::Blank],
            Element::W3SkosCoreAltLabel => vec![TypePart::Blank],
            Element::W3OntolexLexicalForm => vec![TypePart::Blank],
            Element::W3OntolexRepresentation => vec![TypePart::Blank],
            Element::W3SkosCorePrefLabel => vec![TypePart::Blank],
            Element::CreativeCommonsLicense => vec![TypePart::Blank],
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

    pub fn values(&self) -> Vec<DbOperationCacheValue> {
        match self {
            Element::TextInLanguage(til) => vec![
                til.0.values()[0].to_owned(),
                til.1.values()[0].to_owned(),
                ],
            Element::WikiPage(wp) => vec![
                wp.0.values()[0].to_owned(),
                wp.1.values()[0].to_owned(),
                ],
            Element::Text(t) => t.values(),
            Element::LatLon(l) => l.values(),
            Element::Entity(e) => e.values(),
            Element::EntityStatement(es) => es.values(),
            Element::DateTime(dt) => dt.values(),
            Element::Property(p) => p.values(),
            Element::PropertyDirect(s) => vec![s.into()],
            Element::PropertyDirectNormalized(s) => vec![s.into()],
            Element::PropertyStatement(s) => vec![s.into()],
            Element::PropertyStatementValue(s) => vec![s.into()],
            Element::PropertyStatementValueNormalized(s) => vec![s.into()],
            Element::PropertyReference(s) => vec![s.into()],
            Element::PropertyReferenceValue(s) => vec![s.into()],
            Element::PropertyReferenceValueNormalized(s) => vec![s.into()],
            Element::PropertyQualifier(s) => vec![s.into()],
            Element::PropertyQualifierValue(s) => vec![s.into()],
            Element::PropertyQualifierValueNormalized(s) => vec![s.into()],
            Element::Reference(uuid) => uuid.values(),
            Element::Value(uuid) => uuid.values(),
            Element::Url(s) => s.values(),
            Element::Int(s) => vec![DbOperationCacheValue::Expression(format!("{s}"))],
            Element::Float(s) => vec![DbOperationCacheValue::Expression(format!("{s}"))],
            Element::WikibaseOntology(s) => vec![s.into()],
            Element::SchemaOrg(s) => vec![s.into()],
            Element::W3Owl(s) => vec![s.into()],
            Element::RdfSchemaLabel => vec![],
            Element::WasDerivedFrom => vec![],
            Element::PurlLanguage => vec![],
            Element::W3RdfSyntaxNsType => vec![],
            Element::W3SkosCoreAltLabel => vec![],
            Element::W3OntolexLexicalForm => vec![],
            Element::W3OntolexRepresentation => vec![],
            Element::W3SkosCorePrefLabel => vec![],
            Element::CreativeCommonsLicense => vec![],
        }
    }
}
