use regex::Regex;

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
    PropertyQualifier(String),
    PropertyQualifierValue(String),
    Reference(UUID40),
    Value(UUID32),
    DateTime(DateTime),
    LatLon(LatLon),
    Int(i64),
    Float(f64),

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

    Url(String),
}

impl Element {
    pub fn from_str(element: String) -> Option<Self> {
        if let Some(caps) = RE_WIKI_URL.captures(&element) {
            let server = caps.get(1).map_or("", |m| m.as_str()).to_string();
            let page = caps.get(2).map_or("", |m| m.as_str()).to_string();
            return Some(Element::WikiPage((server.into(),page.into())))
        }
        let mut parts: Vec<_> = element.split("/").collect();
        let key = parts.pop().unwrap().to_string();
        let root = parts.join("/");
        match root.as_str() {
            "http://www.wikidata.org/entity" => Some(Element::Entity(*Entity::from_str(&key)?)),
            "http://www.wikidata.org/entity/statement" => Some(Element::EntityStatement(*EntityStatement::from_str(&key)?)),
            "http://www.wikidata.org/prop" => Some(Element::Property(*Entity::from_str(&key)?)),
            "http://www.wikidata.org/prop/direct" => Some(Element::PropertyDirect(key)),
            "http://www.wikidata.org/prop/direct-normalized" => Some(Element::PropertyDirectNormalized(key)),
            "http://www.wikidata.org/prop/statement" => Some(Element::PropertyStatement(key)),
            "http://www.wikidata.org/prop/statement/value" => Some(Element::PropertyStatementValue(key)),
            "http://www.wikidata.org/prop/statement/value-normalized" => Some(Element::PropertyStatementValueNormalized(key)),
            "http://www.wikidata.org/prop/reference" => Some(Element::PropertyReference(key)),
            "http://www.wikidata.org/prop/reference/value" => Some(Element::PropertyReferenceValue(key)),
            "http://www.wikidata.org/prop/qualifier" => Some(Element::PropertyQualifier(key)),
            "http://www.wikidata.org/prop/qualifier/value" => Some(Element::PropertyQualifierValue(key)),
            "http://www.wikidata.org/reference" => Some(Element::Reference(*UUID40::from_str(&key)?)),
            "http://www.wikidata.org/value" => Some(Element::Value(key.into())),
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
                    _ => Some(Element::Url(element)),
                }
            }
            "http://purl.org/dc/terms" => {
                match key.as_str() {
                    "language" => Some(Element::PurlLanguage),
                    _ => Some(Element::Url(element)),
                }
            }
            "http://www.w3.org/2000/01" => {
                match key.as_str() {
                    "rdf-schema#label" => Some(Element::RdfSchemaLabel),
                    _ => Some(Element::Url(element)),
                }
            }
            "http://www.w3.org/ns" => {
                match key.as_str() {
                    "prov#wasDerivedFrom" => Some(Element::WasDerivedFrom),
                    _ => Some(Element::Url(element)),
                }
            }
            "http://www.w3.org/1999/02" => {
                match key.as_str() {
                    "22-rdf-syntax-ns#type" => Some(Element::W3RdfSyntaxNsType),
                    _ => Some(Element::Url(element)),
                }
            }
            "http://www.w3.org/ns/lemon" => {
                match key.as_str() {
                    "ontolex#lexicalForm" => Some(Element::W3OntolexLexicalForm),
                    "ontolex#representation" => Some(Element::W3OntolexRepresentation),
                    _ => Some(Element::Url(element)),
                }
            }
            "http://www.w3.org/2004/02/skos" => {
                match key.as_str() {
                    "core#altLabel" => Some(Element::W3SkosCoreAltLabel),
                    _ => Some(Element::Url(element)),
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
                    _ => Some(Element::Url(element)),
                }
            }
            _ => Some(Element::Url(element)),
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
            Element::DateTime(_) => "DateTime",
            Element::LatLon(_) => "LatLon",
            Element::Int(_) => "Integer",
            Element::Float(_) => "Decimal",
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
            Element::Url(_) => "Url",
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
        Element::Url(value[0].to_owned())
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
            "PropertyDirectNormalized" => vec![format!("{prefix}0")],
            "PropertyStatement" => vec![format!("{prefix}0")],
            "PropertyStatementValue" => vec![format!("{prefix}0")],
            "PropertyStatementValueNormalized" => vec![format!("{prefix}0")],
            "PropertyReference" => vec![format!("{prefix}0")],
            "PropertyReferenceValue" => vec![format!("{prefix}0")],
            "PropertyQualifier" => vec![format!("{prefix}0")],
            "PropertyQualifierValue" => vec![format!("{prefix}0")],
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
            Element::PropertyQualifier(s) => format!("PropertyQualifier_{s}"),
            Element::PropertyQualifierValue(s) => format!("PropertyQualifierValue_{s}"),
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
            Element::Latitude => self.name().to_string(),
            Element::Longitude => self.name().to_string(),
            Element::RdfSchemaLabel => self.name().to_string(),
            Element::WasDerivedFrom => self.name().to_string(),
            Element::PurlLanguage => self.name().to_string(),
            Element::W3RdfSyntaxNsType => self.name().to_string(),
            Element::W3SkosCoreAltLabel => self.name().to_string(),
            Element::W3OntolexLexicalForm => self.name().to_string(),
            Element::W3OntolexRepresentation => self.name().to_string(),
            Element::SchemaOrgInLanguage => self.name().to_string(),
            Element::SchemaOrgIsPartOf => self.name().to_string(),
            Element::SchemaOrgAbout => self.name().to_string(),
            Element::SchemaOrgDescription => self.name().to_string(),
            Element::SchemaOrgName => self.name().to_string(),
            Element::SchemaOrgArticle => self.name().to_string(),
            Element::SchemaOrgDateModified => self.name().to_string(),
            Element::SchemaOrgVersion => self.name().to_string(),
            Element::OntologyBadge => self.name().to_string(),
            Element::OntologyRank => self.name().to_string(),
            Element::OntologyBestRank => self.name().to_string(),
            Element::OntologyNormalRank => self.name().to_string(),
            Element::OntologyIdentifiers => self.name().to_string(),
            Element::OntologyStatementProperty => self.name().to_string(),
            Element::OntologyLemma => self.name().to_string(),
            Element::OntologyStatements => self.name().to_string(),
            Element::OntologySitelinks => self.name().to_string(),
            Element::OntologyPropertyType => self.name().to_string(),
            Element::OntologyExternalId => self.name().to_string(),
            Element::OntologyClaim => self.name().to_string(),
            Element::OntologyDirectClaim => self.name().to_string(),
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
            Element::PropertyQualifier(_) => vec![TypePart::ShortText],
            Element::PropertyQualifierValue(_) => vec![TypePart::ShortText],
            Element::Int(_) => vec![TypePart::Int],
            Element::Float(_) => vec![TypePart::Float],
            Element::Value(_) => vec![TypePart::Text],
            Element::Url(_) => vec![TypePart::Text],
            Element::Latitude => vec![TypePart::Blank],
            Element::Longitude => vec![TypePart::Blank],
            Element::RdfSchemaLabel => vec![TypePart::Blank],
            Element::WasDerivedFrom => vec![TypePart::Blank],
            Element::PurlLanguage => vec![TypePart::Blank],
            Element::W3RdfSyntaxNsType => vec![TypePart::Blank],
            Element::W3SkosCoreAltLabel => vec![TypePart::Blank],
            Element::W3OntolexLexicalForm => vec![TypePart::Blank],
            Element::W3OntolexRepresentation => vec![TypePart::Blank],
            Element::SchemaOrgInLanguage => vec![TypePart::Blank],
            Element::SchemaOrgIsPartOf => vec![TypePart::Blank],
            Element::SchemaOrgAbout => vec![TypePart::Blank],
            Element::SchemaOrgDescription => vec![TypePart::Blank],
            Element::SchemaOrgName => vec![TypePart::Blank],
            Element::SchemaOrgArticle => vec![TypePart::Blank],
            Element::SchemaOrgDateModified => vec![TypePart::Blank],
            Element::SchemaOrgVersion => vec![TypePart::Blank],
            Element::OntologyBadge => vec![TypePart::Blank],
            Element::OntologyRank => vec![TypePart::Blank],
            Element::OntologyBestRank => vec![TypePart::Blank],
            Element::OntologyNormalRank => vec![TypePart::Blank],
            Element::OntologyIdentifiers => vec![TypePart::Blank],
            Element::OntologyStatementProperty => vec![TypePart::Blank],
            Element::OntologyLemma => vec![TypePart::Blank],
            Element::OntologyStatements => vec![TypePart::Blank],
            Element::OntologySitelinks => vec![TypePart::Blank],
            Element::OntologyPropertyType => vec![TypePart::Blank],
            Element::OntologyExternalId => vec![TypePart::Blank],
            Element::OntologyClaim => vec![TypePart::Blank],
            Element::OntologyDirectClaim => vec![TypePart::Blank],
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
            Element::PropertyQualifier(s) => vec![s.into()],
            Element::PropertyQualifierValue(s) => vec![s.into()],
            Element::Reference(uuid) => uuid.values(),
            Element::Value(uuid) => uuid.values(),
            Element::Url(s) => vec![s.into()],
            Element::Int(s) => vec![DbOperationCacheValue::Expression(format!("{s}"))],
            Element::Float(s) => vec![DbOperationCacheValue::Expression(format!("{s}"))],
            Element::Latitude => vec![],
            Element::Longitude => vec![],
            Element::RdfSchemaLabel => vec![],
            Element::WasDerivedFrom => vec![],
            Element::PurlLanguage => vec![],
            Element::W3RdfSyntaxNsType => vec![],
            Element::W3SkosCoreAltLabel => vec![],
            Element::W3OntolexLexicalForm => vec![],
            Element::W3OntolexRepresentation => vec![],
            Element::SchemaOrgInLanguage => vec![],
            Element::SchemaOrgIsPartOf => vec![],
            Element::SchemaOrgAbout => vec![],
            Element::SchemaOrgDescription => vec![],
            Element::SchemaOrgName => vec![],
            Element::SchemaOrgArticle => vec![],
            Element::SchemaOrgDateModified => vec![],
            Element::SchemaOrgVersion => vec![],
            Element::OntologyBadge => vec![],
            Element::OntologyRank => vec![],
            Element::OntologyBestRank => vec![],
            Element::OntologyNormalRank => vec![],
            Element::OntologyIdentifiers => vec![],
            Element::OntologyStatementProperty => vec![],
            Element::OntologyLemma => vec![],
            Element::OntologyStatements => vec![],
            Element::OntologySitelinks => vec![],
            Element::OntologyPropertyType => vec![],
            Element::OntologyExternalId => vec![],
            Element::OntologyClaim => vec![],
            Element::OntologyDirectClaim => vec![],
        }
    }
}
