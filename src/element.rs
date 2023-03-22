use crate::type_part::TypePart;


#[derive(Clone, Debug)]
pub enum Element {
    Text(String),
    TextInLanguage((String,String)), // (text,language)
    WikiPage((String,String)), // (server,page)
    Entity(String),
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
    pub fn get_table_name(&self) -> String {
        match self {
            Element::Text(_) => "Text".to_string(),
            Element::TextInLanguage(_) => "TextInLanguage".to_string(),
            Element::WikiPage(_) => "WikiPage".to_string(),
            Element::Entity(_) => "Entity".to_string(),
            Element::EntityStatement(_) => "EntityStatement".to_string(),
            Element::Property(s) => format!("Property_{s}"),
            Element::PropertyDirect(s) => format!("PropertyDirect_{s}"),
            Element::PropertyDirectNormalized(s) => format!("PropertyDirectNormalized_{s}"),
            Element::PropertyStatement(s) => format!("PropertyStatement_{s}"),
            Element::PropertyStatementValue(s) => format!("PropertyStatementValue_{s}"),
            Element::PropertyStatementValueNormalized(s) => format!("PropertyStatementValueNormalized_{s}"),
            Element::PropertyReference(s) => format!("PropertyReference_{s}"),
            Element::PropertyReferenceValue(s) => format!("PropertyReferenceValue_{s}"),
            Element::PropertyQualifier(s) => format!("PropertyQualifier_{s}"),
            Element::PropertyQualifierValue(s) => format!("PropertyQualifierValue_{s}"),
            Element::Reference(_) => "Reference".to_string(),
            Element::Value(_) => "Value".to_string(),
            Element::Latitude => "Latitude".to_string(),
            Element::Longitude => "Longitude".to_string(),
            Element::RdfSchemaLabel => "RdfSchemaLabel".to_string(),
            Element::WasDerivedFrom => "WasDerivedFrom".to_string(),
            Element::PurlLanguage => "PurlLanguage".to_string(),
            Element::W3RdfSyntaxNsType => "W3RdfSyntaxNsType".to_string(),
            Element::W3SkosCoreAltLabel => "W3SkosCoreAltLabel".to_string(),
            Element::W3OntolexLexicalForm => "W3OntolexLexicalForm".to_string(),
            Element::W3OntolexRepresentation => "W3OntolexRepresentation".to_string(),
            Element::SchemaOrgInLanguage => "SchemaOrgInLanguage".to_string(),
            Element::SchemaOrgIsPartOf => "SchemaOrgIsPartOf".to_string(),
            Element::SchemaOrgAbout => "SchemaOrgAbout".to_string(),
            Element::SchemaOrgDescription => "SchemaOrgDescription".to_string(),
            Element::SchemaOrgName => "SchemaOrgName".to_string(),
            Element::SchemaOrgArticle => "SchemaOrgArticle".to_string(),
            Element::SchemaOrgDateModified => "SchemaOrgDateModified".to_string(),
            Element::SchemaOrgVersion => "SchemaOrgVersion".to_string(),
            Element::OntologyBadge => "OntologyBadge".to_string(),
            Element::OntologyRank => "OntologyRank".to_string(),
            Element::OntologyBestRank => "OntologyBestRank".to_string(),
            Element::OntologyNormalRank => "OntologyNormalRank".to_string(),
            Element::OntologyIdentifiers => "OntologyIdentifiers".to_string(),
            Element::OntologyStatementProperty => "OntologyStatementProperty".to_string(),
            Element::OntologyLemma => "OntologyLemma".to_string(),
            Element::OntologyStatements => "OntologyStatements".to_string(),
            Element::OntologySitelinks => "OntologySitelinks".to_string(),
            Element::OntologyPropertyType => "OntologyPropertyType".to_string(),
            Element::OntologyExternalId => "OntologyExternalId".to_string(),
            Element::OntologyClaim => "OntologyClaim".to_string(),
            Element::OntologyDirectClaim => "OntologyDirectClaim".to_string(),
            Element::Other(_) => "Other".to_string(),
        }
    }

    pub fn get_type_parts(&self) -> Vec<TypePart> {
        match self {
            Element::Text(_) => vec![TypePart::Text],
            Element::TextInLanguage(_) => vec![TypePart::Text,TypePart::ShortText],
            Element::WikiPage(_) => vec![TypePart::ShortText,TypePart::Text],
            Element::Entity(_) => vec![TypePart::ShortText],
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
            Element::Entity(s) => vec![s.to_owned()],
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
