use std::{io::{self, BufRead}, fs::File, collections::VecDeque, sync::Arc};
use regex::Regex;
use crate::{element::Element, app_state::AppState};

lazy_static! {
    static ref RE_WIKI_URL: Regex = Regex::new(r#"^https?://(.+?)/wiki/(.+)$"#).expect("RE_WIKI_URL does not parse");
}



#[derive(Clone, Debug)]
pub struct Parser {
}

impl Parser {

    fn parse_element(&self, element: String) -> Option<Element> {
        if let Some(caps) = RE_WIKI_URL.captures(&element) {
            let server = caps.get(1).map_or("", |m| m.as_str()).to_string();
            let page = caps.get(2).map_or("", |m| m.as_str()).to_string();
            return Some(Element::WikiPage((server,page)))
        }
        let mut parts: Vec<_> = element.split("/").collect();
        let key = parts.pop().unwrap().to_string();
        let root = parts.join("/");
        match root.as_str() {
            "http://www.wikidata.org/entity" => Some(Element::Entity(key)),
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

    fn skip_whitespace(&self, chars: &mut VecDeque<char>) {
        while chars.front()==Some(&' ') {
            let _ = chars.pop_front();
        }
    }

    // TODO FIXME this is bad, slow, no backslash-escape etc
    fn read_part(&self, chars: &mut VecDeque<char>) -> Option<Element> {
        let mut ret = String::new();
        let first = chars.front()?;
        if *first=='<' {
            let _ = chars.pop_front();
            let mut complete = false;
            while let Some(c) = chars.pop_front() {
                if c=='>' {
                    complete = true;
                    break;
                }
                ret.push(c);
            }
            if !complete { // Didn't end with '>'
                return None;
            }
            self.parse_element(ret)
        } else if *first=='"' {
            let _ = chars.pop_front();
            let mut complete = false;
            while let Some(c) = chars.pop_front() {
                if c=='"' {
                    complete = true;
                    break;
                }
                ret.push(c);
            }
            if !complete { // Didn't end with '>'
                return None;
            }
            let mut language = String::new();
            if chars.front()!=Some(&'@') {
                return Some(Element::Text(ret));
            }
            let _ = chars.pop_front(); // @
            while chars.front().is_some() && chars.front()!=Some(&' ') {
                let c = chars.pop_front().unwrap();
                language.push(c);
            }
            Some(Element::TextInLanguage((ret,language)))
        } else {
            None
        }
    }

    async fn parse_line(&self, line: String, app: &Arc<AppState>) -> Option<()> {
        let mut chars = line.chars().collect();
        let part1 = self.read_part(&mut chars)?;
        self.skip_whitespace(&mut chars);
        let part2 = self.read_part(&mut chars)?;
        self.skip_whitespace(&mut chars);
        let part3 = self.read_part(&mut chars)?;
        let _ = app.add(part1,part2,part3).await;
        Some(())
    }

    pub async fn import_from_file(&self, filename: &str, app: &Arc<AppState>) {
        let file = File::open(filename).unwrap();
        for line in io::BufReader::new(file).lines() {
            match line {
                Ok(line) => {
                    let _ = self.parse_line(line, app).await; // Ignore errors
                }
                Err(_) => continue, // Ignore error
            }
        }    
    }
}