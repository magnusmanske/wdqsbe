use std::{io::{self, BufRead, Lines}, fs::File, sync::Arc};
use nom::{IResult, bytes::complete::{tag, take_until, take_until1}, branch::alt, character::complete::space1};
use crate::{element::Element, app_state::AppState, error::WDSQErr, database_wrapper::DatabaseWrapper, lat_lon::LatLon, element_type::ElementType, date_time::DateTime};
use bzip2::read::MultiBzDecoder;
use flate2::read::GzDecoder;

#[derive(Clone, Debug)]
pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Self {}
    }

    fn parse_line(line: &str) -> Result<(Element,Element,Element),WDSQErr> {
        fn element_url(input: &str) -> IResult<&str, Element> {
            let (input, _) = tag("<")(input)?;
            let (input, s) = take_until1(">")(input)?;
            let (input, _) = tag(">")(input)?;
            let element = Element::from_str(s).unwrap();
            Ok((input, element))
        }

        fn element_underscore(input: &str) -> IResult<&str, Element> {
            let (input, _) = tag("_")(input)?;
            let (input, s) = take_until(" ")(input)?;
            let element = Element::Text(s.into());
            Ok((input, element))
        }

        fn string_type(input: &str) -> IResult<&str, &str> {
            let (input, _) = tag("^^")(input)?;
            let (input, _) = tag("<")(input)?;
            let (input, s) = take_until1(">")(input)?;
            let (input, _) = tag(">")(input)?;
            Ok((input, s))
        }

        fn string_language(input: &str) -> IResult<&str, &str> {
            let (input, _) = tag("@")(input)?;
            let (input, s) = take_until1(" ")(input)?;
            Ok((input, s))
        }

        fn element_from_type(s: &str, type_s: &str) -> Option<Element> {
            match type_s {
                "http://www.w3.org/2001/XMLSchema#dateTime" => return Some(Element::DateTime(*DateTime::from_str(&s)?)),
                "http://www.opengis.net/ont/geosparql#wktLiteral" => return Some(Element::LatLon(*LatLon::from_str(&s)?)),
                "http://www.w3.org/2001/XMLSchema#decimal" => return Some(Element::Float(s.parse::<f64>().ok()?)),
                "http://www.w3.org/2001/XMLSchema#double" => return Some(Element::Float(s.parse::<f64>().ok()?)), // For now, same as decimal
                "http://www.w3.org/2001/XMLSchema#integer" => return Some(Element::Int(s.parse::<i64>().ok()?)),
                other => {
                    println!("element_from_type: Unknown type '{other}' for '{s}'");
                    return Some(Element::Url(s.into()));
                }
            }
        }

        fn element_string(input: &str) -> IResult<&str, Element> {
            let (input, _) = tag("\"")(input)?;
            let (input, s) = take_until("\"")(input)?;
            let (input, _) = tag("\"")(input)?;
            if let Ok((input, type_s)) = string_type(input) {
                let element = match element_from_type(s, type_s) {
                    Some(element) => element,
                    None => {
                        println!("element_string: type parsing has failed: '{input}' / '{type_s}'");
                        Element::Text(s.into())
                    }
                };
                return Ok((input, element));
            }
            if let Ok((input, language_s)) = string_language(input) {
                let element = Element::TextInLanguage((s.into(),language_s.into()));
                return Ok((input, element));
            }
            Ok((input, Element::Text(s.into())))
        }

        fn element(input: &str) -> IResult<&str, Element> {
            alt((element_url,element_underscore,element_string))(input)
        }

        let input: &str = &line;
        let (input,part1) = element(input)?;
        let (input,_) = space1(input)?;
        let (input,part2) = element(input)?;
        let (input,_) = space1(input)?;
        let (_,part3) = element(input)?;

        if let Element::Url(url) = &part2 {
            println!("parse_line: Property is URL, but should not be: {url:?}");
        }
        Ok((part1,part2,part3))
    }

    async fn read_lines<T: BufRead>(&self, lines_iter: &mut Lines<T>, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        let mut wrapper = DatabaseWrapper::new(app.clone());
        while let Some(line) = lines_iter.next() {
            if let Ok(line) = line {
                let _ = match Self::parse_line(&line) {
                    Ok((part1,part2,part3)) => wrapper.add(part1,&part2,part3).await,
                    Err(e) => {
                        eprintln!("PARSER ERROR:\n{line}\n{e}");
                        Ok(())
                    }
                };
            }
        }
        wrapper.flush_insert_caches().await
    }

    pub async fn import_from_file(&self, filename: &str, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        let file = File::open(filename)?;
        let buffer_size = 1024*1024;
        match filename.split('.').last() {
            Some("bz2") => self.read_lines(&mut io::BufReader::with_capacity(buffer_size, MultiBzDecoder::new(file)).lines(), app).await,
            Some("gz") => self.read_lines(&mut io::BufReader::with_capacity(buffer_size, GzDecoder::new(file)).lines(), app).await,
            _ => self.read_lines(&mut io::BufReader::with_capacity(buffer_size, file).lines(), app).await,
        }
    }

}