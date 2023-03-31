use std::{io::{self, BufRead, Lines}, fs::File, sync::Arc};
use futures::future::join_all;
use nom::{IResult, bytes::complete::{tag, take_until, take_until1}, branch::alt, character::complete::space1, error::Error};
use crate::{element::Element, app_state::AppState, error::WDSQErr, database_wrapper::DatabaseWrapper, lat_lon::LatLon, element_type::ElementType, date_time::DateTime};
use bzip2::read::MultiBzDecoder;
use flate2::read::GzDecoder;

#[derive(Clone, Debug)]
pub struct Parser {
}

impl Parser {
    async fn parse_line(line: String, wrapper: Arc<DatabaseWrapper>) -> Result<(),WDSQErr> {
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
        let (input,_) = space1::<_, Error<_>>(input)?;
        let (input,part2) = element(input)?;
        let (input,_) = space1::<_, Error<_>>(input)?;
        let (_,part3) = element(input)?;

        if let Element::Url(url) = &part2 {
            println!("parse_line: Property is URL, but should not be: {url:?}");
        }
        // println!("{line}\n{part1:?}\n{part2:?}\n{part3:?}\n");
        wrapper.add(part1,part2,part3).await?;

        Ok(())
    }

    async fn process_lines(&self, lines: &Vec<String>, wrapper: &Arc<DatabaseWrapper>) -> Result<(),WDSQErr> {
        if lines.is_empty() {
            return Ok(());
        }
        let tasks: Vec<_> = lines
            .iter()
            .cloned()
            .map(|line|{
                let wrapper = wrapper.clone();
                tokio::spawn(async { Self::parse_line(line, wrapper).await })
            })
            .collect();
        wrapper.first_err(join_all(tasks).await, false)?;
        Ok(())
    }

    pub async fn import_from_file(&self, filename: &str, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        let file = File::open(filename)?;
        match filename.split('.').last() {
            Some("bz2") => self.read_lines(&mut io::BufReader::new(MultiBzDecoder::new(file)).lines(), app).await,
            Some("gz") => self.read_lines(&mut io::BufReader::new(GzDecoder::new(file)).lines(), app).await,
            _ => self.read_lines(&mut io::BufReader::new(file).lines(), app).await,
        }
    }

    async fn read_lines<T: BufRead>(&self, lines_iter: &mut Lines<T>, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        let mut lines = vec![];
        let wrapper = Arc::new(DatabaseWrapper::new(app.clone()));
        while let Some(line) = lines_iter.next() {
            let line = line.unwrap();
            lines.push(line);
            if lines.len()>app.parallel_parsing {
                self.process_lines(&lines, &wrapper).await?;
                lines.clear();
            }
        }
        self.process_lines(&lines, &wrapper).await?;
        wrapper.flush_insert_caches().await
    }

}