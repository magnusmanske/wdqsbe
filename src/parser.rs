use std::{io::{self, BufRead, Lines}, fs::File, sync::Arc};
use nom::{IResult, bytes::complete::{tag, take_until, take_until1}, branch::alt, character::complete::space1, error::{VerboseError, VerboseErrorKind}};
use tokio::sync::Mutex;
use crate::{element::Element, app_state::AppState, error::WDQSErr, database_wrapper::DatabaseWrapper, lat_lon::LatLon, element_type::ElementType, date_time::DateTime};
use bzip2::read::MultiBzDecoder;
use flate2::read::GzDecoder;

const MAX_CONCURRENT_THREADS: usize = 50000;

#[derive(Clone, Debug)]
pub struct Parser {
    wrapper: Arc<DatabaseWrapper>,
}

impl Parser {
    pub fn new(app: Arc<AppState>) -> Self {
        Self {
            wrapper: Arc::new(DatabaseWrapper::new(app)),
        }
    }

    fn parse_line(line: &str) -> Result<(Element,Element,Element),WDQSErr> {
        type Res<T, U> = IResult<T, U, VerboseError<T>>;
        
        fn element_url(input: &str) -> Res<&str, Element> {
            let (input, _) = tag("<")(input)?;
            let (input, s) = take_until1(">")(input)?;
            let (input, _) = tag(">")(input)?;
            let element = Element::from_str(s).unwrap();
            Ok((input, element))
        }

        fn element_underscore(input: &str) -> Res<&str, Element> {
            let (input, _) = tag("_")(input)?;
            let (input, s) = take_until(" ")(input)?;
            let element = Element::Text(s.into());
            Ok((input, element))
        }

        fn string_type(input: &str) -> Res<&str, &str> {
            let (input, _) = tag("^^")(input)?;
            let (input, _) = tag("<")(input)?;
            let (input, s) = take_until1(">")(input)?;
            let (input, _) = tag(">")(input)?;
            Ok((input, s))
        }

        fn string_language(input: &str) -> Res<&str, &str> {
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
                _ => None,
            }
        }

        fn element_string(input: &str) -> Res<&str, Element> {
            let (input, _) = tag("\"")(input)?;
            let (input, s) = take_until("\"")(input)?;
            let (input, _) = tag("\"")(input)?;
            if let Ok((input, type_s)) = string_type(input) {
                let element = match element_from_type(s, type_s) {
                    Some(element) => element,
                    None => {
                        eprintln!("element_string: type parsing has failed: '{input}' / '{type_s}'");
                        return Err(nom::Err::Error(VerboseError { errors: vec![(input, VerboseErrorKind::Context("element_string: type parsing has failed"))] }));
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

        fn element(input: &str) -> Res<&str, Element> {
            alt((element_url,element_underscore,element_string))(input)
        }

        fn parse_line_sub(input: &str) -> Res<&str, (Element,Element,Element)> {
            let (input,part1) = element(input)?;
            let (input,_) = space1(input)?;
            let (input,part2) = element(input)?;
            let (input,_) = space1(input)?;
            let (_,part3) = element(input)?;
            Ok((input,(part1,part2,part3)))
        }

        fn parse_line_internal(line: &str) -> Result<(Element,Element,Element),WDQSErr> {
            let (part1,part2,part3) = match parse_line_sub(line) {
                Ok((_,part123)) => part123,
                Err(e) => return Err(WDQSErr::String(e.to_string())),
            };
            if let Element::Url(url) = &part2 {
                Err(WDQSErr::String(format!("parse_line: Property is URL, but should not be: {url:?}")))
            } else {
                Ok((part1,part2,part3))
            }
        }

        match parse_line_internal(line) {
            Ok(ret) => Ok(ret),
            Err(e) => {
                eprintln!("Parsing error");
                Err(e)
            },
        }

    }

    async fn read_lines<T: BufRead>(&self, lines_iter: &mut Lines<T>) -> Result<(),WDQSErr> {
        let counter = Arc::new(Mutex::new(0 as usize));
        while let Some(line) = lines_iter.next() {
            if let Ok(line) = line {

                while *counter.lock().await>MAX_CONCURRENT_THREADS {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
        
                let wrapper = self.wrapper.clone();
                let counter = counter.clone();
                *counter.lock().await += 1;
                tokio::task::spawn(async move {
                    match Self::parse_line(&line) {
                        Ok((part1,part2,part3)) => {
                            if let Err(e) = wrapper.add(part1,&part2,part3).await {
                                eprintln!("WARPPER.ADD ERROR:{e} line:\n{line}\n")
                            }
                        }
                        Err(e) => {
                            eprintln!("PARSER ERROR:{e} line:\n{line}\n");
                        }
                    };
                    *counter.lock().await -= 1;
                });
            }
        }
        while *counter.lock().await>0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        self.wrapper.flush_insert_caches().await
    }

    pub async fn import_from_file(&self, filename: &str) -> Result<(),WDQSErr> {
        let file = File::open(filename)?;
        let buffer_size = 1024*1024;
        match filename.split('.').last() {
            Some("bz2") => self.read_lines(&mut io::BufReader::with_capacity(buffer_size, MultiBzDecoder::new(file)).lines()).await,
            Some("gz") => self.read_lines(&mut io::BufReader::with_capacity(buffer_size, GzDecoder::new(file)).lines()).await,
            _ => self.read_lines(&mut io::BufReader::with_capacity(buffer_size, file).lines()).await,
        }
    }

}