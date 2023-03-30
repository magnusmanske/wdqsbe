use std::{io::{self, BufRead, Lines}, fs::File, collections::VecDeque, sync::Arc};
use futures::future::join_all;
use crate::{element::Element, app_state::AppState, error::WDSQErr, database_wrapper::DatabaseWrapper, lat_lon::LatLon, element_type::ElementType, date_time::DateTime};
use bzip2::read::MultiBzDecoder;
use flate2::read::GzDecoder;

const TASKS_IN_PARALLEL: usize = 100;

#[derive(Clone, Debug)]
pub struct Parser {
}

impl Parser {
    fn skip_whitespace(chars: &mut VecDeque<char>) {
        while chars.front()==Some(&' ') {
            let _ = chars.pop_front();
        }
    }

    // TODO FIXME this is bad, slow, no backslash-escape etc
    fn read_part(chars: &mut VecDeque<char>) -> Option<Element> {
        let mut ret = String::new();
        let first = chars.front()?;
        if *first=='<' {
            let _ = chars.pop_front()?;
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
            Element::from_str(ret)
        } else if *first=='"' {
            let _ = chars.pop_front()?;
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
            if chars.front()==Some(&'^') {
                while chars.front()==Some(&'^') {
                    chars.pop_front();
                }
                chars.pop_front(); // <
                let mut var_type = String::new();
                while chars.front().is_some() && chars.front()!=Some(&'>') {
                    let c = chars.pop_front().unwrap();
                    var_type.push(c);
                }
                chars.pop_front(); // >
                match var_type.as_str() {
                    "http://www.w3.org/2001/XMLSchema#dateTime" => return Some(Element::DateTime(*DateTime::from_str(&ret)?)),
                    "http://www.opengis.net/ont/geosparql#wktLiteral" => return Some(Element::LatLon(*LatLon::from_str(&ret)?)),
                    "http://www.w3.org/2001/XMLSchema#decimal" => return Some(Element::Float(ret.parse::<f64>().ok()?)),
                    "http://www.w3.org/2001/XMLSchema#double" => return Some(Element::Float(ret.parse::<f64>().ok()?)), // For now, same as decimal
                    "http://www.w3.org/2001/XMLSchema#integer" => return Some(Element::Int(ret.parse::<i64>().ok()?)),
                    other => {
                        println!("Unknown var_type {other}: {ret}");
                        return Some(Element::Url(ret.into()));
                    }
                }
            }
            let mut language = String::new();
            if chars.front()!=Some(&'@') {
                return Some(Element::Text(ret.into()));
            }
            let _ = chars.pop_front()?; // @
            while chars.front().is_some() && chars.front()!=Some(&' ') {
                let c = chars.pop_front().unwrap();
                language.push(c);
            }
            Some(Element::TextInLanguage((ret.into(),language.into())))
        } else {
            None
        }
    }

    async fn parse_line(line: String, wrapper: Arc<DatabaseWrapper>) -> Result<(),WDSQErr> {
        let mut chars = line.chars().collect();
        let part1 = Self::read_part(&mut chars).ok_or_else(||WDSQErr::ParserError(line.to_owned()))?;
        Self::skip_whitespace(&mut chars);
        let part2 = Self::read_part(&mut chars).ok_or_else(||WDSQErr::ParserError(line.to_owned()))?;
        Self::skip_whitespace(&mut chars);
        let part3 = Self::read_part(&mut chars).ok_or_else(||WDSQErr::ParserError(line.to_owned()))?;
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
        wrapper.first_err(join_all(tasks).await)?;
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
            if lines.len()>TASKS_IN_PARALLEL {
                self.process_lines(&lines, &wrapper).await?;
                lines.clear();
            }
        }
        self.process_lines(&lines, &wrapper).await?;
        wrapper.flush_insert_caches().await
    }

}