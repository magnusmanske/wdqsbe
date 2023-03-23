use std::{io::{self, BufRead}, fs::File, collections::VecDeque, sync::Arc};
use futures::future::join_all;
use crate::{element::Element, app_state::AppState, error::WDSQErr, database_wrapper::DatabaseWrapper};


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
            Element::from_str(ret)
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

    async fn parse_line(line: String, wrapper: Arc<DatabaseWrapper>) -> Result<(),WDSQErr> {
        let mut chars = line.chars().collect();
        let part1 = Self::read_part(&mut chars).ok_or_else(||WDSQErr::ParserError)?;
        Self::skip_whitespace(&mut chars);
        let part2 = Self::read_part(&mut chars).ok_or_else(||WDSQErr::ParserError)?;
        Self::skip_whitespace(&mut chars);
        let part3 = Self::read_part(&mut chars).ok_or_else(||WDSQErr::ParserError)?;
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
                tokio::spawn(async {
                    Self::parse_line(line, wrapper).await
                })
            })
            .collect();

        let tasks = tasks.into_iter();
        join_all(tasks).await
            .iter()
            .filter(|result|result.is_err())
            .for_each(|result|{
                if let Err(e) = result {
                    println!("{e}")
                }
            });

        Ok(())
    }

    pub async fn import_from_file(&self, filename: &str, app: &Arc<AppState>) -> Result<(),WDSQErr> {
        let wrapper = Arc::new(DatabaseWrapper::new(app.clone()));
        let file = File::open(filename).unwrap();
        let mut lines = vec![];
        for line in io::BufReader::new(file).lines() {
            // let _ = self.parse_line(line?, &wrapper).await;
            lines.push(line?);
            if lines.len()>TASKS_IN_PARALLEL {
                self.process_lines(&lines, &wrapper).await?;
                lines.clear();
            }
        }
        self.process_lines(&lines, &wrapper).await?;
        wrapper.flush_insert_caches().await
    }
}