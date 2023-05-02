use std::{sync::Arc, num::ParseIntError, string::FromUtf8Error};

#[derive(Clone, Debug)]
pub enum WDQSErr {
    String(String),
    MySQL(Arc<mysql_async::Error>),
    IO(Arc<std::io::Error>),
    Serde(Arc<serde_json::Error>),
    ParseInt(ParseIntError),
    FromUtf8(FromUtf8Error),
    ParserError(String),
    NomError(String),
}

impl std::error::Error for WDQSErr {}

impl std::fmt::Display for WDQSErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            WDQSErr::String(s) => f.write_str(s),
            WDQSErr::MySQL(e) => f.write_str(&e.to_string()),
            WDQSErr::IO(e) => f.write_str(&e.to_string()),
            WDQSErr::Serde(e) => f.write_str(&e.to_string()),
            WDQSErr::ParseInt(e) => f.write_str(&e.to_string()),
            WDQSErr::FromUtf8(e) => f.write_str(&e.to_string()),
            WDQSErr::ParserError(e) => f.write_str(&e.to_string()),
            WDQSErr::NomError(e) => f.write_str(&e.to_string()),
        }
    }
}

impl From<String> for WDQSErr {  
    fn from(e: String) -> Self {Self::String(e)}
}

impl From<&str> for WDQSErr {  
    fn from(e: &str) -> Self {Self::String(e.to_string())}
}

impl From<mysql_async::Error> for WDQSErr {  
    fn from(e: mysql_async::Error) -> Self {Self::MySQL(Arc::new(e))}
}

impl From<std::io::Error> for WDQSErr {  
    fn from(e: std::io::Error) -> Self {Self::IO(Arc::new(e))}
}

impl From<serde_json::Error> for WDQSErr {  
    fn from(e: serde_json::Error) -> Self {Self::Serde(Arc::new(e))}
}

impl From<ParseIntError> for WDQSErr {  
    fn from(e: ParseIntError) -> Self {Self::ParseInt(e)}
}

impl From<FromUtf8Error> for WDQSErr {  
    fn from(e: FromUtf8Error) -> Self {Self::FromUtf8(e)}
}

impl From<nom::Err<nom::error::Error<&str>>> for WDQSErr {  
    fn from(e: nom::Err<nom::error::Error<&str>>) -> Self {Self::String(e.to_string())}
}
