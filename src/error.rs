use std::{sync::Arc, num::ParseIntError, string::FromUtf8Error};

#[derive(Clone, Debug)]
pub enum WDSQErr {
    String(String),
    MySQL(Arc<mysql_async::Error>),
    IO(Arc<std::io::Error>),
    Serde(Arc<serde_json::Error>),
    ParseInt(ParseIntError),
    FromUtf8(FromUtf8Error),
}

impl std::error::Error for WDSQErr {}

impl std::fmt::Display for WDSQErr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::String(s) => f.write_str(s),
            Self::MySQL(e) => f.write_str(&e.to_string()),
            Self::IO(e) => f.write_str(&e.to_string()),
            Self::Serde(e) => f.write_str(&e.to_string()),
            Self::ParseInt(e) => f.write_str(&e.to_string()),
            Self::FromUtf8(e) => f.write_str(&e.to_string()),
        }
    }
}

impl From<String> for WDSQErr {  
    fn from(e: String) -> Self {Self::String(e)}
}

impl From<&str> for WDSQErr {  
    fn from(e: &str) -> Self {Self::String(e.to_string())}
}

impl From<mysql_async::Error> for WDSQErr {  
    fn from(e: mysql_async::Error) -> Self {Self::MySQL(Arc::new(e))}
}

impl From<std::io::Error> for WDSQErr {  
    fn from(e: std::io::Error) -> Self {Self::IO(Arc::new(e))}
}

impl From<serde_json::Error> for WDSQErr {  
    fn from(e: serde_json::Error) -> Self {Self::Serde(Arc::new(e))}
}

impl From<ParseIntError> for WDSQErr {  
    fn from(e: ParseIntError) -> Self {Self::ParseInt(e)}
}

impl From<FromUtf8Error> for WDSQErr {  
    fn from(e: FromUtf8Error) -> Self {Self::FromUtf8(e)}
}
