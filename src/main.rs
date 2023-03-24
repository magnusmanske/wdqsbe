#[macro_use]
extern crate lazy_static;

extern crate tokio;

use std::sync::Arc;
use app_state::AppState;
use error::WDSQErr;
use query_triples::QueryTriples;

pub mod type_part;
pub mod element;
pub mod parser;
pub mod database_table;
pub mod db_operation_cache;
pub mod database_wrapper;
pub mod app_state;
pub mod query_part;
pub mod query_triples;
pub mod error;


#[tokio::main]
async fn main() -> Result<(), WDSQErr> {
    let app = Arc::new(AppState::from_config_file("config.json").unwrap());
    app.init_from_db().await?;
    if false { // Import
        let filename = "/Users/mm6/Downloads/new_triples.nt";
        let parser = parser::Parser{};
        parser.import_from_file(filename, &app).await?;
    } else {
        let mut qt1 = QueryTriples::from_str(&app,"?person","wdt:P31","wd:Q5").await?;
        let qt2 = QueryTriples::from_str(&app,"?person","wdt:P21","wd:Q6581072").await?;
        qt1.and(&qt2)?;
        println!("{:?}",&qt1.result);
    }
    Ok(())
}
