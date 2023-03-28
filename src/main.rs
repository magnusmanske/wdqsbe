#[macro_use]
extern crate lazy_static;

extern crate tokio;

use std::sync::Arc;
use app_state::AppState;
use error::WDSQErr;
use query_triples::QueryTriples;

pub mod type_part;
pub mod entity;
pub mod element;
pub mod parser;
pub mod database_table;
pub mod db_operation_cache;
pub mod database_wrapper;
pub mod app_state;
pub mod query_part;
pub mod query_triples;
pub mod error;


use clap::{Arg, Command};

#[tokio::main]
async fn main() -> Result<(), WDSQErr> {
    let matches = Command::new("wdqsbe")
        .version("0.1.0")
        .author("Magnus Manske <magnusmanske@googlemail.com>")
        .about("A command line tool to query Wikidata")
        .arg(Arg::new("import")
            .short('i')
            .long("import")
            .value_name("FILE")
            .help("Import triples from FILE")
            .num_args(1))
        .get_matches();

    let app = Arc::new(AppState::from_config_file("config.json").unwrap());
    app.init_from_db().await?;
    if let Some(filename) = matches.get_one::<String>("import") {
        let parser = parser::Parser{};
        parser.import_from_file(filename, &app).await?;
    } else { // query
        let mut qt1 = QueryTriples::from_str(&app,"?person","wdt:P31","wd:Q5").await?;
        let qt2 = QueryTriples::from_str(&app,"?person","wdt:P21","wd:Q6581072").await?;
        qt1.and(&qt2)?;
        println!("{:?}",&qt1.result);
        let result = qt1.run().await?;
        println!("{:?}",result);
    }
    Ok(())
}
