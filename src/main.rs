#[macro_use]
extern crate lazy_static;

extern crate tokio;

use std::sync::Arc;
use app_state::AppState;
use error::WDSQErr;
use query_triples::QueryTriples;

pub mod type_part;
pub mod element_type;
pub mod entity;
pub mod entity_statement;
pub mod lat_lon;
pub mod text_id;
pub mod uuid;
pub mod date_time;
pub mod element;
pub mod parser;
pub mod database_table;
pub mod db_operation_cache;
pub mod database_wrapper;
pub mod app_state;
pub mod query_part;
pub mod query_triples;
pub mod error;
pub mod app_state_mysql_live;
pub mod app_state_mysql_stdout;


use clap::{Arg, Command};
use serde_json::json;

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
            .help("Import triples from FILE (NT dump, plain text or gz/bzip)")
            .num_args(1))
        .arg(Arg::new("dbtype")
            .short('d')
            .long("dbtype")
            .value_name("KEY")
            .help("Use database type KEY")
            .num_args(1))
        .get_matches();

    let mut config = AppState::get_config_from_file("config.json").unwrap();
    if let Some(dbtype) = matches.get_one::<String>("dbtype") {
        config["db_type"] = json!(dbtype);
    }
    let app = Arc::new(AppState::from_config(&config));
    app.init_from_db().await?;
    if let Some(filename) = matches.get_one::<String>("import") {
        let parser = parser::Parser::new();
        parser.import_from_file(filename, &app).await?;
    } else { // query
        let mut qt1 = QueryTriples::from_str(&app,"?person","wdt:P31","wd:Q5").await?;
        let qt2 = QueryTriples::from_str(&app,"?person","wdt:P21","wd:Q6581072").await?;
        qt1.and(&qt2)?;
        println!("{:?}",&qt1.result);
        let result = qt1.run(&app).await?;
        println!("{:?}",result);
    }
    Ok(())
}
