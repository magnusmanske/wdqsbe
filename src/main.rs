#[macro_use]
extern crate lazy_static;

extern crate tokio;

use std::sync::Arc;
use app_state::AppState;
use element::Element;
use error::WDSQErr;
use query_part::QueryPart;
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
        let qp1 = QueryPart::Unknown;
        let qp2 = QueryPart::Element(Element::PropertyDirect("P31".into()));
        let qp3 = QueryPart::Element(Element::Entity("Q5".into()));
        let qt = QueryTriples::new(&app,qp1,qp2,qp3);
        let result = qt.filter_tables().await;
        let result = qt.group_tables(result).await;
        let result = qt.process_grouped_tables(result).await?;
        println!("{result:?}");
    }
    Ok(())
}
