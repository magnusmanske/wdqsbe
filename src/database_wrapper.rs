use std::{sync::Arc, collections::HashMap};
use futures::future::join_all;
use crate::{error::*, element::Element, db_operation_cache::DbOperationCache, app_state::AppState};


#[derive(Debug, Clone)]
pub struct DatabaseWrapper {
    app: Arc<AppState>,
    insert_cache: HashMap<String,DbOperationCache>,
}

impl DatabaseWrapper {
    pub fn new(app: Arc<AppState>) -> Self {
        Self {
            app,
            insert_cache: HashMap::new(),
        }
    }

    pub async fn add(&mut self, s: Element, p: &Element, o: Element) -> Result<(),WDSQErr> {
        let table = self.app.table(&s,p,&o).await?;
        let mut values = s.values();
        values.append(&mut o.values());

        if let Some(cache) = self.insert_cache.get_mut(&table.name) {
            cache.add(&s, &o, &table, values, &self.app).await?;
            return Ok(())
        }

        // Add new
        self.insert_cache
            .entry(table.name.to_owned())
            .or_insert(DbOperationCache::new());

        self.insert_cache.get_mut(&table.name).unwrap()
            .add(&s, &o, &table, values, &self.app)
            .await?;
        Ok(())
    }

    pub async fn flush_insert_caches(&mut self) -> Result<(),WDSQErr> {
        let mut tasks = vec![];
        for (_,cache) in self.insert_cache.drain() {
            let app = self.app.clone();
            tasks.push(tokio::spawn(async move { cache.force_flush(&app).await }));
        }
        Self::first_err(join_all(tasks).await, true)?;
        Ok(())
    }

    pub fn first_err(results: Vec<Result<Result<(), WDSQErr>, tokio::task::JoinError>>, exit: bool) -> Result<(),WDSQErr> {
        let errors: Vec<_> = results
            .iter()
            .filter_map(|result|result.as_ref().ok()) // Remove Join errors to get to the WDSQErr
            .filter(|result|result.is_err())
            .collect();
        if let Some(Err(e)) = errors.get(0) {
            if errors.len()>1 || !exit {
                eprintln!("{errors:?}");
            }
            if exit {
                return Err(e.to_string().into());
            } else {
                return Ok(());
            }
        }
        Ok(())
    }

}
