use std::{sync::Arc, collections::HashMap};
use futures::future::join_all;
use tokio::sync::Mutex;
use crate::{error::*, element::Element, db_operation_cache::DbOperationCache, app_state::AppState};


#[derive(Debug, Clone)]
pub struct DatabaseWrapper {
    app: Arc<AppState>,
    insert_cache: Arc<Mutex<HashMap<String,DbOperationCache>>>,
}

impl DatabaseWrapper {
    pub fn new(app: Arc<AppState>) -> Self {
        Self {
            app,
            insert_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn add(&self, s: Element, p: &Element, o: Element) -> Result<(),WDQSErr> {
        let table = self.app.table(&s,p,&o).await?;
        let mut values = s.values();
        values.append(&mut o.values());

        let mut ic = self.insert_cache.lock().await;
        match ic.get_mut(&table.name) {
            Some(cache) => cache.add(values, &self.app).await,
            None => {
                ic.entry(table.name.to_owned())
                    .or_insert(DbOperationCache::new(&s,&o,&table))
                    .add(values, &self.app)
                    .await
            }
        }
    }

    pub async fn flush_insert_caches(&self) -> Result<(),WDQSErr> {
        let mut tasks = vec![];
        for (_,mut cache) in self.insert_cache.lock().await.drain() {
            let app = self.app.clone();
            tasks.push(tokio::spawn(async move { cache.force_flush(&app).await }));
        }
        Self::first_err(join_all(tasks).await, true)?;
        Ok(())
    }

    pub fn first_err(results: Vec<Result<Result<(), WDQSErr>, tokio::task::JoinError>>, exit: bool) -> Result<(),WDQSErr> {
        let errors: Vec<_> = results
            .iter()
            .filter_map(|result|result.as_ref().ok()) // Remove Join errors to get to the WDQSErr
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
