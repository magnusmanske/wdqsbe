use std::{collections::HashMap, sync::Arc};
use futures::future::join_all;
use tokio::sync::RwLock;
use crate::{error::*, element::Element, db_operation_cache::DbOperationCache, app_state::AppState};


#[derive(Debug, Clone)]
pub struct DatabaseWrapper {
    app: Arc<AppState>,
    insert_cache: Arc<RwLock<HashMap<String,DbOperationCache>>>,
}

impl DatabaseWrapper {
    pub fn new(app: Arc<AppState>) -> Self {
        Self {
            app,
            insert_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add(&self, s: Element, p: &Element, o: Element) -> Result<(),WDSQErr> {
        let table = self.app.table(&s,p,&o).await?;
        let mut values = s.values();
        values.append(&mut o.values());

        if let Some(cache) = self.insert_cache.read().await.get(&table.name) {
            cache.add(&s, &o, &table, values, &self.app).await?;
            return Ok(())
        }

        // Add new
        self.insert_cache.write().await
            .entry(table.name.to_owned())
            .or_insert(DbOperationCache::new());

        self.insert_cache.read().await.get(&table.name).unwrap()
            .add(&s, &o, &table, values, &self.app)
            .await?;
        Ok(())
    }

    pub async fn flush_insert_caches(&self) -> Result<(),WDSQErr> {
        let mut insert_cache = self.insert_cache.write().await;
        let tasks: Vec<_> = insert_cache
            .iter()
            .map(|(_table_name,cache)|{
                let cache = cache.clone();
                let app = self.app.clone();
                tokio::spawn(async move { cache.force_flush(&app).await })
            })
            .collect();
        Self::first_err(join_all(tasks).await, true)?;
        insert_cache.clear();
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
                println!("{errors:?}");
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
