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


    pub async fn add(&self, s: Element, p: Element, o: Element) -> Result<(),WDSQErr> {
        let table = self.app.table(s.clone(),p,o.clone()).await?;
        let mut values = s.values();
        values.append(&mut o.values());
        let mut cache = self.insert_cache.write().await;
        cache
            .entry(table.name.to_owned())
            .or_insert(DbOperationCache::new())
            .add(&s, &o, &table, values, &self.app)
            .await?;
        Ok(())
    }

    pub async fn flush_insert_caches(&self) -> Result<(),WDSQErr> {
        let mut insert_cache = self.insert_cache.write().await;
        let tasks: Vec<_> = insert_cache
            .iter_mut()
            .map(|(_table_name,cache)|{
                let mut cache = cache.clone();
                let app = self.app.clone();
                tokio::spawn(async move {
                    cache.force_flush(&app).await
                })
            })
            .collect();
        self.first_err(join_all(tasks).await)?;
        insert_cache.clear();
        Ok(())
    }

    pub fn first_err(&self, results: Vec<Result<Result<(), WDSQErr>, tokio::task::JoinError>>) -> Result<(),WDSQErr> {
        let result = results
            .iter()
            .filter(|result|result.is_err())
            .nth(0);
        if let Some(Err(e)) = result {
            return Err(e.to_string().into());
        }
        Ok(())
    }

}
