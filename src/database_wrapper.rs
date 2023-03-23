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
            .map(|(_table_name,cache)|cache.force_flush(&self.app))
            .collect();
        let result = join_all(tasks).await
            .iter()
            .filter(|result|result.is_err())
            .cloned()
            .nth(0);
        if let Some(result)= result {
                result?;
        }
        // for (_table_name,cache) in insert_cache.iter_mut() {
        //     cache.force_flush(&self).await?;
        // }
        insert_cache.clear();
        Ok(())
    }}
