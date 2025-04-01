use crate::processor::LakehouseProcessor;
use common::Result;
use futures::future::BoxFuture;
use uuid::Uuid;

pub struct QueryExecutor<'a> {
    processor: &'a LakehouseProcessor,
    table_name: String,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(processor: &'a LakehouseProcessor) -> Self {
        Self {
            processor,
            table_name: format!("query_{}", Uuid::new_v4()),
        }
    }

    pub async fn execute<F, T>(&self, query_fn: F) -> Result<T>
    where
        F: FnOnce(&str) -> BoxFuture<'_, Result<T>>,
    {
        let result = query_fn(&self.table_name).await;
        self.processor.deregister_table(&self.table_name).await?;
        result
    }
}