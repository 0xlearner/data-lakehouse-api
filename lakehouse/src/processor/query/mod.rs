use super::*;
use crate::processor::core::LakehouseProcessor;
use crate::services::query::QueryExecutor;
use crate::utils::arrow::batches_to_json;
use datafusion::dataframe::DataFrame;
use serde_json::Value;
use std::pin::Pin;

impl LakehouseProcessor {
    pub async fn execute_sql(&self, sql: &str) -> Result<DataFrame> {
        self.ctx.sql(sql).await.map_err(|e| e.into())
    }

    pub async fn query_bronze_data(
        self: Arc<Self>,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        limit: usize,
    ) -> Result<Vec<Value>> {
        let query_executor = QueryExecutor::new(&self);

        let files = self
            .list_bronze_parquet_files(city_code, year, month, day)
            .await?;

        if files.is_empty() {
            return Err(common::Error::NotFound("No parquet files found".into()));
        }

        let file_path = files[0].clone();
        let ctx = Arc::new(self.ctx.clone());

        query_executor
            .execute(|table_name: &str| {
                let file_path = file_path.clone();
                println!("File path: {:?}", file_path);
                let table_name = table_name.to_string();
                println!("Table name: {:?}", table_name);
                let ctx = Arc::clone(&ctx);
                let self_ref = Arc::clone(&self);

                Box::pin(async move {
                    self_ref
                        .register_bronze_vendors(&table_name, &file_path)
                        .await?;
                    let sql = format!("SELECT * FROM \"{}\" LIMIT {}", table_name, limit);
                    let df = ctx.sql(&sql).await?;
                    let batches = df.collect().await?;
                    batches_to_json(batches)
                }) as Pin<Box<dyn Future<Output = Result<Vec<Value>>> + Send>>
            })
            .await
    }
}
