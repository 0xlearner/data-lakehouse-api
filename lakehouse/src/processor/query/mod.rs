use super::*;
use crate::processor::core::LakehouseProcessor;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::{
    file_format::parquet::ParquetFormat,
    listing::{ListingOptions, ListingTable, ListingTableConfig},
};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::TableProvider;
use serde_json::Value;
use crate::utils::arrow::batches_to_json;
use crate::services::query::QueryExecutor;
use std::pin::Pin;

impl LakehouseProcessor {
    pub async fn execute_sql(&self, sql: &str) -> Result<DataFrame> {
        self.ctx.sql(sql).await.map_err(|e| e.into())
    }

    pub fn get_cached_schema(&self, schema_name: &str) -> Option<Arc<Schema>> {
        self.schema_cache.read().unwrap().get(schema_name).cloned()
    }

    pub fn cache_schema(&self, name: &'static str, schema: Schema) {
        self.schema_cache.write().unwrap().insert(name, Arc::new(schema));
    }

    pub async fn infer_schema(
        &self,
        file_uri: &str,
    ) -> Result<Arc<Schema>> {

        // Create ListingTableUrl from the path
        let table_url = ListingTableUrl::parse(file_uri.to_string())?;
        
        let parquet_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(parquet_format))
            .with_file_extension("parquet");

        // Create config with single path
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options);

        // Infer schema using the session state
        let config = config.infer_schema(&self.ctx.state()).await?;
        
        // Create table and get schema
        let table = Arc::new(ListingTable::try_new(config)?);
        Ok(table.schema().clone())
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
    
        let files = self.list_bronze_parquet_files(city_code, year, month, day).await?;
    
        if files.is_empty() {
            return Err(common::Error::NotFound("No parquet files found".into()));
        }
    
        let file_path = files[0].clone();
        let ctx = Arc::new(self.ctx.clone());
    
        query_executor.execute(|table_name: &str| {
            let file_path = file_path.clone();
            println!("File path: {:?}", file_path);
            let table_name = table_name.to_string();
            println!("Table name: {:?}", table_name);
            let ctx = Arc::clone(&ctx);
            let self_ref = Arc::clone(&self);

            Box::pin(async move {
                self_ref.register_bronze_vendors(&table_name, &file_path).await?;
                let sql = format!("SELECT * FROM \"{}\" LIMIT {}", table_name, limit);
                let df = ctx.sql(&sql).await?;
                let batches = df.collect().await?;
                batches_to_json(batches)
            }) as Pin<Box<dyn Future<Output = Result<Vec<Value>>> + Send>>
        })
        .await
    }
}
