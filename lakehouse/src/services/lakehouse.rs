use common::Result;
use common::config::Settings;
use crate::processor::{LakehouseProcessor, StorageConfig};
use crate::utils::arrow::batches_to_json;
use std::sync::Arc;
use serde_json::Value;
use uuid::Uuid;

pub struct LakehouseService {
    processor: Arc<LakehouseProcessor>,
    storage_config: StorageConfig,
}

impl LakehouseService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let s3_config = crate::storage::S3Config {
            endpoint: settings.minio.endpoint.clone(),
            region: settings.minio.region.clone(),
            access_key: settings.minio.access_key.clone(),
            secret_key: settings.minio.secret_key.clone(),
            bronze_bucket: settings.minio.bronze_bucket.clone(),
            metadata_bucket: settings.minio.metadata_bucket.clone(),
        };
        let processor = LakehouseProcessor::new(&s3_config).await?;
        
        let storage_config = StorageConfig::from_settings(settings).await?;
        
        processor.register_buckets(&storage_config).await?;

        Ok(Self {
            processor: Arc::new(processor),
            storage_config,
        })
    }
    
    pub async fn process_city_data(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        file_content: Option<&[u8]>, // Add this parameter
    ) -> Result<String> {
        let source_path = format!(
            "s3://{}/city_id={}/year={}/month={:02}/day={:02}/",
            self.storage_config.source_bucket.bucket(),
            city_code, year, month, day
        );
    
        // Process the data with file content for deduplication
        let processed_data = self.processor.process_bronze_data(
            &source_path,
            city_code,
            year,
            month,
            day,
            file_content, // Pass through the file content
        ).await?;
    
        // Store the data with proper path structure
        let bronze_key = self.processor.store_bronze_data(
            processed_data,
            city_code,
            year,
            month,
            day,
        ).await?;
    
        Ok(bronze_key)
    }

    // New method for cleaner API
    pub async fn query_raw_vendors_by_date(
        &self,
        city: &str,
        year: i32,
        month: u32,
        day: u32,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let file_path = format!(
            "city_id={}/year={}/month={:02}/day={:02}/",
            city, year, month, day
        );
        self.query_raw_vendors(&file_path, limit).await
    }

    // New method for cleaner API
    pub async fn query_bronze_vendors_by_date(
        &self,
        city: &str,
        year: i32,
        month: u32,
        day: u32,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let table_name = format!("bronze_query_{}", Uuid::new_v4());
        let s3_path = format!(
            "s3://{}/vendors/city_id={}/year={}/month={:02}/day={:02}/",
            self.storage_config.bronze_bucket.bucket(),
            city, year, month, day
        );
    
        if let Some(bronze_schema) = self.processor.get_cached_schema("bronze_vendors") {
            self.processor.validate_schema(&s3_path, &bronze_schema).await?;
        }
    
        self.processor
            .register_bronze_vendors(&table_name, &s3_path)
            .await?;
    
        let results = self.execute_and_fetch(&table_name, limit).await;
        self.processor.deregister_table(&table_name).await?;
        results
    }

    // Keep existing file_path-based methods
    pub async fn query_raw_vendors(
        &self,
        file_path: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let table_name = format!("raw_query_{}", Uuid::new_v4());
        let s3_path = format!(
            "s3://{}/{}",
            self.storage_config.source_bucket.bucket(),
            file_path.trim_start_matches('/')
        );
        
        if let Some(raw_schema) = self.processor.get_cached_schema("raw_vendors") {
            self.processor.validate_schema(&s3_path, &raw_schema).await?;
        }

        self.processor
            .register_raw_vendors(&table_name, &s3_path)
            .await?;

        let results = self.execute_and_fetch(&table_name, limit).await;
        self.processor.deregister_table(&table_name).await?;
        results
    }

    async fn execute_and_fetch(
        &self,
        table_name: &str,
        limit: usize,
    ) -> Result<Vec<Value>> {
        // Use prepared statement style to avoid SQL injection and parsing issues
        let sql = format!("SELECT * FROM \"{}\" LIMIT {}", table_name, limit);
        let df = self.processor.execute_sql(&sql).await?;
    
        let batches = df.collect().await?;
        batches_to_json(batches)
    }
}
