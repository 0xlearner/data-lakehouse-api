use crate::processor::config::StorageConfig;
use crate::processor::{LakehouseProcessor, ProcessingRequest};
use common::Result;
use common::config::Settings;
use serde_json::Value;
use std::sync::Arc;

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
            source_bucket: settings.minio.source_bucket.clone(),
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

    async fn list_source_files(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
        let files = self
            .processor
            .list_source_parquet_files(city_code, year, month, day)
            .await?;

        if files.is_empty() {
            println!(
                "No files found for city: {}, date: {}-{:02}-{:02}",
                city_code, year, month, day
            );
        } else {
            println!(
                "Found {} files for city: {}, date: {}-{:02}-{:02}",
                files.len(),
                city_code,
                year,
                month,
                day
            );
        }

        Ok(files)
    }

    pub async fn process_city_data(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<String> {
        println!(
            "Starting data processing for city: {}, date: {}-{:02}-{:02}",
            city_code, year, month, day
        );

        // Get list of files first
        let source_files = self.list_source_files(city_code, year, month, day).await?;
        if source_files.is_empty() {
            return Err(common::Error::NotFound("No source files found".to_string()));
        }

        let request = ProcessingRequest::new_s3_direct_with_files(
            city_code,
            year,
            month,
            day,
            self.storage_config.source_bucket.bucket(),
            source_files,
        );

        // Process the data
        match self.processor.process_bronze_data(request).await {
            Ok((processed_data, target_path)) => {
                println!("Successfully processed data to bronze layer");
                let bronze_key = self
                    .processor
                    .store_bronze_data(processed_data, &target_path)
                    .await?;
                Ok(bronze_key)
            }
            Err(e @ common::Error::DuplicateData(_)) => {
                println!("Skipping duplicate data: {}", e);
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn query_bronze_data(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        limit: usize,
    ) -> Result<Vec<Value>> {
        // Clone the Arc<LakehouseProcessor> for use in the query
        let processor = Arc::clone(&self.processor);

        println!(
            "Querying bronze data for city: {}, date: {}-{:02}-{:02}, limit: {}",
            city_code, year, month, day, limit
        );

        processor
            .query_bronze_data(city_code, year, month, day, limit)
            .await
    }
}
