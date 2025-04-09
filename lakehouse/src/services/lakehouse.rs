use crate::processor::config::StorageConfig;
use crate::processor::{LakehouseProcessor, ProcessingRequest};
use crate::services::utils::parse_s3_path_components;
use crate::storage::{S3Config, S3Manager}; // Added S3Manager import and S3Config explicitly
use common::Result;
use common::config::Settings;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct LakehouseService {
    processor: Arc<LakehouseProcessor>,
    storage_config: StorageConfig,
    s3_manager: S3Manager, // Added field
}

impl LakehouseService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        // Use the same config for both S3Manager and LakehouseProcessor initially
        let s3_config = S3Config {
            endpoint: settings.minio.endpoint.clone(),
            region: settings.minio.region.clone(),
            access_key: settings.minio.access_key.clone(),
            secret_key: settings.minio.secret_key.clone(),
            source_bucket: settings.minio.source_bucket.clone(),
            bronze_bucket: settings.minio.bronze_bucket.clone(),
            silver_bucket: settings.minio.silver_bucket.clone(),
            metadata_bucket: settings.minio.metadata_bucket.clone(),
        };

        // Create S3Manager
        let s3_manager = S3Manager::new(s3_config.clone());

        // Create Processor (which likely uses its own S3 interactions internally)
        let processor = LakehouseProcessor::new(&s3_config).await?;

        let storage_config = StorageConfig::from_settings(settings).await?;

        processor.register_buckets(&storage_config).await?;

        Ok(Self {
            processor: Arc::new(processor),
            storage_config,
            s3_manager, // Initialize the s3_manager field
        })
    }

    /// Processes all valid source parquet files found in the source bucket concurrently.
    pub async fn process_all_source_data(&self, concurrency: usize) -> Result<ProcessAllResult> {
        println!("Starting processing for all source data through silver layer...");
        let source_files = self.s3_manager.list_all_source_parquet_files().await?;

        if source_files.is_empty() {
            println!("No source parquet files found matching the expected path structure.");
            return Ok(ProcessAllResult::default());
        }

        println!(
            "Found {} potential source files to process.",
            source_files.len()
        );

        let mut results = ProcessAllResult::default();
        let mut futures = FuturesUnordered::new();
        let mut file_path_map: HashMap<usize, String> = HashMap::new();

        for (index, file_path) in source_files.into_iter().enumerate() {
            let components = match parse_s3_path_components(&file_path) {
                Ok(comp) => comp,
                Err(e) => {
                    eprintln!(
                        "Skipping file: Failed to parse path components from '{}': {}",
                        file_path, e
                    );
                    let parse_error = common::Error::Other(format!(
                        "Failed to parse path components from '{}': {}",
                        file_path, e
                    ));
                    results.record_outcome(&file_path, Err(parse_error));
                    continue;
                }
            };

            file_path_map.insert(index, file_path.clone());

            let processor = Arc::clone(&self.processor);
            let storage_config = self.storage_config.clone();
            let components_clone = components.clone();
            let current_file_path = file_path.clone();

            let future = async move {
                let request = ProcessingRequest::new_s3_direct_with_files(
                    &components_clone.city_code,
                    components_clone.year,
                    components_clone.month,
                    components_clone.day,
                    &storage_config.source_bucket.bucket(),
                    vec![current_file_path.clone()],
                );

                let outcome = async {
                    // Process bronze first
                    let (bronze_data, bronze_path) = processor.process_bronze_data(request).await?;

                    // Check if bronze data is empty
                    if bronze_data.df.clone().count().await? == 0 {
                        return Ok(ProcessingOutcome::Skipped(
                            "skipped-empty-dataframe".to_string(),
                        ));
                    }

                    // Store bronze data
                    processor
                        .store_bronze_data(bronze_data, &bronze_path)
                        .await?;

                    // Process silver
                    let (silver_data, silver_paths) = processor
                        .process_silver_data(
                            &bronze_path,
                            &components_clone.city_code,
                            components_clone.year,
                            components_clone.month,
                            components_clone.day,
                        )
                        .await?;

                    // Store silver data
                    let stored_paths = processor
                        .store_silver_data(silver_data, &silver_paths)
                        .await?;

                    Ok(ProcessingOutcome::SilverSuccess(stored_paths))
                }
                .await;

                (index, outcome)
            };

            futures.push(Box::pin(future));

            if futures.len() >= concurrency {
                if let Some((completed_index, outcome)) = futures.next().await {
                    if let Some(processed_path) = file_path_map.remove(&completed_index) {
                        results.record_outcome(&processed_path, outcome);
                    }
                }
            }
        }

        // Process remaining futures
        while let Some((completed_index, outcome)) = futures.next().await {
            if let Some(processed_path) = file_path_map.remove(&completed_index) {
                results.record_outcome(&processed_path, outcome);
            }
        }

        println!(
            "Finished processing through silver layer. Success: {}, Skipped: {}, Failed: {}",
            results.success,
            results.skipped_known + results.skipped_parse_error,
            results.failed
        );

        Ok(results)
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
} // End impl LakehouseService

// --- Helper Structs ---
//
#[derive(Debug)]
pub enum ProcessingOutcome {
    Skipped(String),
    BronzeSuccess(String),
    SilverSuccess(HashMap<String, String>),
}

#[derive(Debug, Default)]
pub struct ProcessAllResult {
    pub success: u32,
    pub skipped_known: u32,
    pub skipped_parse_error: u32,
    pub failed: u32,
    pub successful_files: Vec<String>,
    pub successful_silver_paths: HashMap<String, Vec<String>>, // Add this field for silver paths
    pub failed_files: Vec<(String, String)>,
}

impl ProcessAllResult {
    fn record_outcome(&mut self, file_path: &str, result: Result<ProcessingOutcome>) {
        match result {
            Ok(outcome) => match outcome {
                ProcessingOutcome::Skipped(reason) => match reason.as_str() {
                    "skipped-duplicate-data"
                    | "skipped-stale-data"
                    | "skipped-schema-validation-error"
                    | "skipped-empty-dataframe"
                    | "skipped-no-source-files" => {
                        self.skipped_known += 1;
                    }
                    _ => {
                        self.skipped_known += 1;
                    }
                },
                ProcessingOutcome::BronzeSuccess(path) => {
                    self.success += 1;
                    self.successful_files.push(path);
                }
                ProcessingOutcome::SilverSuccess(paths) => {
                    self.success += 1;
                    self.successful_files.push(file_path.to_string());
                    self.successful_silver_paths
                        .insert(file_path.to_string(), paths.into_values().collect());
                }
            },
            Err(e) => {
                if matches!(e, common::Error::Other(ref msg) if msg.starts_with("Failed to parse path components"))
                {
                    self.skipped_parse_error += 1;
                    self.failed_files
                        .push((file_path.to_string(), format!("Path Parse Error: {}", e)));
                } else {
                    self.failed += 1;
                    self.failed_files
                        .push((file_path.to_string(), e.to_string()));
                }
            }
        }
    }
}
