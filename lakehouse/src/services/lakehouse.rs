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
        println!("Starting processing for all source data...");
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

        // --- CHANGE HERE ---
        // No need for TaskInfo struct. Just map index to file_path.
        let mut file_path_map: HashMap<usize, String> = HashMap::new();
        // --- END CHANGE ---

        for (index, file_path) in source_files.into_iter().enumerate() {
            let components = match parse_s3_path_components(&file_path) {
                Ok(comp) => comp,
                Err(e) => {
                    eprintln!(
                        "Skipping file: Failed to parse path components from '{}': {}",
                        file_path, e
                    );
                    // Create a specific error variant if desired, or use InvalidArgument
                    let parse_error = common::Error::Other(format!(
                        "Failed to parse path components from '{}': {}",
                        file_path, e
                    ));
                    results.record_outcome(&file_path, Err(parse_error)); // Record parse error using the common::Result type
                    continue;
                }
            };

            // --- CHANGE HERE ---
            // Store only the file_path associated with the index
            file_path_map.insert(index, file_path.clone());
            // --- END CHANGE ---

            // Clone Arcs and necessary data for the async block
            let processor = Arc::clone(&self.processor);
            let storage_config = self.storage_config.clone();
            // file_path is cloned above, clone components for the future
            let components_clone = components.clone(); // Clone components explicitly for the move block
            let current_file_path = file_path.clone(); // Clone file_path for the move block

            let future = async move {
                // Use the cloned components and file_path inside the future
                let request = ProcessingRequest::new_s3_direct_with_files(
                    &components_clone.city_code,
                    components_clone.year,
                    components_clone.month,
                    components_clone.day,
                    &storage_config.source_bucket.bucket(),
                    vec![current_file_path], // Use the path for this specific task
                );

                // Call the internal processing function
                let outcome = processor.process_bronze_data(request).await;

                // Post-processing: Store if successful and data exists
                let final_outcome = match outcome {
                    Ok((processed_data, target_path)) => {
                        match processed_data.df.clone().count().await {
                            Ok(0) => Ok("skipped-empty-dataframe".to_string()),
                            Ok(_) => {
                                // Store the data
                                match processor
                                    .store_bronze_data(processed_data, &target_path)
                                    .await
                                {
                                    Ok(key) => Ok(key), // Success, return the key
                                    Err(e) => Err(e),   // Error during storage
                                }
                            }
                            Err(e) => Err(common::Error::DataFusion(e)), // Error checking count
                        }
                    }
                    Err(common::Error::DuplicateData(_)) => {
                        Ok("skipped-duplicate-data".to_string())
                    }
                    Err(common::Error::StaleData(_)) => Ok("skipped-stale-data".to_string()),
                    Err(common::Error::SchemaValidation(_)) => {
                        Ok("skipped-schema-validation-error".to_string())
                    }
                    Err(e) => Err(e), // Propagate other processing errors
                };

                (index, final_outcome) // Return index and result
            };

            futures.push(Box::pin(future)); // Pin the future

            // Drain buffer if full
            if futures.len() >= concurrency {
                if let Some((completed_index, outcome)) = futures.next().await {
                    // --- CHANGE HERE ---
                    // Retrieve file_path directly using completed_index
                    if let Some(processed_path) = file_path_map.remove(&completed_index) {
                        results.record_outcome(&processed_path, outcome);
                    } else {
                        // This case should ideally not happen if map insertion is correct
                        eprintln!(
                            "Error: Completed future for unknown index {} (no path found in map)",
                            completed_index
                        );
                        // Optionally record as a general failure without a path?
                        // results.record_outcome("unknown_path", outcome);
                    }
                    // --- END CHANGE ---
                }
            }
        } // End of for loop

        // Process remaining futures
        while let Some((completed_index, outcome)) = futures.next().await {
            // --- CHANGE HERE ---
            // Retrieve file_path directly using completed_index
            if let Some(processed_path) = file_path_map.remove(&completed_index) {
                results.record_outcome(&processed_path, outcome);
            } else {
                eprintln!(
                    "Error: Completed future for unknown index {} (no path found in map)",
                    completed_index
                );
                // Optionally record as a general failure without a path?
                // results.record_outcome("unknown_path", outcome);
            }
            // --- END CHANGE ---
        }

        // Sanity check (optional)
        if !file_path_map.is_empty() {
            eprintln!(
                "Warning: {} file paths remained in the map after processing.",
                file_path_map.len()
            );
            // Log remaining keys/paths if needed
            for (idx, path) in file_path_map {
                eprintln!(" - Remaining task index {}: {}", idx, path);
            }
        }

        println!(
            "Finished processing all source data. Success: {}, Skipped (Known): {}, Skipped (Parse Err): {}, Failed: {}",
            results.success, results.skipped_known, results.skipped_parse_error, results.failed
        );
        if !results.failed_files.is_empty() {
            eprintln!("Failed files:");
            for (file, err) in &results.failed_files {
                eprintln!("  - {}: {}", file, err);
            }
        }

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

#[derive(Debug, Default)]
pub struct ProcessAllResult {
    pub success: u32,
    pub skipped_known: u32, // Duplicate, Stale, Schema Validation, Empty DF etc.
    pub skipped_parse_error: u32, // Could not parse path components
    pub failed: u32,        // Other errors during processing/storage
    pub successful_files: Vec<String>, // Stores target paths/keys of successful operations
    pub failed_files: Vec<(String, String)>, // Stores (source_file_path, error_message)
}

impl ProcessAllResult {
    // Updated record_outcome to handle different error types more clearly
    fn record_outcome(&mut self, file_path: &str, result: Result<String>) {
        match result {
            Ok(status) => {
                // Check for specific skip statuses first
                match status.as_str() {
                    "skipped-duplicate-data"
                    | "skipped-stale-data"
                    | "skipped-schema-validation-error"
                    | "skipped-empty-dataframe"
                    | "skipped-no-source-files" => {
                        self.skipped_known += 1;
                        // Optionally log skipped files here if needed
                        // println!("Skipped (Known): {} -> {}", file_path, status);
                    }
                    // Assume any other Ok(String) is a success (e.g., the bronze key)
                    _ => {
                        self.success += 1;
                        self.successful_files.push(status); // status is the bronze key/path
                    }
                }
            }
            Err(e) => {
                // Check if the error originated from path parsing specifically
                if matches!(e, common::Error::Other(ref msg) if msg.starts_with("Failed to parse path components"))
                {
                    // This case should ideally be caught before calling record_outcome,
                    // but handling here provides robustness.
                    self.skipped_parse_error += 1;
                    eprintln!("Recording path parse error for {}: {}", file_path, e);
                    // Add to failed_files as well for detailed reporting
                    self.failed_files
                        .push((file_path.to_string(), format!("Path Parse Error: {}", e)));
                } else {
                    // Treat other errors as processing/storage failures
                    self.failed += 1;
                    self.failed_files
                        .push((file_path.to_string(), e.to_string()));
                    eprintln!("Failed processing {}: {}", file_path, e);
                }
            }
        }
    }
}
