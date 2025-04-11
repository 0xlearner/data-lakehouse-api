use crate::processor::config::StorageConfig;
use crate::processor::{LakehouseProcessor, ProcessingRequest};
use crate::services::utils::parse_s3_path_components;
use crate::processor::metadata::DatasetMetadata; // Added for type hint
use crate::storage::{S3Config, S3Manager};
use common::Result;
use common::config::Settings;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct LakehouseService {
    processor: Arc<LakehouseProcessor>,
    // storage_config: StorageConfig,
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
        let mut file_path_map: HashMap<usize, String> = HashMap::new(); // Keep track of file paths by index

        for (index, file_path) in source_files.into_iter().enumerate() {
            let components = match parse_s3_path_components(&file_path) {
                Ok(comp) => comp,
                Err(e) => {
                    // Record parse error directly as a skip
                    let reason = format!("Failed to parse path components from '{}': {}", file_path, e);
                    results.record_outcome(&file_path, "Parse", Ok(ProcessingOutcome::Skipped(reason)));
                    continue;
                }
            };

            file_path_map.insert(index, file_path.clone()); // Store path before moving

            let processor = Arc::clone(&self.processor);
            // storage_config is likely not needed here anymore
            let components_clone = components.clone(); // Clone components for the async block
            let current_file_path = file_path.clone(); // Clone file_path for the async block

            let future = async move {
                let request = ProcessingRequest::new_s3_direct_with_files(
                    &components_clone.city_code,
                    components_clone.year,
                    components_clone.month,
                    components_clone.day,
                    // Pass source bucket name directly if needed by request
                    &processor.s3_manager.config.source_bucket, // Assuming processor has s3_manager access
                    vec![current_file_path.clone()], // Pass the specific file path
                );

                // Define stages for error reporting
                let mut current_stage = "Bronze";
                let outcome: Result<ProcessingOutcome> = async {
                    // Process bronze first
                    // Returns Result<(DatasetMetadata, String)>
                    let (bronze_metadata, bronze_path): (DatasetMetadata, String) = processor.process_bronze_data(request).await?;

                    // Check processing stats for row count instead of statistics
                    if bronze_metadata.metrics.record_count == 0 {
                        println!("Skipping Silver for {}: Bronze resulted in 0 rows.", current_file_path);
                        // If Bronze succeeded but was empty, we count it as a known skip for Silver stage.
                        return Ok(ProcessingOutcome::Skipped("skipped-empty-bronze-output".to_string()));
                    }

                    // --- Bronze storage is handled internally by process_to_bronze --- REMOVED store_bronze_data

                    // Update stage for error reporting before calling silver
                    current_stage = "Silver";

                    // Process silver - Returns Result<()>
                    processor
                        .process_silver_data(
                            &bronze_path, // Pass the bronze output path
                            &components_clone.city_code,
                            components_clone.year,
                            components_clone.month,
                            components_clone.day,
                        )
                        .await?; // Propagate error if silver fails

                    // --- Silver storage is handled internally by process_to_silver --- REMOVED store_silver_data

                    // If silver processing completes without error
                    Ok(ProcessingOutcome::SilverSuccess)
                }
                .await;

                // Return index, stage, and outcome
                (index, current_stage, outcome)
            };

            futures.push(Box::pin(future));

            // Process futures as they complete to maintain concurrency level
            if futures.len() >= concurrency {
                if let Some((completed_index, stage, outcome)) = futures.next().await {
                    if let Some(processed_path) = file_path_map.remove(&completed_index) {
                        results.record_outcome(&processed_path, stage, outcome);
                    } else {
                         eprintln!("Error: Could not find file path for completed index {}", completed_index);
                    }
                }
            }
        }

        // Process any remaining futures
        while let Some((completed_index, stage, outcome)) = futures.next().await {
             if let Some(processed_path) = file_path_map.remove(&completed_index) {
                 results.record_outcome(&processed_path, stage, outcome);
             } else {
                  eprintln!("Error: Could not find file path for completed index {}", completed_index);
             }
        }

        // Updated print statement with new result fields
        println!(
            "Finished processing. Silver Success: {}, Skipped Known: {}, Failed Bronze: {}, Failed Silver: {}, Parse Errors: {}",
            results.success_silver,
            results.skipped_known,
            results.failed_bronze,
            results.failed_silver,
            results.skipped_parse_error
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
// Updated ProcessingOutcome based on internal storage
#[derive(Debug)]
pub enum ProcessingOutcome {
    Skipped(String), // Reason for skipping (e.g., duplicate, parse error)
    // BronzeSuccess might not be needed if SilverSuccess is the final goal
    // BronzeSuccess(String), // Bronze target path (metadata handled internally)
    SilverSuccess, // Silver processing completed successfully
}

#[derive(Debug, Default)]
// Updated ProcessAllResult for clearer stage tracking
pub struct ProcessAllResult {
    pub success_silver: u32, // Count files reaching silver successfully
    pub skipped_known: u32, // Skips due to known reasons (duplicate, empty, etc.)
    pub skipped_parse_error: u32, // Skips due to path parsing errors
    pub failed_bronze: u32, // Failures during bronze processing
    pub failed_silver: u32, // Failures during silver processing (after successful bronze)
    pub failed_files: Vec<(String, String, String)>, // (file_path, stage, error)
}

impl ProcessAllResult {
    fn record_outcome(&mut self, file_path: &str, stage: &str, result: Result<ProcessingOutcome>) {
        match result {
            Ok(outcome) => match outcome {
                ProcessingOutcome::Skipped(reason) => {
                    // Check if skip is due to parse error specifically
                    if reason.starts_with("Failed to parse path components") {
                        self.skipped_parse_error += 1;
                        // Clone the reason before moving it
                        self.failed_files.push((
                            file_path.to_string(),
                            "Parse".to_string(),
                            reason.clone(), // Clone here
                        ));
                    } else {
                        self.skipped_known += 1;
                    }
                    println!("Skipped {}: {}", file_path, reason); // Can use reason here now
                }
                ProcessingOutcome::SilverSuccess => {
                    self.success_silver += 1;
                    println!("Silver Success: {}", file_path);
                }
            },
            Err(e) => {
                // Record failure based on the stage passed in
                if stage == "Bronze" {
                    self.failed_bronze += 1;
                } else if stage == "Silver" {
                    self.failed_silver += 1;
                } else {
                    // Handle unexpected stage?
                    eprintln!("Unknown failure stage '{}' for file {}", stage, file_path);
                }
                self.failed_files.push((file_path.to_string(), stage.to_string(), e.to_string()));
                eprintln!("Failed {} at stage {}: {}", file_path, stage, e);
            }
        }
    }
}

