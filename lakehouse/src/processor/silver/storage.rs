use crate::processor::silver::types::ProcessedSilverData;
use crate::storage::S3Manager;
use crate::storage::s3::ObjectStorage;
use chrono::Utc;
use common::{Error, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

pub struct StorageManager {}

impl StorageManager {
    pub fn new(_s3_manager: Arc<S3Manager>) -> Self {
        Self {}
    }

    pub async fn store_silver_tables(
        &self,
        processed_data: ProcessedSilverData,
        storage: &dyn ObjectStorage,
        target_paths: &HashMap<String, String>, // Still contains RELATIVE directory paths
    ) -> Result<()> {
        let bucket_name = storage.bucket();
        let base_s3_uri = format!("s3://{}/", bucket_name);

        for (table_name, df) in processed_data.derived_tables {
            // 1. Get the base relative PARTITION path (directory) from the map
            let relative_partition_path = target_paths.get(&table_name).ok_or_else(|| {
                Error::Storage(format!("No target path found for table {}", table_name))
            })?;
            let cleaned_relative_partition_path = relative_partition_path.trim_matches('/');

            // 2. Construct the relative DIRECTORY key prefix within the bucket
            let final_relative_dir_key = if cleaned_relative_partition_path.is_empty() {
                format!("table={}", table_name)
            } else {
                format!("table={}/{}", table_name, cleaned_relative_partition_path)
            };
            let final_relative_dir_key = final_relative_dir_key.trim_matches('/');

            // 3. Generate the desired FILENAME part
            let timestamp = Utc::now().format("%Y%m%d%H%M%S"); // Use a consistent timestamp format
            let filename = format!("{}_{}.parquet", table_name, timestamp);

            // 4. Construct the FULL S3 URI including the desired FILENAME
            let target_s3_uri_with_filename = format!(
                "{}/{}/{}", // s3://<bucket>/<relative_dir_key>/<filename>
                base_s3_uri.trim_end_matches('/'),
                final_relative_dir_key,
                filename
            );

            // --- NEW: Repartition the DataFrame to 1 partition ---
            // This forces a single output file but impacts write parallelism.
            println!(
                "Repartitioning DataFrame for table '{}' to 1 partition before writing...",
                table_name
            );
            let df_repartitioned =
                df.repartition(Partitioning::RoundRobinBatch(1))
                    .map_err(|e| {
                        Error::Other(format!(
                            "Failed to repartition DataFrame for table '{}': {}",
                            table_name, e
                        ))
                    })?;
            // --- End Repartition ---

            // Validate schema (on the repartitioned DF, schema should be the same)
            self.validate_schema(&df_repartitioned, &table_name).await?;

            // Get parquet options
            let parquet_options =
                processed_data
                    .parquet_options
                    .get(&table_name)
                    .ok_or_else(|| {
                        Error::Storage(format!("No parquet options found for table {}", table_name))
                    })?;

            println!(
                "Attempting to write single silver file for table '{}' to S3 URI: {}",
                table_name,
                target_s3_uri_with_filename // Log the FULL path including filename
            );

            // 5. Write data using the *full S3 URI with filename* and the *repartitioned DF*
            df_repartitioned // Use the repartitioned DataFrame
                .write_parquet(
                    &target_s3_uri_with_filename, // Use the full path with filename
                    DataFrameWriteOptions::new(), // Options usually don't affect filename directly
                    Some(parquet_options.clone()),
                )
                .await
                .map_err(|e| {
                    Error::Storage(format!(
                        "Failed to write single parquet file for table '{}' to {}: {}",
                        table_name, target_s3_uri_with_filename, e
                    ))
                })?;

            // 6. Construct the relative KEY for verification (including filename)
            let final_relative_file_key = format!("{}/{}", final_relative_dir_key, filename);

            // 7. Verify using the *exact relative file key*
            println!(
                "Attempting verification with exact relative file key: {}",
                final_relative_file_key
            );
            // If keeping verification, modify it to check for the exact file
            self.verify_exact_file_exists(storage, &final_relative_file_key)
                .await?;

            println!(
                "Successfully wrote and verified single silver file for table '{}' to S3 URI: {}",
                table_name, target_s3_uri_with_filename
            );
        } // End loop

        Ok(())
    }

    // Renamed and modified verification to check for an EXACT file key
    async fn verify_exact_file_exists(
        &self,
        storage: &dyn ObjectStorage,
        target_exact_key: &str, // Should be the relative key like "table=.../file.parquet"
    ) -> Result<()> {
        // Key should be clean, but trim just in case
        let clean_key = target_exact_key.trim_matches('/');
        if clean_key.is_empty() {
            return Err(Error::Storage(
                "Cannot verify an empty exact file key".to_string(),
            ));
        }

        println!("Verifying existence of exact file key: {}", clean_key);

        for attempt in 1..=3 {
            // Keep retries for eventual consistency on GET/HEAD too
            // Use check_file_exists which typically uses HEAD object
            match storage.check_file_exists(clean_key).await {
                Ok(true) => {
                    println!(
                        "Verification successful: Found exact file key {}",
                        clean_key
                    );
                    return Ok(());
                }
                Ok(false) => {
                    println!(
                        "Verification attempt {}: File key '{}' not found yet. Retrying...",
                        attempt, clean_key
                    );
                }
                Err(e) => {
                    println!(
                        "Verification attempt {}: Error checking file key '{}': {}. Retrying...",
                        attempt, clean_key, e
                    );
                    // Optionally: return Err(e); // Fail fast on storage errors
                }
            }

            if attempt < 3 {
                let sleep_duration = Duration::from_secs(2u64.pow(attempt)); // 2s, 4s, 8s
                println!(
                    "Sleeping for {:?} before next verification attempt...",
                    sleep_duration
                );
                sleep(sleep_duration).await;
            }
        }

        Err(Error::Storage(format!(
            "Exact file key not found after writing ({} attempts): {}",
            3, clean_key
        )))
    }

    async fn validate_schema(&self, df: &DataFrame, table_name: &str) -> Result<()> {
        let schema = df.schema();

        // Check required fields for all silver tables
        let required_fields = vec!["bronze_row_id", "silver_ingestion_at", "partition_date"];

        for field in required_fields {
            if !schema.fields().iter().any(|f| f.name() == field) {
                return Err(Error::SchemaValidation(
                    format!(
                        "Missing required field {} in silver table {}",
                        field, table_name
                    )
                    .into(),
                ));
            }
        }

        Ok(())
    }
}
