use super::*;
use crate::storage::s3::ObjectStorage;
use crate::processor::metadata::convert;
use chrono::Utc;
use common::{Error, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use std::path::Path;
use url::Url;

pub struct StorageManager {
    storage: Arc<dyn ObjectStorage>,
}

impl StorageManager {
    pub fn new(storage: Arc<dyn ObjectStorage>) -> Self {
        Self { storage }
    }
    pub async fn store_silver_tables(
        &self,
        derived_tables: HashMap<String, DataFrame>,
        target_paths: &HashMap<String, String>,
        schema_manager: &SchemaManager,  // Add SchemaManager parameter
    ) -> Result<()> {
        let bucket_name = self.storage.bucket();
        let base_s3_uri = format!("s3://{}/", bucket_name);

        // Create metadata map for all tables first
        let mut metadata_map = HashMap::new();
        for (table_name, df) in &derived_tables {
            let target_path = target_paths.get(table_name).ok_or_else(|| {
                Error::Storage(format!("No target path found for table {}", table_name))
            })?;

            // Create metadata for each table
            let metadata = convert::create_dataset_metadata(
                &schema_manager.get_schema(df)?,
                "", // source path will be added later
                target_path,
                df,
                "silver",
                None,
                &[],
            ).await?;

            metadata_map.insert(table_name.clone(), metadata);
        }

        // Get parquet options for all tables at once
        let parquet_options_map = schema_manager.create_parquet_options_for_tables(&metadata_map);

        // Process each table with its corresponding options
        for (table_name, df) in derived_tables {
            let df_repartitioned = df.repartition(Partitioning::RoundRobinBatch(1))?;

            self.validate_schema(&df_repartitioned, &table_name).await?;
            
            // Get the specific options for this table
            let parquet_options = parquet_options_map.get(&table_name).ok_or_else(|| {
                Error::Storage(format!(
                    "No parquet options found for table '{}'",
                    table_name
                ))
            })?;

            // Construct the target path
            let relative_partition_path = target_paths.get(&table_name).ok_or_else(|| {
                Error::Storage(format!("No target path found for table {}", table_name))
            })?;

            let target_s3_uri = format!(
                "{}/{}/{}_{}.parquet",
                base_s3_uri.trim_end_matches('/'),
                relative_partition_path.trim_matches('/'),
                table_name,
                Utc::now().format("%Y%m%d%H%M%S")
            );

            println!(
                "Writing silver table '{}' to: {}",
                table_name,
                target_s3_uri
            );

            // Write the data with table-specific options
            df_repartitioned
                .write_parquet(
                    &target_s3_uri,
                    DataFrameWriteOptions::new(),
                    Some(parquet_options.clone()),
                )
                .await?;

        // Extract the exact file key from the S3 URI for verification
        let exact_key = self.object_key_from_s3_path(&target_s3_uri)?;
        
        // Verify the file exists after writing
        println!("Verifying file was written successfully...");
        self.verify_exact_file_exists(&*self.storage, &exact_key).await?;
        println!("File verification successful for table {}", table_name);
        }

        Ok(())
    }

    pub async fn write_marker(
        &self,
        target_path: &str,
        metadata: &DatasetMetadata,
        metadata_ref: String,
        table_name: &str,
    ) -> Result<()> {
        let marker = MetadataMarker {
            dataset_id: metadata.dataset_id.clone(),
            metadata_ref,
            created_at: Utc::now(),
            schema_version: metadata.schema.version.clone(),
            table_name: Some(table_name.to_string()),
        };
        let marker_json = serde_json::to_vec_pretty(&marker)?;
    
        // Ensure target_path has s3:// prefix
        let full_s3_path = if target_path.starts_with("s3://") {
            target_path.to_string()
        } else {
            format!("s3://{}/{}", self.storage.bucket(), target_path)
        };
    
        let object_key = self.object_key_from_s3_path(&full_s3_path)?;
        let target_dir = Path::new(&object_key)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "".to_string());

        let marker_key = if target_dir.is_empty() {
            format!("table={}/{}", table_name, "_SUCCESS")
        } else {
            format!("table={}/{}/{}", table_name, target_dir, "_SUCCESS")
        };

        println!("Silver: Writing marker to key: {}", marker_key);
        self.storage.put_object(&marker_key, &marker_json).await?;
        println!("Silver: Marker file written successfully for table {}", table_name);

        Ok(())
    }

    fn object_key_from_s3_path(&self, s3_path: &str) -> Result<String> {
        // Add validation for s3:// prefix
        if !s3_path.starts_with("s3://") {
            return Err(Error::InvalidInput(format!(
                "Path '{}' must start with 's3://'",
                s3_path
            )));
        }
    
        let parsed_url = Url::parse(s3_path).map_err(|e| {
            Error::InvalidInput(format!(
                "Failed to parse S3 path '{}': {}",
                s3_path, e
            ))
        })?;
    
        if parsed_url.scheme() != "s3" {
            return Err(Error::InvalidInput(format!(
                "Path '{}' is not an S3 path (expected scheme 's3')",
                s3_path
            )));
        }
    
        let key = parsed_url.path().trim_start_matches('/').to_string();
    
        if key.is_empty() {
            return Err(Error::InvalidInput(format!(
                "S3 path '{}' results in an empty object key",
                s3_path
            )));
        }
    
        Ok(key)
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
