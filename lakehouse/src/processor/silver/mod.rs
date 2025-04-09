pub mod metadata;
pub mod schema;
pub mod storage;
pub mod transform;
pub mod types;

use crate::processor::silver::types::ProcessedSilverData;
use metadata::MetadataHandler;
use schema::SchemaManager;
use storage::StorageManager;
use transform::DataTransformer;

use crate::processor::metadata::MetadataRegistry;
use crate::storage::S3Manager;
use common::Result;
use datafusion::prelude::*;
use std::sync::Arc;

pub struct SilverProcessor {
    schema_manager: SchemaManager,
    metadata_handler: MetadataHandler,
    pub storage_manager: StorageManager,
    transformer: DataTransformer,
}

impl SilverProcessor {
    pub fn new(
        ctx: &SessionContext,
        s3_manager: Arc<S3Manager>,
        metadata_registry: Arc<dyn MetadataRegistry>,
    ) -> Self {
        Self {
            schema_manager: SchemaManager::new(),
            metadata_handler: MetadataHandler::new(metadata_registry.clone()),
            storage_manager: StorageManager::new(s3_manager.clone()),
            transformer: DataTransformer::new(ctx.clone()),
        }
    }

    pub async fn process_to_silver(
        &self,
        source_path: &str, // Actual path to bronze data (e.g., s3://...)
        // target_paths: &HashMap<String, String>, // REMOVED
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<ProcessedSilverData> {
        // Load bronze data using the provided source path
        // Ensure DataTransformer::load_data handles the s3:// path correctly
        let df = self.transformer.load_data(source_path).await?;

        // Transform data into multiple silver tables
        let derived_tables = self.transformer.transform_data(df).await?;

        // Create metadata for each derived table
        // --- IMPORTANT ---
        // You MAY need to modify MetadataHandler::create_metadata_for_tables
        // to NOT require the `target_paths` argument if it previously did.
        // It should likely only require source_path and partition info now.
        let metadata = self
            .metadata_handler
            .create_metadata_for_tables(
                &derived_tables,
                source_path,
                // target_paths, // REMOVED
                city_code,
                year,
                month,
                day,
            )
            .await?; // Adjust the called function if necessary

        // Create parquet options for each table
        let parquet_options = self
            .schema_manager
            .create_parquet_options_for_tables(&metadata);

        Ok(ProcessedSilverData {
            derived_tables,
            metadata,
            parquet_options,
        })
    }
}
