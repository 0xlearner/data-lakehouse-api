pub mod metadata;
pub mod storage;
pub mod transform;
pub mod types;


use storage::StorageManager;
use metadata::MetadataHandler;
use crate::models::schema::SchemaManager;
use crate::processor::metadata::types::{DatasetMetadata, MetadataMarker}; // Added DatasetMetadata, MetadataMarker
use crate::processor::metadata::MetadataRegistry;
use crate::storage::s3::ObjectStorage; // Added ObjectStorage
use chrono::Utc; // Added Utc
use common::Result;
use datafusion::prelude::*;
use std::collections::HashMap; // Added HashMap
use std::sync::Arc;

use transform::DataTransformer;


pub struct SilverProcessor {
    schema_manager: SchemaManager,
    metadata_handler: MetadataHandler,
    storage_manager: StorageManager,
    transformer: DataTransformer,
    silver_bucket: String,
}

impl SilverProcessor {
    pub fn new(
        ctx: Arc<SessionContext>, // Keep ctx reference if transformer needs it
        silver_storage: Arc<dyn ObjectStorage>, // Added silver_storage parameter
        metadata_registry: Arc<dyn MetadataRegistry>,
    ) -> Self {
        
        let initial_schemas = HashMap::new(); // Ensure HashMap creation
        Self {
            // Pass the required arguments to SchemaManager::new explicitly
            schema_manager: SchemaManager::new(ctx.clone(), initial_schemas),
            metadata_handler: MetadataHandler::new(metadata_registry.clone()),
            storage_manager: StorageManager::new(silver_storage.clone()),
            transformer: DataTransformer::new(ctx.clone()),
            silver_bucket: silver_storage.bucket().to_string(),
        }
    }


    pub async fn process_to_silver(
        &self,
        source_path: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<HashMap<String, (String, String)>> { // Returns table_name -> (metadata_ref, schema_version)
        println!("Silver: Loading data from source: {}", source_path);
        let df = self.transformer.load_data(source_path).await?;

        // Transform data into multiple silver tables
        let derived_tables = self.transformer.transform_data(df).await?;
        println!("Silver: Transformation complete. Derived tables: {:?}", derived_tables.keys());

        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let mut successful_writes = HashMap::new();

        // Generate target paths for each table
        let mut target_paths = HashMap::new();
        for table_name in derived_tables.keys() {
            let target_path = self.generate_silver_target_paths(
                city_code,
                year,
                month,
                day,
                table_name,
            );
            target_paths.insert(table_name.clone(), target_path);
        }

        // Store all silver tables
        self.storage_manager
            .store_silver_tables(
                derived_tables.clone(),
                &target_paths,
                &self.schema_manager, 
            )
            .await?;

        // Write markers for each table
        for (table_name, df) in derived_tables {
            let target_path = target_paths.get(&table_name).unwrap();
            let full_s3_path = format!(
                "s3://{}/{}/{}_{}.parquet",
                self.silver_bucket,
                target_path,
                table_name,
                timestamp
            );
            
            // Create metadata for the table
            let (metadata, metadata_ref) = self.metadata_handler
                .create_silver_metadata(
                    &df,
                    source_path,
                    &full_s3_path,
                    &table_name,
                    city_code,
                    year,
                    month,
                    day,
                    &timestamp,
                )
                .await?;

            // Write marker for the table
            self.storage_manager
                .write_marker(
                    &full_s3_path,
                    &metadata,
                    metadata_ref.clone(),
                    &table_name,
                )
                .await?;

            // Store successful write information
            successful_writes.insert(
                table_name,
                (metadata_ref, metadata.schema.version),
            );
        }

        Ok(successful_writes)
    }

    fn generate_silver_target_paths(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        table_name: &str,
    ) -> String {
        // Construct the partition path including the table name
        format!(
            "{}/city_id={}/year={}/month={:02}/day={:02}",
            table_name, city_code, year, month, day
        )
    }
}
