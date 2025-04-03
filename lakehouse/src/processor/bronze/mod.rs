mod dedup;
pub mod metadata;
pub mod schema;
pub mod storage;
pub mod transform;
pub mod types;
pub mod validation;

use dedup::DedupHandler;
use metadata::MetadataHandler;
use schema::SchemaManager;
use storage::StorageManager;
use transform::DataTransformer;
pub use types::*;
use validation::DataValidator;

use crate::processor::metadata::MetadataRegistry;
use crate::processor::udf;
use crate::storage::S3Manager;
use common::Result;
use datafusion::prelude::*;
use std::sync::Arc;

pub struct BronzeProcessor {
    validator: DataValidator,
    schema_manager: SchemaManager,
    metadata_handler: MetadataHandler,
    pub storage_manager: StorageManager,
    transformer: DataTransformer,
    dedup_handler: DedupHandler,
}

impl BronzeProcessor {
    pub fn new(
        ctx: &SessionContext,
        s3_manager: Arc<S3Manager>,
        metadata_registry: Arc<dyn MetadataRegistry>,
    ) -> Self {
        udf::register_udfs(ctx).expect("Failed to register UDFs");
        Self {
            validator: DataValidator::new(),
            schema_manager: SchemaManager::new(),
            metadata_handler: MetadataHandler::new(metadata_registry.clone()),
            storage_manager: StorageManager::new(s3_manager.clone()),
            transformer: DataTransformer::new(ctx.clone()),
            dedup_handler: DedupHandler::new(metadata_registry),
        }
    }

    pub async fn process_to_bronze(
        &self,
        source_path: &str,
        target_path: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        file_content: Option<&[u8]>,
    ) -> Result<ProcessedData> {
        // Load and validate data
        let df = self.transformer.load_data(source_path).await?;
        self.validator.validate_data(&df).await?;

        // Extract record IDs for deduplication
        let record_ids = self.validator.extract_record_ids(&df).await?;

        // Process duplicates using the dedup handler
        let (df, dedup_metrics) = self
            .dedup_handler
            .process_duplicates(df, source_path, file_content, &record_ids)
            .await?;

        // Check if we have any data left after deduplication
        if df.clone().count().await? == 0 {
            // Return a special error that will be caught and handled appropriately
            return Err(common::Error::DuplicateData(
                "All records are duplicates".to_string(),
            ));
        }

        // Transform data
        let processed_df = self.transformer.transform_data(df).await?;

        // Create and store metadata
        let mut metadata = self
            .metadata_handler
            .create_and_store_metadata(
                &processed_df,
                source_path,
                target_path,
                city_code,
                year,
                month,
                day,
                file_content,
                &record_ids,
            )
            .await?;

        // Update metadata with dedup metrics
        self.metadata_handler
            .update_dedup_metrics(
                &mut metadata,
                dedup_metrics.total_records,
                dedup_metrics.duplicate_count,
            )
            .await;
        let parquet_options = self.schema_manager.create_parquet_options(&metadata);

        Ok(ProcessedData {
            df: processed_df,
            metadata,
            parquet_options,
        })
    }
}
