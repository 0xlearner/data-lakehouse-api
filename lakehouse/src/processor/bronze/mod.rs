mod dedup;
pub mod metadata;
pub mod transform;
pub mod types;
pub mod validation;
pub mod storage; // Add this line

use crate::models::schema::SchemaManager;
use dedup::DedupHandler;
use metadata::MetadataHandler;
use transform::DataTransformer;
pub use types::*;
use validation::DataValidator;
use storage::StorageManager;

use super::metadata::types::{DatasetMetadata, MetadataMarker};
use crate::processor::metadata::MetadataRegistry;
use crate::processor::udf;
use crate::storage::s3::ObjectStorage;
use chrono::Utc;
use common::Result;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use parquet::file::properties::WriterProperties;
use parquet::format::KeyValue;
use std::collections::HashMap;
use std::sync::Arc;

pub struct BronzeProcessor {
    validator: DataValidator,
    schema_manager: SchemaManager,
    metadata_handler: MetadataHandler,
    transformer: DataTransformer,
    dedup_handler: DedupHandler,
    storage_manager: StorageManager,
}

impl BronzeProcessor {
    pub fn new(
        ctx: Arc<SessionContext>,
        bronze_storage: Arc<dyn ObjectStorage>,
        metadata_registry: Arc<dyn MetadataRegistry>,
    ) -> Self {
        let initial_schemas = HashMap::new();
        udf::register_udfs(&ctx).expect("Failed to register UDFs");
        Self {
            validator: DataValidator::new(),
            schema_manager: SchemaManager::new(ctx.clone(), initial_schemas),
            metadata_handler: MetadataHandler::new(metadata_registry.clone()),
            transformer: DataTransformer::new(ctx.clone()),
            dedup_handler: DedupHandler::new(metadata_registry),
            storage_manager: StorageManager::new(bronze_storage),
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
    ) -> Result<DatasetMetadata> {
        println!("Bronze: Loading data from source: {}", source_path);
        let df = self.transformer.load_data(source_path).await?;
        self.validator.validate_data(&df).await?;

        let record_ids = self.validator.extract_record_ids(&df).await?;

        println!("Bronze: Processing duplicates...");
        let (dedup_df, dedup_metrics) = self
            .dedup_handler
            .process_duplicates(df, source_path, file_content, &record_ids)
            .await?;

        if dedup_df.clone().count().await? == 0 {
            println!("Bronze: All records identified as duplicates. Skipping write.");
            return Err(common::Error::DuplicateData(
                "All records are duplicates".to_string(),
            ));
        }
        println!(
            "Bronze: Deduplication complete. {} records remaining.",
            dedup_df.clone().count().await?
        );

        println!("Bronze: Transforming data...");
        let processed_df = self.transformer.transform_data(dedup_df).await?;
        println!("Bronze: Transformation complete.");

        println!("Bronze: Creating metadata object...");
        let (mut metadata, metadata_ref) = self
            .metadata_handler
            .create_bronze_metadata(
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
        println!("Bronze: Metadata object created.");

        let batches: Vec<RecordBatch> = processed_df.collect().await?;
        if batches.is_empty() {
            println!("Bronze: No data to write after transformation.");
            return Ok(metadata);
        }

        let table_options = self.schema_manager.create_parquet_options(&metadata);
        let writer_props = WriterProperties::builder()
            .set_key_value_metadata(Some(
                table_options
                    .key_value_metadata
                    .into_iter()
                    .map(|(key, value)| KeyValue { key, value })
                    .collect::<Vec<KeyValue>>(),
            ))
            .build();

        self.storage_manager
            .write_dataset(
                batches,
                target_path,
                &metadata,
                metadata_ref,
                writer_props,
            )
            .await?;

        self.metadata_handler
            .update_dedup_metrics(
                &mut metadata,
                dedup_metrics.total_records,
                dedup_metrics.duplicate_count,
            )
            .await;

        Ok(metadata)
    }
}
