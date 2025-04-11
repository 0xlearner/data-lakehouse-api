use super::*;
use crate::processor::metadata::{DatasetMetadata, MetadataRegistry, convert};
use arrow::datatypes::Field;
use chrono::Utc;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::DFSchema;
use serde_json::json;

pub struct MetadataHandler {
    metadata_registry: Arc<dyn MetadataRegistry>,
}

impl MetadataHandler {
    pub fn new(metadata_registry: Arc<dyn MetadataRegistry>) -> Self {
        Self { metadata_registry }
    }

    pub async fn create_bronze_metadata(
        &self,
        df: &DataFrame,
        source_path: &str,
        target_path: &str, // This should be the actual target data path (e.g., s3://bronze-bucket/...)
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        file_content: Option<&[u8]>,
        record_ids: &[String], // Assuming record_ids are handled correctly in create_dataset_metadata
    ) -> Result<(DatasetMetadata, String)> {
        // Return tuple: (metadata, metadata_ref)
        // --- Schema Conversion ---
        let df_schema: &DFSchema = df.schema();
        let fields: Vec<Field> = df_schema
            .iter()
            .map(|(_qualifier, field_arc)| {
                let arrow_field: &Field = field_arc.as_ref();
                // Clone the underlying Arrow Field directly
                arrow_field.clone()
            })
            .collect();
        let arrow_schema = ArrowSchema::new(fields);
        // --- End Schema Conversion ---

        // Create the metadata object
        let metadata = convert::create_dataset_metadata(
            &arrow_schema,
            source_path,
            target_path, // Pass the actual target data path
            df,
            "bronze", // Layer
            file_content,
            record_ids, // Pass record IDs if used by create_dataset_metadata
        )
        .await?;

        // Store metadata in the registry
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string(); // Use a more sortable timestamp format

        // This 'dataset_type' should likely be consistent, e.g., "vendors"
        let dataset_type = "vendors"; // Or get this from config/context

        let metadata_ref = self
            .metadata_registry
            .store_metadata(
                metadata.clone(), // Clone metadata for storage
                dataset_type,     // Consistent dataset type name
                city_code,
                year,
                month,
                day,
                &timestamp, // Unique timestamp for this metadata version
            )
            .await?;

        // --- REMOVED marker creation ---
        // let dataset_id = format!( ... );
        // self.metadata_registry.create_marker(...) // REMOVED

        // Return the created metadata and its reference key
        Ok((metadata, metadata_ref))
    }

    /// Update deduplication metrics in custom metadata
    pub async fn update_dedup_metrics(
        &self,
        metadata: &mut DatasetMetadata,
        total_records: usize,
        duplicate_count: usize,
    ) {
        let dedup_metrics = json!({
            "total_records": total_records,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": (duplicate_count as f64 / total_records as f64) * 100.0,
            "dedup_timestamp": Utc::now().to_rfc3339(),
        });

        metadata.custom_metadata.insert(
            "deduplication_metrics".to_string(),
            dedup_metrics.to_string(),
        );

        // Update quality metrics if needed
        metadata.metrics.duplicate_count = duplicate_count as u64;
        metadata.metrics.unique_record_count = (total_records - duplicate_count) as u64;
    }
}
