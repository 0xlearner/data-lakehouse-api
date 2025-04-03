use super::*;
use crate::processor::metadata::{DatasetMetadata, MetadataRegistry, convert};
use arrow::datatypes::Field;
use chrono::Utc;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use serde_json::json;

pub struct MetadataHandler {
    metadata_registry: Arc<dyn MetadataRegistry>,
}

impl MetadataHandler {
    pub fn new(metadata_registry: Arc<dyn MetadataRegistry>) -> Self {
        Self { metadata_registry }
    }

    pub async fn create_and_store_metadata(
        &self,
        df: &DataFrame,
        source_path: &str,
        target_path: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        file_content: Option<&[u8]>,
        record_ids: &[String],
    ) -> Result<DatasetMetadata> {
        // Convert DataFrame schema to Arrow schema
        let df_schema = df.schema();
        let fields: Vec<Field> = df_schema
            .fields()
            .iter()
            .map(|field| {
                let field = field.as_ref();
                Field::new(field.name(), field.data_type().clone(), field.is_nullable())
            })
            .collect();
        let arrow_schema = ArrowSchema::new(fields);

        // Use the existing create_dataset_metadata function
        let metadata = convert::create_dataset_metadata(
            &arrow_schema,
            source_path,
            target_path,
            df,
            "bronze",
            file_content,
            record_ids,
        )
        .await?;

        // Store metadata and create marker
        let timestamp = Utc::now().format("%H%M%S").to_string();

        let metadata_ref = self
            .metadata_registry
            .store_metadata(
                metadata.clone(),
                "vendors",
                city_code,
                year,
                month,
                day,
                &timestamp,
            )
            .await?;

        // Create marker file
        let dataset_id = format!(
            "vendors_{}_{}_{:02}_{:02}_{}",
            city_code, year, month, day, timestamp
        );

        self.metadata_registry
            .create_marker(&dataset_id, &metadata_ref, &metadata.schema.version)
            .await?;

        Ok(metadata)
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
