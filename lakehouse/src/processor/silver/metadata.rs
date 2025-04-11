use super::*;
use crate::processor::metadata::{DatasetMetadata, MetadataRegistry, convert};
use arrow::datatypes::Field;
use chrono::Utc;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use serde_json::json;

pub struct MetadataHandler {
    metadata_registry: Arc<dyn MetadataRegistry>,
}

impl MetadataHandler {
    pub fn new(metadata_registry: Arc<dyn MetadataRegistry>) -> Self {
        Self { metadata_registry }
    }

    // Combined function to create and store metadata for a single silver table
    pub async fn create_silver_metadata(
        &self,
        df: &DataFrame,
        source_path: &str,      // Source Bronze path
        target_s3_path: &str, // Full target Silver S3 path for this specific file
        table_name: &str,       // Name of the derived silver table
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        timestamp: &str, // Timestamp for the batch
    ) -> Result<(DatasetMetadata, String)> { // Returns (metadata, metadata_ref)
        // --- Schema Conversion (Similar to before) ---
         let df_schema: &DFSchema = df.schema();
         let fields: Vec<Field> = df_schema
             .iter()
             .map(|(_qualifier, field_arc)| {
                 let arrow_field: &Field = field_arc.as_ref();
                 arrow_field.clone()
             })
             .collect();
         let arrow_schema = ArrowSchema::new(fields);
        // --- End Schema Conversion ---

        // Create base metadata object using the common converter
        let mut metadata = convert::create_dataset_metadata(
            &arrow_schema,
            source_path,
            target_s3_path, // Use the specific target file path
            df,
            "silver", // Layer
            None,     // No raw content
            &[],      // No specific record IDs
        )
        .await?;

        // Add Silver-specific transformation metadata
        self.add_transformation_metadata(&mut metadata, table_name, source_path);

        // Store the enhanced metadata in the registry
        let metadata_ref = self
            .metadata_registry
            .store_metadata(
                metadata.clone(), // Clone metadata for storage
                table_name,       // Dataset type is the table name
                city_code,
                year,
                month,
                day,
                timestamp, // Use the provided timestamp
            )
            .await?;

        // Return the final metadata object and its reference key
        Ok((metadata, metadata_ref))
    }

    // Helper function remains the same
    fn add_transformation_metadata(
        &self,
        metadata: &mut DatasetMetadata,
        table_name: &str,
        source_bronze_path: &str,
    ) {
        let transformation_details = json!({
            "table_type": table_name,
            "derived_from": source_bronze_path, // Include source path
            "transformation_type": "silver_transformation", // Or more specific name
            "processed_at": Utc::now().to_rfc3339(),
        });
        metadata.custom_metadata.insert(
            "lakehouse_transformation".to_string(),
            transformation_details.to_string(),
        );
        // Also update lineage if appropriate
        metadata.lineage.transformation = "silver_transformation".to_string(); // Example
    }
}
