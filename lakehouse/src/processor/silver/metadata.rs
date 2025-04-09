use super::*;
use crate::processor::metadata::{DatasetMetadata, MetadataRegistry, convert};
use arrow::datatypes::Field;
use chrono::Utc;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::DFSchema;
use datafusion::dataframe::DataFrame;
use serde_json::json;
use std::collections::HashMap;

pub struct MetadataHandler {
    metadata_registry: Arc<dyn MetadataRegistry>,
}

impl MetadataHandler {
    pub fn new(metadata_registry: Arc<dyn MetadataRegistry>) -> Self {
        Self { metadata_registry }
    }

    pub async fn create_metadata_for_tables(
        &self,
        derived_tables: &HashMap<String, DataFrame>,
        source_path: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<HashMap<String, DatasetMetadata>> {
        let mut metadata_map = HashMap::new();
        let timestamp = Utc::now().format("%Y%m%d_%H%M%S").to_string();

        for (table_name, df) in derived_tables {
            let relative_target_prefix = format!(
                "table={}/city_id={}/year={}/month={:02}/day={:02}",
                table_name, city_code, year, month, day
            );

            // --- CORRECTED SCHEMA CONVERSION AGAIN ---
            let df_schema: &DFSchema = df.schema();

            // Iterate over the schema. The iterator yields tuples: (qualifier, &Arc<Field>)
            let fields: Vec<Field> = df_schema
                .iter()
                .map(|(_qualifier, field_arc)| {
                    // Destructure the tuple, ignore qualifier if not needed
                    // field_arc is &Arc<Field>. Deref to get &Field.
                    let arrow_field: &Field = field_arc.as_ref(); // or simply &**field_arc

                    // Create a *new* Arrow Field. We need to clone properties.
                    // Note: Using arrow_field.clone() might be simpler if no modifications needed.
                    Field::new(
                        arrow_field.name(),              // Get name (&str) from Arrow Field
                        arrow_field.data_type().clone(), // Get data type (&ArrowDataType) and clone
                        arrow_field.is_nullable(),       // Get nullability (bool)
                    )
                    // Simpler alternative if no changes needed:
                    // field_arc.as_ref().clone()
                })
                .collect();
            let arrow_schema = ArrowSchema::new(fields);
            // --- END CORRECTION ---

            let metadata = convert::create_dataset_metadata(
                &arrow_schema,
                source_path,
                &relative_target_prefix,
                df,
                "silver",
                None,
                &[],
            )
            .await?;

            let metadata_ref = self
                .metadata_registry
                .store_metadata(
                    metadata.clone(),
                    table_name,
                    city_code,
                    year,
                    month,
                    day,
                    &timestamp,
                )
                .await?;

            let dataset_id = format!(
                "{}_{}_{}_{:02}_{:02}_{}",
                table_name, city_code, year, month, day, timestamp
            );

            self.metadata_registry
                .create_marker(&dataset_id, &metadata_ref, &metadata.schema.version)
                .await?;

            let mut enhanced_metadata = metadata.clone();
            self.add_transformation_metadata(&mut enhanced_metadata, table_name);

            metadata_map.insert(table_name.clone(), enhanced_metadata);
        }

        Ok(metadata_map)
    }

    fn add_transformation_metadata(&self, metadata: &mut DatasetMetadata, table_name: &str) {
        let transformation_details = json!({
            "table_type": table_name,
            "derived_from": "bronze_vendors",
            "transformation_type": "silver_transformation",
            "processed_at": Utc::now().to_rfc3339(),
        });
        metadata.custom_metadata.insert(
            "lakehouse_transformation".to_string(),
            transformation_details.to_string(),
        );
    }
}
