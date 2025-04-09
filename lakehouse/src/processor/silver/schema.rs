use super::*;
use crate::processor::metadata::{DatasetMetadata, convert};
use crate::processor::silver::types::SilverTableType;
use arrow::datatypes::Field;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::config::TableParquetOptions;
use std::collections::HashMap;

pub struct SchemaManager;

impl SchemaManager {
    pub fn new() -> Self {
        Self
    }

    pub fn get_schema(&self, df: &DataFrame) -> Result<ArrowSchema> {
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
        Ok(arrow_schema)
    }

    pub fn create_parquet_options_for_tables(
        &self,
        metadata_map: &HashMap<String, DatasetMetadata>,
    ) -> HashMap<String, TableParquetOptions> {
        let mut options_map = HashMap::new();

        for (table_name, metadata) in metadata_map {
            let mut options = TableParquetOptions::new();

            // Add standard metadata
            for kv in convert::dataset_to_parquet_metadata(metadata) {
                options.key_value_metadata.insert(kv.key, kv.value);
            }

            // Add table-specific metadata
            options
                .key_value_metadata
                .insert("silver_table_type".to_string(), Some(table_name.clone()));

            options_map.insert(table_name.clone(), options);
        }

        options_map
    }

    pub fn get_required_fields(&self, table_type: &SilverTableType) -> Vec<&'static str> {
        match table_type {
            SilverTableType::Menus => vec!["id", "name", "popular_product_id"],
            SilverTableType::MenuCategories => vec!["id", "name", "menu_id"],
            SilverTableType::Products => vec!["id", "name", "category_id"],
            SilverTableType::ProductVariations => vec!["id", "product_id", "price"],
            SilverTableType::VendorDetails => vec!["code", "name", "address"],
            SilverTableType::Reviews => vec!["id", "rating", "comment"],
            SilverTableType::RatingsDistribution => vec!["total_ratings", "average_rating"],
            // Add other table types as needed
            _ => vec![], // Basic tables might only need the standard metadata fields
        }
    }
}
