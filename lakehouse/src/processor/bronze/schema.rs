use super::*;
use crate::processor::metadata::{DatasetMetadata, convert};
use arrow::datatypes::Field;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::config::TableParquetOptions;

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

    pub fn create_parquet_options(&self, metadata: &DatasetMetadata) -> TableParquetOptions {
        let mut options = TableParquetOptions::new();
        for kv in convert::dataset_to_parquet_metadata(metadata) {
            options.key_value_metadata.insert(kv.key, kv.value);
        }
        options
    }
}
