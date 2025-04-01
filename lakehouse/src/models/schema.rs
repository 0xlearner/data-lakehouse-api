use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use lazy_static::lazy_static;
use std::sync::Arc;
use crate::processor::LakehouseProcessor;
use common::Result;

// Raw source schemas
pub fn raw_vendors_schema() -> Schema {
    Schema::new(vec![
        Field::new("code", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("details", DataType::Utf8, true),
        Field::new("batch_number", DataType::Int32, false),
        Field::new("reviews", DataType::Utf8, true),
        Field::new("ratings", DataType::Utf8, true),
        Field::new("extraction_started_at", DataType::Int64, false),
        Field::new("extraction_completed_at", DataType::Int64, false),
    ])
}

// Bronze layer schemas
pub fn bronze_vendors_schema() -> Schema {
    let raw_schema = raw_vendors_schema();
    let fields = raw_schema.fields();
    let mut field_vec: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
    field_vec.push(Field::new("ingested_at", DataType::Timestamp(TimeUnit::Millisecond, None), false));
    Schema::new(field_vec)
}

pub enum VendorSchemaVersion {
    Raw,
    Bronze,
}

pub fn get_vendor_schema(version: VendorSchemaVersion) -> &'static Schema {
    match version {
        VendorSchemaVersion::Raw => &RAW_VENDORS_SCHEMA,
        VendorSchemaVersion::Bronze => &BRONZE_VENDORS_SCHEMA,
    }
}

// Lazy-loaded static schemas
lazy_static! {
    static ref RAW_VENDORS_SCHEMA: Schema = raw_vendors_schema();
    static ref BRONZE_VENDORS_SCHEMA: Schema = bronze_vendors_schema();
}

pub struct SchemaValidator {}

impl SchemaValidator {
    pub fn new() -> Self {
        Self {}
    }

    pub fn validate_schema(
        &self,
        inferred_schema: &Schema,
        expected_schema: &Schema,
    ) -> Result<()> {
        // Compare field names and types, being lenient about Utf8 vs Utf8View
        for (expected_field, inferred_field) in expected_schema.fields().iter().zip(inferred_schema.fields().iter()) {
            // Compare names
            if expected_field.name() != inferred_field.name() {
                return Err(common::Error::SchemaMismatch(format!(
                    "Field name mismatch. Expected: {}, Found: {}",
                    expected_field.name(),
                    inferred_field.name()
                )));
            }
            
            // Compare types, treating Utf8 and Utf8View as compatible
            let types_match = match (expected_field.data_type(), inferred_field.data_type()) {
                (DataType::Utf8, DataType::Utf8View) => true,
                (DataType::Utf8View, DataType::Utf8) => true,
                (expected, inferred) => expected == inferred,
            };
            
            if !types_match {
                return Err(common::Error::SchemaMismatch(format!(
                    "Type mismatch for field {}. Expected: {:?}, Found: {:?}",
                    expected_field.name(),
                    expected_field.data_type(),
                    inferred_field.data_type()
                )));
            }
        }
        Ok(())
    }

    pub async fn validate_and_get_schema(
        &self,
        processor: &LakehouseProcessor,
        schema_name: &str,
        file_path: &str,
    ) -> Result<Option<Arc<Schema>>> {
        if let Some(schema) = processor.get_cached_schema(schema_name) {
            let inferred_schema = processor.infer_schema(file_path).await?;
            self.validate_schema(&inferred_schema, &schema)?;
            Ok(Some(schema))
        } else {
            Ok(None)
        }
    }
}