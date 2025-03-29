use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use lazy_static::lazy_static;
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
