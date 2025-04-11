use crate::processor::metadata::{DatasetMetadata, convert};
use crate::processor::silver::types::SilverTableType;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::Result; // Assuming common::Result is your standard Result
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::common::config::TableParquetOptions;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::prelude::SessionContext; // Import SessionContext
use lazy_static::lazy_static;
use std::collections::HashMap; // For the cache type
use std::sync::{Arc, RwLock};

// ... (keep existing schema definitions: raw_vendors_schema, bronze_vendors_schema, etc.) ...
lazy_static! {
    static ref RAW_VENDORS_SCHEMA: Schema = raw_vendors_schema();
    static ref BRONZE_VENDORS_SCHEMA: Schema = bronze_vendors_schema();
}
// ... (keep get_vendor_schema, VendorSchemaVersion) ...

// Rename SchemaValidator to SchemaManager and add fields
pub struct SchemaManager {
    ctx: Arc<SessionContext>,
    schema_cache: Arc<RwLock<HashMap<&'static str, Arc<Schema>>>>, // Match cache type in LakehouseProcessor
}

impl SchemaManager {
    // Update constructor to accept necessary Arcs
    pub fn new(
        ctx: Arc<SessionContext>,
        initial_schemas: HashMap<&'static str, Arc<Schema>>,
    ) -> Self {
        let schema_cache = Arc::new(RwLock::new(initial_schemas));
        Self { ctx, schema_cache }
    }

    // --- Moved methods from LakehouseProcessor ---

    pub fn get_cached_schema(&self, schema_name: &str) -> Option<Arc<Schema>> {
        // Use the cached reference directly
        self.schema_cache.read().unwrap().get(schema_name).cloned()
    }

    pub fn cache_schema(&self, name: &'static str, schema: Schema) {
        // Use the cached reference directly
        self.schema_cache
            .write()
            .unwrap()
            .insert(name, Arc::new(schema));
    }

    pub async fn infer_schema(&self, file_uri: &str) -> Result<Arc<Schema>> {
        let table_url = ListingTableUrl::parse(file_uri)?;

        let parquet_format = ParquetFormat::default();
        let listing_options =
            ListingOptions::new(Arc::new(parquet_format)).with_file_extension("parquet");

        let config = ListingTableConfig::new(table_url).with_listing_options(listing_options);

        // Use the SessionContext reference from self.ctx
        let config = config.infer_schema(&self.ctx.state()).await?;

        let table = Arc::new(ListingTable::try_new(config)?);
        Ok(table.schema().clone())
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

    // --- Existing validation logic ---

    pub fn validate_schema(
        &self,
        inferred_schema: &Schema,
        expected_schema: &Schema,
    ) -> Result<()> {
        // ... (keep existing validation logic) ...
        for (expected_field, inferred_field) in expected_schema
            .fields()
            .iter()
            .zip(inferred_schema.fields().iter())
        {
            if expected_field.name() != inferred_field.name() {
                return Err(common::Error::SchemaMismatch(format!(
                    "Field name mismatch. Expected: {}, Found: {}",
                    expected_field.name(),
                    inferred_field.name()
                )));
            }

            let types_match = match (expected_field.data_type(), inferred_field.data_type()) {
                (DataType::Utf8, DataType::Utf8View) => true,
                (DataType::Utf8View, DataType::Utf8) => true,
                (expected, inferred) if expected == inferred => true, // Simplified comparison
                (expected, inferred) => {
                    // Add more detailed logging if needed
                    println!(
                        "Type mismatch detail: Expected {:?}, Found {:?}",
                        expected, inferred
                    );
                    false
                }
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
        // Check if number of fields match (optional, but good practice)
        if expected_schema.fields().len() != inferred_schema.fields().len() {
            return Err(common::Error::SchemaMismatch(format!(
                "Field count mismatch. Expected: {}, Found: {}",
                expected_schema.fields().len(),
                inferred_schema.fields().len()
            )));
        }
        Ok(())
    }

    // Update this method to use self.get_cached_schema and self.infer_schema
    pub async fn validate_and_get_schema(
        &self,
        // processor: &LakehouseProcessor, // No longer needed as argument
        schema_name: &'static str, // Use &'static str to match cache key type
        file_path: &str,
    ) -> Result<Option<Arc<Schema>>> {
        if let Some(schema) = self.get_cached_schema(schema_name) {
            let inferred_schema = self.infer_schema(file_path).await?;
            self.validate_schema(&inferred_schema, &schema)?;
            Ok(Some(schema))
        } else {
            // Maybe infer and cache if not found? Or just return None?
            // Current logic returns None if not cached.
            // Let's infer, cache, and return the newly inferred schema if validation against a known static schema passes
            // Assuming you have a way to get the *expected* static schema by name
            let expected_schema = match schema_name {
                "bronze_vendors" => get_vendor_schema(VendorSchemaVersion::Bronze), // Example lookup
                // Add other known schema names here
                _ => return Ok(None), // Or return an error if the name is unknown
            };

            let inferred_schema = self.infer_schema(file_path).await?;
            self.validate_schema(&inferred_schema, expected_schema)?;
            // Validation passed, cache it
            // Note: inferred_schema is Arc<Schema>, but cache_schema expects Schema.
            // We need to decide if we cache the exact inferred schema or the expected static one.
            // Caching the expected static one ensures consistency.
            let schema_to_cache = expected_schema.clone(); // Clone the static Schema
            self.cache_schema(schema_name, schema_to_cache); // Cache the *expected* schema

            Ok(self.get_cached_schema(schema_name)) // Return the newly cached Arc<Schema>
        }
    }

    pub fn create_parquet_options(&self, metadata: &DatasetMetadata) -> TableParquetOptions {
        let mut options = TableParquetOptions::new();
        for kv in convert::dataset_to_parquet_metadata(metadata) {
            options.key_value_metadata.insert(kv.key, kv.value);
        }
        options
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

// Helper function to get schema definitions (if needed outside lazy_static)
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

pub fn bronze_vendors_schema() -> Schema {
    let raw_schema = raw_vendors_schema();
    let fields = raw_schema.fields();
    let mut field_vec: Vec<Field> = fields.iter().map(|f| f.as_ref().clone()).collect();
    field_vec.push(Field::new(
        "ingested_at",
        DataType::Timestamp(TimeUnit::Millisecond, None), // Ensure this matches inference
        false,
    ));
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
