pub mod metadata;
mod bronze;
mod config;
mod udf;


pub use metadata::{ParquetMetadata, MetadataStore, S3MetadataStore};
pub use bronze::{BronzeProcessor, ProcessedData};
pub use config::StorageConfig;
use crate::storage::S3Config;
use crate::storage::S3Manager;
use crate::storage::s3::S3Storage;
use crate::schema::{
    VendorSchemaVersion, 
    get_vendor_schema, 
    raw_vendors_schema, 
    bronze_vendors_schema
};
pub use udf::register_udfs;

use common::Result;
use std::collections::HashMap;
use datafusion::datasource::{
    file_format::parquet::ParquetFormat,
    listing::{ListingOptions, ListingTable, ListingTableConfig},
    
};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::TableProvider;
use arrow::datatypes::Schema;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use std::sync::RwLock;
use datafusion::prelude::ParquetReadOptions;
use arrow::datatypes::DataType;


/// Main processor interface that coordinates different processing layers
pub struct LakehouseProcessor {
    pub ctx: SessionContext,
    schema_cache: RwLock<HashMap<&'static str, Arc<Schema>>>,
    pub s3_manager: Arc<S3Manager>,
    bronze: BronzeProcessor,
    metadata: S3MetadataStore,
}

impl LakehouseProcessor {
    pub async fn new(config: &S3Config) -> Result<Self> {
        // Initialize schema cache
        let mut schema_cache = HashMap::new();

        schema_cache.insert("raw_vendors", Arc::new(get_vendor_schema(VendorSchemaVersion::Raw)));
        schema_cache.insert("bronze_vendors", Arc::new(get_vendor_schema(VendorSchemaVersion::Bronze)));

        let ctx = SessionContext::new();
        let s3_manager = Arc::new(S3Manager::new(config.clone()));
        
        
        register_udfs(&ctx)?;

        let bronze = BronzeProcessor::new(&ctx, s3_manager.clone());
        
        Ok(Self {
            ctx,
            s3_manager: s3_manager.clone(),
            bronze,
            schema_cache: RwLock::new({
                let mut schema_cache = HashMap::new();
                schema_cache.insert("raw_vendors", Arc::new(raw_vendors_schema()));
                schema_cache.insert("bronze_vendors", Arc::new(bronze_vendors_schema()));
                schema_cache
            }),
            metadata: S3MetadataStore::new(Arc::new(
                S3Storage::new(s3_manager.clone(), &config.metadata_bucket).await?
            )),
        })
    }

    // Simplified processing methods
    pub async fn process_bronze_data(&self, source_path: &str) -> Result<ProcessedData> {
        self.bronze.process_data(source_path).await
    }

    // Store processed data in the bronze layer
    pub async fn store_bronze_data(
        &self,
        data: ProcessedData,
        storage_config: &StorageConfig,
        target_key: &str,
    ) -> Result<()> {
        self.bronze.store_data(data, &storage_config.bronze_bucket, target_key).await
    }

    pub async fn register_s3_storage(&self, bucket: &str) -> Result<()> {
        // Use the existing S3Manager functionality
        self.s3_manager.register_object_store(&self.ctx, bucket).await?;
        
        // Additional logic if needed:
        self.verify_bucket_access(bucket).await?;
        
        Ok(())
    }

    pub async fn register_buckets(&self, config: &StorageConfig) -> Result<()> {
        let buckets = [
            &config.source_bucket,
            &config.bronze_bucket,
            &config.metadata_bucket,
        ];

        for bucket in buckets {
            self.register_s3_storage(bucket.bucket()).await?;
        }

        Ok(())
    }

    async fn verify_bucket_access(&self, bucket: &str) -> Result<()> {
        self.s3_manager.verify_bucket_exists(bucket).await
    }

    // Universal registration method that handles schema versions
    pub async fn register_parquet_table(
        &self,
        table_name: &str,
        file_path: &str,
        schema_version: VendorSchemaVersion,
    ) -> Result<()> {
        let cache_key = match schema_version {
            VendorSchemaVersion::Raw => "raw_vendors",
            VendorSchemaVersion::Bronze => "bronze_vendors",
        };
        
        let schema = match self.get_cached_schema(cache_key) {
            Some(schema) => schema,
            None => {
                let schema = match schema_version {
                    VendorSchemaVersion::Raw => raw_vendors_schema(),
                    VendorSchemaVersion::Bronze => bronze_vendors_schema(),
                };
                let schema_arc = Arc::new(schema);
                self.schema_cache.write().unwrap().insert(cache_key, schema_arc.clone());
                schema_arc
            }
        };
            
        self.register_parquet_with_schema(table_name, file_path, &schema).await
    }


    // Deregister a table
    pub async fn deregister_table(&self, table_name: &str) -> Result<()> {
        self.ctx.deregister_table(table_name)?;
        Ok(())
    }

    // Execute SQL query
    pub async fn execute_sql(&self, sql: &str) -> Result<datafusion::dataframe::DataFrame> {
        self.ctx.sql(sql).await.map_err(|e| e.into())
    }

    // Retrieve metadata for a specific key
    pub async fn get_metadata(
        &self,
        storage_config: &StorageConfig,
        metadata_key: &str,
    ) -> Result<ParquetMetadata> {
        let content = storage_config.metadata_bucket.get_object(metadata_key).await?;
        let metadata: ParquetMetadata = serde_json::from_slice(&content)?;
        Ok(metadata)
    }

    // List metadata for a date range
    pub async fn list_metadata(&self, prefix: &str) -> Result<Vec<ParquetMetadata>> {
        self.metadata.list_metadata(prefix).await
    }

    // Get the current session context
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Register a parquet table with an explicit schema
    pub async fn register_parquet_with_schema(
        &self,
        table_name: &str,
        file_path: &str,
        schema: &Schema,
    ) -> Result<()> {
        // Clean up existing registration if present
        let _ = self.ctx.deregister_table(table_name);
        
        let read_options = ParquetReadOptions::default()
            .schema(schema)
            .table_partition_cols(vec![]);

        self.ctx
            .register_parquet(table_name, file_path, read_options)
            .await
            .map_err(|e| common::Error::Other(format!(
                "Failed to register {} at {}: {}",
                table_name, file_path, e
            )))
    }

    pub fn get_cached_schema(&self, schema_name: &str) -> Option<Arc<Schema>> {
        self.schema_cache.read().unwrap().get(schema_name).cloned()
    }

    pub fn cache_schema(&self, name: &'static str, schema: Schema) { // Take Schema directly
        self.schema_cache.write().unwrap().insert(name, Arc::new(schema));
    }

    pub async fn infer_schema(
        &self,
        file_uri: &str,
    ) -> Result<Arc<Schema>> {

        // Create ListingTableUrl from the path
        let table_url = ListingTableUrl::parse(file_uri.to_string())?;
        
        let parquet_format = ParquetFormat::default();
        let listing_options = ListingOptions::new(Arc::new(parquet_format))
            .with_file_extension("parquet");

        // Create config with single path
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options);

        // Infer schema using the session state
        let config = config.infer_schema(&self.ctx.state()).await?;
        
        // Create table and get schema
        let table = Arc::new(ListingTable::try_new(config)?);
        Ok(table.schema().clone())
    }

    pub async fn validate_schema(
        &self,
        file_path: &str,
        expected_schema: &Schema,
    ) -> Result<()> {
        let inferred_schema = self.infer_schema(file_path).await?;
        
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
            
            // Optionally compare nullability if important
            if expected_field.is_nullable() != inferred_field.is_nullable() {
                println!(
                    "Nullability mismatch for field {}. Expected: {}, Found: {}",
                    expected_field.name(),
                    expected_field.is_nullable(),
                    inferred_field.is_nullable()
                );
            }
        }
        
        Ok(())
    }

    
    pub async fn register_raw_vendors(
        &self,
        table_name: &str,
        file_path: &str,
    ) -> Result<()> {
        self.register_parquet_table(
            table_name,
            file_path,
            VendorSchemaVersion::Raw,
        ).await
    }

    pub async fn register_bronze_vendors(
        &self,
        table_name: &str,
        file_path: &str,
    ) -> Result<()> {
        self.register_parquet_table(
            table_name,
            file_path,
            VendorSchemaVersion::Bronze,
        ).await
    }
}
