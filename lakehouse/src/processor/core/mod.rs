use super::*;
use crate::processor::bronze::BronzeProcessor;
use crate::processor::metadata::{
    MetadataRegistry, 
    S3MetadataRegistry,
    DatasetMetadata,
};
use crate::processor::table::{TableRegistry, ParquetTableRegistry};
use crate::models::schema::{SchemaValidator, VendorSchemaVersion};
use crate::storage::s3::S3Storage;
use crate::storage::{S3Manager, S3Config};
use crate::processor::bronze::ProcessedData;
use crate::models::schema::get_vendor_schema;
use crate::processor::config::StorageConfig;
pub use udf::register_udfs;
use std::sync::RwLock;
use std::collections::HashMap;
use arrow::datatypes::Schema;

pub struct LakehouseProcessor {
    pub(crate) ctx: SessionContext,
    pub(crate) schema_cache: RwLock<HashMap<&'static str, Arc<Schema>>>,
    pub(crate) s3_manager: Arc<S3Manager>,
    pub(crate) bronze: BronzeProcessor,
    pub(crate) metadata_registry: Arc<dyn MetadataRegistry>,
    pub(crate) table_registry: Arc<dyn TableRegistry>,
    pub(crate) schema_validator: SchemaValidator,
}

impl LakehouseProcessor {
    pub async fn new(config: &S3Config) -> Result<Self> {
        let ctx = SessionContext::new();
        let s3_manager = Arc::new(S3Manager::new(config.clone()));
        
        // Initialize schema cache
        let mut schema_cache = HashMap::new();
        schema_cache.insert(
            "raw_vendors", 
            Arc::new(get_vendor_schema(VendorSchemaVersion::Raw).clone())
        );
        schema_cache.insert(
            "bronze_vendors", 
            Arc::new(get_vendor_schema(VendorSchemaVersion::Bronze).clone())
        );

        let table_registry = Arc::new(ParquetTableRegistry);

        register_udfs(&ctx)?;
        
        let metadata_registry = Arc::new(S3MetadataRegistry::new(
            Arc::new(S3Storage::new(s3_manager.clone(), &config.metadata_bucket).await?),
            Arc::new(S3Storage::new(s3_manager.clone(), &config.bronze_bucket).await?),
        ).await?);

        let bronze = BronzeProcessor::new(&ctx, s3_manager.clone(), metadata_registry.clone());
        let schema_validator = SchemaValidator::new();
        
        Ok(Self {
            ctx,
            schema_cache: RwLock::new(schema_cache),
            s3_manager,
            bronze,
            metadata_registry,
            table_registry,
            schema_validator,
        })
    }

    pub async fn process_bronze_data(
        &self,
        request: ProcessingRequest,
    ) -> Result<(ProcessedData, String)> {
        let ProcessingRequest {
            city_code,
            year,
            month,
            day,
            bucket,
            source_files,
            content,
        } = request;

        let file_content = content.as_deref();

        let source_path = if source_files.is_empty() {
            PathBuilder::new(
                &bucket,
                &city_code,
                year,
                month,
                day
            )
            .with_dataset_type("vendors")
            .build_s3_path()
        } else {
            source_files[0].clone()
        };

        let timestamp = Utc::now().format("%H%M%S").to_string();
        let filename = format!("vendors_{}.parquet", timestamp);
        let target_path = PathBuilder::new(
            &self.s3_manager.config.bronze_bucket,
            &city_code,
            year,
            month,
            day
        )
        .with_dataset_type("vendors")
        .build_s3_file_path(&filename);

        println!("Target Path: {}", target_path);

        // Process data and get metadata directly from BronzeProcessor
        let processed_data = self.bronze
            .process_to_bronze(
                &source_path,
                &target_path,
                &city_code,
                year,
                month,
                day,
                file_content,
            )
            .await?;

        Ok((processed_data, target_path))
    }

    pub async fn deregister_table(&self, table_name: &str) -> Result<()> {
        self.table_registry.deregister_table(&self.ctx, table_name).await
    }

    /// Retrieve metadata for a specific dataset
    pub async fn get_metadata(
        &self,
        storage_config: &StorageConfig,
        metadata_key: &str,
    ) -> Result<DatasetMetadata> {
        let content = storage_config.metadata_bucket.get_object(metadata_key).await?;
        let metadata: DatasetMetadata = serde_json::from_slice(&content)?;
        Ok(metadata)
    }

    /// List all metadata entries for a given time range
    pub async fn list_metadata(&self, prefix: &str) -> Result<Vec<DatasetMetadata>> {
        // First try to list using the registry's direct method if available
        if let Ok(metadata_list) = self.metadata_registry.list_metadata(prefix).await {
            return Ok(metadata_list);
        }
        
        // Fallback to manual listing if direct method not available
        let keys = self.metadata_registry.list_objects(prefix).await?;
        let mut results = Vec::new();
        
        for key in keys {
            match self.metadata_registry.get_metadata(&key).await {
                Ok(metadata) => results.push(metadata),
                Err(e) => println!("Failed to load metadata from {}: {}", key, e),
            }
        }
        
        Ok(results)
    }
}