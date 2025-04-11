use super::*;
use crate::models::schema::get_vendor_schema;
use crate::models::schema::{SchemaManager, VendorSchemaVersion};
use crate::processor::bronze::BronzeProcessor;
use crate::processor::config::StorageConfig;
use crate::processor::metadata::{DatasetMetadata, MetadataRegistry, S3MetadataRegistry};
use crate::processor::silver::SilverProcessor;
use crate::processor::table::{ParquetTableRegistry, TableRegistry};
use crate::storage::s3::S3Storage;
use crate::storage::{S3Config, S3Manager};
use std::collections::HashMap;

pub struct LakehouseProcessor {
    pub(crate) ctx: Arc<SessionContext>,
    pub(crate) s3_manager: Arc<S3Manager>,
    pub(crate) bronze: BronzeProcessor,
    pub(crate) silver: SilverProcessor,
    pub(crate) metadata_registry: Arc<dyn MetadataRegistry>,
    pub(crate) table_registry: Arc<dyn TableRegistry>,
    pub(crate) schema_manager: SchemaManager,
}

impl LakehouseProcessor {
    pub async fn new(config: &S3Config) -> Result<Self> {
        let ctx = Arc::new(SessionContext::new());
        let s3_manager = Arc::new(S3Manager::new(config.clone()));

        // Initialize schema cache map (temporary before wrapping in Arc<RwLock>)
        let mut initial_schemas = HashMap::new();
        initial_schemas.insert(
            "raw_vendors",
            Arc::new(get_vendor_schema(VendorSchemaVersion::Raw).clone()),
        );
        initial_schemas.insert(
            "bronze_vendors",
            Arc::new(get_vendor_schema(VendorSchemaVersion::Bronze).clone()),
        );

        let table_registry = Arc::new(ParquetTableRegistry);

        // Create storage for metadata
        let metadata_storage =
            Arc::new(S3Storage::new(s3_manager.clone(), &config.metadata_bucket).await?);
        let bronze_storage =
            Arc::new(S3Storage::new(s3_manager.clone(), &config.bronze_bucket).await?);
        let silver_storage =
            Arc::new(S3Storage::new(s3_manager.clone(), &config.silver_bucket).await?);

        // Initialize metadata registry with both stores
        // Clone storage Arcs when passing to registry AND processors
        let metadata_registry = Arc::new(
            S3MetadataRegistry::new(
                metadata_storage,      
            )
            .await?,
        );

        // Pass ctx.clone() for BronzeProcessor which expects Arc<SessionContext>
        let bronze = BronzeProcessor::new(
            ctx.clone(), 
            bronze_storage.clone(), 
            metadata_registry.clone());
        // Pass &ctx for SilverProcessor which expects &SessionContext
        let silver = SilverProcessor::new(
            ctx.clone(), // Pass reference to SessionContext inside Arc
            silver_storage.clone(), // Clone Arc for SilverProcessor
            metadata_registry.clone()
            
        );
        let schema_manager = SchemaManager::new(Arc::clone(&ctx), initial_schemas);

        Ok(Self {
            ctx,
            s3_manager,
            bronze,
            silver,
            metadata_registry,
            table_registry,
            schema_manager,
        })
    }

    // Updated return type: Result<(DatasetMetadata, String)>
    pub async fn process_bronze_data(
        &self,
        request: ProcessingRequest,
    ) -> Result<(DatasetMetadata, String)> {
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
            PathBuilder::new(&bucket, &city_code, year, month, day)
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
            day,
        )
        .with_dataset_type("vendors")
        .build_s3_file_path(&filename);

        println!("Target Path: {}", target_path);

        // Process data using BronzeProcessor, which now handles writing internally
        // It returns the DatasetMetadata upon success
        let metadata = self
            .bronze
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

        // Return the metadata and the target path used
        Ok((metadata, target_path))
    }

    // Updated return type: Result<()>
    pub async fn process_silver_data(
        &self,
        bronze_path: &str, // This is the ACTUAL S3 path to the source bronze data
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<()> {
        println!(
            "Processing Silver data from Bronze path: {} for partition: {}/{}/{:02}/{:02}",
            bronze_path, city_code, year, month, day
        );

        // Call SilverProcessor, which now handles transformation and writing internally
        self.silver
            .process_to_silver(bronze_path, city_code, year, month, day)
            .await?; // Returns Result<()>

        // No data or paths are returned anymore, storage is handled internally
        Ok(())
    }

    pub async fn deregister_table(&self, table_name: &str) -> Result<()> {
        self.table_registry
            .deregister_table(&self.ctx, table_name)
            .await
    }

    /// Retrieve metadata for a specific dataset
    pub async fn get_metadata(
        &self,
        storage_config: &StorageConfig,
        metadata_key: &str,
    ) -> Result<DatasetMetadata> {
        let content = storage_config
            .metadata_bucket
            .get_object(metadata_key)
            .await?;
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
