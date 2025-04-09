use super::*;
use crate::models::schema::get_vendor_schema;
use crate::models::schema::{SchemaValidator, VendorSchemaVersion};
use crate::processor::bronze::BronzeProcessor;
use crate::processor::bronze::ProcessedData;
use crate::processor::config::StorageConfig;
use crate::processor::metadata::{DatasetMetadata, MetadataRegistry, S3MetadataRegistry};
use crate::processor::silver::SilverProcessor;
use crate::processor::silver::types::ProcessedSilverData;
use crate::processor::table::{ParquetTableRegistry, TableRegistry};
use crate::storage::s3::S3Storage;
use crate::storage::{S3Config, S3Manager};
use arrow::datatypes::Schema;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct LakehouseProcessor {
    pub(crate) ctx: SessionContext,
    pub(crate) schema_cache: RwLock<HashMap<&'static str, Arc<Schema>>>,
    pub(crate) s3_manager: Arc<S3Manager>,
    pub(crate) bronze: BronzeProcessor,
    pub(crate) silver: SilverProcessor,
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
            Arc::new(get_vendor_schema(VendorSchemaVersion::Raw).clone()),
        );
        schema_cache.insert(
            "bronze_vendors",
            Arc::new(get_vendor_schema(VendorSchemaVersion::Bronze).clone()),
        );

        let table_registry = Arc::new(ParquetTableRegistry);

        // Create storage for metadata
        let metadata_storage =
            Arc::new(S3Storage::new(s3_manager.clone(), &config.metadata_bucket).await?);
        let bronze_storage =
            Arc::new(S3Storage::new(s3_manager.clone(), &config.bronze_bucket).await?);

        // Initialize metadata registry with both stores
        let metadata_registry = Arc::new(
            S3MetadataRegistry::new(
                metadata_storage, // registry_store
                bronze_storage,   // data_store
            )
            .await?,
        );

        let bronze = BronzeProcessor::new(&ctx, s3_manager.clone(), metadata_registry.clone());
        let silver = SilverProcessor::new(&ctx, s3_manager.clone(), metadata_registry.clone());
        let schema_validator = SchemaValidator::new();

        Ok(Self {
            ctx,
            schema_cache: RwLock::new(schema_cache),
            s3_manager,
            bronze,
            silver,
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

        // Process data and get metadata directly from BronzeProcessor
        let processed_data = self
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

        Ok((processed_data, target_path))
    }

    pub async fn process_silver_data(
        &self,
        bronze_path: &str, // This is the ACTUAL S3 path to the source bronze data
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<(ProcessedSilverData, HashMap<String, String>)> {
        println!(
            "Processing Silver data from Bronze path: {} for partition: {}/{}/{:02}/{:02}",
            bronze_path, city_code, year, month, day
        );
        // Note: bronze_table_name is typically only needed if you register the path
        // with the DataFusion context for SQL queries within the transformation.
        // If transformer.load_data() directly loads from the S3 path, you might
        // not need to register it explicitly here. Check your load_data implementation.

        // Process data using silver processor - UPDATED CALL
        let processed_data = self
            .silver
            .process_to_silver(bronze_path, city_code, year, month, day)
            .await?; // Now matches the updated signature (5 args after self)

        // Generate target paths *after* processing, using the derived table names
        let derived_table_names: Vec<String> =
            processed_data.derived_tables.keys().cloned().collect();

        // Call the helper method to generate the relative paths map
        let target_paths =
            self.generate_silver_target_paths(city_code, year, month, day, &derived_table_names);

        // Return the processed data and the correctly formatted relative paths map
        Ok((processed_data, target_paths))
    }

    fn generate_silver_target_paths(
        &self, // Takes &self but doesn't currently use it
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        derived_table_names: &[String],
    ) -> HashMap<String, String> {
        // Construct the base relative partition path string ONCE.
        // Format: "city_id=.../year=.../month=.../day=..."
        let base_partition_path = format!(
            "city_id={}/year={}/month={:02}/day={:02}", // Use {:02} for padding month/day
            city_code, year, month, day
        );

        // Create the HashMap by mapping each table name to the base path.
        let target_paths: HashMap<String, String> = derived_table_names
            .iter()
            .map(|table_name| (table_name.clone(), base_partition_path.clone()))
            .collect();

        target_paths
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
