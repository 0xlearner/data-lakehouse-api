// src/processor/bronze.rs
use super::metadata::{DatasetMetadata, MetadataRegistry, convert};
use super::deduplication::DeduplicationValidator;
use chrono::Utc;
use common::Result;
use datafusion::{
    execution::context::SessionContext,
    prelude::*,
    dataframe::DataFrameWriteOptions,
    common::config::TableParquetOptions,
    datasource::file_format::options::ParquetReadOptions,
};
use datafusion::logical_expr::ExprSchemable;
use uuid::Uuid;
use crate::{
    storage::S3Manager,
    schema::raw_vendors_schema,
};
use crate::storage::s3::ObjectStorage;
use std::sync::Arc;
use arrow::datatypes::Schema;
use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use arrow::array::StringArray;
use arrow::array::Array;

#[derive(Clone)]
pub struct ProcessedData {
    pub df: DataFrame,
    pub metadata: DatasetMetadata,
    pub parquet_options: TableParquetOptions,
}

pub struct BronzeProcessor {
    ctx: SessionContext,
    s3_manager: Arc<S3Manager>,
    metadata_registry: Arc<dyn MetadataRegistry>,
}

impl BronzeProcessor {
    pub fn new(
        ctx: &SessionContext, 
        s3_manager: Arc<S3Manager>,
        metadata_registry: Arc<dyn MetadataRegistry>,
    ) -> Self {
        Self { 
            ctx: ctx.clone(),
            s3_manager,
            metadata_registry,
        }
    }

    pub async fn process_to_bronze(
        &self,
        source_path: &str,
        target_path: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        file_content: Option<&[u8]>,
    ) -> Result<ProcessedData> {

        if let Some(content) = file_content {
            if DeduplicationValidator::check_file_duplicate(
                &*self.metadata_registry,
                source_path,
                content
            ).await? {
                return Err(common::Error::DuplicateData("File already processed".into()));
            }
        }
    
        // 2. Timestamp-based freshness check
        if let Some(latest) = DeduplicationValidator::get_latest_ingestion_timestamp(
            &*self.metadata_registry,
            source_path
        ).await? {
            if Utc::now() <= latest {
                return Err(common::Error::StaleData("No new data to process".into()));
            }
        }


        let raw_schema = raw_vendors_schema();
        let temp_table = format!("temp_raw_{}", Uuid::new_v4());
    
        // Register source table with raw schema
        self.register_source_table(&temp_table, source_path, &raw_schema).await?;
    
        // Get all column names from the schema
        let all_columns: Vec<Expr> = raw_schema.fields()
            .iter()
            .map(|f| col(f.name()))
            .collect();

        // Add ingested_at to the columns
        let mut columns = all_columns;
        columns.push(
            lit(Utc::now().timestamp_millis())
                .cast_to(&DataType::Timestamp(TimeUnit::Millisecond, None), 
                    self.ctx.table(&temp_table).await?.schema())?
                .alias("ingested_at")
        );
    
        // Process the data
        let df = self.ctx.table(&temp_table).await?
            .select(columns)?;

        let record_ids = self.extract_record_ids(&df).await?;
        let duplicates = DeduplicationValidator::check_record_duplicates(
            &*self.metadata_registry,
            &record_ids
        ).await?;
        
        if !duplicates.is_empty() {
            println!("Found duplicate records: {:?}", duplicates);
            // Handle based on your business requirements:
            // - Skip processing
            // - Overwrite
            // - Merge
            // - etc.
        }
    
        // Create metadata
        let record_count = df.clone().count().await? as u64;
        let timestamp = Utc::now().format("%H%M%S").to_string();
        
        let metadata = convert::create_dataset_metadata(
            &raw_schema,
            source_path,
            target_path,
            record_count,
            "bronze",
            file_content,
        )?;

        // Store metadata in registry
        let metadata_ref = self.metadata_registry.store_metadata(
            metadata.clone(),
            "vendors",
            city_code,
            year,
            month,
            day,
            &timestamp,
        ).await?;

        // Create marker file
        let dataset_id = format!(
            "vendors_{}_{}_{:02}_{:02}_{}",
            city_code, year, month, day, timestamp
        );
        
        self.metadata_registry.create_marker(
            &dataset_id,
            &metadata_ref,
            &metadata.schema.version,
        ).await?;

        // Convert metadata to parquet options
        let parquet_options = self.create_parquet_options(&metadata);
    
        // Clean up
        self.ctx.deregister_table(&temp_table)?;
    
        Ok(ProcessedData {
            df,
            metadata,
            parquet_options,
        })
    }

    pub async fn process_data(
        &self,
        source_path: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        file_content: Option<&[u8]>,
    ) -> Result<ProcessedData> {
        self.process_to_bronze(source_path, "", city_code, year, month, day,file_content).await
    }

    pub async fn store_data(
        &self,
        data: ProcessedData,
        storage: &dyn ObjectStorage,
        target_key: &str,
    ) -> Result<()> {
        let target_url = format!("s3://{}/{}", storage.bucket(), target_key);
        let required_fields = ["ingested_at", "extraction_started_at", "extraction_completed_at"];

        for field in required_fields {
            if !data.df.schema().fields().iter().any(|f| f.name() == field) {
                return Err(common::Error::SchemaValidation(
                    format!("Missing required field {} in bronze data", field).into()
                ));
            }
        }
        
        // Validate schema matches bronze before writing
        if !data.df.schema().fields().iter().any(|f| f.name() == "ingested_at") {
            return Err(common::Error::SchemaValidation(
                "Missing ingested_at field in bronze data".into()
            ));
        }

        data.df.write_parquet(
            &target_url,
            DataFrameWriteOptions::new(),
            Some(data.parquet_options),
        ).await?;

        self.verify_file_exists(storage, target_key).await
    }

    async fn register_source_table(
        &self,
        table_name: &str,
        source_path: &str,
        schema: &Schema,
    ) -> Result<()> {
        if let Some(url) = url::Url::parse(source_path).ok() {
            if url.scheme() == "s3" {
                if let Some(bucket) = url.host_str() {
                    self.s3_manager.register_object_store(&self.ctx, bucket).await?;
                }
            }
        }

        let options = ParquetReadOptions::default()
            .schema(schema)
            .table_partition_cols(vec![]);
            
        self.ctx
            .register_parquet(table_name, source_path, options)
            .await
            .map_err(common::Error::DataFusion)
    }

    fn create_parquet_options(&self, metadata: &DatasetMetadata) -> TableParquetOptions {
        let mut options = TableParquetOptions::new();
        for kv in convert::dataset_to_parquet_metadata(metadata) {
            options.key_value_metadata.insert(kv.key, kv.value);
        }
        options
    }

    async fn verify_file_exists(
        &self,
        storage: &dyn ObjectStorage,
        target_key: &str
    ) -> Result<()> {
        // Construct full S3 path for verification
        let full_path = format!("s3://{}/{}", storage.bucket(), target_key);
        
        // Use the object store's native checking
        let exists = storage.check_file_exists(target_key).await?;
        
        if exists {
            Ok(())
        } else {
            Err(common::Error::Storage(format!(
                "File not found after writing: {}",
                full_path
            )))
        }
    }

    async fn extract_record_ids(&self,df: &DataFrame) -> Result<Vec<String>> {
        let id_col = "code";
        
        let batches = df.clone()
            .select_columns(&[id_col])?
            .collect()
            .await?;
        
        let mut record_ids = Vec::new();
        for batch in batches {
            let array = batch.column(0);
            let string_array = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| common::Error::Internal("Code column is not string type".into()))?;
            
            for i in 0..string_array.len() {
                if !string_array.is_null(i) {
                    record_ids.push(string_array.value(i).to_string());
                }
            }
        }
        
        Ok(record_ids)
    }
}
