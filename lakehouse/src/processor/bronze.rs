// src/processor/bronze.rs
use super::metadata::ParquetMetadata;
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
use std::sync::Arc;
use arrow::datatypes::Schema;
use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;

#[derive(Clone)]
pub struct ProcessedData {
    pub df: DataFrame,
    pub metadata: ParquetMetadata,
    pub parquet_options: TableParquetOptions,
}

pub struct BronzeProcessor {
    ctx: SessionContext,
    s3_manager: Arc<S3Manager>,
}

impl BronzeProcessor {
    pub fn new(ctx: &SessionContext, s3_manager: Arc<S3Manager>) -> Self {
        Self { 
            ctx: ctx.clone(),
            s3_manager,
        }
    }

    /// New method that accepts explicit target path
    pub async fn process_to_bronze(
        &self,
        source_path: &str,
        target_path: &str,
    ) -> Result<ProcessedData> {
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
    
        // Use the DataFrame API with all columns
        let df = self.ctx.table(&temp_table).await?
            .select(columns)?;
    
        // Create metadata
        let record_count = df.clone().count().await? as i64;
        let metadata = ParquetMetadata::new(
            source_path.to_string(),
            record_count,
            source_path.to_string(),
            target_path.to_string(),
        );
    
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

    /// Original method maintained for backward compatibility
    pub async fn process_data(&self, source_path: &str) -> Result<ProcessedData> {
        self.process_to_bronze(source_path, "").await
    }

    /// Store data with schema validation
    pub async fn store_data(
        &self,
        data: ProcessedData,
        storage: &Arc<dyn crate::storage::s3::ObjectStorage>,
        target_key: &str,
    ) -> Result<()> {
        let target_url = format!("s3://{}/{}", storage.bucket(), target_key);
        
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

    // Updated to accept explicit schema
    async fn register_source_table(
        &self,
        table_name: &str,
        source_path: &str,
        schema: &Schema,
    ) -> Result<()> {
        // Register the object store if needed
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

    // Keep existing helper methods
    fn create_parquet_options(&self, metadata: &ParquetMetadata) -> TableParquetOptions {
        let mut options = TableParquetOptions::new();
        for kv in metadata.to_key_value_metadata() {
            options.key_value_metadata.insert(kv.key, kv.value);
        }
        options
    }

    async fn verify_file_exists(
        &self,
        storage: &Arc<dyn crate::storage::s3::ObjectStorage>,
        target_key: &str
    ) -> Result<()> {
        storage.check_file_exists(target_key)
            .await
            .and_then(|exists| if exists {
                Ok(()) 
            } else {
                Err(common::Error::Storage(format!("File not found: {}", target_key)))
            })
    }
}
