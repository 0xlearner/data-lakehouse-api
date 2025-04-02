// src/processor/bronze.rs
use super::deduplication::{DeduplicationValidator, DuplicateType};
use super::metadata::{DatasetMetadata, MetadataRegistry, convert};
use super::udf;
use crate::storage::s3::ObjectStorage;
use crate::{models::schema::raw_vendors_schema, storage::S3Manager};
use arrow::array::Array;
use arrow::array::StringArray;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use chrono::Utc;
use common::Result;
use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::logical_expr::ExprSchemable;
use datafusion::scalar::ScalarValue;
use datafusion::{
    common::config::TableParquetOptions, dataframe::DataFrameWriteOptions,
    datasource::file_format::options::ParquetReadOptions, execution::context::SessionContext,
    prelude::*,
};
use std::sync::Arc;
use uuid::Uuid;

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
        udf::register_udfs(ctx).expect("Failed to register UDFs");

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
        // Enhanced duplicate checks
        if let Some(content) = file_content {
            let dup_check = DeduplicationValidator::check_file_duplicate(
                &*self.metadata_registry,
                source_path,
                content,
            )
            .await?;

            if dup_check.is_duplicate {
                return Err(match dup_check.duplicate_type {
                    Some(DuplicateType::ContentHash) => common::Error::DuplicateData(format!(
                        "Exact duplicate file content detected. Reference: {}",
                        dup_check.existing_metadata_ref.unwrap_or_default()
                    )),
                    Some(DuplicateType::SourcePath) => common::Error::DuplicateData(format!(
                        "Same source path processed recently. Reference: {}",
                        dup_check.existing_metadata_ref.unwrap_or_default()
                    )),
                    _ => common::Error::DuplicateData("Duplicate data detected".into()),
                });
            }
        }

        // 2. Timestamp-based freshness check
        if let Some(latest) = DeduplicationValidator::get_latest_ingestion_timestamp(
            &*self.metadata_registry,
            source_path,
        )
        .await?
        {
            if Utc::now() <= latest {
                return Err(common::Error::StaleData("No new data to process".into()));
            }
        }

        let raw_schema = raw_vendors_schema();
        let temp_table = format!("temp_raw_{}", Uuid::new_v4());

        // Register source table with raw schema
        self.register_source_table(&temp_table, source_path, &raw_schema)
            .await?;

        // Process the data
        // Apply data validations using UDFs
        let df = self.ctx.table(&temp_table).await?;

        let original_schema = df.schema();
        let mut select_exprs: Vec<Expr> = original_schema
            .fields()
            .iter()
            .map(|f| col(f.name())) // Create col("column_name") for each field
            .collect();

        let is_valid_json = df.registry().udf("is_valid_json")?;
        let to_timestamp = df.registry().udf("to_timestamp")?;

        select_exprs.extend(vec![
            is_valid_json
                .call(vec![col("details")])
                .alias("valid_details"),
            is_valid_json
                .call(vec![col("reviews")])
                .alias("valid_reviews"),
            is_valid_json
                .call(vec![col("ratings")])
                .alias("valid_ratings"),
            to_timestamp
                .call(vec![col("extraction_started_at")])
                .alias("valid_extraction_started_at"),
            to_timestamp
                .call(vec![col("extraction_completed_at")])
                .alias("valid_extraction_completed_at"),
            lit(Utc::now().timestamp_millis())
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                    df.schema(),
                )?
                .alias("ingested_at"),
        ]);

        let validated_df = df.clone().select(select_exprs)?;

        // Validate JSON fields are valid
        let invalid_json_count = validated_df
            .clone()
            .filter(
                col("valid_details")
                    .and(col("valid_reviews"))
                    .and(col("valid_ratings"))
                    .not(),
            )?
            .count()
            .await?;

        if invalid_json_count > 0 {
            return Err(common::Error::DataValidation(format!(
                "Found {} records with invalid JSON data",
                invalid_json_count
            )));
        }

        // Validate timestamp order
        let invalid_timestamps = validated_df
            .clone()
            .filter(col("valid_extraction_started_at").lt(col("valid_extraction_completed_at")))?
            .count()
            .await?;

        if invalid_timestamps > 0 {
            return Err(common::Error::DataValidation(format!(
                "Found {} records where completion time is before start time",
                invalid_timestamps
            )));
        }

        // Remove validation columns and create final DataFrame
        let validated_df = validated_df.drop_columns(&[
            "valid_details",
            "valid_reviews",
            "valid_ratings",
            "valid_extraction_started_at",
            "valid_extraction_completed_at",
        ])?;

        println!("Validated Data");

        let record_ids = self.extract_record_ids(&validated_df).await?;
        let previously_processed_ids =
            DeduplicationValidator::check_record_duplicates(&*self.metadata_registry, &record_ids)
                .await?;

        if !previously_processed_ids.is_empty() {
            println!(
                "Found {} records in this batch that were already processed. Filtering them out.",
                previously_processed_ids.len()
            );

            // Convert the Vec<String> of duplicate IDs to a Vec<Expr> of literals
            let duplicate_literals = previously_processed_ids
                .into_iter()
                // Create Utf8 (String) literals for each ID
                .map(|id| lit(ScalarValue::Utf8(Some(id))))
                .collect::<Vec<_>>();

            // FIX: Use the in_list method with negated = true for NOT IN
            let predicate = col("code").in_list(duplicate_literals, true); // true means negated (NOT IN)

            // Apply the filter to remove rows with duplicate codes
            let validated_df = validated_df.clone().filter(predicate)?;

            // Optional: Check if any rows remain after filtering
            let remaining_rows = validated_df.clone().count().await?;
            if remaining_rows == 0 {
                println!("All records in the batch were duplicates. No data to write.");
                return Err(common::Error::DuplicateData(format!(
                    "All records were duplicates"
                )));
                // Depending on requirements, you might return Ok with an empty ProcessedData
                // or a specific status/error. For now, let's proceed but the write will be empty.
                // If an empty output is an error, return Err(...) here.
                // E.g.: return Err(common::Error::NoData("All records were duplicates".into()));
            } else {
                println!(
                    "{} records remain after duplicate filtering.",
                    remaining_rows
                );
            }
        } else {
            println!("No previously processed duplicate records found in this batch.");
        }

        // Create metadata
        let df_schema = validated_df.schema();
        let fields: Vec<Field> = df_schema
            .fields()
            .iter()
            .map(|field| {
                let field = field.as_ref();
                Field::new(field.name(), field.data_type().clone(), field.is_nullable())
            })
            .collect();

        let arrow_schema = ArrowSchema::new(fields);
        let timestamp = Utc::now().format("%H%M%S").to_string();

        let metadata = convert::create_dataset_metadata(
            &arrow_schema,
            source_path,
            target_path,
            &validated_df, // Pass the DataFrame
            "bronze",
            file_content,
            &record_ids,
        )
        .await?;

        // Store metadata in registry
        let metadata_ref = self
            .metadata_registry
            .store_metadata(
                metadata.clone(),
                "vendors",
                city_code,
                year,
                month,
                day,
                &timestamp,
            )
            .await?;

        // Create marker file
        let dataset_id = format!(
            "vendors_{}_{}_{:02}_{:02}_{}",
            city_code, year, month, day, timestamp
        );

        self.metadata_registry
            .create_marker(&dataset_id, &metadata_ref, &metadata.schema.version)
            .await?;

        // Convert metadata to parquet options
        let parquet_options = self.create_parquet_options(&metadata);

        // Clean up
        self.ctx.deregister_table(&temp_table)?;

        Ok(ProcessedData {
            df: validated_df,
            metadata,
            parquet_options,
        })
    }

    pub async fn store_data(
        &self,
        data: ProcessedData,
        storage: &dyn ObjectStorage,
        target_key: &str,
    ) -> Result<()> {
        let schema = data.df.schema();
        let target_url = format!("s3://{}/{}", storage.bucket(), target_key);

        // Schema validation
        let required_fields = [
            "ingested_at",
            "extraction_started_at",
            "extraction_completed_at",
        ];
        for field in required_fields {
            if !schema.fields().iter().any(|f| f.name() == field) {
                return Err(common::Error::SchemaValidation(
                    format!("Missing required field {} in bronze data", field).into(),
                ));
            }
        }

        // Check for null values in ingested_at column
        let null_count = data
            .df
            .clone()
            .filter(col("ingested_at").is_null())?
            .count()
            .await?;

        if null_count > 0 {
            return Err(common::Error::SchemaValidation(
                format!("Found {} null values in ingested_at column", null_count).into(),
            ));
        }

        // Write data after validation
        data.df
            .write_parquet(
                &target_url,
                DataFrameWriteOptions::new(),
                Some(data.parquet_options),
            )
            .await?;

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
                    self.s3_manager
                        .register_object_store(&self.ctx, bucket)
                        .await?;
                }
            }
        }

        let options = ParquetReadOptions::default()
            .schema(schema)
            .table_partition_cols(vec![])
            .file_extension(".parquet");

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
        target_key: &str,
    ) -> Result<()> {
        // Remove any s3:// prefix if present
        let clean_key = target_key
            .strip_prefix("s3://")
            .and_then(|s| s.split_once('/'))
            .map(|(_, key)| key)
            .unwrap_or(target_key);

        // Remove any leading slash
        let clean_key = clean_key.strip_prefix('/').unwrap_or(clean_key);

        let exists = storage.check_file_exists(clean_key).await?;

        if exists {
            Ok(())
        } else {
            Err(common::Error::Storage(format!(
                "File not found after writing: {}",
                target_key
            )))
        }
    }

    async fn extract_record_ids(&self, df: &DataFrame) -> Result<Vec<String>> {
        let id_col = "code";

        let batches = df.clone().select_columns(&[id_col])?.collect().await?;

        let mut record_ids = Vec::new();
        for batch in batches {
            let array = batch.column(0);
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
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
