use common::Result;
use common::config::Settings;
use crate::processor::{LakehouseProcessor, StorageConfig};
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::{
    Int32Array, 
    Int64Array, 
    StringArray,
    TimestampMillisecondArray, 
    TimestampMicrosecondArray,
    TimestampNanosecondArray, 
    TimestampSecondArray
};
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::{Value, Number};
use arrow::array::Array;
use chrono::{Utc, TimeZone};
use uuid::Uuid;

pub struct LakehouseService {
    processor: Arc<LakehouseProcessor>,
    storage_config: StorageConfig,
}

impl LakehouseService {
    pub async fn new(settings: &Settings) -> Result<Self> {
        let s3_config = crate::storage::S3Config {
            endpoint: settings.minio.endpoint.clone(),
            region: settings.minio.region.clone(),
            access_key: settings.minio.access_key.clone(),
            secret_key: settings.minio.secret_key.clone(),
            metadata_bucket: settings.minio.metadata_bucket.clone(),
        };
        let processor = LakehouseProcessor::new(&s3_config).await?;
        
        let storage_config = StorageConfig::from_settings(settings).await?;
        
        processor.register_buckets(&storage_config).await?;

        Ok(Self {
            processor: Arc::new(processor),
            storage_config,
        })
    }
    
    pub async fn process_city_data(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<String> {
        let source_path = format!(
            "s3://{}/city_id={}/year={}/month={:02}/day={:02}/",
            self.storage_config.source_bucket.bucket(),
            city_code, year, month, day
        );
        
        let bronze_key = format!(
            "vendors/city_id={}/year={}/month={:02}/day={:02}/{}.parquet",
            city_code, year, month, day,
            Utc::now().format("%H%M%S")
        );

        if let Some(raw_schema) = self.processor.get_cached_schema("raw_vendors") {
            self.processor.validate_schema(&source_path, &raw_schema).await?;
        }

        let processed_data = self.processor.process_bronze_data(&source_path).await?;
        
        if let Some(bronze_schema) = self.processor.get_cached_schema("bronze_vendors") {
            if !processed_data.df.schema().fields().iter()
                .zip(bronze_schema.fields().iter())
                .all(|(a, b)| a == b) {
                return Err(common::Error::SchemaMismatch(
                    "Processed data doesn't match bronze schema".into()
                ));
            }
        }
        self.processor.store_bronze_data(processed_data, &self.storage_config, &bronze_key).await?;
        Ok(bronze_key)
    }

    // New method for cleaner API
    pub async fn query_raw_vendors_by_date(
        &self,
        city: &str,
        year: i32,
        month: u32,
        day: u32,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let file_path = format!(
            "city_id={}/year={}/month={:02}/day={:02}/",
            city, year, month, day
        );
        self.query_raw_vendors(&file_path, limit).await
    }

    // New method for cleaner API
    pub async fn query_bronze_vendors_by_date(
        &self,
        city: &str,
        year: i32,
        month: u32,
        day: u32,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let table_name = format!("bronze_query_{}", Uuid::new_v4());
        let s3_path = format!(
            "s3://{}/vendors/city_id={}/year={}/month={:02}/day={:02}/",
            self.storage_config.bronze_bucket.bucket(),
            city, year, month, day
        );
    
        if let Some(bronze_schema) = self.processor.get_cached_schema("bronze_vendors") {
            self.processor.validate_schema(&s3_path, &bronze_schema).await?;
        }
    
        self.processor
            .register_bronze_vendors(&table_name, &s3_path)
            .await?;
    
        let results = self.execute_and_fetch(&table_name, limit).await;
        self.processor.deregister_table(&table_name).await?;
        results
    }

    // Keep existing file_path-based methods
    pub async fn query_raw_vendors(
        &self,
        file_path: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let table_name = format!("raw_query_{}", Uuid::new_v4());
        let s3_path = format!(
            "s3://{}/{}",
            self.storage_config.source_bucket.bucket(),
            file_path.trim_start_matches('/')
        );
        
        if let Some(raw_schema) = self.processor.get_cached_schema("raw_vendors") {
            self.processor.validate_schema(&s3_path, &raw_schema).await?;
        }

        self.processor
            .register_raw_vendors(&table_name, &s3_path)
            .await?;

        let results = self.execute_and_fetch(&table_name, limit).await;
        self.processor.deregister_table(&table_name).await?;
        results
    }

    pub async fn query_bronze_vendors(
        &self,
        file_path: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let table_name = format!("bronze_query_{}", Uuid::new_v4());
        let s3_path = format!(
            "s3://{}/{}",
            self.storage_config.bronze_bucket.bucket(),
            file_path.trim_start_matches('/')
        );
    
        if let Some(bronze_schema) = self.processor.get_cached_schema("bronze_vendors") {
            self.processor.validate_schema(&s3_path, &bronze_schema).await?;
        }
    
        self.processor
            .register_bronze_vendors(&table_name, &s3_path)
            .await?;
    
        let results = self.execute_and_fetch(&table_name, limit).await;
        self.processor.deregister_table(&table_name).await?;
        results
    }

    async fn execute_and_fetch(
        &self,
        table_name: &str,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        // Use prepared statement style to avoid SQL injection and parsing issues
        let sql = format!("SELECT * FROM \"{}\" LIMIT {}", table_name, limit);
        let df = self.processor.execute_sql(&sql).await?;
    
        let batches = df.collect().await?;
        Self::batches_to_json(batches)
    }

    fn batches_to_json(batches: Vec<RecordBatch>) -> Result<Vec<serde_json::Value>> {
        let mut json_rows = Vec::new();
        
        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row = serde_json::Map::new();
                
                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    let value = Self::arrow_array_to_json(column, row_idx)?;
                    row.insert(field.name().clone(), value);
                }
                
                json_rows.push(Value::Object(row));
            }
        }
        
        Ok(json_rows)
    }

    fn arrow_array_to_json(array: &dyn Array, index: usize) -> Result<Value> {
        if array.is_null(index) {
            return Ok(Value::Null);
        }
    
        Ok(match array.data_type() {
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Value::Number(Number::from(array.value(index)))
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Value::Number(Number::from(array.value(index)))
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Value::String(array.value(index).to_string())
            }
            DataType::Timestamp(unit, _) => {
                match unit {
                    TimeUnit::Millisecond => {
                        let array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                        let ts = array.value(index);
                        let datetime = chrono::Utc.timestamp_millis_opt(ts).unwrap();
                        Value::String(datetime.to_rfc3339())
                    },
                    TimeUnit::Microsecond => {
                        let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                        let ts = array.value(index);
                        let datetime = chrono::Utc.timestamp_micros(ts).unwrap();
                        Value::String(datetime.to_rfc3339())
                    },
                    TimeUnit::Nanosecond => {
                        let array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                        let ts = array.value(index);
                        let seconds = ts / 1_000_000_000;
                        let nanos = (ts % 1_000_000_000) as u32;
                        let datetime = chrono::Utc.timestamp_opt(seconds, nanos).unwrap();
                        Value::String(datetime.to_rfc3339())
                    },
                    TimeUnit::Second => {
                        let array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                        let ts = array.value(index);
                        let datetime = chrono::Utc.timestamp_opt(ts, 0).unwrap();
                        Value::String(datetime.to_rfc3339())
                    },
                }
            }
            _ => Value::Null,
        })
    }
}