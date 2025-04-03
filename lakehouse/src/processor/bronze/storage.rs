use super::*;
use crate::storage::S3Manager;
use crate::storage::s3::ObjectStorage;
use common::{Error, Result};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

pub struct StorageManager {}

impl StorageManager {
    pub fn new(_s3_manager: Arc<S3Manager>) -> Self {
        Self {}
    }

    pub async fn store_data(
        &self,
        data: ProcessedData,
        storage: &dyn ObjectStorage,
        target_key: &str,
    ) -> Result<()> {
        // Validate schema
        self.validate_schema(&data.df).await?;

        // Write data to parquet file
        let target_url = format!("s3://{}/{}", storage.bucket(), target_key);

        data.df
            .write_parquet(
                &target_url,
                DataFrameWriteOptions::new(),
                Some(data.parquet_options),
            )
            .await?;

        // Verify file was written successfully
        self.verify_file_exists(storage, target_key).await?;

        Ok(())
    }

    async fn verify_file_exists(
        &self,
        storage: &dyn ObjectStorage,
        target_key: &str,
    ) -> Result<()> {
        // Clean up the key by removing any s3:// prefix and leading slashes
        let clean_key = target_key
            .strip_prefix("s3://")
            .and_then(|s| s.split_once('/'))
            .map(|(_, key)| key)
            .unwrap_or(target_key)
            .trim_start_matches('/');

        // Retry verification a few times with exponential backoff
        for attempt in 1..=3 {
            if storage.check_file_exists(clean_key).await? {
                return Ok(());
            }

            if attempt < 3 {
                sleep(Duration::from_secs(2u64.pow(attempt))).await;
            }
        }

        Err(Error::Storage(format!(
            "File not found after writing: {}",
            target_key
        )))
    }

    async fn validate_schema(&self, df: &DataFrame) -> Result<()> {
        let schema = df.schema();

        // Check required fields
        let required_fields = [
            "ingested_at",
            "extraction_started_at",
            "extraction_completed_at",
        ];
        for field in required_fields {
            if !schema.fields().iter().any(|f| f.name() == field) {
                return Err(Error::SchemaValidation(
                    format!("Missing required field {} in bronze data", field).into(),
                ));
            }
        }

        // Validate no nulls in required fields
        let null_count = df
            .clone()
            .filter(col("ingested_at").is_null())?
            .count()
            .await?;

        if null_count > 0 {
            return Err(Error::SchemaValidation(
                format!("Found {} null values in ingested_at column", null_count).into(),
            ));
        }

        Ok(())
    }
}
