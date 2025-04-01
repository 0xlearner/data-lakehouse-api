use super::*;
use crate::processor::core::LakehouseProcessor;
use crate::storage::s3::{ObjectStorage, S3Storage};
// use crate::utils::paths::PathBuilder;
use crate::processor::bronze::ProcessedData;
use crate::processor::config::StorageConfig;

impl LakehouseProcessor {

    pub async fn store_bronze_data(
        &self,
        data: ProcessedData,
        target_key: &str,  // Now takes the pre-determined path
    ) -> Result<String> {

        // Remove the s3:// prefix and split into bucket and key
        let path = target_key.strip_prefix("s3://")
        .ok_or_else(|| common::Error::InvalidInput("Invalid S3 URI".into()))?;

        let key = path.split_once('/')
            .ok_or_else(|| common::Error::InvalidInput("Invalid S3 path".into()))?;


        let storage: Arc<dyn ObjectStorage> = Arc::new(
            S3Storage::new(self.s3_manager.clone(), &self.s3_manager.config.bronze_bucket).await?
        );
        
        // Use the key without leading slash
        let clean_key = key.1.strip_prefix('/').unwrap_or(key.1);
        self.bronze.store_data(data, &*storage, clean_key).await?;
            
        Ok(clean_key.to_string())  // Return the same path for consistency
        
        }

    pub async fn list_source_parquet_files(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
            
        let files = self.s3_manager
            .list_source_files(city_code, year, month, day)
            .await?;
            
        Ok(files)
    }

    pub async fn list_bronze_parquet_files(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
            
        let files = self.s3_manager
            .list_bronze_files(city_code, year, month, day)
            .await?;
            
        Ok(files)
    }

    pub async fn register_s3_storage(&self, bucket: &str) -> Result<()> {
        self.s3_manager.register_object_store(&self.ctx, bucket).await?;
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
}
