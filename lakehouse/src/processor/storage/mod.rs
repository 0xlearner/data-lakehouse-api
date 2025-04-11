use super::*;
use crate::processor::config::StorageConfig;
use crate::processor::core::LakehouseProcessor;

impl LakehouseProcessor {

    pub async fn list_source_parquet_files(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
        let files = self
            .s3_manager
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
        let files = self
            .s3_manager
            .list_bronze_files(city_code, year, month, day)
            .await?;

        Ok(files)
    }

    pub async fn register_s3_storage(&self, bucket: &str) -> Result<()> {
        self.s3_manager
            .register_object_store(&self.ctx, bucket)
            .await?;
        self.verify_bucket_access(bucket).await?;
        Ok(())
    }

    pub async fn register_buckets(&self, config: &StorageConfig) -> Result<()> {
        let buckets = [
            &config.source_bucket,
            &config.bronze_bucket,
            &config.silver_bucket,
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
