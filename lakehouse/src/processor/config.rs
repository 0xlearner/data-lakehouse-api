use crate::storage::S3Manager;
use crate::storage::s3::ObjectStorage;
use crate::storage::s3::S3Storage;
use common::Result;
use common::config::Settings;
use std::sync::Arc;

#[derive(Clone)]
pub struct StorageConfig {
    pub silver_bucket: Arc<dyn ObjectStorage>,
    pub bronze_bucket: Arc<dyn ObjectStorage>,
    pub metadata_bucket: Arc<dyn ObjectStorage>,
    pub source_bucket: Arc<dyn ObjectStorage>,
    pub s3_manager: Arc<S3Manager>,
}

impl StorageConfig {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let s3_config = crate::storage::S3Config {
            endpoint: settings.minio.endpoint.clone(),
            region: settings.minio.region.clone(),
            access_key: settings.minio.access_key.clone(),
            secret_key: settings.minio.secret_key.clone(),
            source_bucket: settings.minio.source_bucket.clone(),
            bronze_bucket: settings.minio.bronze_bucket.clone(),
            silver_bucket: settings.minio.silver_bucket.clone(),
            metadata_bucket: settings.minio.metadata_bucket.clone(),
        };

        let s3_manager = Arc::new(S3Manager::new(s3_config));

        Ok(Self {
            silver_bucket: Arc::new(
                S3Storage::new(s3_manager.clone(), &settings.minio.silver_bucket).await?,
            ),
            bronze_bucket: Arc::new(
                S3Storage::new(s3_manager.clone(), &settings.minio.bronze_bucket).await?,
            ),
            metadata_bucket: Arc::new(
                S3Storage::new(s3_manager.clone(), &settings.minio.metadata_bucket).await?,
            ),
            source_bucket: Arc::new(
                S3Storage::new(s3_manager.clone(), &settings.minio.source_bucket).await?,
            ),
            s3_manager,
        })
    }
}
