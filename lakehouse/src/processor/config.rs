use std::sync::Arc;
use crate::storage::S3Manager;
use crate::storage::s3::ObjectStorage;
use common::config::Settings;
use common::Result;
use crate::storage::s3::S3Storage;



#[derive(Clone)]
pub struct StorageConfig {
    pub bronze_bucket: Arc<dyn ObjectStorage>,
    pub metadata_bucket: Arc<dyn ObjectStorage>,
    pub source_bucket: Arc<dyn ObjectStorage>,
    pub s3_manager: Arc<S3Manager>,
}

impl StorageConfig {
    pub async fn from_settings(settings: &Settings) -> Result<Self> {
        let s3_config = crate::storage::S3Config{
            endpoint: settings.minio.endpoint.clone(),
            region: settings.minio.region.clone(),
            access_key: settings.minio.access_key.clone(),
            secret_key: settings.minio.secret_key.clone(),
            metadata_bucket: settings.minio.metadata_bucket.clone(),
        };
        
        let s3_manager = Arc::new(S3Manager::new(s3_config));
        
        Ok(Self {
            bronze_bucket: Arc::new(S3Storage::new(s3_manager.clone(), &settings.minio.bronze_bucket).await?),
            metadata_bucket: Arc::new(S3Storage::new(s3_manager.clone(), &settings.minio.metadata_bucket).await?),
            source_bucket: Arc::new(S3Storage::new(s3_manager.clone(), &settings.minio.source_bucket).await?),
            s3_manager,
        })
    }
}
