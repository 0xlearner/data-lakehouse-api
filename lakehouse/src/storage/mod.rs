pub mod s3;

use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use common::Result;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    pub metadata_bucket: String,
    pub bronze_bucket: String,
}

#[derive(Clone)]
pub struct S3Manager {
    pub config: S3Config,
    client_cache: Arc<dashmap::DashMap<String, Arc<S3Client>>>,
    object_store_cache: Arc<dashmap::DashMap<String, Arc<object_store::aws::AmazonS3>>>,
}

impl S3Manager {
    pub fn new(config: S3Config) -> Self {
        Self {
            config,
            client_cache: Arc::new(dashmap::DashMap::new()),
            object_store_cache: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub async fn get_client(&self, bucket: &str) -> Result<Arc<S3Client>> {
        if let Some(client) = self.client_cache.get(bucket) {
            return Ok(client.clone());
        }

        let credentials = Credentials::new(
            &self.config.access_key,
            &self.config.secret_key,
            None,
            None,
            "static",
        );

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(&self.config.endpoint)
            .region(Region::new(self.config.region.clone()))
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        let client = Arc::new(aws_sdk_s3::Client::from_conf(s3_config));
        self.client_cache.insert(bucket.to_string(), client.clone());
        Ok(client)
    }

    pub async fn get_object_store(&self, bucket: &str) -> Result<Arc<object_store::aws::AmazonS3>> {
        if let Some(store) = self.object_store_cache.get(bucket) {
            return Ok(store.clone());
        }

        let s3 = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&self.config.region)
            .with_access_key_id(&self.config.access_key)
            .with_secret_access_key(&self.config.secret_key)
            .with_endpoint(&self.config.endpoint)
            .with_allow_http(true)
            .build()?;

        let store = Arc::new(s3);
        self.object_store_cache.insert(bucket.to_string(), store.clone());
        Ok(store)
    }

    pub async fn register_object_store(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        bucket: &str,
    ) -> Result<()> {
        let store = self.get_object_store(bucket).await?;
        let url = Url::parse(&format!("s3://{}", bucket))?;
        ctx.runtime_env().register_object_store(&url, store);
        Ok(())
    }

    /// Verifies that a bucket exists and is accessible
    pub async fn verify_bucket_exists(&self, bucket: &str) -> Result<()> {
        let client = self.get_client(bucket).await?;
        
        match client.head_bucket()
            .bucket(bucket)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(common::Error::Storage(format!(
                "Cannot access bucket '{}': {}",
                bucket, e
            ))),
        }
    }
}
