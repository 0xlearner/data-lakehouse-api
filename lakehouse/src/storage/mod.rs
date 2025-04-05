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
    pub source_bucket: String,
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
        self.object_store_cache
            .insert(bucket.to_string(), store.clone());
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

        match client.head_bucket().bucket(bucket).send().await {
            Ok(_) => Ok(()),
            Err(e) => Err(common::Error::Storage(format!(
                "Cannot access bucket '{}': {}",
                bucket, e
            ))),
        }
    }

    /// Lists all parquet files in the source bucket.
    pub async fn list_all_source_parquet_files(&self) -> Result<Vec<String>> {
        println!(
            "Listing all parquet files in source bucket: {}",
            self.config.source_bucket
        );
        let options = ListOptions {
            prefix: None, // List from the root (or specify a base prefix if all relevant data is under e.g., "raw/")
            extensions: Some(vec![".parquet".to_string()]),
            ..Default::default()
        };
        self.list_files(&self.config.source_bucket, options).await
    }

    pub async fn list_source_files(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
        let prefix = format!(
            "city_id={}/year={}/month={:02}/day={:02}/",
            city_code, year, month, day
        );

        let options = ListOptions {
            prefix: Some(prefix),
            extensions: Some(vec![".parquet".to_string()]),
            ..Default::default()
        };

        self.list_files(&self.config.source_bucket, options).await
    }

    pub async fn list_bronze_files(
        &self,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
        let prefix = format!(
            "city_id={}/year={}/month={:02}/day={:02}/",
            city_code, year, month, day
        );

        let options = ListOptions {
            prefix: Some(prefix),
            extensions: Some(vec![".parquet".to_string()]),
            ..Default::default()
        };

        self.list_files(&self.config.bronze_bucket, options).await
    }

    /// Lists files in a specified bucket with filtering options
    ///
    /// # Arguments
    /// * `bucket` - The bucket to list files from
    /// * `options` - Optional filtering and listing parameters
    pub async fn list_files(&self, bucket: &str, options: ListOptions) -> Result<Vec<String>> {
        let client = self.get_client(bucket).await?;
        let mut request = client.list_objects_v2().bucket(bucket);

        if let Some(ref prefix) = options.prefix {
            request = request.prefix(prefix);
        }

        if let Some(ref delimiter) = options.delimiter {
            request = request.delimiter(delimiter);
        }

        if let Some(max_keys) = options.max_keys {
            request = request.max_keys(max_keys);
        }

        let objects = request.send().await?;
        let mut files = Vec::new();
        let contents = objects.contents();
        if !contents.is_empty() {
            for object in contents {
                if let Some(key) = object.key() {
                    let should_include = match &options.extensions {
                        Some(extensions) => extensions.iter().any(|ext| key.ends_with(ext)),
                        None => true,
                    };

                    if should_include {
                        files.push(format!("s3://{}/{}", bucket, key));
                    }
                }
            }
        }

        if files.is_empty() {
            println!(
                "No files found in bucket: {} with prefix: {:?}",
                bucket, options.prefix
            );
        } else {
            println!("Found {} files in bucket: {}", files.len(), bucket);
        }

        Ok(files)
    }
}

pub struct ListOptions {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub max_keys: Option<i32>,
    pub extensions: Option<Vec<String>>,
}

impl Default for ListOptions {
    fn default() -> Self {
        Self {
            prefix: None,
            delimiter: None,
            max_keys: None,
            extensions: None,
        }
    }
}
