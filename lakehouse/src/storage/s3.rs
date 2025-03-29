use async_trait::async_trait;
use aws_sdk_s3::error::SdkError;
use bytes::Bytes;
use common::Result;
use std::sync::Arc;
use aws_sdk_s3::Client as S3Client;
use crate::storage::S3Manager;

#[async_trait]
pub trait ObjectStorage: Send + Sync {
    async fn put_object(&self, key: &str, data: &[u8]) -> Result<()>;
    async fn get_object(&self, key: &str) -> Result<Vec<u8>>;
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;
    async fn check_file_exists(&self, key: &str) -> Result<bool>;
    fn bucket(&self) -> &str;
    async fn list_partition_files(
        &self,
        city_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>>;
}

// Implement for S3
pub struct S3Storage {
    bucket: String,
    client: Arc<S3Client>,
}

impl S3Storage {
    pub async fn new(s3_manager: Arc<S3Manager>, bucket: &str) -> Result<Self> {
        let client = s3_manager.get_client(bucket).await?;
        
        Ok(Self {
            client,
            bucket: bucket.to_string(),
        })
    }
}

#[async_trait]
impl ObjectStorage for S3Storage {
    async fn put_object(&self, key: &str, data: &[u8]) -> Result<()> {
        let body = Bytes::copy_from_slice(data);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body.into())
            .send()
            .await
            .map_err(|e| match e {
                SdkError::ServiceError(err) => common::Error::Storage(err.into_err().to_string()),
                _ => common::Error::Storage(e.to_string()),
            })?;

        Ok(())
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        let response = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| match e {
                SdkError::ServiceError(err) if err.err().is_no_such_key() => {
                    common::Error::Storage(format!("Object {} not found in bucket {}", key, self.bucket))
                }
                SdkError::ServiceError(err) => common::Error::Storage(err.into_err().to_string()),
                _ => common::Error::Storage(e.to_string()),
            })?;

        let data = response
            .body
            .collect()
            .await
            .map_err(|e| common::Error::Storage(e.to_string()))?
            .into_bytes()
            .to_vec();

        Ok(data)
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let response = request
                .send()
                .await
                .map_err(|e| match e {
                    SdkError::ServiceError(err) => common::Error::Storage(err.into_err().to_string()),
                    _ => common::Error::Storage(e.to_string()),
                })?;

            if let Some(contents) = response.contents {
                for object in contents {
                    if let Some(key) = object.key {
                        objects.push(key);
                    }
                }
            }

            continuation_token = response.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }

        Ok(objects)
    }

    async fn check_file_exists(&self, key: &str) -> Result<bool> {
        match self.client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(SdkError::ServiceError(err)) if err.err().is_not_found() => Ok(false),
            Err(e) => Err(common::Error::Storage(e.to_string())),
        }
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }

    async fn list_partition_files(
        &self,
        city_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<String>> {
        let prefix = format!(
            "vendors/city_id={}/year={}/month={:02}/day={:02}/",
            city_id, year, month, day
        );
        
        self.list_objects(&prefix).await
    }
}
