use common::Result;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, ObjectCannedAcl};
use aws_smithy_runtime_api::client::result::SdkError;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tracing::{debug, error, info};

pub struct MinioUploader {
    pub client: S3Client,
    bucket: String,
}

impl MinioUploader {
    pub async fn new(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
        region: &str,
    ) -> Result<Self> {
        debug!(
            endpoint = endpoint,
            bucket = bucket,
            region = region,
            "Initializing MinIO uploader"
        );

        let credentials =
            Credentials::new(access_key, secret_key, None, None, "static-credentials");

        let region = Region::new(region.to_string());

        let s3_config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(region)
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        let client = S3Client::from_conf(s3_config);

        // Verify bucket exists and is accessible
        debug!("Verifying bucket access");
        let bucket_exists = client.head_bucket().bucket(bucket).send().await;

        if let Err(e) = bucket_exists {
            error!(
                error = ?e,
                bucket = bucket,
                "Failed to access bucket"
            );
            return Err(common::Error::Storage(format!(
                "Cannot access bucket '{}': {}",
                bucket, e
            )));
        }

        Ok(Self {
            client,
            bucket: bucket.to_string(),
        })
    }

    pub async fn upload_file(&self, local_path: &Path, s3_key: &str) -> Result<()> {
        debug!(
            local_path = ?local_path,
            s3_key = s3_key,
            "Starting file upload"
        );

        // Read file metadata
        let metadata = std::fs::metadata(local_path)?;
        let file_size = metadata.len();

        let body = ByteStream::from_path(local_path).await?;

        debug!(file_size = file_size, "Uploading file to MinIO");

        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .body(body)
            .content_type("application/json")
            .acl(ObjectCannedAcl::BucketOwnerFullControl)
            .send()
            .await;

        match result {
            Ok(_) => {
                debug!(s3_key = s3_key, "File uploaded successfully");
                Ok(())
            }
            Err(e) => {
                let error_msg = match &e {
                    SdkError::ServiceError(service_error) => {
                        error!(
                            error = ?service_error.err(),
                            raw_response = ?service_error.raw(),
                            "MinIO service error"
                        );
                        format!(
                            "MinIO service error: {} (raw: {:?})",
                            service_error.err(),
                            service_error.raw()
                        )
                    }
                    _ => {
                        error!(
                            error = ?e,
                            "MinIO upload error"
                        );
                        format!("MinIO error: {}", e)
                    }
                };
                Err(common::Error::Storage(error_msg))
            }
        }
    }

    pub async fn upload_parquet_file(&self, file_path: &Path, s3_key: &str) -> Result<()> {
        const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MB chunks
        let file_size = std::fs::metadata(file_path)?.len() as usize;

        if file_size > CHUNK_SIZE {
            self.upload_multipart(file_path, s3_key, file_size, CHUNK_SIZE)
                .await
        } else {
            self.upload_single_part(file_path, s3_key).await
        }
    }

    async fn upload_single_part(&self, file_path: &Path, s3_key: &str) -> Result<()> {
        info!("Using single-part upload");
        let body = ByteStream::from_path(file_path).await?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(s3_key)
            .body(body)
            .content_type("application/x-parquet")
            .send()
            .await?;

        Ok(())
    }

    async fn upload_multipart(
        &self,
        file_path: &Path,
        s3_key: &str,
        file_size: usize,
        chunk_size: usize,
    ) -> Result<()> {
        info!(
            file_size_mb = file_size / 1024 / 1024,
            "Large file detected, using multipart upload"
        );

        // Initialize multipart upload
        let create_multipart_res = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(s3_key)
            .content_type("application/x-parquet")
            .send()
            .await?;

        let upload_id = create_multipart_res
            .upload_id()
            .ok_or_else(|| common::Error::Storage("Failed to get upload ID".to_string()))?;

        let mut part_number = 1;
        let mut completed_parts = Vec::new();
        let mut file = File::open(file_path)?;

        // Upload parts
        loop {
            let mut buffer = vec![0; chunk_size];
            let n = file.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            buffer.truncate(n);

            let part_res = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(s3_key)
                .upload_id(upload_id)
                .body(ByteStream::from(buffer))
                .part_number(part_number)
                .send()
                .await?;

            completed_parts.push(
                CompletedPart::builder()
                    .e_tag(part_res.e_tag.unwrap_or_default())
                    .part_number(part_number)
                    .build(),
            );

            info!(
                part_number = part_number,
                total_parts = (file_size + chunk_size - 1) / chunk_size,
                "Uploaded part"
            );

            part_number += 1;
        }

        // Complete multipart upload
        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(s3_key)
            .upload_id(upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;

        Ok(())
    }
}
