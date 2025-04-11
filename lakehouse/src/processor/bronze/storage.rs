use super::*;
use crate::storage::s3::ObjectStorage;
use common::{Error, Result};
use datafusion::arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::path::Path;
use std::sync::Arc;
use url::Url;

pub struct StorageManager {
    storage: Arc<dyn ObjectStorage>,
}

impl StorageManager {
    pub fn new(storage: Arc<dyn ObjectStorage>) -> Self {
        Self { storage }
    }

    fn object_key_from_s3_path(&self, s3_path: &str) -> Result<String> {
        let parsed_url = Url::parse(s3_path)?;

        if parsed_url.scheme() != "s3" {
            return Err(Error::InvalidInput(format!(
                "Path '{}' is not an S3 path (expected scheme 's3')",
                s3_path
            )));
        }

        let key = parsed_url.path().trim_start_matches('/').to_string();

        if key.is_empty() {
            return Err(Error::InvalidInput(format!(
                "S3 path '{}' results in an empty object key",
                s3_path
            )));
        }

        Ok(key)
    }

    pub async fn write_data(
        &self,
        batches: Vec<RecordBatch>,
        target_path: &str,
        table_options: WriterProperties,
    ) -> Result<()> {
        let mut buffer: Vec<u8> = Vec::new();
        
        let mut writer = ArrowWriter::try_new(
            &mut buffer,
            batches[0].schema(),
            Some(table_options),
        )?;

        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;

        let object_key = self.object_key_from_s3_path(target_path)?;
        self.storage.put_object(&object_key, &buffer).await?;
        println!("Bronze: Data written successfully to {}", target_path);

        Ok(())
    }

    pub async fn write_marker(
        &self,
        target_path: &str,
        metadata: &DatasetMetadata,
        metadata_ref: String,
    ) -> Result<()> {
        let marker = MetadataMarker {
            dataset_id: metadata.dataset_id.clone(),
            metadata_ref,
            created_at: Utc::now(),
            schema_version: metadata.schema.version.clone(),
            table_name: None,
        };
        let marker_json = serde_json::to_vec_pretty(&marker)?;

        let object_key = self.object_key_from_s3_path(target_path)?;
        let target_dir = Path::new(&object_key)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "".to_string());

        let marker_key = if target_dir.is_empty() {
            "_SUCCESS".to_string()
        } else {
            format!("{}/{}", target_dir, "_SUCCESS")
        };

        println!("Bronze: Writing marker to key: {}", marker_key);
        self.storage.put_object(&marker_key, &marker_json).await?;
        println!("Bronze: Marker file written successfully.");

        Ok(())
    }

    // This is the high-level orchestration method that uses the other methods
    pub async fn write_dataset(
        &self,
        batches: Vec<RecordBatch>,
        target_path: &str,
        metadata: &DatasetMetadata,
        metadata_ref: String,
        table_options: WriterProperties,
    ) -> Result<()> {
        // First write the data
        self.write_data(batches, target_path, table_options).await?;
        
        // Then write the marker
        self.write_marker(target_path, metadata, metadata_ref).await?;
        
        Ok(())
    }
}
