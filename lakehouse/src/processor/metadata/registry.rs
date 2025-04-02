// registry.rs
use crate::storage::s3::ObjectStorage;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Utc};
use dashmap::DashMap;
use std::sync::Arc;

use super::types::*;
use common::Result;

#[async_trait]
pub trait MetadataRegistry: Send + Sync {
    async fn store_metadata(
        &self,
        metadata: DatasetMetadata,
        dataset_type: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        timestamp: &str,
    ) -> Result<String>;

    async fn create_marker(
        &self,
        dataset_id: &str,
        metadata_ref: &str,
        schema_version: &str,
    ) -> Result<String>;

    async fn get_metadata(&self, metadata_ref: &str) -> Result<DatasetMetadata>;
    async fn find_metadata(&self, dataset_id: &str) -> Result<DatasetMetadata>;
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;
    async fn list_metadata(&self, prefix: &str) -> Result<Vec<DatasetMetadata>>;
    async fn update_indexes(&self, key: &str, metadata: &DatasetMetadata) -> Result<()>;
    async fn find_metadata_by_source_path(&self, path: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_content_hash(&self, hash: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_record_id(&self, record_id: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_source(&self, source_path: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_city_date(
        &self,
        dataset_type: &str,
        city_code: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<DatasetMetadata>>;
}

pub struct S3MetadataRegistry {
    registry_store: Arc<dyn ObjectStorage>,
    data_store: Arc<dyn ObjectStorage>,
    source_path_index: Arc<DashMap<String, Vec<String>>>,
    content_hash_index: Arc<DashMap<String, Vec<String>>>,
    record_id_index: Arc<DashMap<String, Vec<String>>>,
    source_system_index: Arc<DashMap<String, Vec<String>>>,
}

impl S3MetadataRegistry {
    pub async fn new(
        registry_store: Arc<dyn ObjectStorage>,
        data_store: Arc<dyn ObjectStorage>,
    ) -> Result<Self> {
        let registry = Self {
            registry_store,
            data_store,
            source_path_index: Arc::new(DashMap::new()),
            content_hash_index: Arc::new(DashMap::new()),
            record_id_index: Arc::new(DashMap::new()),
            source_system_index: Arc::new(DashMap::new()),
        };

        registry.build_index().await?;
        Ok(registry)
    }

    async fn build_index(&self) -> Result<()> {
        let keys = self.registry_store.list_objects("datasets/").await?;

        for key in keys {
            if let Ok(metadata) = self.get_metadata(&key).await {
                // Index by source path
                self.source_path_index
                    .entry(metadata.lineage.source_path.clone())
                    .or_default()
                    .push(key.clone());

                // Index by content hash
                self.content_hash_index
                    .entry(metadata.content_hash.clone())
                    .or_default()
                    .push(key.clone());

                // Index by source system
                self.source_system_index
                    .entry(metadata.lineage.source_system.clone())
                    .or_default()
                    .push(key.clone());

                // Index by record IDs (if present)
                if let Some(ids) = metadata.custom_metadata.get("record_ids") {
                    for id in ids.split(',') {
                        self.record_id_index
                            .entry(id.trim().to_string())
                            .or_default()
                            .push(key.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn generate_metadata_key(
        &self,
        layer: &str,
        dataset_type: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        timestamp: &str,
    ) -> String {
        format!(
            "datasets/{}/{}_{}_{}_{:02}_{:02}_{}.json",
            layer, dataset_type, city_code, year, month, day, timestamp
        )
    }

    async fn find_metadata_by_index<'a>(
        &self,
        index: &DashMap<String, Vec<String>>,
        key: &str,
        validator: Option<Box<dyn Fn(&DatasetMetadata) -> bool + Send + Sync>>,
    ) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        if let Some(keys) = index.get(key) {
            for key in keys.value() {
                if let Ok(metadata) = self.get_metadata(key).await {
                    if let Some(ref validate) = validator {
                        if validate(&metadata) {
                            results.push(metadata);
                        }
                    } else {
                        results.push(metadata);
                    }
                }
            }
        }
        Ok(results)
    }
}

#[async_trait]
impl MetadataRegistry for S3MetadataRegistry {
    // Simplified implementations using the generic helper
    async fn find_metadata_by_source_path(&self, path: &str) -> Result<Vec<DatasetMetadata>> {
        // Clone the path string to make it owned and therefore 'static
        let path_owned = path.to_string();
        self.find_metadata_by_index(
            &self.source_path_index,
            path,
            Some(Box::new(move |metadata| {
                metadata.lineage.source_path == path_owned
            })),
        )
        .await
    }

    async fn find_metadata_by_content_hash(&self, hash: &str) -> Result<Vec<DatasetMetadata>> {
        self.find_metadata_by_index(&self.content_hash_index, hash, None)
            .await
    }

    async fn find_metadata_by_record_id(&self, record_id: &str) -> Result<Vec<DatasetMetadata>> {
        self.find_metadata_by_index(&self.record_id_index, record_id, None)
            .await
    }

    async fn find_metadata_by_source(&self, source: &str) -> Result<Vec<DatasetMetadata>> {
        self.find_metadata_by_index(&self.source_system_index, source, None)
            .await
    }

    async fn store_metadata(
        &self,
        metadata: DatasetMetadata,
        dataset_type: &str,
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        timestamp: &str,
    ) -> Result<String> {
        let key = self.generate_metadata_key(
            &metadata.layer,
            dataset_type,
            city_code,
            year,
            month,
            day,
            timestamp,
        );

        // Store the metadata
        let metadata_json = serde_json::to_vec(&metadata)?;
        self.registry_store.put_object(&key, &metadata_json).await?;

        // Update indexes
        self.update_indexes(&key, &metadata).await?;

        Ok(key)
    }

    async fn update_indexes(&self, key: &str, metadata: &DatasetMetadata) -> Result<()> {
        // Update source path index
        self.source_path_index
            .entry(metadata.lineage.source_path.clone())
            .or_default()
            .push(key.to_string());

        // Update content hash index
        self.content_hash_index
            .entry(metadata.content_hash.clone())
            .or_default()
            .push(key.to_string());

        // Update source system index
        self.source_system_index
            .entry(metadata.lineage.source_system.clone())
            .or_default()
            .push(key.to_string());

        // Update record ID index if present
        if let Some(ids) = metadata.custom_metadata.get("record_ids") {
            for id in ids.split(',') {
                self.record_id_index
                    .entry(id.trim().to_string())
                    .or_default()
                    .push(key.to_string());
            }
        }

        Ok(())
    }

    async fn create_marker(
        &self,
        dataset_id: &str,
        metadata_ref: &str,
        schema_version: &str,
    ) -> Result<String> {
        let marker = MetadataMarker {
            dataset_id: dataset_id.to_string(),
            metadata_ref: metadata_ref.to_string(),
            created_at: Utc::now(),
            schema_version: schema_version.to_string(),
        };

        let key = format!(".metadata/{}.json", dataset_id);
        let marker_json = serde_json::to_vec(&marker)?;

        self.data_store.put_object(&key, &marker_json).await?;

        Ok(key)
    }

    async fn get_metadata(&self, metadata_ref: &str) -> Result<DatasetMetadata> {
        let content = self.registry_store.get_object(metadata_ref).await?;
        serde_json::from_slice(&content).map_err(Into::into)
    }

    async fn find_metadata(&self, dataset_id: &str) -> Result<DatasetMetadata> {
        // First check for a marker in the data bucket
        let marker_key = format!(".metadata/{}.json", dataset_id);
        if let Ok(marker_content) = self.data_store.get_object(&marker_key).await {
            let marker: MetadataMarker = serde_json::from_slice(&marker_content)?;
            return self.get_metadata(&marker.metadata_ref).await;
        }

        // Fallback to searching the registry
        let prefix = "datasets/";
        let keys = self.registry_store.list_objects(prefix).await?;

        for key in keys {
            if key.contains(dataset_id) {
                return self.get_metadata(&key).await;
            }
        }

        Err(common::Error::Other(format!(
            "Metadata for dataset {} not found",
            dataset_id
        )))
    }

    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        self.registry_store.list_objects(prefix).await
    }

    async fn find_metadata_by_city_date(
        &self,
        dataset_type: &str,
        city_code: &str,
        start_date: DateTime<Utc>,
        end_date: DateTime<Utc>,
    ) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        let mut current_date = start_date.date_naive();
        let end_date = end_date.date_naive();

        while current_date <= end_date {
            let prefix = format!(
                "datasets/bronze/{}_{}_{}_{:02}_{:02}",
                dataset_type,
                city_code,
                current_date.year(),
                current_date.month(),
                current_date.day()
            );

            let keys = self.registry_store.list_objects(&prefix).await?;
            for key in keys {
                match self.get_metadata(&key).await {
                    Ok(metadata) => results.push(metadata),
                    Err(e) => println!("Failed to load metadata from {}: {}", key, e),
                }
            }

            current_date = current_date.succ_opt().unwrap();
        }

        Ok(results)
    }

    async fn list_metadata(&self, prefix: &str) -> Result<Vec<DatasetMetadata>> {
        // Default implementation that uses list_objects + get_metadata
        let keys = self.list_objects(prefix).await?;
        let mut results = Vec::new();

        for key in keys {
            match self.get_metadata(&key).await {
                Ok(metadata) => results.push(metadata),
                Err(e) => println!("Failed to load metadata from {}: {}", key, e),
            }
        }

        Ok(results)
    }
}
