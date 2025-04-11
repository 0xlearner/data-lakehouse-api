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
    async fn get_metadata(&self, metadata_ref: &str) -> Result<DatasetMetadata>;
    async fn find_metadata(&self, dataset_id: &str) -> Result<DatasetMetadata>;
    async fn list_objects(&self, prefix: &str) -> Result<Vec<String>>;
    async fn list_metadata(&self, prefix: &str) -> Result<Vec<DatasetMetadata>>;
    async fn update_indexes(&self, key: &str, metadata: &DatasetMetadata) -> Result<()>;
    async fn find_metadata_by_source_path(&self, path: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_content_hash(&self, hash: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_record_id(&self, record_id: &str) -> Result<Vec<DatasetMetadata>>;
    async fn find_metadata_by_source(&self, source_system: &str) -> Result<Vec<DatasetMetadata>>; // Renamed param for clarity
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
    // bronze_data_store: Arc<dyn ObjectStorage>, // Removed unused field
    // silver_data_store: Arc<dyn ObjectStorage>, // Removed unused field
    source_path_index: Arc<DashMap<String, Vec<String>>>,
    content_hash_index: Arc<DashMap<String, Vec<String>>>,
    record_id_index: Arc<DashMap<String, Vec<String>>>,
    source_system_index: Arc<DashMap<String, Vec<String>>>,
}

impl S3MetadataRegistry {
    pub async fn new(
        registry_store: Arc<dyn ObjectStorage>,
    ) -> Result<Self> {
        let registry = Self {
            registry_store,
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
        for key /* type: String */ in keys {
            match self.get_metadata(&key).await { // Pass &String which derefs to &str
                Ok(metadata) => {
                    // Index by source path, content hash, source system (all String)
                    self.source_path_index
                        .entry(metadata.lineage.source_path.clone())
                        .or_default()
                        .push(key.clone()); // key is String, clone is String

                    self.content_hash_index
                        .entry(metadata.content_hash.clone())
                        .or_default()
                        .push(key.clone());

                    self.source_system_index
                        .entry(metadata.lineage.source_system.clone())
                        .or_default()
                        .push(key.clone());

                    // Index by record IDs (custom metadata)
                    // --- REFINED HANDLING ---
                    if let Some(ids_value /* type: &String */) = metadata.custom_metadata.get("record_ids") {
                        // Directly split the &String (derefs to &str)
                        for id /* type: &str */ in ids_value.split(',') {
                            let trimmed_id = id.trim();
                            if !trimmed_id.is_empty() {
                                self.record_id_index
                                    .entry(trimmed_id.to_string()) // Convert &str -> String for map key
                                    .or_default()
                                    .push(key.clone()); // key is String, push String
                            }
                        }
                    }
                    // --- END REFINEMENT ---
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to load metadata for key '{}' during index build: {}",
                        key, e
                    );
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
        key: &str, // Lookup key is &str
        validator: Option<Box<dyn Fn(&DatasetMetadata) -> bool + Send + Sync>>,
    ) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        if let Some(keys_ref) = index.get(key) {
            for key_ref /* type: &String */ in keys_ref.value() {
                match self.get_metadata(key_ref).await { // key_ref (&String) -> &str
                    Ok(metadata) => {
                        if let Some(ref validate) = validator {
                            if validate(&metadata) {
                                results.push(metadata);
                            }
                        } else {
                            results.push(metadata);
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to get metadata for key '{}' during index lookup: {}",
                            key_ref, e
                        );
                    }
                }
            }
        }
        Ok(results)
    }
}

#[async_trait]
impl MetadataRegistry for S3MetadataRegistry {
    // --- find_metadata_by_* methods are likely correct assuming String fields ---
    async fn find_metadata_by_source_path(&self, path: &str) -> Result<Vec<DatasetMetadata>> {
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
        let hash_owned = hash.to_string();
        self.find_metadata_by_index(
            &self.content_hash_index,
            hash,
            Some(Box::new(move |metadata| {
                metadata.content_hash == hash_owned
            })),
        )
        .await
    }

    async fn find_metadata_by_record_id(&self, record_id: &str) -> Result<Vec<DatasetMetadata>> {
        self.find_metadata_by_index(&self.record_id_index, record_id, None) // No validator needed for direct key lookup
            .await
    }

    async fn find_metadata_by_source(&self, source_system: &str) -> Result<Vec<DatasetMetadata>> {
        let source_owned = source_system.to_string();
        self.find_metadata_by_index(
            &self.source_system_index,
            source_system,
            Some(Box::new(move |metadata| {
                metadata.lineage.source_system == source_owned
            })),
        )
        .await
    }
    // --- END ---

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

        let metadata_json = serde_json::to_vec(&metadata)?;
        self.registry_store.put_object(&key, &metadata_json).await?;

        self.update_indexes(&key, &metadata).await?;

        Ok(key)
    }

    async fn update_indexes(&self, key: &str, metadata: &DatasetMetadata) -> Result<()> {
        // Update required String field indexes
        self.source_path_index
            .entry(metadata.lineage.source_path.clone())
            .or_default()
            .push(key.to_string()); // key is &str, convert -> String

        self.content_hash_index
            .entry(metadata.content_hash.clone())
            .or_default()
            .push(key.to_string());

        self.source_system_index
            .entry(metadata.lineage.source_system.clone())
            .or_default()
            .push(key.to_string());

        // Update record ID index (custom metadata)
        // --- REFINED HANDLING ---
        if let Some(ids_value /* type: &String */) = metadata.custom_metadata.get("record_ids") {
            // Directly split the &String (derefs to &str)
            for id /* type: &str */ in ids_value.split(',') {
                let trimmed_id = id.trim();
                if !trimmed_id.is_empty() {
                    self.record_id_index
                        .entry(trimmed_id.to_string()) // Convert &str -> String for map key
                        .or_default()
                        .push(key.to_string()); // key is &str, convert -> String
                }
            }
        }
        // --- END REFINEMENT ---

        Ok(())
    }

    async fn get_metadata(&self, metadata_ref: &str) -> Result<DatasetMetadata> {
        let content = self.registry_store.get_object(metadata_ref).await?;
        serde_json::from_slice(&content).map_err(Into::into)
    }

    async fn find_metadata(&self, dataset_id: &str) -> Result<DatasetMetadata> {
        let prefix = "datasets/";
        let keys = self.registry_store.list_objects(prefix).await?;

        for key in keys {
            if key.contains(&format!("{}.json", dataset_id))
                || key.contains(&format!("{}_", dataset_id))
            {
                match self.get_metadata(&key).await {
                    Ok(metadata) => {
                        if metadata.dataset_id == dataset_id {
                            return Ok(metadata);
                        }
                        eprintln!(
                            "Warning: Filename match for key '{}', but dataset_id field ('{}') does not match target ('{}').",
                            key, metadata.dataset_id, dataset_id
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to get metadata for potential match key '{}': {}",
                            key, e
                        );
                    }
                }
            }
        }

        Err(common::Error::NotFound(format!(
            "Metadata for dataset {} not found in registry",
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
        let end_date_naive = end_date.date_naive();

        while current_date <= end_date_naive {
            for layer in ["bronze", "silver"] {
                let prefix = format!(
                    "datasets/{}/{}_{}_{}_{:02}_{:02}",
                    layer,
                    dataset_type,
                    city_code,
                    current_date.year(),
                    current_date.month(),
                    current_date.day()
                );
                match self.registry_store.list_objects(&prefix).await {
                    Ok(keys) => {
                        for key in keys {
                            match self.get_metadata(&key).await {
                                Ok(metadata) => results.push(metadata),
                                Err(e) => eprintln!(
                                    "Warning: Failed to load metadata from {}: {}",
                                    key, e
                                ),
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!(
                            "Warning: Failed to list objects with prefix '{}': {}",
                            prefix, e
                        );
                    }
                }
            }

            match current_date.succ_opt() {
                Some(next_date) => current_date = next_date,
                None => break,
            }
        }
        Ok(results)
    }

    async fn list_metadata(&self, prefix: &str) -> Result<Vec<DatasetMetadata>> {
        let keys = self.list_objects(prefix).await?;
        let mut results = Vec::new();
        for key in keys {
            match self.get_metadata(&key).await {
                Ok(metadata) => results.push(metadata),
                Err(e) => eprintln!("Warning: Failed to load metadata from {}: {}", key, e),
            }
        }
        Ok(results)
    }
}
