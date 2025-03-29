// src/processor/metadata.rs
use common::Result;
use chrono::{DateTime, Utc};
use parquet::format::KeyValue;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::storage::s3::ObjectStorage;
use uuid::Uuid;
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ParquetMetadata {
    pub processing_timestamp: i64,
    pub source_identifier: String,
    pub version: i32,
    pub record_count: i64,
    pub schema_version: String,
    pub checksum: String,
    pub target_path: String,
    pub source_path: String,
    pub target_key: String,
    #[serde(default)]
    pub additional_metadata: HashMap<String, String>,
}

impl ParquetMetadata {
    pub fn new(
        source_identifier: impl Into<String>,
        record_count: i64,
        source_path: impl Into<String>,
        target_path: impl Into<String>
    ) -> Self {
        Self {
            processing_timestamp: Utc::now().timestamp(),
            source_identifier: source_identifier.into(),
            version: 1,
            record_count,
            schema_version: "1.0".to_string(),
            checksum: Uuid::new_v4().to_string(),
            target_path: target_path.into(),
            source_path: source_path.into(),
            target_key: String::new(),
            additional_metadata: HashMap::new(),
        }
    }

    pub fn with_additional_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.additional_metadata.insert(key.into(), value.into());
        self
    }

    pub fn to_key_value_metadata(&self) -> Vec<KeyValue> {
        let mut metadata = vec![
            KeyValue {
                key: "processing_timestamp".to_string(),
                value: Some(self.processing_timestamp.to_string()),
            },
            KeyValue {
                key: "source_identifier".to_string(),
                value: Some(self.source_identifier.clone()),
            },
            KeyValue {
                key: "version".to_string(),
                value: Some(self.version.to_string()),
            },
            KeyValue {
                key: "record_count".to_string(),
                value: Some(self.record_count.to_string()),
            },
            KeyValue {
                key: "schema_version".to_string(),
                value: Some(self.schema_version.clone()),
            },
            KeyValue {
                key: "checksum".to_string(),
                value: Some(self.checksum.clone()),
            },
            KeyValue {
                key: "target_path".to_string(),
                value: Some(self.target_path.clone()),
            },
            KeyValue {
                key: "source_path".to_string(),
                value: Some(self.source_path.clone()),
            },
            KeyValue {
                key: "target_key".to_string(),
                value: Some(self.target_key.clone()),
            },
        ];
    
        // Add additional metadata
        metadata.extend(
            self.additional_metadata
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(v.clone()),
                })
        );
        
        metadata
    }

    pub fn from_key_value_metadata(kv_metadata: &[KeyValue]) -> Option<Self> {
        let metadata: HashMap<String, String> = kv_metadata
            .iter()
            .filter_map(|kv| kv.value.as_ref().map(|v| (kv.key.clone(), v.clone())))
            .collect();

        Some(Self {
            processing_timestamp: metadata.get("processing_timestamp")?.parse().ok()?,
            source_identifier: metadata.get("source_identifier")?.clone(),
            version: metadata.get("version")?.parse().ok()?,
            record_count: metadata.get("record_count")?.parse().ok()?,
            schema_version: metadata.get("schema_version")?.clone(),
            checksum: metadata.get("checksum")?.clone(),
            target_path: metadata.get("target_path")?.clone(),
            source_path: metadata.get("source_path")?.clone(),
            target_key: metadata.get("target_key")?.clone(),
            additional_metadata: metadata
                .into_iter()
                .filter(|(k, _)| !matches!(k.as_str(), 
                    "processing_timestamp" | "source_identifier" | "version" | 
                    "record_count" | "schema_version" | "checksum" | 
                    "target_path" | "source_path" | "target_key"
                ))
                .collect(),
        })
    }
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn store_metadata(&self, metadata: ParquetMetadata) -> Result<String>;
    async fn get_metadata(&self, key: &str) -> Result<ParquetMetadata>;
    async fn list_metadata(&self, prefix: &str) -> Result<Vec<ParquetMetadata>>;
    async fn update_metadata(&self, key: &str, updates: HashMap<String, String>) -> Result<ParquetMetadata>;
}

pub struct S3MetadataStore {
    storage: Arc<dyn ObjectStorage>,
}

impl S3MetadataStore {
    pub fn new(storage: Arc<dyn ObjectStorage>) -> Self {
        Self { storage }
    }

    fn generate_metadata_key(&self) -> String {
        format!(
            "metadata/{}/{}.json",
            Utc::now().format("%Y/%m/%d"),
            Uuid::new_v4()
        )
    }
}

#[async_trait]
impl MetadataStore for S3MetadataStore {
    async fn store_metadata(&self, metadata: ParquetMetadata) -> Result<String> {
        let key = self.generate_metadata_key();
        let metadata_json = serde_json::to_vec(&metadata)?;
        self.storage.put_object(&key, &metadata_json).await?;
        Ok(key)
    }

    async fn get_metadata(&self, key: &str) -> Result<ParquetMetadata> {
        let content = self.storage.get_object(key).await?;
        serde_json::from_slice(&content).map_err(Into::into)
    }

    async fn list_metadata(&self, prefix: &str) -> Result<Vec<ParquetMetadata>> {
        let keys = self.storage.list_objects(prefix).await?;
        let mut results = Vec::with_capacity(keys.len());
        
        for key in keys {
            match self.get_metadata(&key).await {
                Ok(metadata) => results.push(metadata),
                Err(e) => println!("Failed to load metadata from {}: {}", key, e),
            }
        }
        
        Ok(results)
    }

    async fn update_metadata(&self, key: &str, updates: HashMap<String, String>) -> Result<ParquetMetadata> {
        let mut metadata = self.get_metadata(key).await?;
        
        for (key, value) in updates {
            match key.as_str() {
                "schema_version" => metadata.schema_version = value,
                "target_key" => metadata.target_key = value,
                _ => { metadata.additional_metadata.insert(key, value); }
            }
        }

        let metadata_json = serde_json::to_vec(&metadata)?;
        self.storage.put_object(key, &metadata_json).await?;
        
        Ok(metadata)
    }
}

// Utility functions for date-based metadata operations
pub async fn list_metadata_by_date_range(
    store: &dyn MetadataStore,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
) -> Result<Vec<ParquetMetadata>> {
    let mut metadata_list = Vec::new();
    let mut current_date = start_date.date_naive();
    let end_date = end_date.date_naive();

    while current_date <= end_date {
        let prefix = format!("metadata/{}", current_date.format("%Y/%m/%d"));
        let mut daily_metadata = store.list_metadata(&prefix).await?;
        metadata_list.append(&mut daily_metadata);
        current_date = current_date.succ_opt().unwrap();
    }
    
    Ok(metadata_list)
}

pub async fn search_metadata(
    store: &dyn MetadataStore,
    start_date: DateTime<Utc>,
    end_date: DateTime<Utc>,
    filters: HashMap<String, String>,
) -> Result<Vec<ParquetMetadata>> {
    let all_metadata = list_metadata_by_date_range(store, start_date, end_date).await?;
    
    Ok(all_metadata
        .into_iter()
        .filter(|metadata| {
            filters.iter().all(|(key, value)| {
                match key.as_str() {
                    "source_identifier" => metadata.source_identifier == *value,
                    "schema_version" => metadata.schema_version == *value,
                    "target_key" => metadata.target_key == *value,
                    _ => metadata.additional_metadata.get(key).map_or(false, |v| v == value),
                }
            })
        })
        .collect())
}
