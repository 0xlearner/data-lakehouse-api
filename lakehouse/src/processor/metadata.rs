// src/processor/metadata.rs
use common::Result;
use chrono::{DateTime, Utc, Datelike};
use parquet::format::KeyValue;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use crate::storage::s3::ObjectStorage;
use async_trait::async_trait;
use std::sync::Arc;
use arrow::datatypes::Schema;
use serde_json::json;
use sha2::{Sha256, Digest};
use dashmap::DashMap;

/// Comprehensive dataset metadata
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetMetadata {
    /// Unique identifier for this dataset version
    pub dataset_id: String,
    
    /// Data layer (bronze/silver/gold)
    pub layer: String,
    
    /// Schema information
    pub schema: SchemaMetadata,
    
    /// Data lineage
    pub lineage: LineageMetadata,
    
    /// Processing information
    pub processing: ProcessingMetadata,
    
    /// Governance information
    pub governance: GovernanceMetadata,
    
    /// Quality metrics
    pub metrics: QualityMetrics,
    
    /// Additional custom metadata
    #[serde(default)]
    pub custom_metadata: HashMap<String, String>,

    pub content_hash: String,

    pub source_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchemaMetadata {
    /// Arrow schema in JSON format
    pub schema_json: String,
    
    /// Schema version
    pub version: String,
    
    /// Last modified timestamp
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LineageMetadata {
    /// Source system identifier
    pub source_system: String,
    
    /// Source file/dataset path
    pub source_path: String,
    
    /// Target path for the processed data
    pub target_path: String,
    
    /// Parent dataset IDs
    pub parents: Vec<String>,
    
    /// Transformation description
    pub transformation: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProcessingMetadata {
    /// Processing timestamp
    pub timestamp: DateTime<Utc>,
    
    /// Processing job ID
    pub job_id: String,
    
    /// Processing duration in seconds
    pub duration_secs: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GovernanceMetadata {
    /// Data classification level
    pub classification: String,
    
    /// Retention period in days
    pub retention_days: u32,
    
    /// Owner contact
    pub owner: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QualityMetrics {
    /// Record count
    pub record_count: u64,
    
    /// Null value percentage
    pub null_percentage: f64,
    
    /// Checksum for data validation
    pub checksum: String,
}

/// Lightweight metadata marker stored with data
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetadataMarker {
    /// Dataset ID
    pub dataset_id: String,
    
    /// Reference to full metadata
    pub metadata_ref: String,
    
    /// Creation timestamp
    pub created_at: DateTime<Utc>,
    
    /// Quick access to schema version
    pub schema_version: String,
}



#[async_trait]
pub trait MetadataRegistry: Send + Sync {
    /// Store comprehensive metadata with descriptive naming
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
    // Indexes for faster lookups
    source_path_index: Arc<DashMap<String, Vec<String>>>, // source_path → [metadata_keys]
    content_hash_index: Arc<DashMap<String, Vec<String>>>, // content_hash → [metadata_keys]
    record_id_index: Arc<DashMap<String, Vec<String>>>,    // record_id → [metadata_keys]
    source_system_index: Arc<DashMap<String, Vec<String>>>, // source_system → [metadata_keys]
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
        
        // Build initial index
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
    
    /// Generate a descriptive metadata key
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
            layer,
            dataset_type,
            city_code,
            year,
            month,
            day,
            timestamp
        )
    }
}

#[async_trait]
impl MetadataRegistry for S3MetadataRegistry {
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

    async fn find_metadata_by_source_path(&self, path: &str) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        if let Some(keys) = self.source_path_index.get(path) {
            for key in keys.value() {
                if let Ok(metadata) = self.get_metadata(key).await {
                    results.push(metadata);
                }
            }
        }
        Ok(results)
    }

    async fn find_metadata_by_content_hash(&self, hash: &str) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        if let Some(keys) = self.content_hash_index.get(hash) {
            for key in keys.value() {
                if let Ok(metadata) = self.get_metadata(key).await {
                    results.push(metadata);
                }
            }
        }
        Ok(results)
    }

    async fn find_metadata_by_record_id(&self, record_id: &str) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        if let Some(keys) = self.record_id_index.get(record_id) {
            for key in keys.value() {
                if let Ok(metadata) = self.get_metadata(key).await {
                    results.push(metadata);
                }
            }
        }
        Ok(results)
    }

    async fn find_metadata_by_source(&self, source: &str) -> Result<Vec<DatasetMetadata>> {
        let mut results = Vec::new();
        if let Some(keys) = self.source_system_index.get(source) {
            for key in keys.value() {
                if let Ok(metadata) = self.get_metadata(key).await {
                    results.push(metadata);
                }
            }
        }
        Ok(results)
    }
}

/// Utility functions for converting between metadata formats
pub mod convert {
    use super::*;

    /// Convert DatasetMetadata to parquet key-value metadata
    pub fn dataset_to_parquet_metadata(metadata: &DatasetMetadata) -> Vec<KeyValue> {
        let mut kv_metadata = vec![
            KeyValue {
                key: "dataset_id".to_string(),
                value: Some(metadata.dataset_id.clone()),
            },
            KeyValue {
                key: "layer".to_string(),
                value: Some(metadata.layer.clone()),
            },
            KeyValue {
                key: "schema_version".to_string(),
                value: Some(metadata.schema.version.clone()),
            },
            KeyValue {
                key: "record_count".to_string(),
                value: Some(metadata.metrics.record_count.to_string()),
            },
            KeyValue {
                key: "source_path".to_string(),
                value: Some(metadata.lineage.source_path.clone()),
            },
            KeyValue {
                key: "processing_timestamp".to_string(),
                value: Some(metadata.processing.timestamp.to_rfc3339()),
            },
        ];

        // Add custom metadata
        kv_metadata.extend(
            metadata.custom_metadata
                .iter()
                .map(|(k, v)| KeyValue {
                    key: k.to_string(),
                    value: Some(v.clone()),
                })
        );

        kv_metadata
    }

    fn schema_to_json(schema: &Schema) -> Result<String> {
        let fields = schema.fields().iter().map(|field| {
            json!({
                "name": field.name(),
                "data_type": format!("{:?}", field.data_type()),
                "nullable": field.is_nullable(),
                "metadata": field.metadata(),
            })
        }).collect::<Vec<_>>();
        
        serde_json::to_string(&fields).map_err(Into::into)
    }

    /// Create DatasetMetadata from schema and processing info
    pub fn create_dataset_metadata(
        schema: &Schema,
        source_path: &str,
        target_path: &str,
        record_count: u64,
        layer: &str,
        file_content: Option<&[u8]>, // Optional for content hashing
    ) -> Result<DatasetMetadata> {
        let schema_json = schema_to_json(schema)?;
        
        // Generate content hash if file content is provided
        let content_hash = if let Some(content) = file_content {
            let mut hasher = Sha256::new();
            hasher.update(content);
            format!("{:x}", hasher.finalize())
        } else {
            // Fallback hash using schema and timestamp
            let mut hasher = Sha256::new();
            hasher.update(&schema_json);
            hasher.update(Utc::now().to_rfc3339().as_bytes());
            format!("{:x}", hasher.finalize())
        };
    
        Ok(DatasetMetadata {
            dataset_id: format!("{}_{}", layer, Utc::now().format("%Y%m%d_%H%M%S")),
            layer: layer.to_string(),
            schema: SchemaMetadata {
                schema_json,
                version: "1.0".to_string(),
                last_modified: Utc::now(),
            },
            lineage: LineageMetadata {
                source_system: "raw".to_string(),
                source_path: source_path.to_string(),
                target_path: target_path.to_string(),
                parents: Vec::new(),
                transformation: "Initial processing".to_string(),
            },
            processing: ProcessingMetadata {
                timestamp: Utc::now(),
                job_id: "bronze_processor".to_string(),
                duration_secs: 0.0,
            },
            governance: GovernanceMetadata {
                classification: "internal".to_string(),
                retention_days: 365,
                owner: "data_team".to_string(),
            },
            metrics: QualityMetrics {
                record_count,
                null_percentage: 0.0,
                checksum: "todo".to_string(),
            },
            custom_metadata: HashMap::new(),
            content_hash,
            source_path: source_path.to_string(), // Added content hash
            // source_path is already included in LineageMetadata
        })
    }
}
