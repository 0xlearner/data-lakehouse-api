// models.rs
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetMetadata {
    pub dataset_id: String,
    pub layer: String,
    pub schema: SchemaMetadata,
    pub lineage: LineageMetadata,
    pub processing: ProcessingMetadata,
    pub governance: GovernanceMetadata,
    pub metrics: QualityMetrics,
    #[serde(default)]
    pub custom_metadata: HashMap<String, String>,
    pub content_hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SchemaMetadata {
    pub schema_json: String,
    pub version: String,
    pub last_modified: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LineageMetadata {
    pub source_system: String,
    pub source_path: String,
    pub target_path: String,
    pub parents: Vec<String>,
    pub transformation: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProcessingMetadata {
    pub timestamp: DateTime<Utc>,
    pub job_id: String,
    pub duration_secs: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GovernanceMetadata {
    pub classification: String,
    pub retention_days: u32,
    pub owner: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QualityMetrics {
    pub record_count: u64,
    pub null_percentage: f64,
    pub checksum: String,
    pub calculated_at: DateTime<Utc>,
    pub column_stats: HashMap<String, ColumnStats>,
    pub duplicate_count: u64,
    pub unique_record_count: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ColumnStats {
    pub null_count: u64,
    pub distinct_count: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetadataMarker {
    pub dataset_id: String,
    pub metadata_ref: String,
    pub created_at: DateTime<Utc>,
    pub schema_version: String,
    pub table_name: Option<String>,
}
