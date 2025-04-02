// convert.rs
use super::types::*;
use arrow::datatypes::Schema;
use chrono::Utc;
use common::Result;
use datafusion::dataframe::DataFrame;
use parquet::format::KeyValue;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

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
    kv_metadata.extend(metadata.custom_metadata.iter().map(|(k, v)| KeyValue {
        key: k.to_string(),
        value: Some(v.clone()),
    }));

    kv_metadata
}

fn schema_to_json(schema: &Schema) -> Result<String> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "data_type": format!("{:?}", field.data_type()),
                "nullable": field.is_nullable(),
                "metadata": field.metadata(),
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_string(&fields).map_err(Into::into)
}

pub async fn create_dataset_metadata(
    schema: &Schema,
    source_path: &str,
    target_path: &str,
    df: &DataFrame, // Changed parameter to accept DataFrame
    layer: &str,
    file_content: Option<&[u8]>,
    record_ids: &[String],
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

    // Calculate quality metrics
    let metrics = QualityMetrics::new(df).await?;

    let mut metadata = DatasetMetadata {
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
            job_id: format!("bronze_processor_{}", Utc::now().format("%Y%m%d_%H%M%S")),
            duration_secs: 0.0,
        },
        governance: GovernanceMetadata {
            classification: "internal".to_string(),
            retention_days: 365,
            owner: "0xlearner".to_string(), // Use current user
        },
        metrics,
        custom_metadata: HashMap::new(),
        content_hash,
    };

    if !record_ids.is_empty() {
        let ids_string = record_ids.join(",");
        metadata
            .custom_metadata
            .insert("record_ids".to_string(), ids_string);
    }

    Ok(metadata)
}
