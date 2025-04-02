use super::metadata::MetadataRegistry;
use chrono::{DateTime, Utc};
use common::Result;
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub struct DeduplicationValidator;

#[derive(Debug)]
pub struct DuplicationCheckResult {
    pub is_duplicate: bool,
    pub duplicate_type: Option<DuplicateType>,
    pub existing_metadata_ref: Option<String>,
}

#[derive(Debug)]
pub enum DuplicateType {
    ContentHash,
    SourcePath,
    RecordId,
}

impl DeduplicationValidator {
    pub async fn check_file_duplicate(
        registry: &dyn MetadataRegistry,
        file_path: &str,
        file_content: &[u8],
    ) -> Result<DuplicationCheckResult> {
        // Calculate hash first
        let mut hasher = Sha256::new();
        hasher.update(file_content);
        let file_hash = format!("{:x}", hasher.finalize());

        // Check by content hash first as it's most reliable
        let existing_by_hash = registry.find_metadata_by_content_hash(&file_hash).await?;
        if !existing_by_hash.is_empty() {
            // Use the dataset_id from the first found metadata entry
            let dataset_id = existing_by_hash
                .first()
                .map(|meta| meta.dataset_id.clone())
                .unwrap_or_else(|| "unknown".to_string()); // Provide a fallback if somehow empty

            return Ok(DuplicationCheckResult {
                is_duplicate: true,
                duplicate_type: Some(DuplicateType::ContentHash),
                existing_metadata_ref: Some(dataset_id),
            });
        }

        // Then check by source path
        let existing_by_path = registry.find_metadata_by_source_path(file_path).await?;
        if !existing_by_path.is_empty() {
            // If any metadata exists for this source path, consider it a duplicate.
            // No need to check the timestamp anymore based on the current logic.
            // Use the dataset_id from the first found metadata entry
            let dataset_id = existing_by_path
                .first()
                .map(|meta| meta.dataset_id.clone())
                .unwrap_or_else(|| "unknown".to_string()); // Provide a fallback

            return Ok(DuplicationCheckResult {
                is_duplicate: true,
                duplicate_type: Some(DuplicateType::SourcePath),
                existing_metadata_ref: Some(dataset_id),
            });
        }

        Ok(DuplicationCheckResult {
            is_duplicate: false,
            duplicate_type: None,
            existing_metadata_ref: None,
        })
    }

    pub async fn check_record_duplicates(
        registry: &dyn MetadataRegistry,
        record_ids: &[String],
    ) -> Result<HashSet<String>> {
        let mut duplicates = HashSet::new();

        // Check records in batches to avoid too many API calls
        const BATCH_SIZE: usize = 100;
        for chunk in record_ids.chunks(BATCH_SIZE) {
            let futures = chunk
                .iter()
                .map(|record_id| registry.find_metadata_by_record_id(record_id));

            // Process batch of futures concurrently
            let results = futures::future::join_all(futures).await;

            for (record_id, result) in chunk.iter().zip(results) {
                match result {
                    Ok(existing) if !existing.is_empty() => {
                        // Only consider records processed within the last 24 hours as duplicates
                        let latest = existing.iter().map(|m| m.processing.timestamp).max();

                        if let Some(timestamp) = latest {
                            if (Utc::now() - timestamp).num_hours() < 24 {
                                duplicates.insert(record_id.clone());
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => println!("Error checking record {}: {}", record_id, e),
                }
            }
        }

        Ok(duplicates)
    }

    pub async fn get_latest_ingestion_timestamp(
        registry: &dyn MetadataRegistry,
        source_path: &str,
    ) -> Result<Option<DateTime<Utc>>> {
        let metadata_list = registry.find_metadata_by_source(source_path).await?;

        let latest = metadata_list
            .into_iter()
            .map(|m| m.processing.timestamp)
            .max();

        Ok(latest)
    }
}
