use super::metadata::MetadataRegistry;
use chrono::{DateTime, Utc};
use sha2::{Sha256, Digest};
use common::Result;

pub struct DeduplicationValidator;

impl DeduplicationValidator {
    pub async fn check_file_duplicate(
        registry: &dyn MetadataRegistry,
        file_path: &str,
        file_content: &[u8],
    ) -> Result<bool> {
        let mut hasher = Sha256::new();
        hasher.update(file_content);
        let file_hash = format!("{:x}", hasher.finalize());
        
        let existing_by_path = registry.find_metadata_by_source_path(file_path).await?;
        let existing_by_hash = registry.find_metadata_by_content_hash(&file_hash).await?;
        
        Ok(!existing_by_path.is_empty() || !existing_by_hash.is_empty())
    }
    
    pub async fn check_record_duplicates(
        registry: &dyn MetadataRegistry,
        record_ids: &[String],
    ) -> Result<Vec<String>> {
        let mut duplicates = Vec::new();
        
        for record_id in record_ids {
            let existing = registry.find_metadata_by_record_id(record_id).await?;
            if !existing.is_empty() {
                duplicates.push(record_id.clone());
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