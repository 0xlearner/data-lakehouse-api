use super::*;
use crate::processor::deduplication::{DeduplicationValidator, DuplicateType};
use crate::processor::metadata::MetadataRegistry;
use chrono::{DateTime, Utc};
use common::{Error, Result};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use std::collections::HashSet;

pub struct DedupHandler {
    metadata_registry: Arc<dyn MetadataRegistry>,
}

impl DedupHandler {
    pub fn new(metadata_registry: Arc<dyn MetadataRegistry>) -> Self {
        Self { metadata_registry }
    }

    /// Check for duplicates and process data accordingly
    pub async fn process_duplicates(
        &self,
        df: DataFrame,
        source_path: &str,
        file_content: Option<&[u8]>,
        record_ids: &[String],
    ) -> Result<(DataFrame, DedupMetrics)> {
        let start_time = Utc::now();

        // Get total count early before moving df
        let total_records_count = df.clone().count().await? as usize;

        // 1. Check file-level duplicates if content is available
        if let Some(content) = file_content {
            let dup_check = DeduplicationValidator::check_file_duplicate(
                &*self.metadata_registry,
                source_path,
                content,
            )
            .await?;

            if dup_check.is_duplicate {
                return Err(match dup_check.duplicate_type {
                    Some(DuplicateType::ContentHash) => Error::DuplicateData(format!(
                        "Exact duplicate file content detected. Reference: {}",
                        dup_check.existing_metadata_ref.unwrap_or_default()
                    )),
                    Some(DuplicateType::SourcePath) => Error::DuplicateData(format!(
                        "Same source path processed recently. Reference: {}",
                        dup_check.existing_metadata_ref.unwrap_or_default()
                    )),
                    _ => Error::DuplicateData("Duplicate data detected".into()),
                });
            }
        }

        // 2. Check freshness using existing validator
        if let Some(latest) = DeduplicationValidator::get_latest_ingestion_timestamp(
            &*self.metadata_registry,
            source_path,
        )
        .await?
        {
            if Utc::now() <= latest {
                return Err(common::Error::StaleData("No new data to process".into()));
            }
        }

        // 3. Check record-level duplicates using existing validator
        let duplicates =
            DeduplicationValidator::check_record_duplicates(&*self.metadata_registry, record_ids)
                .await?;

        println!(
            "Found {} duplicates out of {} records",
            duplicates.len(),
            record_ids.len()
        );

        // Early return if all records are duplicates
        if duplicates.len() == total_records_count {
            println!("All records are duplicates, skipping further processing");
            // Create metrics to log
            let metrics = DedupMetrics {
                total_records: total_records_count,
                duplicate_count: duplicates.len(),
                unique_count: 0,
                processing_time_ms: (Utc::now() - start_time).num_milliseconds(),
                processed_at: Utc::now(),
            };

            // Log metrics for visibility
            self.log_metrics(&metrics);

            // Return empty DataFrame with metrics
            let ctx = SessionContext::new();
            let empty_df = ctx.read_empty()?.limit(0, Some(0))?;

            return Ok((empty_df, metrics));
        }

        // 4. Filter out duplicate records - pass a clone to prevent ownership issues
        let (filtered_df, remaining_count) =
            self.filter_duplicates(df.clone(), &duplicates).await?;

        // 5. Create metrics - we already have total_records_count
        let metrics = DedupMetrics {
            total_records: total_records_count,
            duplicate_count: duplicates.len(),
            unique_count: remaining_count,
            processing_time_ms: (Utc::now() - start_time).num_milliseconds(),
            processed_at: Utc::now(),
        };

        Ok((filtered_df, metrics))
    }

    async fn filter_duplicates(
        &self,
        df: DataFrame,
        duplicates: &HashSet<String>,
    ) -> Result<(DataFrame, usize)> {
        // Get the count first
        let total_count = df.clone().count().await? as usize;

        if duplicates.is_empty() {
            return Ok((df, total_count));
        }

        let id_column = "code";

        // Create an expression for filtering
        let duplicate_literals: Vec<Expr> = duplicates
            .iter()
            .map(|id| lit(ScalarValue::Utf8(Some(id.clone()))))
            .collect();

        // IMPORTANT: The third parameter to in_list is a boolean that determines whether to use
        // IN (true) or NOT IN (false). Since we want to filter OUT the duplicates, we need
        // to use NOT IN, which means we should use false.
        //
        // But there's a catch: we need to invert the entire operation because we want to EXCLUDE
        // records that are duplicates, not INCLUDE them.

        // Option 1: Using NOT IN (col NOT IN duplicates) to only keep non-duplicates
        let expr = col(id_column).in_list(duplicate_literals, true); // false = NOT IN

        // Apply the filter - this keeps rows where the ID is NOT in the duplicates list
        let filtered_df = df.filter(expr)?;

        let remaining_count = filtered_df.clone().count().await? as usize;

        // Sanity check: remaining_count should be (total_count - duplicates.len())
        // If this is not the case, we have an issue
        if remaining_count != total_count - duplicates.len() {
            println!(
                "WARNING: Expected {} remaining records but got {}",
                total_count - duplicates.len(),
                remaining_count
            );
        }

        Ok((filtered_df, remaining_count))
    }

    pub fn log_metrics(&self, metrics: &DedupMetrics) {
        println!(
            "[{}] Deduplication Results:",
            metrics.processed_at.format("%Y-%m-%d %H:%M:%S")
        );
        println!("Total records processed: {}", metrics.total_records);
        println!("Duplicate records found: {}", metrics.duplicate_count);
        println!("Unique records remaining: {}", metrics.unique_count);
        println!(
            "Duplicate percentage: {:.2}%",
            (metrics.duplicate_count as f64 / metrics.total_records as f64) * 100.0
        );
        println!("Processing time: {}ms", metrics.processing_time_ms);
    }
}

#[derive(Debug, Clone)]
pub struct DedupMetrics {
    pub total_records: usize,
    pub duplicate_count: usize,
    pub unique_count: usize,
    pub processing_time_ms: i64,
    pub processed_at: DateTime<Utc>,
}
