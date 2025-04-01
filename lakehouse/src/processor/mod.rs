mod core;
mod registration;
mod query;
mod storage;
pub mod metadata;
pub mod table;
pub mod deduplication;
pub mod bronze;
pub mod config;
pub mod udf;

// Re-export main types and traits
pub use self::core::LakehouseProcessor;
pub use deduplication::DeduplicationValidator;
use crate::utils::paths::PathBuilder;

// Types shared across modules
#[derive(Debug, Clone)]
pub struct ProcessingRequest {
    pub city_code: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    bucket: String,
    source_files: Vec<String>,
    content: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum ProcessingType {
    S3Direct,
    FileContent(Vec<u8>),
}

// Common imports used across modules
use chrono::Utc;
use common::Result;
use std::sync::Arc;
use arrow::datatypes::Schema;
use datafusion::execution::context::SessionContext;

// ProcessingRequest implementation
impl ProcessingRequest {

    pub fn new_s3_direct_with_files(
        city_code: &str,
        year: i32,
        month: u32,
        day: u32,
        bucket: &str,
        source_files: Vec<String>,
    ) -> Self {
        Self {
            city_code: city_code.to_string(),
            year,
            month,
            day,
            bucket: bucket.to_string(),
            source_files,
            content: None,
        }
    }
}
