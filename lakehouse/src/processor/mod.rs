pub mod bronze;
pub mod config;
mod core;
pub mod deduplication;
mod metadata;
mod query;
mod registration;
pub mod silver;
mod storage;
pub mod table;
pub mod udf;

// Re-export main types and traits
pub use self::core::LakehouseProcessor;
pub use crate::processor::deduplication::DeduplicationValidator;
use crate::utils::paths::PathBuilder;

// Types shared across modules
#[derive(Debug, Clone)]
pub struct ProcessingRequest {
    pub city_code: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    bucket: String,
    pub source_files: Vec<String>,
    content: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum ProcessingType {
    S3Direct,
    FileContent(Vec<u8>),
}

// Common imports used across modules
use arrow::datatypes::Schema;
use chrono::Utc;
use common::Result;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;

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
