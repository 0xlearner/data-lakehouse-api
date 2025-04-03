use crate::processor::metadata::DatasetMetadata;
use datafusion::common::config::TableParquetOptions;
use datafusion::prelude::*;

pub struct ProcessedData {
    pub df: DataFrame,
    pub metadata: DatasetMetadata,
    pub parquet_options: TableParquetOptions,
}
