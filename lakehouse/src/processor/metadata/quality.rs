// quality.rs
use super::types::*;
use arrow::array::ArrayRef;
use chrono::Utc;
use common::Result;
use datafusion::dataframe::DataFrame;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

impl QualityMetrics {
    fn convert_array_value(column: &ArrayRef, row_idx: usize) -> Result<String> {
        match column.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        common::Error::Other("Failed to downcast to StringArray".to_string())
                    })?;
                Ok(array.value(row_idx).to_string())
            }
            arrow::datatypes::DataType::Int32 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .ok_or_else(|| {
                        common::Error::Other("Failed to downcast to Int32Array".to_string())
                    })?;
                Ok(array.value(row_idx).to_string())
            }
            arrow::datatypes::DataType::Int64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        common::Error::Other("Failed to downcast to Int64Array".to_string())
                    })?;
                Ok(array.value(row_idx).to_string())
            }
            arrow::datatypes::DataType::Float32 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Float32Array>()
                    .ok_or_else(|| {
                        common::Error::Other("Failed to downcast to Float32Array".to_string())
                    })?;
                Ok(array.value(row_idx).to_string())
            }
            arrow::datatypes::DataType::Float64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .ok_or_else(|| {
                        common::Error::Other("Failed to downcast to Float64Array".to_string())
                    })?;
                Ok(array.value(row_idx).to_string())
            }
            _ => Ok(format!("{:?}", column)),
        }
    }

    pub async fn new(df: &DataFrame) -> Result<Self> {
        let batches = df.clone().collect().await?;
        let record_count = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;
        let schema = df.schema();
        let mut column_stats = HashMap::new();
        let mut total_nulls = 0u64;

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let mut null_count = 0u64;
            let mut distinct_values = HashSet::new();
            let mut min_value: Option<String> = None;
            let mut max_value: Option<String> = None;

            for batch in &batches {
                let column = batch.column(col_idx);
                null_count += column.null_count() as u64;

                for row_idx in 0..batch.num_rows() {
                    if !column.is_null(row_idx) {
                        let value = Self::convert_array_value(column, row_idx)?;
                        distinct_values.insert(value.clone());

                        if let Some(ref mut min) = min_value {
                            if &value < min {
                                *min = value.clone();
                            }
                        } else {
                            min_value = Some(value.clone());
                        }

                        if let Some(ref mut max) = max_value {
                            if &value > max {
                                *max = value.clone();
                            }
                        } else {
                            max_value = Some(value.clone());
                        }
                    }
                }
            }

            total_nulls += null_count;

            column_stats.insert(
                field.name().clone(),
                ColumnStats {
                    null_count,
                    distinct_count: Some(distinct_values.len() as u64),
                    min_value,
                    max_value,
                },
            );
        }

        // Calculate overall null percentage
        let null_percentage = if record_count > 0 {
            (total_nulls as f64) / (record_count as f64 * schema.fields().len() as f64) * 100.0
        } else {
            0.0
        };

        // Calculate checksum using key metrics
        let mut hasher = Sha256::new();

        // Add record count to checksum
        hasher.update(record_count.to_le_bytes());

        // Add schema field names and types to checksum
        for field in schema.fields() {
            hasher.update(field.name().as_bytes());
            hasher.update(format!("{:?}", field.data_type()).as_bytes());
        }

        // Add column statistics to checksum
        for (col_name, stats) in &column_stats {
            hasher.update(col_name.as_bytes());
            hasher.update(stats.null_count.to_le_bytes());
            if let Some(distinct) = stats.distinct_count {
                hasher.update(distinct.to_le_bytes());
            }
            if let Some(ref min) = stats.min_value {
                hasher.update(min.as_bytes());
            }
            if let Some(ref max) = stats.max_value {
                hasher.update(max.as_bytes());
            }
        }

        let checksum = format!("{:x}", hasher.finalize());

        Ok(Self {
            record_count,
            null_percentage,
            checksum,
            calculated_at: Utc::now(),
            column_stats,
        })
    }
}
