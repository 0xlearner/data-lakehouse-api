use common::{Error, Result};
use datafusion::arrow::array::Array;
use datafusion::logical_expr::col;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;

pub struct DataValidator;

impl DataValidator {
    pub fn new() -> Self {
        Self
    }

    pub async fn validate_data(&self, df: &DataFrame) -> Result<()> {
        self.validate_json_fields(df).await?;
        self.validate_timestamps(df).await?;
        Ok(())
    }

    async fn validate_json_fields(&self, df: &DataFrame) -> Result<()> {
        let is_valid_json = df.registry().udf("is_valid_json")?;

        // Create validation expressions for each JSON field
        let validation_df = df.clone().select(vec![
            is_valid_json
                .call(vec![col("details")])
                .alias("valid_details"),
            is_valid_json
                .call(vec![col("reviews")])
                .alias("valid_reviews"),
            is_valid_json
                .call(vec![col("ratings")])
                .alias("valid_ratings"),
        ])?;

        // Count records with invalid JSON
        let invalid_json_count = validation_df
            .filter(
                col("valid_details")
                    .and(col("valid_reviews"))
                    .and(col("valid_ratings"))
                    .not(),
            )?
            .count()
            .await?;

        if invalid_json_count > 0 {
            return Err(Error::DataValidation(format!(
                "Found {} records with invalid JSON data",
                invalid_json_count
            )));
        }

        Ok(())
    }

    async fn validate_timestamps(&self, df: &DataFrame) -> Result<()> {
        let to_timestamp = df.registry().udf("to_timestamp")?;

        let validation_df = df.clone().select(vec![
            to_timestamp
                .call(vec![col("extraction_started_at")])
                .alias("valid_extraction_started_at"),
            to_timestamp
                .call(vec![col("extraction_completed_at")])
                .alias("valid_extraction_completed_at"),
        ])?;

        let invalid_timestamps = validation_df
            .filter(col("valid_extraction_started_at").gt(col("valid_extraction_completed_at")))?
            .count()
            .await?;

        if invalid_timestamps > 0 {
            return Err(Error::DataValidation(format!(
                "Found {} records where completion time is before start time",
                invalid_timestamps
            )));
        }

        Ok(())
    }

    pub async fn extract_record_ids(&self, df: &DataFrame) -> Result<Vec<String>> {
        let id_column = "code";

        // Print the specific type for debugging
        println!(
            "Code column type: {:?}",
            df.schema().field_with_name(None, id_column)?.data_type()
        );

        // Create a SQL query to get the values as strings
        let sql_expr = format!("{} as id_string", id_column);
        let df_with_string = df.clone().select(vec![df.parse_sql_expr(&sql_expr)?])?;

        // Collect the results
        let batches = df_with_string.collect().await?;
        let mut record_ids = Vec::new();

        for batch in batches {
            println!("Batch schema: {:?}", batch.schema());
            let col_idx = batch.schema().index_of("id_string")?;
            let array = batch.column(col_idx);

            // Try different array type handling approaches
            if let Some(utf8_array) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
                for i in 0..batch.num_rows() {
                    if !utf8_array.is_null(i) {
                        record_ids.push(utf8_array.value(i).to_string());
                    }
                }
            } else if let Some(large_utf8_array) = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
            {
                for i in 0..batch.num_rows() {
                    if !large_utf8_array.is_null(i) {
                        record_ids.push(large_utf8_array.value(i).to_string());
                    }
                }
            } else {
                // Fallback: Convert to string manually
                for i in 0..batch.num_rows() {
                    let scalar_value = ScalarValue::try_from_array(array, i)?;
                    let string_val = scalar_value.to_string();
                    // Remove quotes if they exist (often present in string representations)
                    let clean_val = string_val.trim_matches('"').to_string();
                    record_ids.push(clean_val);
                }
            }
        }

        println!("Extracted {} record IDs", record_ids.len());
        Ok(record_ids)
    }
}
