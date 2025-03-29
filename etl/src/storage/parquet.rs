use common::Result;
use crate::models::Vendor;
use arrow::array::{Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetConverter;

impl ParquetConverter {
    pub fn convert_vendors_to_parquet(vendors: &[Vendor], output_path: &str) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("details", DataType::Utf8, true),
            Field::new("batch_number", DataType::Int32, false),
            Field::new("reviews", DataType::Utf8, true),
            Field::new("ratings", DataType::Utf8, true),
            Field::new("extraction_started_at", DataType::Int64, false),
            Field::new("extraction_completed_at", DataType::Int64, false),
        ]));

        // Create owned String vectors first
        let details_strings: Vec<Option<String>> = vendors
            .iter()
            .map(|v| {
                v.details
                    .as_ref()
                    .map(|d| serde_json::to_string(d).unwrap_or_default())
            })
            .collect();

        let reviews_strings: Vec<Option<String>> = vendors
            .iter()
            .map(|v| {
                v.reviews
                    .as_ref()
                    .map(|r| serde_json::to_string(r).unwrap_or_default())
            })
            .collect();

        let ratings_strings: Vec<Option<String>> = vendors
            .iter()
            .map(|v| {
                v.ratings
                    .as_ref()
                    .map(|r| serde_json::to_string(r).unwrap_or_default())
            })
            .collect();

        // Now create the arrays using references to the owned strings
        let codes: StringArray = vendors.iter().map(|v| Some(v.code.as_str())).collect();

        let names: StringArray = vendors.iter().map(|v| Some(v.name.as_str())).collect();

        let details: StringArray = details_strings.iter().map(|s| s.as_deref()).collect();

        let batch_numbers: Int32Array = vendors.iter().map(|v| Some(v.batch_number)).collect();

        let reviews: StringArray = reviews_strings.iter().map(|s| s.as_deref()).collect();

        let ratings: StringArray = ratings_strings.iter().map(|s| s.as_deref()).collect();

        let extraction_started_at: Int64Array = vendors
            .iter()
            .map(|v| Some(v.extraction_started_at.timestamp()))
            .collect();

        let extraction_completed_at: Int64Array = vendors
            .iter()
            .map(|v| Some(v.extraction_completed_at.timestamp()))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(codes),
                Arc::new(names),
                Arc::new(details),
                Arc::new(batch_numbers),
                Arc::new(reviews),
                Arc::new(ratings),
                Arc::new(extraction_started_at),
                Arc::new(extraction_completed_at),
            ],
        )?;

        let file = File::create(output_path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(())
    }
}
