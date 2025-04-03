use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit;
use chrono::Utc;
use common::Result;
use datafusion::logical_expr::ExprSchemable;
use datafusion::prelude::*;

pub struct DataTransformer {
    ctx: SessionContext,
}

impl DataTransformer {
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    pub async fn load_data(&self, source_path: &str) -> Result<DataFrame> {
        // Register the source table with a temporary name
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let temp_table = format!("temp_source_{}", timestamp);

        // Register the table
        self.ctx
            .register_parquet(&temp_table, source_path, ParquetReadOptions::default())
            .await?;

        // Read the data
        let df = self.ctx.table(&temp_table).await?;

        // Deregister the temporary table
        self.ctx.deregister_table(&temp_table)?;

        Ok(df)
    }

    pub async fn transform_data(&self, df: DataFrame) -> Result<DataFrame> {
        let mut select_exprs: Vec<Expr> =
            df.schema().fields().iter().map(|f| col(f.name())).collect();

        // Add ingested_at timestamp
        select_exprs.push(
            lit(Utc::now().timestamp_millis())
                .cast_to(
                    &DataType::Timestamp(TimeUnit::Millisecond, None),
                    df.schema(),
                )?
                .alias("ingested_at"),
        );

        // Apply transformations
        let transformed_df = df.select(select_exprs)?;

        Ok(transformed_df)
    }
}
