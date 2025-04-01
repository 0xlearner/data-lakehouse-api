use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use common::Result;
use arrow::datatypes::Schema;
use datafusion::datasource::file_format::options::ParquetReadOptions;


#[async_trait]
pub trait TableRegistry: Send + Sync + 'static {
    async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        file_path: &str,
        schema: &Schema,
    ) -> Result<()>;
    
    async fn deregister_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<()>;
}

pub struct ParquetTableRegistry;

#[async_trait]
impl TableRegistry for ParquetTableRegistry {
    async fn register_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
        file_path: &str,
        schema: &Schema,
    ) -> Result<()> {
        
        let options = ParquetReadOptions::default()
            .schema(schema)
            .table_partition_cols(vec![]);
            
        ctx.register_parquet(table_name, file_path, options)
            .await
            .map_err(|e| common::Error::Other(format!(
                "Failed to register {} at {}: {}",
                table_name, file_path, e
            )))
    }

    async fn deregister_table(
        &self,
        ctx: &SessionContext,
        table_name: &str,
    ) -> Result<()> {
        ctx.deregister_table(table_name)?;
        Ok(())
    }
}