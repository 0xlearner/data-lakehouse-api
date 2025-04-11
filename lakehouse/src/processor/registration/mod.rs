use crate::models::schema::{VendorSchemaVersion, bronze_vendors_schema, raw_vendors_schema};
use crate::processor::LakehouseProcessor;
use arrow::datatypes::Schema;
use common::Result;
use std::sync::Arc;

impl LakehouseProcessor {
    pub async fn register_parquet_table(
        &self,
        table_name: &str,
        file_path: &str,
        schema_version: VendorSchemaVersion,
    ) -> Result<()> {
        let cache_key = match schema_version {
            VendorSchemaVersion::Raw => "raw_vendors",
            VendorSchemaVersion::Bronze => "bronze_vendors",
        };

        let schema = match self
            .schema_manager
            .validate_and_get_schema(cache_key, file_path)
            .await?
        {
            Some(schema) => schema,
            None => {
                let schema = match schema_version {
                    VendorSchemaVersion::Raw => raw_vendors_schema(),
                    VendorSchemaVersion::Bronze => bronze_vendors_schema(),
                };
                let schema_arc = Arc::new(schema.clone());
                self.schema_manager.cache_schema(cache_key, schema);
                schema_arc
            }
        };

        self.register_parquet_with_schema(table_name, file_path, &schema)
            .await
    }

    pub async fn register_parquet_with_schema(
        &self,
        table_name: &str,
        file_path: &str,
        schema: &Schema,
    ) -> Result<()> {
        self.table_registry
            .register_table(&self.ctx, table_name, file_path, schema)
            .await
    }

    pub async fn register_raw_vendors(&self, table_name: &str, file_path: &str) -> Result<()> {
        self.register_parquet_table(table_name, file_path, VendorSchemaVersion::Raw)
            .await
    }

    pub async fn register_bronze_vendors(&self, table_name: &str, file_path: &str) -> Result<()> {
        self.register_parquet_table(table_name, file_path, VendorSchemaVersion::Bronze)
            .await
    }
}
