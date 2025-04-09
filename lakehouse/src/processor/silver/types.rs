use crate::processor::metadata::DatasetMetadata;
use datafusion::common::config::TableParquetOptions;
use datafusion::prelude::*;
use std::collections::HashMap;

pub struct ProcessedSilverData {
    pub derived_tables: HashMap<String, DataFrame>,
    pub metadata: HashMap<String, DatasetMetadata>,
    pub parquet_options: HashMap<String, TableParquetOptions>,
}

pub struct SilverTable {
    pub name: String,
    pub df: DataFrame,
    pub metadata: DatasetMetadata,
    pub parquet_options: TableParquetOptions,
}

// All possible silver tables
#[derive(Debug, Clone)]
pub enum SilverTableType {
    Menus,
    MenuCategories,
    Products,
    ProductVariations,
    Chain,
    City,
    Cuisines,
    Deals,
    Discounts,
    Tags,
    VendorLegalInformation,
    VendorDetails,
    Reviews,
    RatingsDistribution,
}

impl SilverTableType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Menus => "menus",
            Self::MenuCategories => "menu_categories",
            Self::Products => "products",
            Self::ProductVariations => "product_variations",
            Self::Chain => "chain",
            Self::City => "city",
            Self::Cuisines => "cuisines",
            Self::Deals => "deals",
            Self::Discounts => "discounts",
            Self::Tags => "tags",
            Self::VendorLegalInformation => "vendor_legal_information",
            Self::VendorDetails => "vendor_details",
            Self::Reviews => "reviews",
            Self::RatingsDistribution => "ratings_distribution",
        }
    }
}
