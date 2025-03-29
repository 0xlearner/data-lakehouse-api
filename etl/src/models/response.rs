use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct VendorListResponse {
    pub data: VendorData,
}

#[derive(Debug, Deserialize)]
pub struct VendorData {
    pub items: Vec<VendorItem>,
    pub returned_count: i32,
    pub available_count: i32,
}

#[derive(Debug, Deserialize)]
pub struct VendorItem {
    pub code: String,
}

#[derive(Debug, Deserialize)]
pub struct VendorDetailResponse {
    pub data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct ReviewsResponse {
    pub data: Vec<serde_json::Value>,
}
