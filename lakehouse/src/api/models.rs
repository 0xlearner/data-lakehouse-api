use serde::{Deserialize, Serialize};

// Request models
#[derive(Deserialize)]
pub struct VendorQueryParams {
    pub city_id: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub limit: Option<usize>,
    pub stream: Option<bool>,
}

#[derive(Deserialize)]
pub struct DataLayerQuery {
    pub layer: Option<String>, // "raw" or "bronze"
}

// Response models
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PaginatedResponse<T> {
    pub data: T,
    pub pagination: Pagination,
}

#[derive(Debug, Serialize)]
pub struct Pagination {
    pub offset: usize,
    pub limit: usize,
    pub has_more: bool,
}
