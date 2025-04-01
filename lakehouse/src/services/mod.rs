pub mod lakehouse;
pub mod query;
pub use lakehouse::LakehouseService;

use axum::{
    response::IntoResponse,
    http::StatusCode,
    Json
};
use crate::api::models::ApiResponse;

pub struct AppError(pub common::Error);

impl AppError {
    pub fn bad_request(message: String) -> Self {
        AppError(common::Error::InvalidInput(message))
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status_code = match self.0 {
            common::Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
            common::Error::Forbidden => StatusCode::FORBIDDEN,
            common::Error::RateLimit => StatusCode::TOO_MANY_REQUESTS,
            common::Error::GatewayTimeout => StatusCode::GATEWAY_TIMEOUT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let body = Json(ApiResponse::<()>::error(self.0.to_string()));
        (status_code, body).into_response()
    }
}

impl From<common::Error> for AppError {
    fn from(err: common::Error) -> Self {
        AppError(err)
    }
}
