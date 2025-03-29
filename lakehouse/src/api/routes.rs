use axum::{
    response::sse::{Event, Sse},
    routing::get,
    Router,
    extract::{State, Query, Path},
    response::IntoResponse,
    response::Response,
    body::Body,
    Json
};
use std::sync::Arc;
use futures::stream::{self, Stream};
use std::convert::Infallible;
use serde_json::json;

use crate::services::lakehouse::LakehouseService;
use super::models::{ApiResponse, VendorQueryParams, DataLayerQuery};
use crate::services::AppError;

// Unified streaming endpoint for both raw and bronze
pub async fn stream_vendors(
    Path(layer): Path<String>,
    Query(params): Query<VendorQueryParams>,
    State(service): State<Arc<LakehouseService>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = stream::unfold(
        (0, service, params, layer),
        |(current_offset, service, params, layer)| async move {
            let batch_size = 1000;
            let result = match layer.as_str() {
                "raw" => service.query_raw_vendors_by_date(
                    &params.city_id,
                    params.year,
                    params.month,
                    params.day,
                    batch_size,
                ).await,
                "bronze" => service.query_bronze_vendors_by_date(
                    &params.city_id,
                    params.year,
                    params.month,
                    params.day,
                    batch_size,
                ).await,
                _ => Err(common::Error::InvalidInput("Invalid data layer".into())),
            };

            match result {
                Ok(batch) => {
                    if batch.is_empty() {
                        None
                    } else {
                        let event = Event::default().json_data(batch).unwrap();
                        Some((Ok(event), (current_offset + batch_size, service, params, layer)))
                    }
                }
                Err(e) => {
                    let error_event = Event::default().json_data(json!({
                        "error": e.to_string()
                    })).unwrap();
                    Some((Ok(error_event), (current_offset + batch_size, service, params, layer)))
                }
            }
        }
    );

    Sse::new(stream)
}

pub async fn query_vendors(
    Query(params): Query<VendorQueryParams>,
    Query(layer): Query<DataLayerQuery>,
    State(service): State<Arc<LakehouseService>>,
) -> Result<Response<Body>, AppError> {
    let limit = params.limit.unwrap_or(100).min(1000);

    if params.stream.unwrap_or(false) {
        let layer = layer.layer.unwrap_or("bronze".to_string());
        let stream_response = stream_vendors(Path(layer), Query(params), State(service)).await;
        return Ok(stream_response.into_response());
    }

    let result = match layer.layer.as_deref().unwrap_or("bronze") {
        "raw" => service.query_raw_vendors_by_date(
            &params.city_id,
            params.year,
            params.month,
            params.day,
            limit,
        ).await
        .map_err(AppError::from)?,
        "bronze" => service.query_bronze_vendors_by_date(
            &params.city_id,
            params.year,
            params.month,
            params.day,
            limit,
        ).await
        .map_err(AppError::from)?,
        _ => return Err(AppError::bad_request("Invalid data layer".into())),
    };

    Ok(Json(ApiResponse::success(json!({
        "data": result,
        "pagination": {
            "limit": limit,
            "has_more": result.len() == limit
        }
    }))).into_response())
}
// Define all API routes
pub fn routes(service: Arc<LakehouseService>) -> Router {
    Router::new()
        .route("/api/vendors", get(query_vendors))
        .route("/api/vendors/{layer}/stream", get(stream_vendors))
        .with_state(service)
}