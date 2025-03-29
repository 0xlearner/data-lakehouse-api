use crate::client::ClientPool;
use common::Result;
use crate::models::{
    RatingsDistribution, ReviewsResponse, VendorDetailResponse, VendorListResponse,
};
use crate::utils::retry_with_backoff;
use crate::utils::time::sleep_with_jitter;
use http::StatusCode;
use std::sync::Arc;
use tracing::{debug, error};

const MAX_RETRIES: u32 = 3;
const BASE_DELAY_MS: u64 = 1000;

#[derive(Clone)]
pub struct ApiService {
    client_pool: Arc<ClientPool>,
}

impl ApiService {
    pub fn new(client_pool: Arc<ClientPool>) -> Self {
        Self { client_pool }
    }

    pub async fn fetch_vendor_page(
        &self,
        city_id: &str,
        offset: i32,
        limit: i32,
    ) -> Result<VendorListResponse> {
        let url = format!(
            "https://disco.deliveryhero.io/listing/api/v1/pandora/vendors?\
             city_id={}&offset={}&limit={}&\
             configuration=&country=pk&language_id=1&sort=&vertical=restaurants",
            city_id, offset, limit
        );

        let client = self.client_pool.next_client();

        retry_with_backoff(MAX_RETRIES, BASE_DELAY_MS, || async {
            let request = client.get(&url);
            let response = client.send(request).await?;

            debug!(
                status = response.status().as_u16(),
                url = url,
                "API response received"
            );

            if response.status() == StatusCode::OK {
                let body = response.bytes().await?;

                if let Err(e) = serde_json::from_slice::<serde_json::Value>(&body) {
                    let body_str = String::from_utf8_lossy(&body);
                    error!(
                        error = %e,
                        body = %body_str,
                        "Invalid JSON response"
                    );
                    return Err(common::Error::Json(e));
                }

                return serde_json::from_slice(&body).map_err(|e| {
                    let body_str = String::from_utf8_lossy(&body);
                    error!(
                        error = %e,
                        body = %body_str,
                        "Failed to parse vendor page response"
                    );
                    common::Error::from(e)
                });
            }

            Err(common::Error::Http(response.error_for_status().unwrap_err()))
        })
        .await
    }

    pub async fn fetch_vendor_details(&self, code: &str) -> Result<Option<serde_json::Value>> {
        let url = format!(
            "https://pk.fd-api.com/api/v5/vendors/{}?\
             include=menus,bundles,multiple_discounts&language_id=1&\
             opening_type=delivery&basket_currency=PKR",
            code
        );

        let mut attempt = 0;
        let max_retries = MAX_RETRIES;
        let base_delay = BASE_DELAY_MS;

        loop {
            if attempt >= max_retries {
                return Err(common::Error::MaxRetriesExceeded);
            }

            let client_index =
                (self.client_pool.current_index() + attempt as usize) % self.client_pool.len();
            let client = self.client_pool.get_client(client_index);

            debug!(
                vendor_code = code,
                url = url,
                attempt = attempt + 1,
                client_index = client_index,
                "Attempting to fetch vendor details"
            );

            let request = client.get(&url);
            match client.send(request).await {
                Ok(response) => match response.status() {
                    StatusCode::OK => {
                        let body = response.bytes().await?;
                        let detail: VendorDetailResponse =
                            serde_json::from_slice(&body).map_err(|e| {
                                let body_str = String::from_utf8_lossy(&body);
                                error!(
                                    error = %e,
                                    body = %body_str,
                                    "Failed to parse vendor details response"
                                );
                                common::Error::from(e)
                            })?;
                        return Ok(Some(detail.data));
                    }
                    StatusCode::BAD_REQUEST => {
                        debug!(
                            vendor_code = code,
                            "Received 400 Bad Request for vendor details, skipping"
                        );
                        return Ok(None);
                    }
                    status => {
                        error!(
                            status = status.as_u16(),
                            vendor_code = code,
                            "Unexpected status code"
                        );
                        return Err(common::Error::Http(response.error_for_status().unwrap_err()));
                    }
                },
                Err(common::Error::Forbidden) => {
                    debug!(
                        vendor_code = code,
                        url = url,
                        client_index = client_index,
                        "Received 403, will try with different client"
                    );
                    attempt += 1;
                    sleep_with_jitter(base_delay * 2u64.pow(attempt as u32), 1000).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn fetch_vendor_ratings(&self, vendor_code: &str) -> Result<RatingsDistribution> {
        let url = format!(
            "https://reviews-api-pk.fd-api.com/ratings-distribution/vendor/{}?\
             global_entity_id=FP_PK",
            vendor_code
        );

        let client = self.client_pool.next_client();

        retry_with_backoff(MAX_RETRIES, BASE_DELAY_MS, || async {
            let request = client.get(&url);
            let response = client.send(request).await?;

            debug!(
                status = response.status().as_u16(),
                url = url,
                "API response received"
            );

            if response.status() == StatusCode::OK {
                let body = response.bytes().await?;
                return serde_json::from_slice(&body).map_err(|e| {
                    let body_str = String::from_utf8_lossy(&body);
                    error!(
                        error = %e,
                        body = %body_str,
                        "Failed to parse vendor ratings response"
                    );
                    common::Error::from(e)
                });
            }

            Err(common::Error::Http(response.error_for_status().unwrap_err()))
        })
        .await
    }

    pub async fn fetch_vendor_reviews(&self, vendor_code: &str) -> Result<Vec<serde_json::Value>> {
        let url = format!(
            "https://reviews-api-pk.fd-api.com/reviews/vendor/{}?\
             global_entity_id=FP_PK&limit=30&created_at=desc&has_dish=true",
            vendor_code
        );

        let client = self.client_pool.next_client();

        retry_with_backoff(MAX_RETRIES, BASE_DELAY_MS, || async {
            let request = client.get(&url);
            let response = client.send(request).await?;

            debug!(
                status = response.status().as_u16(),
                url = url,
                "API response received"
            );

            if response.status() == StatusCode::OK {
                let body= response.bytes().await?;
                let reviews: ReviewsResponse = serde_json::from_slice(&body).map_err(|e| {
                    let body_str = String::from_utf8_lossy(&body);
                    error!(
                        error = %e,
                        body = %body_str,
                        "Failed to parse vendor reviews response"
                    );
                    common::Error::from(e)
                })?;
                return Ok(reviews.data);
            }

            Err(common::Error::Http(response.error_for_status().unwrap_err()))
        })
        .await
    }
}
