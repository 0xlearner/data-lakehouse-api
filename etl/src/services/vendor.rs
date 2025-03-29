use common::Result;
use crate::models::Vendor;
use crate::services::api::ApiService;
use crate::storage::json::JsonWriter;
use crate::utils::time::sleep_with_jitter;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

#[derive(Clone)]
pub struct VendorService {
    api_service: ApiService,
}

impl VendorService {
    pub fn new(api_service: ApiService) -> Self {
        Self { api_service }
    }

    pub async fn process_vendor_batch(
        &self,
        vendor_codes: Vec<String>,
        json_writer: &Arc<Mutex<JsonWriter>>,
        batch_number: i32,
        total_batches: i32,
    ) -> Result<()> {
        info!(
            batch_number = batch_number,
            total_batches = total_batches,
            "Processing vendor batch"
        );

        for (index, code) in vendor_codes.iter().enumerate() {
            info!(
                batch_number = batch_number,
                total_batches = total_batches,
                vendor_index = index + 1,
                vendors_count = vendor_codes.len(),
                vendor_code = code,
                "Processing vendor"
            );

            // Add random delay between vendors
            sleep_with_jitter(1500, 1000).await;

            // Get vendor details first
            match self.api_service.fetch_vendor_details(code).await {
                Ok(Some(details)) => {
                    // Add delay before fetching reviews and ratings
                    sleep_with_jitter(800, 400).await;

                    let (reviews_result, ratings_result) = tokio::join!(
                        self.api_service.fetch_vendor_reviews(code),
                        self.api_service.fetch_vendor_ratings(code)
                    );

                    let extraction_completed_at = chrono::Utc::now();

                    let vendor = Vendor {
                        code: code.clone(),
                        name: details
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("Unknown")
                            .to_string(),
                        details: Some(details),
                        batch_number,
                        reviews: reviews_result.ok(),
                        ratings: ratings_result.ok(),
                        extraction_started_at: chrono::Utc::now(),
                        extraction_completed_at,
                    };

                    let mut writer = json_writer.lock().await;
                    if let Err(e) = writer.write_vendor(&vendor).await {
                        error!(
                            error = %e,
                            vendor_code = code,
                            "Error writing vendor to file"
                        );
                    }
                }
                Ok(None) => {
                    // Vendor details returned 400, skip reviews and ratings
                    info!(
                        vendor_code = code,
                        batch_number = batch_number,
                        total_batches = total_batches,
                        vendor_index = index + 1,
                        vendors_count = vendor_codes.len(),
                        "Skipping vendor due to 400 response"
                    );

                    let extraction_completed_at = chrono::Utc::now();

                    // Still write the vendor with minimal information
                    let vendor = Vendor {
                        code: code.clone(),
                        name: "Unknown".to_string(),
                        details: None,
                        batch_number,
                        reviews: None,
                        ratings: None,
                        extraction_started_at: chrono::Utc::now(),
                        extraction_completed_at,
                    };

                    let mut writer = json_writer.lock().await;
                    if let Err(e) = writer.write_vendor(&vendor).await {
                        error!(
                            error = %e,
                            vendor_code = code,
                            "Error writing vendor to file"
                        );
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        vendor_code = code,
                        batch_number = batch_number,
                        total_batches = total_batches,
                        vendor_index = index + 1,
                        vendors_count = vendor_codes.len(),
                        "Failed to fetch vendor details"
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}
