pub mod models;
pub mod client;
pub mod services;
pub mod utils;
pub mod storage;

pub use models::{Vendor, VendorListResponse};
pub use client::pool::ClientPool;

use common::config::Settings;
use common::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::{Datelike, Utc};
use std::fs::{self, File};
use tracing::{info, error};
use tracing_subscriber::{
    fmt::{self, time::UtcTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    filter::{EnvFilter, LevelFilter},
    Layer,
};
use tempfile::NamedTempFile;
use std::io::BufReader;
use std::path::Path;

use crate::storage::ParquetConverter;
use crate::services::api::ApiService;
use crate::services::vendor::VendorService;
use crate::storage::JsonWriter;
use crate::storage::minio::MinioUploader;
use crate::utils::time::sleep_with_jitter;

pub async fn run_etl_pipeline(config_path: &str) -> Result<()> {
    // Load the ETL configuration
    let settings = Settings::new(config_path)?;
    
    // Set up logging
    setup_logging()?;
    
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let user_login = std::env::var("USER_LOGIN").unwrap_or_else(|_| "default_user".to_string());

    info!(
        timestamp = timestamp,
        user = user_login,
        "Starting extraction"
    );

    let client_pool = Arc::new(ClientPool::new(settings.clone())?);
    let api_service = ApiService::new(client_pool.clone());
    let vendor_service = VendorService::new(api_service.clone());

    // Process each city from the configuration
    for city_id in &settings.cities.codes {
        info!(city_id = city_id, "Processing city");

        // Get output directory from environment variable or use a default
        let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "data".to_string());

        let filename = format!("vendors_city_{}_{}_.json", city_id, timestamp.replace(" ", "_"));
        // Create temporary Parquet file
        let temp_parquet = NamedTempFile::new()?;
        let json_writer = JsonWriter::new(&filename).await?;
        let json_writer = Arc::new(Mutex::new(json_writer));
        
        // Get initial page to determine total count and page size
        let initial_response = api_service.fetch_vendor_page(city_id, 0, 48).await?;
        let total_vendors = initial_response.data.available_count;
        let page_size = initial_response.data.returned_count;
        let total_pages = (total_vendors as f32 / page_size as f32).ceil() as i32;

        info!(
            total_vendors = total_vendors,
            total_pages = total_pages,
            page_size = page_size,
            "Vendor pagination details"
        );

        // Start timer
        let start_time = std::time::Instant::now();

        // Process all pages
        for page in 0..1 {
            let offset = page * page_size;
            
            if page > 0 {
                sleep_with_jitter(2000, 1000).await;
            }
            
            let response = api_service.fetch_vendor_page(city_id, offset, page_size).await?;
            let vendor_codes: Vec<String> = response.data.items
                .into_iter()
                .map(|item| item.code)
                .collect();

            info!(
                page = page + 1,
                total_pages = total_pages,
                vendors_count = vendor_codes.len(),
                "Processing vendor batch"
            );

            match vendor_service.process_vendor_batch(
                vendor_codes,
                &json_writer,
                page + 1,
                total_pages,
            ).await {
                Ok(_) => info!(
                    page = page + 1,
                    total_pages = total_pages,
                    "Batch processed successfully"
                ),
                Err(e) => {
                    error!(
                        error = %e,
                        page = page + 1,
                        total_pages = total_pages,
                        "Failed to process batch"
                    );
                    return Err(e.into());
                }
            }
        }

        // Finish writing and upload for this city
        let final_count = {
            let mut writer = json_writer.lock().await;  
            writer.finish().await?;
            writer.get_count()
        };

        let total_time = start_time.elapsed();
        let minutes = total_time.as_secs_f64() / 60.0;
        let vendors_per_second = final_count as f64 / total_time.as_secs_f64();

        info!(
            city_id = city_id,
            timestamp = timestamp,
            user = user_login,
            total_vendors = final_count,
            total_pages = total_pages,
            page_size = page_size,
            total_minutes = minutes,
            vendors_per_second = vendors_per_second,
            output_file = filename,
            "Extraction completed"
        );

        // Upload to MinIO
        info!(city_id = city_id, "Starting MinIO upload");

        // Initialize MinIO uploader once
        let minio_uploader = MinioUploader::new(
            &settings.minio.endpoint,
            &settings.minio.access_key,
            &settings.minio.secret_key,
            &settings.minio.source_bucket,
            &settings.minio.region,
        ).await?;
        
        info!(
            city_id = city_id,
            json_file = &filename,
            "Converting JSON to Parquet"
        );
    
        // Read JSON and convert to Parquet
        let file_path = Path::new(&output_dir).join(&filename);
        let json_file = File::open(&file_path)?;
        let reader = BufReader::new(json_file);
        let vendors: Vec<Vendor> = serde_json::from_reader(reader)?;
    
        // Convert to Parquet
        ParquetConverter::convert_vendors_to_parquet(
            &vendors,
            temp_parquet.path().to_str().unwrap()
        )?;
    
        // Generate partitioned S3 key
        let now = Utc::now();
        let s3_key = format!(
            "city_id={}/year={}/month={:02}/day={:02}/vendors_{}.parquet",
            city_id,
            now.year(),
            now.month(),
            now.day(),
            now.timestamp()
        );

        // Get file size before upload
        let file_size = temp_parquet.as_file().metadata()?.len();

        info!(
            s3_key = &s3_key,
            file_size_mb = file_size / (1024 * 1024),
            "Uploading Parquet file to S3"
        );

        // Upload file
        minio_uploader.upload_parquet_file(temp_parquet.path(), &s3_key).await?;

        info!(
            s3_key = s3_key,
            vendors_count = vendors.len(),
            file_size_mb = file_size / (1024 * 1024),
            "Successfully uploaded Parquet file to S3"
        );
    
        // Optionally cleanup the JSON file
        if let Err(e) = std::fs::remove_file(&file_path) {
            error!(
                error = %e,
                filename = file_path.to_string_lossy().to_string(),
                "Failed to remove JSON file"
            );
        }
    }
    info!("All cities processed successfully");
    Ok(())
}

// You'll need to add this function to support loading settings from a specific path
fn setup_logging() -> Result<()> {
    // Create logs directory if it doesn't exist
    fs::create_dir_all("logs")?;
    
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let user_login = std::env::var("USER_LOGIN").unwrap_or_else(|_| "default_user".to_string());
    
    // Generate log filename
    let log_file = format!("logs/foodpanda_etl_{}_{}_.log", 
        timestamp.replace(" ", "_"),
        user_login
    );
    
    // Create log file
    let file = File::create(&log_file)?;

    // Set up filter
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::default()
                .add_directive(LevelFilter::INFO.into())
        });

    // Create file writer layer
    let file_layer = fmt::Layer::new()
        .with_timer(UtcTime::rfc_3339())
        .with_level(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .json()
        .with_current_span(true)
        .with_writer(file)
        .with_filter(filter);

    let stdout_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| {
            EnvFilter::default()
                .add_directive(LevelFilter::DEBUG.into())
        });

    // Create stdout layer (console output)
    let stdout_layer = fmt::Layer::new()
        .with_timer(UtcTime::rfc_3339())
        .with_level(true)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true)
        .json()
        .with_current_span(true)
        .with_filter(stdout_filter);

    // Initialize both layers
    tracing_subscriber::registry()
        .with(file_layer)
        .with(stdout_layer)
        .init();
    
    Ok(())
}
