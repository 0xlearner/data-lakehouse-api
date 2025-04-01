pub mod services;
pub mod api;
pub mod processor;
pub mod storage;
pub mod models;
pub mod utils;


use std::sync::Arc;
use common::config::Settings;
use common::Result;
use services::lakehouse::LakehouseService;
use tokio::net::TcpListener;
use std::net::SocketAddr;

/// Runs the complete lakehouse pipeline
pub async fn run_lakehouse_pipeline(config_path: &str) -> Result<()> {
    // Load configuration
    let config = Settings::new(config_path)?;

    // Initialize lakehouse service
    let service = Arc::new(LakehouseService::new(&config).await?);
    
    let year = config.date.year;
    let month = config.date.month;
    let day = config.date.day;
    
    // Process data for each city with current date
    for city in &config.cities.codes {
        println!("Processing data for city: {} ({}/{:02}/{:02})", 
                city, year, month, day);
                
        match service.process_city_data(city, year, month, day).await {
            Ok(key) => println!("Successfully processed city {} data, stored at: {}", city, key),
            Err(e) => eprintln!("Error processing city {}: {}", city, e),
        }
    }
    
    // Create API router
    let api_router = api::routes(Arc::clone(&service));
    
    // Start the server
    let addr = SocketAddr::from(([127, 0, 0, 1], config.api_port));
    let listener = TcpListener::bind(addr).await?;
    println!("Lakehouse API server listening on {}", addr);
    axum::serve(listener, api_router).await?;
    
    Ok(())
}