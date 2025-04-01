use std::sync::Arc;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use common::config::Settings;
use lakehouse::services::lakehouse::LakehouseService;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize configuration
    let config = Settings::new("config/lakehouse.toml")?;

    // Initialize lakehouse service
    let lakehouse = Arc::new(LakehouseService::new(&config).await?);

    let year = config.date.year;
    let month = config.date.month;
    let day = config.date.day;
    
    // Process data for each city with current date
    for city in &config.cities.codes {
        println!("Processing data for city: {} ({}/{:02}/{:02})", 
                city, year, month, day);
                
        match lakehouse.process_city_data(city, year, month, day).await {
            Ok(key) => println!("Successfully processed city {} data, stored at: {}", city, key),
            Err(e) => eprintln!("Error processing city {}: {}", city, e),
        }
    }

    // Create API router
    let api_router = lakehouse::api::routes(Arc::clone(&lakehouse));

    // Start the server
    let addr = SocketAddr::from(([127, 0, 0, 1], config.api_port));
    let listener = TcpListener::bind(addr).await?;  // Use Tokio's async version
    println!("API server listening on {}", addr);
    axum::serve(listener, api_router).await?;

    Ok(())
}
