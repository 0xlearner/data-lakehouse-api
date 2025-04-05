pub mod api;
pub mod models;
pub mod processor;
pub mod services;
pub mod storage;
pub mod utils;

use common::Result;
use common::config::Settings;
use services::lakehouse::LakehouseService;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

/// Runs the complete lakehouse pipeline by processing all available source data
/// and then starts the API server.
pub async fn run_lakehouse_pipeline(config_path: &str) -> Result<()> {
    // Load configuration
    println!("Loading configuration from: {}", config_path);
    let config = Settings::new(config_path)?;
    println!("Configuration loaded successfully.");

    // Initialize lakehouse service
    println!("Initializing Lakehouse Service...");
    let service = Arc::new(LakehouseService::new(&config).await?);
    println!("Lakehouse Service initialized.");

    // --- Run Bulk Data Processing ---
    // Get concurrency level from settings
    let concurrency = config.pipeline.concurrency;
    println!(
        "Starting bulk processing of all source data with concurrency level: {}",
        concurrency
    );

    // Call the function to process all detected source files
    match service.process_all_source_data(concurrency).await {
        Ok(results) => {
            // Log the results of the bulk processing
            println!("Finished bulk processing.");
            println!("  Successfully processed: {}", results.success);
            println!("  Skipped (Known Conditions): {}", results.skipped_known);
            println!(
                "  Skipped (Path Parse Error): {}",
                results.skipped_parse_error
            );
            println!("  Failed: {}", results.failed);

            // Optionally log details of failed files
            if !results.failed_files.is_empty() {
                eprintln!("Details of failed files:");
                for (file, err) in results.failed_files {
                    eprintln!("    - File: {}, Error: {}", file, err);
                }
                // TODO: decide if a certain number of failures is critical
                // For example:
                // if results.failed > 10 { // Some threshold
                //    return Err(common::Error::PipelineFailed("Too many files failed during bulk processing".into()));
                // }
            }
            println!("Bulk processing completed.");
        }
        Err(e) => {
            // Handle critical errors during the overall process_all_source_data call
            eprintln!(
                "Critical error occurred during process_all_source_data: {}",
                e
            );
            // Depending on the error, you might want to stop the application
            return Err(common::Error::Other(format!(
                // Assuming common::Error::Other exists
                "process_all_source_data execution failed: {}",
                e
            )));
            // Or use a specific error variant like PipelineFailed if you defined one
            // return Err(common::Error::PipelineFailed(format!(
            //    "process_all_source_data execution failed: {}", e
            // )))
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
