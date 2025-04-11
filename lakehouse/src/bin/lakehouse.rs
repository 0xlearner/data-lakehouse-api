use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use common::Result;
use common::config::Settings;
use lakehouse::services::lakehouse::LakehouseService;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration
    let config = Settings::new("config/lakehouse.toml")?;
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
            println!("  Successfully processed to Silver: {}", results.success_silver);
            println!("  Skipped (Known Conditions): {}", results.skipped_known);
            println!(
                "  Skipped (Path Parse Error): {}",
                results.skipped_parse_error
            );
            println!("  Failed in Bronze: {}", results.failed_bronze);
            println!("  Failed in Silver: {}", results.failed_silver);

            // Optionally log details of failed files
            if !results.failed_files.is_empty() {
                eprintln!("Details of failed files:");
                for (file, stage, error) in results.failed_files {
                    eprintln!("    - File: {}, Stage: {}, Error: {}", file, stage, error);
                }
                // TODO: decide if a certain number of failures is critical
                // For example:
                // if results.failed_bronze + results.failed_silver > 10 { // Some threshold
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
    let api_router = lakehouse::api::routes(Arc::clone(&service));

    // Start the server
    let addr = SocketAddr::from(([127, 0, 0, 1], config.api_port));
    let listener = TcpListener::bind(addr).await?; // Use Tokio's async version
    println!("API server listening on {}", addr);
    axum::serve(listener, api_router).await?;

    Ok(())
}
