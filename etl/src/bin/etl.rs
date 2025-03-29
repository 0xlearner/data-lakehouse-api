use std::process;
use etl::run_etl_pipeline;

#[tokio::main]
async fn main() {
    // Get config path from command line args or use default
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config/etl.toml".to_string());

    println!("Starting ETL pipeline with config: {}", config_path);
    
    if let Err(e) = run_etl_pipeline(&config_path).await{
        eprintln!("ETL pipeline error: {}", e);
        process::exit(1);
    }
}
