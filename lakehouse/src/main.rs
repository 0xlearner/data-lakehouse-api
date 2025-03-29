
use clap::{Command, Arg};
use std::process;


#[tokio::main]
async fn main() {
    let matches = Command::new("Data Pipeline Manager")
        .version("1.0")
        .author("Your Name")
        .about("Manages Lakehouse data processing pipelines")
        .subcommand(
            Command::new("lakehouse")
                .about("Run the S3 to Lakehouse pipeline")
                .arg(
                    Arg::new("config")
                        .short('c')
                        .long("config")
                        .value_name("FILE")
                        .help("Sets a custom config file"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("lakehouse", lakehouse_matches)) => {
            let config_path = lakehouse_matches.get_one::<String>("config")
                .map(|s| s.as_str())
                .unwrap_or("config/iceberg.toml");
            println!("Starting Iceberg pipeline with config: {}", config_path);
            
            if let Err(e) = lakehouse::run_lakehouse_pipeline(config_path).await {
                eprintln!("Iceberg pipeline error: {}", e);
                process::exit(1);
            }
        }
        _ => {
            println!("No subcommand specified. Use --help for usage information.");
            process::exit(1);
        }
    }
}
