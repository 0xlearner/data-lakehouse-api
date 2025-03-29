use clap::{Command, Arg};
use std::process;


#[tokio::main]
async fn main() {
    let matches = Command::new("ETL Pipeline Manager")
        .version("1.0")
        .author("Your Name")
        .about("Manages ETL pipeline")
        .subcommand(
            Command::new("etl")
                .about("Run the ETL pipeline")
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
        Some(("etl", etl_matches)) => {
            let config_path = etl_matches.get_one::<String>("config")
                .map(|s| s.as_str())
                .unwrap_or("config/etl.toml");
            println!("Starting ETL pipeline with config: {}", config_path);
            
            if let Err(e) = etl::run_etl_pipeline(config_path).await {
                eprintln!("ETL pipeline error: {}", e);
                process::exit(1);
            }
        },
        
        _ => {
            eprintln!("Please specify a valid subcommand");
            process::exit(1);
        }
    }
}
