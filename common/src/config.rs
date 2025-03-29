use config::{Config, ConfigError};
use serde::Deserialize;
use std::collections::HashMap;
use tracing::debug;

#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub cities: Cities,
    pub minio: MinioConfig,
    #[serde(default = "default_api_config")]
    pub api: ApiConfig,
    pub date: DateConfig,
    #[serde(default = "default_s3_endpoint")]
    pub s3_endpoint: String,
    #[serde(default = "default_s3_region")]
    pub s3_region: String,
    #[serde(default = "default_s3_bucket")]
    pub s3_bronze_bucket: String,
    #[serde(default = "default_api_port")]
    pub api_port: u16,
}



#[derive(Debug, Deserialize, Clone)]
pub struct Cities {
    pub codes: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DateConfig {
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MinioConfig {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub source_bucket: String,  // food-panda-vendors
    pub bronze_bucket: String,  // bronze (target)
    pub metadata_bucket: String, // metadata
}

#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_api_config() -> ApiConfig {
    ApiConfig {
        headers: HashMap::new(),
    }
}

fn default_s3_endpoint() -> String {
    "http://localhost:9000".to_string()
}

fn default_s3_region() -> String {
    "us-east-1".to_string()
}

fn default_s3_bucket() -> String {
    "bronze".to_string()
}

fn default_api_port() -> u16 {
    3000
}

impl Settings {
    pub fn new(path: &str) -> Result<Self, ConfigError> {
        let builder = Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(config::Environment::with_prefix("APP"));

        // Build the configuration
        let config = builder.build()?;

        // Debug log the raw configuration
        if let Ok(headers) = config.get_table("api.headers") {
            debug!(?headers, "Loaded API headers from configuration");
        }

        // Try to deserialize the entire configuration
        let settings: Settings = config.try_deserialize()?;

        // Debug log the parsed headers
        debug!(
            headers = ?settings.api.headers,
            "Parsed API headers"
        );

        Ok(settings)
    }
}
