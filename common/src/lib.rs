use arrow::error::ArrowError;
use aws_sdk_s3::primitives::ByteStreamError;
use aws_smithy_runtime_api::client::result::CreateUnhandledError;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;
use parquet::errors::ParquetError;
use datafusion::error::DataFusionError;
use thiserror::Error;
use url::ParseError;

pub mod config;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(#[from] rquest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    
    #[error("UTF-8 conversion error: {0}")]
    Utf8(std::string::FromUtf8Error),

    #[error("S3 error: {0}")]
    S3(#[from] aws_sdk_s3::Error),

    #[error("AWS SDK error: {0}")]
    AwsSdk(String),

    #[error("Rate limit exceeded")]
    RateLimit,

    #[error("Forbidden - Access denied")]
    Forbidden,

    #[error("Gateway timeout")]
    GatewayTimeout,

    #[error("Maximum retries exceeded")]
    MaxRetriesExceeded,

    #[error("Lock error: {0}")]
    Lock(#[from] tokio::sync::TryLockError),

    #[error("Configuration error: {0}")]
    Config(#[from] ::config::ConfigError),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("ByteStream error: {0}")]
    ByteStream(#[from] ByteStreamError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("Invalid input: {0}")]
    InvalidInput(String),  // Add this variant

    #[error("Schema validation error: {0}")]
    SchemaValidation(String),

    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    #[error("Invalid Uri: {0}")]
    InvalidUri(String),

    #[error("{0}")]
    Other(String),
}

// Implement From for various SdkError types
impl<E: std::fmt::Debug + CreateUnhandledError> From<SdkError<E, Response>> for Error {
    fn from(err: SdkError<E, Response>) -> Self {
        Error::AwsSdk(format!("{:?}", err))
    }
}

impl From<object_store::Error> for Error {
    fn from(err: object_store::Error) -> Self {
        Error::Other(format!("Object store error: {}", err))
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Utf8(err)
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::InvalidInput(format!("URL parse error: {}", err))
    }
}
