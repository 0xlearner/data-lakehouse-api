use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vendor {
    pub code: String,
    pub name: String,
    pub details: Option<serde_json::Value>,
    pub batch_number: i32,
    pub reviews: Option<Vec<serde_json::Value>>,
    pub ratings: Option<super::ratings::RatingsDistribution>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub extraction_started_at: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub extraction_completed_at: DateTime<Utc>,
}
