use common::{Error, Result};
use once_cell::sync::Lazy;
use regex::Regex;

// Use once_cell::sync::Lazy for efficient regex compilation
static S3_PATH_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"city_id=(?P<city_id>[^/]+)/year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/")
        .expect("Invalid S3 path regex")
});

#[derive(Debug, Clone)]
pub struct PathComponents {
    pub city_code: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

/// Parses city_code, year, month, and day from an S3 path string.
/// Expects a path containing ".../city_id=.../year=.../month=.../day=.../"
pub fn parse_s3_path_components(s3_path: &str) -> Result<PathComponents> {
    S3_PATH_REGEX
        .captures(s3_path)
        .and_then(|caps| {
            let city_code = caps.name("city_id")?.as_str().to_string();
            let year = caps.name("year")?.as_str().parse::<i32>().ok()?;
            let month = caps.name("month")?.as_str().parse::<u32>().ok()?;
            let day = caps.name("day")?.as_str().parse::<u32>().ok()?;

            // Basic validation (optional, but good practice)
            if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
                return None; // Invalid month or day
            }

            Some(PathComponents {
                city_code,
                year,
                month,
                day,
            })
        })
        .ok_or_else(|| {
            Error::InvalidInput(format!("Failed to parse path components from: {}", s3_path))
        })
}
