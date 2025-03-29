use rand::Rng;
use std::time::Duration;

pub async fn sleep_with_jitter(base_ms: u64, jitter_ms: u64) {
    let jitter = rand::rng().random_range(0..=jitter_ms);
    tokio::time::sleep(Duration::from_millis(base_ms + jitter)).await;
}
