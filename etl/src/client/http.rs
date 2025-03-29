use common::config::Settings;
use common::Result;
use http::StatusCode;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use rquest::{Client, RequestBuilder, Response};
use rquest_util::Emulation;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};

pub struct HttpClient {
    client: Client,
    headers: HeaderMap, // Store headers at struct level
}

impl HttpClient {
    pub fn new(settings: Settings, emulation: Emulation) -> Result<Self> {
        let mut headers = HeaderMap::new();

        // Add configured headers
        for (key, value) in settings.api.headers.iter() {
            if let (Ok(header_name), Ok(header_value)) = (
                HeaderName::from_bytes(key.as_bytes()),
                HeaderValue::from_str(value),
            ) {
                headers.insert(header_name, header_value);
                debug!(header_key = key, header_value = value, "Adding header");
            } else {
                error!(
                    header_key = key,
                    header_value = value,
                    "Invalid header value"
                );
            }
        }

        debug!(
            emulation = ?emulation,
            "Creating client with emulation"
        );

        let client = Client::builder()
            .emulation(emulation)
            .timeout(Duration::from_secs(30))
            .build()?;

        Ok(Self { client, headers })
    }

    pub fn get(&self, url: &str) -> RequestBuilder {
        let mut request = self.client.get(url);

        // Apply headers to each request
        for (key, value) in self.headers.iter() {
            request = request.header(key, value);
        }

        debug!(
            url = url,
            headers = ?self.headers,
            "Creating GET request with headers"
        );

        request
    }

    pub async fn send(&self, request: RequestBuilder) -> Result<Response> {
        const MAX_RETRIES: u32 = 3;
        const BASE_DELAY_MS: u64 = 2000;

        let mut attempts = 0;
        loop {
            attempts += 1;

            let built_request = request
                .try_clone()
                .expect("Failed to clone request")
                .build()?;

            debug!(
                url = %built_request.url(),
                attempt = attempts,
                headers = ?built_request.headers().iter()
                    .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("invalid")))
                    .collect::<Vec<_>>(),
                "Sending request"
            );

            match request
                .try_clone()
                .expect("Failed to clone request")
                .send()
                .await
            {
                Ok(response) => {
                    debug!(
                        status = response.status().as_u16(),
                        url = %response.url(),
                        response_headers = ?response.headers().iter()
                            .map(|(k, v)| (k.as_str(), v.to_str().unwrap_or("invalid")))
                            .collect::<Vec<_>>(),
                        "Response received"
                    );

                    match response.status() {
                        StatusCode::TOO_MANY_REQUESTS => {
                            if attempts >= MAX_RETRIES {
                                return Err(common::Error::RateLimit);
                            }
                            sleep(Duration::from_millis(
                                BASE_DELAY_MS * 2u64.pow(attempts - 1),
                            ))
                            .await;
                            continue;
                        }
                        StatusCode::FORBIDDEN => {
                            debug!(
                                url = %response.url(),
                                "Received 403 Forbidden"
                            );
                            return Err(common::Error::Forbidden);
                        }
                        StatusCode::GATEWAY_TIMEOUT => {
                            if attempts >= MAX_RETRIES {
                                return Err(common::Error::Http(
                                    response.error_for_status().unwrap_err(),
                                ));
                            }
                            debug!(
                                url = %response.url(),
                                attempt = attempts,
                                "Gateway timeout, retrying after delay"
                            );
                            sleep(Duration::from_millis(
                                BASE_DELAY_MS * 2u64.pow(attempts - 1),
                            ))
                            .await;
                            continue;
                        }
                        _ => return Ok(response), // Return successful responses immediately
                    }
                }
                Err(e) => {
                    if attempts >= MAX_RETRIES {
                        return Err(e.into());
                    }

                    debug!(
                        error = %e,
                        attempt = attempts,
                        "Connection error, retrying after delay"
                    );

                    sleep(Duration::from_millis(
                        BASE_DELAY_MS * 2u64.pow(attempts - 1),
                    ))
                    .await;
                    continue;
                }
            }
        }
    }
}
