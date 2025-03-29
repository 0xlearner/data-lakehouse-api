use crate::client::http::HttpClient;
use common::config::Settings;
use common::Result;
use rquest_util::Emulation;
use tracing::debug;

pub struct ClientPool {
    clients: Vec<HttpClient>,
    current: std::sync::atomic::AtomicUsize,
}

impl ClientPool {
    pub fn new(settings: Settings) -> Result<Self> {
        let emulations = vec![
            Emulation::Firefox136,
            Emulation::Chrome133,
            Emulation::Safari18_3,
            Emulation::Edge134,
        ];

        debug!("Creating client pool with {} emulations", emulations.len());

        let clients = emulations
            .into_iter()
            .map(|emulation| {
                debug!("Creating client with emulation: {:?}", emulation);
                HttpClient::new(settings.clone(), emulation)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            clients,
            current: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub fn next_client(&self) -> &HttpClient {
        let current = self
            .current
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        &self.clients[current % self.clients.len()]
    }

    pub fn get_client(&self, index: usize) -> &HttpClient {
        &self.clients[index % self.clients.len()]
    }

    pub fn current_index(&self) -> usize {
        self.current.load(std::sync::atomic::Ordering::SeqCst) % self.clients.len()
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }
}
