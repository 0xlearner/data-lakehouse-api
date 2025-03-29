use common::Result;
use crate::models::Vendor;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncWriteExt, BufWriter as TokioBufWriter};

pub struct JsonWriter {
    writer: TokioBufWriter<TokioFile>,
    count: AtomicUsize,
    is_first: bool,
}

impl JsonWriter {
    pub async fn new(filename: &str) -> Result<Self> {
        // Get the output directory from environment variable or use a default
        let output_dir = std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "data".to_string());

        // Create the output directory if it doesn't exist
        tokio::fs::create_dir_all(&output_dir).await?;

        // Combine the directory and filename
        let path = Path::new(&output_dir).join(filename);
        let file_path = path.to_str().unwrap();
        let file = TokioFile::create(file_path).await?;
        let mut writer = TokioBufWriter::new(file);
        writer.write_all(b"[\n").await?;

        Ok(Self {
            writer,
            count: AtomicUsize::new(0),
            is_first: true,
        })
    }

    pub async fn write_vendor(&mut self, vendor: &Vendor) -> Result<()> {
        if !self.is_first {
            self.writer.write_all(b",\n").await?;
        }
        self.is_first = false;

        let json = serde_json::to_vec(&vendor)?;
        self.writer.write_all(&json).await?;
        self.count.fetch_add(1, Ordering::SeqCst);
        self.writer.flush().await?;

        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.writer.write_all(b"\n]").await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub fn get_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}
