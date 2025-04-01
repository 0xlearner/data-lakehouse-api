pub struct PathBuilder {
    bucket: String,
    city_code: String,
    year: i32,
    month: u32,
    day: u32,
    dataset_type: Option<String>,
    file_name: Option<String>, 
}

impl PathBuilder {
    pub fn new(bucket: &str, city_code: &str, year: i32, month: u32, day: u32) -> Self {
        Self {
            bucket: bucket.to_string(),
            city_code: city_code.to_string(),
            year,
            month,
            day,
            dataset_type: None,
            file_name: None, 
        }
    }

    pub fn with_dataset_type(mut self, dataset_type: &str) -> Self {
        self.dataset_type = Some(dataset_type.to_string());
        self
    }

    pub fn build_s3_path(&self) -> String {
        format!(
            "s3://{}/city_id={}/year={}/month={:02}/day={:02}",
            self.bucket,
            self.city_code,
            self.year,
            self.month,
            self.day
        )
    }

    pub fn build_storage_path(&self) -> String {
        format!(
            "city_id={}/year={}/month={:02}/day={:02}",
            self.city_code,
            self.year,
            self.month,
            self.day
        )
    }

    pub fn build_file_path(&self, filename: &str) -> String {
        format!("{}/{}", self.build_storage_path(), filename)
    }

    pub fn with_file_name(mut self, file_name: &str) -> Self {
        self.file_name = Some(file_name.to_string());
        self
    }

    pub fn build_s3_file_path(&self, filename: &str) -> String {
        format!(
            "s3://{}/{}",
            self.bucket,
            self.build_file_path(filename)
        )
    }
}