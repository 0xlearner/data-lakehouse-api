use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::json::reader::{ReaderBuilder, infer_json_schema_from_iterator};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use common::Result;
use datafusion::prelude::*;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

pub struct DataTransformer {
    ctx: Arc<SessionContext>,
}

impl DataTransformer {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self { ctx }
    }

    pub async fn load_data(&self, source_path: &str) -> Result<DataFrame> {
        // Register the source table with a temporary name
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let temp_table = format!("temp_bronze_source_{}", timestamp);

        self.ctx
            .register_parquet(&temp_table, source_path, ParquetReadOptions::default())
            .await?;

        // Read the data
        let df = self.ctx.table(&temp_table).await?;

        // Deregister the temporary table
        self.ctx.deregister_table(&temp_table)?;

        Ok(df)
    }

    pub async fn transform_data(&self, df: DataFrame) -> Result<HashMap<String, DataFrame>> {
        let mut transformed_tables = HashMap::new();

        // Extract JSON columns
        let details_data = self.extract_json_column(&df, "details").await?;
        let reviews_data = self.extract_json_column(&df, "reviews").await?;
        let ratings_data = self.extract_json_column(&df, "ratings").await?;

        // Process each component
        let (
            menus,
            categories,
            products,
            variations,
            chain,
            city,
            cuisines,
            deals,
            discounts,
            tags,
            vendor_legal,
        ) = self.process_details_data(&details_data)?;

        // Convert JSON strings to DataFrames
        transformed_tables.insert("menus".to_string(), self.json_to_df(&menus).await?);
        transformed_tables.insert(
            "menu_categories".to_string(),
            self.json_to_df(&categories).await?,
        );
        transformed_tables.insert("products".to_string(), self.json_to_df(&products).await?);
        transformed_tables.insert(
            "product_variations".to_string(),
            self.json_to_df(&variations).await?,
        );
        transformed_tables.insert("chain".to_string(), self.json_to_df(&chain).await?);
        transformed_tables.insert("city".to_string(), self.json_to_df(&city).await?);
        transformed_tables.insert("cuisines".to_string(), self.json_to_df(&cuisines).await?);
        transformed_tables.insert("deals".to_string(), self.json_to_df(&deals).await?);
        transformed_tables.insert("discounts".to_string(), self.json_to_df(&discounts).await?);
        transformed_tables.insert("tags".to_string(), self.json_to_df(&tags).await?);
        transformed_tables.insert(
            "vendor_legal_information".to_string(),
            self.json_to_df(&vendor_legal).await?,
        );

        // Process reviews
        let reviews = self.process_reviews_data(&reviews_data)?;
        transformed_tables.insert("reviews".to_string(), self.json_to_df(&reviews).await?);

        // Process ratings
        let ratings = self.process_ratings_data(&ratings_data)?;
        transformed_tables.insert(
            "ratings_distribution".to_string(),
            self.json_to_df(&ratings).await?,
        );

        // Process vendor details
        let vendor_details = self.process_vendor_details(&details_data)?;
        transformed_tables.insert(
            "vendor_details".to_string(),
            self.json_to_df(&vendor_details).await?,
        );

        Ok(transformed_tables)
    }

    async fn extract_json_column(
        &self,
        df: &DataFrame,
        column_name: &str,
    ) -> Result<Vec<(i64, String)>> {
        let column_df = df.clone().select(vec![col(column_name)])?;
        let batches = column_df.collect().await?;

        let mut result = Vec::new();
        let mut row_counter = 0_i64;

        for batch in batches {
            let array = batch.column(0);
            let len = array.len();

            match array.data_type() {
                DataType::LargeUtf8 => {
                    let string_array = array
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .expect("Failed to downcast to LargeStringArray");
                    for i in 0..len {
                        if !string_array.is_null(i) {
                            result.push((row_counter, string_array.value(i).to_string()));
                        }
                        row_counter += 1;
                    }
                }
                DataType::Utf8 => {
                    let string_array = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .expect("Failed to downcast to StringArray");
                    for i in 0..len {
                        if !string_array.is_null(i) {
                            result.push((row_counter, string_array.value(i).to_string()));
                        }
                        row_counter += 1;
                    }
                }
                DataType::Utf8View => {
                    let string_array = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringViewArray>()
                        .expect("Failed to downcast to StringViewArray");
                    for i in 0..len {
                        if !string_array.is_null(i) {
                            result.push((row_counter, string_array.value(i).to_string()));
                        }
                        row_counter += 1;
                    }
                }
                // Add other type handlers as needed
                _ => {
                    return Err(common::Error::Other(format!(
                        "Unsupported column type for JSON extraction: {:?}",
                        array.data_type()
                    )));
                }
            }
        }

        Ok(result)
    }

    async fn json_to_df(&self, json_strings: &[String]) -> Result<DataFrame> {
        // Create memory table from JSON strings
        let schema = self.infer_json_schema(json_strings)?;
        let batches = self.json_strings_to_batches(json_strings, schema.clone())?;

        if batches.is_empty() {
            // Return empty DataFrame with schema
            return self.ctx.read_empty().map_err(|e| {
                common::Error::Other(format!("Failed to create empty DataFrame: {}", e))
            });
        }

        // Convert multiple batches into a single batch
        let combined_batch = if batches.len() == 1 {
            batches[0].clone()
        } else {
            arrow::compute::concat_batches(&schema, &batches).map_err(|e| {
                common::Error::Other(format!("Failed to concatenate batches: {}", e))
            })?
        };

        // Create DataFrame from the single batch
        self.ctx.read_batch(combined_batch).map_err(|e| {
            common::Error::Other(format!("Failed to create DataFrame from batch: {}", e))
        })
    }

    fn infer_json_schema(&self, json_strings: &[String]) -> Result<SchemaRef> {
        // Filter out empty strings and create Value iterator
        let json_values: Vec<_> = json_strings
                .iter()
                .filter_map(|v_str| {
                    if v_str.trim().is_empty() {
                        None
                    } else {
                        match serde_json::from_str::<Value>(v_str) {
                            Ok(v) => Some(Ok(v)),
                            Err(e) => {
                                eprintln!(
                                    "WARN: Skipping invalid JSON during schema inference: {}. JSON: '{}'",
                                    e, v_str
                                );
                                None
                            }
                        }
                    }
                })
                .collect();

        if json_values.is_empty() {
            return Err(common::Error::Other(
                "Cannot infer schema: No valid, non-empty JSON strings provided.".into(),
            ));
        }

        // Infer schema from JSON values
        let schema = infer_json_schema_from_iterator(json_values.into_iter())
            .map_err(|e| common::Error::Other(format!("Schema inference failed: {}", e)))?;

        // Enhance schema with additional metadata
        let enhanced_fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
            .collect();

        Ok(Arc::new(Schema::new(enhanced_fields)))
    }

    fn json_strings_to_batches(
        &self,
        json_strings: &[String],
        schema: SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        if json_strings.is_empty() {
            return Ok(vec![]);
        }

        // Filter and join valid JSON strings
        let valid_json_data = json_strings
            .iter()
            .filter(|s| !s.trim().is_empty() && serde_json::from_str::<Value>(s).is_ok())
            .map(|s| s.as_str())
            .collect::<Vec<&str>>()
            .join("\n");

        if valid_json_data.is_empty() {
            println!("WARN: No valid JSON data remained after filtering for batch creation.");
            return Ok(vec![]);
        }

        let mut cursor = Cursor::new(valid_json_data);
        let builder = ReaderBuilder::new(schema).with_batch_size(8192);

        let mut reader = builder.build(&mut cursor).map_err(|e| {
            common::Error::Other(format!("Failed to build Arrow JSON reader: {}", e))
        })?;

        let mut batches = Vec::new();

        // Read all batches
        while let Some(batch_result) = reader.next() {
            match batch_result {
                Ok(batch) => {
                    if batch.num_rows() > 0 {
                        batches.push(batch);
                    }
                }
                Err(e) => {
                    eprintln!(
                        "WARN: Error reading JSON batch with Arrow reader: {}. Some data might be lost.",
                        e
                    );
                    break;
                }
            }
        }

        Ok(batches)
    }

    fn extract_sorting_ids(&self, menu_obj: &Map<String, Value>) -> Option<Vec<Value>> {
        menu_obj
            .get("tags")
            .and_then(Value::as_object)
            .and_then(|tags_obj| tags_obj.get("popular"))
            .and_then(Value::as_object)
            .and_then(|pop_obj| pop_obj.get("metadata"))
            .and_then(Value::as_object)
            .and_then(|meta_obj| meta_obj.get("sorting"))
            .and_then(Value::as_array)
            .cloned()
    }

    fn process_categories(
        &self,
        categories: Vec<Value>,
        bronze_row_id: i64,
        ingestion_time: &DateTime<Utc>,
        partition_date_str: &str,
        menu_id: &Value,
        cats: &mut Vec<String>,
        prods: &mut Vec<String>,
        vars: &mut Vec<String>,
    ) -> Result<()> {
        for category_val in categories {
            if let Value::Object(mut cat_obj) = category_val {
                self.add_metadata(
                    &mut cat_obj,
                    bronze_row_id,
                    ingestion_time,
                    partition_date_str,
                );
                cat_obj.insert("menu_id".to_string(), menu_id.clone());

                let products_val = cat_obj.remove("products");

                // Store category
                match serde_json::to_string(&cat_obj) {
                    Ok(s) => cats.push(s),
                    Err(e) => eprintln!("ERROR serializing category: {}", e),
                }

                // Process products if they exist
                if let Some(Value::Array(products)) = products_val {
                    self.process_products(
                        products,
                        bronze_row_id,
                        ingestion_time,
                        partition_date_str,
                        cat_obj.get("id").cloned().unwrap_or(Value::Null),
                        prods,
                        vars,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn process_products(
        &self,
        products: Vec<Value>,
        bronze_row_id: i64,
        ingestion_time: &DateTime<Utc>,
        partition_date_str: &str,
        category_id: Value,
        prods: &mut Vec<String>,
        vars: &mut Vec<String>,
    ) -> Result<()> {
        for product_val in products {
            if let Value::Object(mut prod_obj) = product_val {
                self.add_metadata(
                    &mut prod_obj,
                    bronze_row_id,
                    ingestion_time,
                    partition_date_str,
                );
                prod_obj.insert("category_id".to_string(), category_id.clone());

                let variations_val = prod_obj.remove("product_variations");
                prod_obj.remove("dietary_attributes"); // Remove complex nested structure

                // Store product
                match serde_json::to_string(&prod_obj) {
                    Ok(s) => prods.push(s),
                    Err(e) => eprintln!("ERROR serializing product: {}", e),
                }

                // Process variations if they exist
                if let Some(Value::Array(variations)) = variations_val {
                    for var_val in variations {
                        if let Value::Object(mut var_obj) = var_val {
                            self.add_metadata(
                                &mut var_obj,
                                bronze_row_id,
                                ingestion_time,
                                partition_date_str,
                            );
                            var_obj.insert(
                                "product_id".to_string(),
                                prod_obj.get("id").cloned().unwrap_or(Value::Null),
                            );
                            var_obj.remove("dietary_attributes"); // Remove complex nested structure

                            match serde_json::to_string(&var_obj) {
                                Ok(s) => vars.push(s),
                                Err(e) => eprintln!("ERROR serializing variation: {}", e),
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn process_menu_with_sorting(
        &self,
        menu_obj: Map<String, Value>,
        sorting_ids: Option<Vec<Value>>,
        bronze_row_id: i64,
        ingestion_time: &DateTime<Utc>,
        partition_date_str: &str,
        menus: &mut Vec<String>,
    ) -> Result<()> {
        match sorting_ids {
            Some(ids) if !ids.is_empty() => {
                for popular_id_val in ids {
                    let mut menu_clone = menu_obj.clone();
                    // Add metadata to each menu clone
                    self.add_metadata(
                        &mut menu_clone,
                        bronze_row_id,
                        ingestion_time,
                        partition_date_str,
                    );
                    menu_clone.insert("popular_product_id".to_string(), popular_id_val);
                    match serde_json::to_string(&menu_clone) {
                        Ok(s) => menus.push(s),
                        Err(e) => eprintln!("ERROR serializing exploded menu: {}", e),
                    }
                }
            }
            _ => {
                let mut menu_clone = menu_obj.clone();
                // Add metadata to the single menu
                self.add_metadata(
                    &mut menu_clone,
                    bronze_row_id,
                    ingestion_time,
                    partition_date_str,
                );
                menu_clone.insert("popular_product_id".to_string(), Value::Null);
                match serde_json::to_string(&menu_clone) {
                    Ok(s) => menus.push(s),
                    Err(e) => eprintln!("ERROR serializing single menu: {}", e),
                }
            }
        }
        Ok(())
    }

    fn process_top_level_object(
        &self,
        parsed_details: &Map<String, Value>,
        key: &str,
        bronze_row_id: i64,
        ingestion_time: &DateTime<Utc>,
        partition_date_str: &str,
        target_vec: &mut Vec<String>,
    ) -> Result<()> {
        if let Some(Value::Object(mut obj)) = parsed_details.get(key).cloned() {
            self.add_metadata(&mut obj, bronze_row_id, ingestion_time, partition_date_str);
            match serde_json::to_string(&obj) {
                Ok(s) => target_vec.push(s),
                Err(e) => eprintln!("ERROR serializing {} object: {}", key, e),
            }
        }
        Ok(())
    }

    fn process_top_level_array(
        &self,
        parsed_details: &Map<String, Value>,
        key: &str,
        bronze_row_id: i64,
        ingestion_time: &DateTime<Utc>,
        partition_date_str: &str,
        target_vec: &mut Vec<String>,
    ) -> Result<()> {
        if let Some(Value::Array(arr)) = parsed_details.get(key) {
            for item_val in arr {
                if let Value::Object(mut obj) = item_val.clone() {
                    self.add_metadata(&mut obj, bronze_row_id, ingestion_time, partition_date_str);
                    match serde_json::to_string(&obj) {
                        Ok(s) => target_vec.push(s),
                        Err(e) => eprintln!("ERROR serializing {} item: {}", key, e),
                    }
                }
            }
        }
        Ok(())
    }

    fn process_review_ratings(&self, review_obj: &mut Map<String, Value>) -> Result<()> {
        let ratings_arr = review_obj.get("ratings").and_then(Value::as_array).cloned();

        review_obj.remove("ratings");

        if let Some(ratings) = ratings_arr {
            for rating in ratings {
                if let Value::Object(rating_obj) = rating {
                    if let (Some(Value::String(topic)), Some(score)) =
                        (rating_obj.get("topic"), rating_obj.get("score"))
                    {
                        let column_name =
                            format!("ratings_topic_{}", topic.replace("restaurant_", ""));
                        review_obj.insert(column_name, score.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn process_review_variations(&self, review_obj: &mut Map<String, Value>) -> Result<()> {
        let variations_arr = review_obj
            .get("productVariations")
            .and_then(Value::as_array)
            .cloned();

        review_obj.remove("productVariations");

        let mut variation_ids = Vec::new();
        let mut product_ids = Vec::new();
        let mut variation_titles = Vec::new();
        let mut unit_prices = Vec::new();

        if let Some(variations) = variations_arr {
            for variation in variations {
                if let Value::Object(var_obj) = variation {
                    // Extract variation fields
                    if let Some(id) = var_obj.get("id") {
                        variation_ids.push(id.clone());
                    }
                    if let Some(title) = var_obj.get("defaultTitle") {
                        variation_titles.push(title.clone());
                    }
                    if let Some(price) = var_obj.get("unitPrice") {
                        unit_prices.push(price.clone());
                    }

                    // Extract product fields
                    if let Some(Value::Object(product)) = var_obj.get("product") {
                        if let Some(id) = product.get("id") {
                            product_ids.push(id.clone());
                        }
                    }
                }
            }
        }

        // Add the flattened arrays back to the review object
        review_obj.insert("variation_ids".to_string(), Value::Array(variation_ids));
        review_obj.insert("product_ids".to_string(), Value::Array(product_ids));
        review_obj.insert(
            "variation_titles".to_string(),
            Value::Array(variation_titles),
        );
        review_obj.insert("unit_prices".to_string(), Value::Array(unit_prices));

        Ok(())
    }

    // We'll implement these methods next
    fn process_details_data(
        &self,
        details_data: &[(i64, String)],
    ) -> Result<(
        Vec<String>, // menus
        Vec<String>, // categories
        Vec<String>, // products
        Vec<String>, // variations
        Vec<String>, // chain
        Vec<String>, // city
        Vec<String>, // cuisines
        Vec<String>, // deals
        Vec<String>, // discounts
        Vec<String>, // tags
        Vec<String>, // vendor_legal
    )> {
        let mut menus = vec![];
        let mut cats = vec![];
        let mut prods = vec![];
        let mut vars = vec![];
        let mut chains = vec![];
        let mut cities = vec![];
        let mut cuisines = vec![];
        let mut deals = vec![];
        let mut discounts = vec![];
        let mut tags = vec![];
        let mut vendor_legals = vec![];

        for (bronze_row_id, details_json_str) in details_data {
            let ingestion_time = Utc::now();
            let partition_date_str = ingestion_time.format("%Y-%m-%d").to_string();

            let parsed_details: Map<String, Value> = match serde_json::from_str(details_json_str) {
                Ok(Value::Object(map)) => map,
                Ok(_) => {
                    eprintln!(
                        "WARN: Parsed details JSON is not an object for bronze_row_id: {}. Skipping row.",
                        bronze_row_id
                    );
                    continue;
                }
                Err(e) => {
                    eprintln!(
                        "WARN: Failed to parse details JSON for bronze_row_id: {}: {}. Skipping row.",
                        bronze_row_id, e
                    );
                    continue;
                }
            };

            // Process menus and nested structures
            if let Some(Value::Array(menu_arr)) = parsed_details.get("menus") {
                for menu_val in menu_arr {
                    if let Value::Object(mut menu_obj) = menu_val.clone() {
                        self.add_metadata(
                            &mut menu_obj,
                            *bronze_row_id,
                            &ingestion_time,
                            &partition_date_str,
                        );

                        let menu_id = menu_obj.get("id").cloned().unwrap_or(Value::Null);
                        let sorting_ids = self.extract_sorting_ids(&menu_obj);

                        let categories_val = menu_obj.remove("menu_categories");

                        // Process categories and their nested structures
                        if let Some(Value::Array(categories)) = categories_val {
                            self.process_categories(
                                categories,
                                *bronze_row_id,
                                &ingestion_time,
                                &partition_date_str,
                                &menu_id,
                                &mut cats,
                                &mut prods,
                                &mut vars,
                            )?;
                        }

                        // Process menu with sorting IDs
                        self.process_menu_with_sorting(
                            menu_obj,
                            sorting_ids,
                            *bronze_row_id,
                            &ingestion_time,
                            &partition_date_str,
                            &mut menus,
                        )?;
                    }
                }
            }

            // Process top-level objects
            self.process_top_level_object(
                &parsed_details,
                "chain",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut chains,
            )?;
            self.process_top_level_object(
                &parsed_details,
                "city",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut cities,
            )?;
            self.process_top_level_object(
                &parsed_details,
                "vendor_legal_information",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut vendor_legals,
            )?;

            // Process top-level arrays
            self.process_top_level_array(
                &parsed_details,
                "cuisines",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut cuisines,
            )?;
            self.process_top_level_array(
                &parsed_details,
                "deals",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut deals,
            )?;
            self.process_top_level_array(
                &parsed_details,
                "discounts",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut discounts,
            )?;
            self.process_top_level_array(
                &parsed_details,
                "tags",
                *bronze_row_id,
                &ingestion_time,
                &partition_date_str,
                &mut tags,
            )?;
        }

        Ok((
            menus,
            cats,
            prods,
            vars,
            chains,
            cities,
            cuisines,
            deals,
            discounts,
            tags,
            vendor_legals,
        ))
    }

    fn process_reviews_data(&self, reviews_data: &[(i64, String)]) -> Result<Vec<String>> {
        let mut reviews = vec![];

        for (bronze_row_id, review_json_str) in reviews_data {
            let reviews_value: Value = match serde_json::from_str(review_json_str) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "ERROR parsing reviews JSON for bronze_row_id {}: {}. Skipping.",
                        bronze_row_id, e
                    );
                    continue;
                }
            };

            let reviews_arr = match reviews_value {
                Value::Array(arr) => arr,
                Value::Object(_) => vec![reviews_value],
                _ => {
                    eprintln!(
                        "ERROR: Unexpected reviews format for bronze_row_id {}. Skipping.",
                        bronze_row_id
                    );
                    continue;
                }
            };

            let ingestion_time = Utc::now();
            let partition_date_str = ingestion_time.format("%Y-%m-%d").to_string();

            for review_val in reviews_arr {
                if let Value::Object(mut review_obj) = review_val {
                    self.add_metadata(
                        &mut review_obj,
                        *bronze_row_id,
                        &ingestion_time,
                        &partition_date_str,
                    );

                    // Process ratings
                    self.process_review_ratings(&mut review_obj)?;

                    // Process product variations
                    self.process_review_variations(&mut review_obj)?;

                    match serde_json::to_string(&review_obj) {
                        Ok(s) => reviews.push(s),
                        Err(e) => eprintln!("ERROR serializing review: {}", e),
                    }
                }
            }
        }

        Ok(reviews)
    }

    fn process_ratings_data(&self, ratings_data: &[(i64, String)]) -> Result<Vec<String>> {
        let mut ratings_rows = vec![];

        for (bronze_row_id, ratings_json_str) in ratings_data {
            let ratings_value: Value = match serde_json::from_str(ratings_json_str) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "ERROR parsing ratings JSON for bronze_row_id {}: {}. Skipping.",
                        bronze_row_id, e
                    );
                    continue;
                }
            };

            if let Value::Object(mut ratings_obj) = ratings_value {
                let ingestion_time = Utc::now();
                let partition_date_str = ingestion_time.format("%Y-%m-%d").to_string();

                self.add_metadata(
                    &mut ratings_obj,
                    *bronze_row_id,
                    &ingestion_time,
                    &partition_date_str,
                );

                // Process ratings array
                if let Some(ratings_arr) = ratings_obj
                    .remove("ratings")
                    .and_then(|v| v.as_array().cloned())
                {
                    for rating in ratings_arr {
                        if let Value::Object(rating_obj) = rating {
                            if let (
                                Some(Value::Number(score)),
                                Some(Value::Number(count)),
                                Some(Value::Number(percentage)),
                            ) = (
                                rating_obj.get("score"),
                                rating_obj.get("count"),
                                rating_obj.get("percentage"),
                            ) {
                                let count_column = format!("score_{}_count", score);
                                let percentage_column = format!("score_{}_percentage", score);

                                ratings_obj.insert(count_column, Value::Number(count.clone()));
                                ratings_obj
                                    .insert(percentage_column, Value::Number(percentage.clone()));
                            }
                        }
                    }
                }

                match serde_json::to_string(&ratings_obj) {
                    Ok(s) => ratings_rows.push(s),
                    Err(e) => eprintln!("ERROR serializing ratings distribution: {}", e),
                }
            }
        }

        Ok(ratings_rows)
    }

    fn process_vendor_details(&self, details_data: &[(i64, String)]) -> Result<Vec<String>> {
        let mut details_rows = vec![];

        for (bronze_row_id, details_json_str) in details_data {
            let details_value: Value = match serde_json::from_str(details_json_str) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!(
                        "ERROR parsing details JSON for bronze_row_id {}: {}. Skipping.",
                        bronze_row_id, e
                    );
                    continue;
                }
            };

            if let Value::Object(details_obj) = details_value {
                let mut row_obj = Map::new();
                let ingestion_time = Utc::now();
                let partition_date_str = ingestion_time.format("%Y-%m-%d").to_string();

                self.add_metadata(
                    &mut row_obj,
                    *bronze_row_id,
                    &ingestion_time,
                    &partition_date_str,
                );

                // Extract specific fields
                let fields_to_extract = [
                    "code",
                    "address",
                    "address_line_2",
                    "name",
                    "customer_phone",
                    "hero_image",
                    "hero_listing_image",
                    "is_active",
                    "is_new_until",
                    "is_premium",
                    "is_promoted",
                    "latitude",
                    "longitude",
                    "minimum_order_amount",
                    "post_code",
                    "tag",
                    "trade_register_number",
                    "url_key",
                    "vertical",
                    "vertical_parent",
                    "vertical_segment",
                    "website",
                ];

                for field in fields_to_extract.iter() {
                    if let Some(value) = details_obj.get(*field) {
                        row_obj.insert(field.to_string(), value.clone());
                    }
                }

                match serde_json::to_string(&row_obj) {
                    Ok(s) => details_rows.push(s),
                    Err(e) => eprintln!("ERROR serializing vendor details: {}", e),
                }
            }
        }

        Ok(details_rows)
    }

    // Helper methods...
    fn add_metadata(
        &self,
        obj: &mut Map<String, Value>,
        bronze_row_id: i64,
        ingestion_time: &DateTime<Utc>,
        partition_date: &str,
    ) {
        obj.insert(
            "bronze_row_id".to_string(),
            Value::Number(bronze_row_id.into()),
        );
        obj.insert(
            "silver_ingestion_at".to_string(),
            Value::String(ingestion_time.to_rfc3339()),
        );
        obj.insert(
            "partition_date".to_string(),
            Value::String(partition_date.to_string()),
        );
    }
}
