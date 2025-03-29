use common::Result;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{create_udf, Volatility};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::arrow::array::{BooleanArray, StringArray, Int64Array, TimestampMillisecondArray};
use datafusion::common::DataFusionError;
use std::sync::Arc;
use chrono::DateTime;
use serde_json::Value;

/// Registers all UDFs with the SessionContext
pub fn register_udfs(ctx: &SessionContext) -> Result<()> {
    // JSON validation UDF
    let is_valid_json = create_udf(
        "is_valid_json",
        vec![DataType::Utf8],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(|args| validate_json(args).map_err(|e| DataFusionError::Internal(e.to_string()))),
    );

    // Timestamp conversion UDF
    let to_timestamp = create_udf(
        "to_timestamp",
        vec![DataType::Int64],
        DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Millisecond, None),
        Volatility::Immutable,
        Arc::new(|args| convert_to_timestamp(args).map_err(|e| DataFusionError::Internal(e.to_string()))),
    );

    // JSON extraction UDF
    let extract_json_field = create_udf(
        "extract_json_field",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| extract_field_from_json(args).map_err(|e| DataFusionError::Internal(e.to_string()))),
    );

    // JSON array length UDF
    let json_array_length = create_udf(
        "json_array_length",
        vec![DataType::Utf8],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(|args| get_json_array_length(args).map_err(|e| DataFusionError::Internal(e.to_string()))),
    );

    // Register all UDFs
    ctx.register_udf(is_valid_json);
    ctx.register_udf(to_timestamp);
    ctx.register_udf(extract_json_field);
    ctx.register_udf(json_array_length);

    Ok(())
}

/// Validates if a string is valid JSON
fn validate_json(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let str_array = match &args[0] {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected string array".to_string()))?,
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Internal("Scalar inputs not supported".to_string()).into());
        }
    };

    let result: BooleanArray = str_array
        .iter()
        .map(|opt_str| {
            opt_str.map(|s| serde_json::from_str::<Value>(s).is_ok())
        })
        .collect::<BooleanArray>();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Converts Unix timestamp (milliseconds) to Arrow Timestamp
fn convert_to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let int_array = match &args[0] {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Internal("Expected int64 array".to_string()))?,
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Internal("Scalar inputs not supported".to_string()).into());
        }
    };

    let result: TimestampMillisecondArray = int_array
    .iter()
    .map(|opt_ts| {
        opt_ts.map(|ts| {
            DateTime::from_timestamp_millis(ts)
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0)
        })
    })
    .collect();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Extracts a field from a JSON string using a path expression
fn extract_field_from_json(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let json_array = match &args[0] {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected string array".to_string()))?,
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Internal("Scalar inputs not supported".to_string()).into());
        }
    };

    let path_array = match &args[1] {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected string array".to_string()))?,
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Internal("Scalar inputs not supported".to_string()).into());
        }
    };

    let result: StringArray = json_array
        .iter()
        .zip(path_array.iter())
        .map(|(json_opt, path_opt)| {
            match (json_opt, path_opt) {
                (Some(json_str), Some(path)) => {
                    serde_json::from_str::<Value>(json_str)
                        .ok()
                        .and_then(|value| {
                            let mut current = &value;
                            for key in path.split('.') {
                                current = current.get(key)?;
                            }
                            Some(current.to_string())
                        })
                },
                _ => None,
            }
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Gets the length of a JSON array
fn get_json_array_length(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let json_array = match &args[0] {
        ColumnarValue::Array(array) => array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected string array".to_string()))?,
        ColumnarValue::Scalar(_) => {
            return Err(DataFusionError::Internal("Scalar inputs not supported".to_string()).into());
        }
    };

    let result: Int64Array = json_array
        .iter()
        .map(|json_opt| {
            json_opt
                .and_then(|json_str| serde_json::from_str::<Value>(json_str).ok())
                .and_then(|value| if value.is_array() { Some(value.as_array().unwrap().len() as i64) } else { None })
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;

    #[test]
    fn test_validate_json() {
        let input = StringArray::from(vec![
            Some(r#"{"key": "value"}"#),
            Some(r#"invalid json"#),
            None,
            Some(r#"[1,2,3]"#),
        ]);
        
        let result = validate_json(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        
        if let ColumnarValue::Array(array) = result {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(bool_array.value(0), true);
            assert_eq!(bool_array.value(1), false);
            assert_eq!(bool_array.value(2), false);
            assert_eq!(bool_array.value(3), true);
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_convert_to_timestamp() {
        let input = Int64Array::from(vec![
            Some(1634567890123),
            None,
            Some(1634567890124),
        ]);
        
        let result = convert_to_timestamp(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        
        if let ColumnarValue::Array(array) = result {
            let ts_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            assert_eq!(ts_array.is_null(1), true);
            assert!(ts_array.value(0) > 0);
            assert!(ts_array.value(2) > 0);
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_extract_json_field() {
        let json_input = StringArray::from(vec![
            Some(r#"{"user": {"name": "John"}}"#),
            Some(r#"{"user": {"name": "Jane"}}"#),
            None,
        ]);
        
        let path_input = StringArray::from(vec![
            Some("user.name"),
            Some("user.name"),
            Some("user.name"),
        ]);
        
        let result = extract_field_from_json(&[
            ColumnarValue::Array(Arc::new(json_input)),
            ColumnarValue::Array(Arc::new(path_input)),
        ]).unwrap();
        
        if let ColumnarValue::Array(array) = result {
            let str_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            assert_eq!(str_array.value(0), "\"John\"");
            assert_eq!(str_array.value(1), "\"Jane\"");
            assert_eq!(str_array.is_null(2), true);
        } else {
            panic!("Expected Array result");
        }
    }

    #[test]
    fn test_json_array_length() {
        let input = StringArray::from(vec![
            Some(r#"[1,2,3]"#),
            Some(r#"[]"#),
            None,
            Some(r#"{"not": "array"}"#),
        ]);
        
        let result = get_json_array_length(&[ColumnarValue::Array(Arc::new(input))]).unwrap();
        
        if let ColumnarValue::Array(array) = result {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(int_array.value(0), 3);
            assert_eq!(int_array.value(1), 0);
            assert_eq!(int_array.is_null(2), true);
            assert_eq!(int_array.is_null(3), true);
        } else {
            panic!("Expected Array result");
        }
    }
}
