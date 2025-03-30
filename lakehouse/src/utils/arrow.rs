use arrow::record_batch::RecordBatch;
use arrow::array::{
    Array,
    Int32Array, 
    Int64Array, 
    StringArray,
    TimestampMillisecondArray, 
    TimestampMicrosecondArray,
    TimestampNanosecondArray, 
    TimestampSecondArray
};
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::{Value, Number};
use chrono::TimeZone;
use common::Result;

pub fn batches_to_json(batches: Vec<RecordBatch>) -> Result<Vec<Value>> {
    let mut json_rows = Vec::new();
    
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = serde_json::Map::new();
            
            for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = arrow_array_to_json(column, row_idx)?;
                row.insert(field.name().clone(), value);
            }
            
            json_rows.push(Value::Object(row));
        }
    }
    
    Ok(json_rows)
}

pub fn arrow_array_to_json(array: &dyn Array, index: usize) -> Result<Value> {
    if array.is_null(index) {
        return Ok(Value::Null);
    }

    Ok(match array.data_type() {
        DataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(Number::from(array.value(index)))
        }
        DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(Number::from(array.value(index)))
        }
        DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(array.value(index).to_string())
        }
        DataType::Timestamp(unit, _) => {
            match unit {
                TimeUnit::Millisecond => {
                    let array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    let ts = array.value(index);
                    let datetime = chrono::Utc.timestamp_millis_opt(ts).unwrap();
                    Value::String(datetime.to_rfc3339())
                },
                TimeUnit::Microsecond => {
                    let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    let ts = array.value(index);
                    let datetime = chrono::Utc.timestamp_micros(ts).unwrap();
                    Value::String(datetime.to_rfc3339())
                },
                TimeUnit::Nanosecond => {
                    let array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    let ts = array.value(index);
                    let seconds = ts / 1_000_000_000;
                    let nanos = (ts % 1_000_000_000) as u32;
                    let datetime = chrono::Utc.timestamp_opt(seconds, nanos).unwrap();
                    Value::String(datetime.to_rfc3339())
                },
                TimeUnit::Second => {
                    let array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    let ts = array.value(index);
                    let datetime = chrono::Utc.timestamp_opt(ts, 0).unwrap();
                    Value::String(datetime.to_rfc3339())
                },
            }
        }
        _ => Value::Null,
    })
}