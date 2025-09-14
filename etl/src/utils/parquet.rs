use arrow::array::*;
use arrow::datatypes::{DataType, Field, TimeUnit};

use chrono::{NaiveDate, NaiveTime};
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::types::Type;

use crate::conversions::{ArrayCell, Cell};

#[derive(Debug, Error)]
pub enum ParquetError {
    #[error("invalid input value: {0}")]
    InvalidInput(String),

    #[error("unsupported data type: {0}")]
    UnsupportedDataType(String),
}

pub fn cell_to_parquet_value(cells: &[Cell], dt: &DataType) -> Result<ArrayRef, ParquetError> {
    let n = cells.len();

    Ok(match dt {
        DataType::Null => Arc::new(NullArray::new(n)),

        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Bool(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected bool".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Int16 => {
            let mut b = Int16Builder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::I16(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected i16".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Int32 => {
            let mut b = Int32Builder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::I32(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected i32".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::UInt32 => {
            let mut b = UInt32Builder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::U32(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected u32".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Int64 => {
            let mut b = Int64Builder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::I64(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected i64".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Float32 => {
            let mut b = Float32Builder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::F32(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected f32".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Float64 => {
            let mut b = Float64Builder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::F64(v) => b.append_value(*v),
                    _ => return Err(ParquetError::InvalidInput("expected f64".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::String(s) => b.append_value(s),
                    Cell::Json(j) => b.append_value(j.to_string()),
                    Cell::Uuid(u) => b.append_value(u.to_string()),
                    _ => {
                        return Err(ParquetError::InvalidInput(
                            "expected utf8-compatible".into(),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }

        DataType::LargeUtf8 => {
            let mut b = LargeStringBuilder::new();
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::String(s) => b.append_value(s),
                    Cell::Json(j) => b.append_value(j.to_string()),
                    Cell::Uuid(u) => b.append_value(u.to_string()),
                    _ => {
                        return Err(ParquetError::InvalidInput(
                            "expected utf8-compatible".into(),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }

        DataType::Binary => {
            let mut b = BinaryBuilder::new();
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Bytes(v) => b.append_value(v),
                    _ => return Err(ParquetError::InvalidInput("expected binary".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::LargeBinary => {
            let mut b = LargeBinaryBuilder::new();
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Bytes(v) => b.append_value(v),
                    _ => return Err(ParquetError::InvalidInput("expected binary".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::FixedSizeBinary(16) => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(n, 16);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Uuid(u) => {
                        b.append_value(u.as_bytes()).map_err(|e| {
                            ParquetError::InvalidInput(format!("Failed to append UUID: {}", e))
                        })?;
                    }
                    _ => {
                        return Err(ParquetError::InvalidInput(
                            "expected uuid as fixed[16]".into(),
                        ))
                    }
                }
            }
            Arc::new(b.finish())
        }

        DataType::Date32 => {
            let mut b = Date32Builder::with_capacity(n);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Date(d) => b.append_value((*d - epoch).num_days() as i32),
                    _ => return Err(ParquetError::InvalidInput("expected date32".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Time64(TimeUnit::Microsecond) => {
            let mut b = Time64MicrosecondBuilder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::Time(t) => {
                        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                        let duration = t.signed_duration_since(midnight);
                        let micros = duration.num_microseconds().unwrap_or(0);
                        b.append_value(micros);
                    }
                    _ => return Err(ParquetError::InvalidInput("expected time64(us)".into())),
                }
            }
            Arc::new(b.finish())
        }

        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(n);
            for c in cells {
                match c {
                    Cell::Null => b.append_null(),
                    Cell::TimeStamp(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                    Cell::TimeStampTz(ts) => b.append_value(ts.timestamp_micros()),
                    _ => return Err(ParquetError::InvalidInput("expected timestamp(us)".into())),
                }
            }
            let array = b.finish();
            Arc::new(array.with_timezone_opt(tz.clone()))
        }

        DataType::List(child) => {
            let mut lb = ListBuilder::with_capacity(child_builder(child.data_type())?, n);
            for c in cells {
                match c {
                    Cell::Null | Cell::Array(ArrayCell::Null) => lb.append(false),
                    Cell::Array(arr) => {
                        append_array_into_list(lb.values(), child.data_type(), arr)?;
                        lb.append(true);
                    }
                    _ => return Err(ParquetError::InvalidInput("expected list".into())),
                }
            }
            Arc::new(lb.finish())
        }

        DataType::LargeList(child) => {
            let mut lb = LargeListBuilder::with_capacity(child_builder(child.data_type())?, n);
            for c in cells {
                match c {
                    Cell::Null | Cell::Array(ArrayCell::Null) => lb.append(false),
                    Cell::Array(arr) => {
                        append_array_into_list(lb.values(), child.data_type(), arr)?;
                        lb.append(true);
                    }
                    _ => return Err(ParquetError::InvalidInput("expected largelist".into())),
                }
            }
            Arc::new(lb.finish())
        }

        other => {
            return Err(ParquetError::UnsupportedDataType(
                format!("{other:?}").into(),
            ))
        }
    })
}

fn child_builder(dt: &DataType) -> Result<Box<dyn ArrayBuilder>, ParquetError> {
    Ok(match dt {
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::UInt32 => Box::new(UInt32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float32 => Box::new(Float32Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        DataType::Binary => Box::new(BinaryBuilder::new()),
        DataType::FixedSizeBinary(16) => Box::new(FixedSizeBinaryBuilder::new(16)),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Time64(TimeUnit::Microsecond) => Box::new(Time64MicrosecondBuilder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            Box::new(TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone()))
        }
        DataType::List(child) => {
            let inner = child_builder(child.data_type())?;
            Box::new(ListBuilder::new(inner))
        }
        DataType::LargeList(child) => {
            let inner = child_builder(child.data_type())?;
            Box::new(LargeListBuilder::new(inner))
        }
        _ => {
            return Err(ParquetError::InvalidInput(
                "unsupported child DataType for list".into(),
            ))
        }
    })
}

fn append_array_into_list(
    values: &mut dyn ArrayBuilder,
    child_dt: &DataType,
    arr: &ArrayCell,
) -> Result<(), ParquetError> {
    match (child_dt, arr) {
        (DataType::Boolean, ArrayCell::Bool(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Int16, ArrayCell::I16(vs)) => {
            let b = values.as_any_mut().downcast_mut::<Int16Builder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Int32, ArrayCell::I32(vs)) => {
            let b = values.as_any_mut().downcast_mut::<Int32Builder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::UInt32, ArrayCell::U32(vs)) => {
            let b = values.as_any_mut().downcast_mut::<UInt32Builder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Int64, ArrayCell::I64(vs)) => {
            let b = values.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Float32, ArrayCell::F32(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Float64, ArrayCell::F64(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(*x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Utf8, ArrayCell::String(vs)) => {
            let b = values.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Utf8, ArrayCell::Json(vs)) => {
            let b = values.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(x.to_string()),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Binary, ArrayCell::Bytes(vs)) => {
            let b = values.as_any_mut().downcast_mut::<BinaryBuilder>().unwrap();
            for v in vs {
                match v {
                    Some(x) => b.append_value(x),
                    None => b.append_null(),
                }
            }
        }
        (DataType::FixedSizeBinary(16), ArrayCell::Uuid(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<FixedSizeBinaryBuilder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(x) => b
                        .append_value(x.as_bytes())
                        .map_err(|e| ParquetError::InvalidInput(e.to_string()))?,
                    None => b.append_null(),
                }
            }
        }
        (DataType::Date32, ArrayCell::Date(vs)) => {
            let b = values.as_any_mut().downcast_mut::<Date32Builder>().unwrap();
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            for v in vs {
                match v {
                    Some(d) => b.append_value((*d - epoch).num_days() as i32),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Time64(TimeUnit::Microsecond), ArrayCell::Time(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<Time64MicrosecondBuilder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(t) => {
                        let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
                        let duration = t.signed_duration_since(midnight);
                        let micros = duration.num_microseconds().unwrap_or(0);
                        b.append_value(micros);
                    }
                    None => b.append_null(),
                }
            }
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), ArrayCell::TimeStamp(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(ts) => b.append_value(ts.and_utc().timestamp_micros()),
                    None => b.append_null(),
                }
            }
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), ArrayCell::TimeStampTz(vs)) => {
            let b = values
                .as_any_mut()
                .downcast_mut::<TimestampMicrosecondBuilder>()
                .unwrap();
            for v in vs {
                match v {
                    Some(ts) => b.append_value(ts.timestamp_micros()),
                    None => b.append_null(),
                }
            }
        }

        // NUMERIC/DECIMAL mapping not provided – you can add Decimal128 here:
        (DataType::Decimal128(_, _), ArrayCell::Numeric(_)) => {
            return Err(ParquetError::UnsupportedDataType(
                "Decimal128 mapping from PgNumeric not implemented".into(),
            ))
        }

        // Nested lists: ArrayCell has no nested variant; if you need List<List<T>>, extend ArrayCell.
        (DataType::List(_), _) | (DataType::LargeList(_), _) => {
            return Err(ParquetError::UnsupportedDataType(
                "Nested lists require nested ArrayCell; not implemented".into(),
            ))
        }

        _ => {
            return Err(ParquetError::UnsupportedDataType(
                "array element/DataType mismatch".into(),
            ))
        }
    }
    Ok(())
}

/// Precise PostgreSQL to Parquet type mapping
pub fn postgres_to_parquet_type(typ: &Type) -> DataType {
    match typ {
        // Exact type mappings
        &Type::BOOL => DataType::Boolean,
        &Type::INT2 => DataType::Int64,
        &Type::INT4 => DataType::Int64,
        &Type::INT8 => DataType::Int64,
        &Type::FLOAT4 => DataType::Float64,
        &Type::FLOAT8 => DataType::Float64,
        &Type::NUMERIC => DataType::Float64,

        // Strings
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::TEXT | &Type::NAME => DataType::Utf8,
        &Type::UUID => DataType::Utf8,
        &Type::JSON | &Type::JSONB => DataType::Utf8,

        // Dates/Times - use Databricks preferred formats
        &Type::DATE => DataType::Date32,
        &Type::TIME => DataType::Time64(TimeUnit::Microsecond),
        &Type::TIMESTAMP => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        &Type::TIMESTAMPTZ => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),

        // Binary
        &Type::BYTEA => DataType::Binary,
        &Type::OID => DataType::Int64,

        // Arrays - map to lists
        &Type::BOOL_ARRAY => DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
        &Type::INT2_ARRAY => DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
        &Type::INT4_ARRAY => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        &Type::INT8_ARRAY => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        &Type::FLOAT4_ARRAY => {
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
        }
        &Type::FLOAT8_ARRAY => {
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
        }
        &Type::TEXT_ARRAY => DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),

        _ => DataType::Utf8, // Fallback to string
    }
}
