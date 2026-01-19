use chrono::NaiveDate;
use clickhouse_arrow::native::values::Value;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ArrayCell, TableRow},
};

const UNIX_EPOCH: NaiveDate =
    NaiveDate::from_ymd_opt(1970, 1, 1).expect("unix epoch is a valid date");

fn array_cell_to_value(cell: ArrayCell) -> EtlResult<Value> {
    use ArrayCell::*;
    use clickhouse_arrow::native::values::Date32;
    Ok(Value::Array(match cell {
        Bool(items) => items
            .into_iter()
            .map(|v| v.map(|b| Value::UInt8(b as u8)).unwrap_or(Value::Null))
            .collect(),
        String(items) => items
            .into_iter()
            .map(|v| {
                v.map(|s| Value::String(s.into_bytes()))
                    .unwrap_or(Value::Null)
            })
            .collect(),
        I16(items) => items
            .into_iter()
            .map(|v| v.map(Value::Int16).unwrap_or(Value::Null))
            .collect(),
        I32(items) => items
            .into_iter()
            .map(|v| v.map(Value::Int32).unwrap_or(Value::Null))
            .collect(),
        U32(items) => items
            .into_iter()
            .map(|v| v.map(Value::UInt32).unwrap_or(Value::Null))
            .collect(),
        I64(items) => items
            .into_iter()
            .map(|v| v.map(Value::Int64).unwrap_or(Value::Null))
            .collect(),
        F32(items) => items
            .into_iter()
            .map(|v| v.map(Value::Float32).unwrap_or(Value::Null))
            .collect(),
        F64(items) => items
            .into_iter()
            .map(|v| v.map(Value::Float64).unwrap_or(Value::Null))
            .collect(),
        Date(naive_dates) => naive_dates
            .into_iter()
            .map(|v| {
                v.map(|d| {
                    Value::Date32(Date32(d.signed_duration_since(UNIX_EPOCH).num_days() as i32))
                })
                .unwrap_or(Value::Null)
            })
            .collect(),
        Timestamp(naive_date_times) => naive_date_times
            .into_iter()
            .map(|v| {
                v.map(|dt| {
                    Value::DateTime(
                        clickhouse_arrow::native::values::DateTime::from_chrono_infallible_utc(
                            dt.and_utc(),
                        ),
                    )
                })
                .unwrap_or(Value::Null)
            })
            .collect(),
        TimestampTz(date_times) => date_times
            .into_iter()
            .map(|v| {
                v.map(|dt| {
                    Value::DateTime(
                        clickhouse_arrow::native::values::DateTime::from_chrono_infallible_utc(dt),
                    )
                })
                .unwrap_or(Value::Null)
            })
            .collect(),
        Uuid(uuids) => uuids
            .into_iter()
            .map(|v| v.map(Value::Uuid).unwrap_or(Value::Null))
            .collect(),
        Json(values) => values
            .into_iter()
            .map(|v| {
                v.map(|j| Ok(Value::String(serde_json::to_vec(&j)?)))
                    .unwrap_or(Ok(Value::Null))
            })
            .collect::<EtlResult<_>>()?,
        Bytes(b) => b
            .into_iter()
            .map(|v| v.map(Value::String).unwrap_or(Value::Null))
            .collect(),
        Numeric(_) | Time(_) => {
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "Unsupported cell type",
                "Cannot convert cell to ClickHouse value"
            ));
        }
    }))
}

fn cell_to_value(cell: etl::types::Cell) -> EtlResult<Value> {
    use clickhouse_arrow::native::values::{Date32, DateTime};
    use etl::types::Cell;
    Ok(match cell {
        Cell::Null => Value::Null,
        Cell::Bool(b) => Value::UInt8(b as u8),
        Cell::I16(i) => Value::Int16(i),
        Cell::I32(i) => Value::Int32(i),
        Cell::I64(i) => Value::Int64(i),
        Cell::U32(u) => Value::UInt32(u),
        Cell::F32(f) => Value::Float32(f),
        Cell::F64(f) => Value::Float64(f),
        Cell::String(s) => Value::String(s.into_bytes()),
        Cell::Uuid(u) => Value::Uuid(u),
        Cell::Date(d) => {
            Value::Date32(Date32(d.signed_duration_since(UNIX_EPOCH).num_days() as i32))
        }
        Cell::Timestamp(ts) => Value::DateTime(DateTime::from_chrono_infallible_utc(ts.and_utc())),
        Cell::TimestampTz(ts) => Value::DateTime(DateTime::from_chrono_infallible_utc(ts)),
        Cell::Json(j) => Value::String(serde_json::to_vec(&j)?),
        Cell::Bytes(b) => Value::String(b),
        Cell::Array(c) => array_cell_to_value(c)?,
        _ => {
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "Unsupported cell type",
                "Cannot convert cell to ClickHouse value"
            ));
        }
    })
}

pub fn apply_row(
    buffer: &mut [Value],
    num_rows: usize,
    num_columns: usize,
    row_idx: usize,
    row: TableRow,
    is_deleted: bool,
) -> EtlResult<()> {
    if row.values.len() != num_columns - 1 {
        return Err(etl_error!(
            ErrorKind::ConversionError,
            "Row has incorrect number of columns",
            format!(
                "Expected {} columns, found {}",
                num_columns - 2,
                row.values.len()
            )
        ));
    }
    for (col_idx, cell) in row.values.into_iter().enumerate() {
        let value = cell_to_value(cell)?;
        buffer[col_idx * num_rows + row_idx] = value;
    }
    buffer[(num_columns - 1) * num_rows + row_idx] = Value::UInt8(is_deleted as u8);
    Ok(())
}
