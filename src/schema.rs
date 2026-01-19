use clickhouse_arrow::{Tz, native::types::Type};
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ColumnSchema, Type as EType},
};

fn postgres_col_type_to_ch_type(col: &ColumnSchema) -> EtlResult<Type> {
    let typ = match &col.typ {
        &EType::BOOL => Type::UInt8,
        &EType::CHAR | &EType::BPCHAR | &EType::VARCHAR | &EType::NAME | &EType::TEXT => {
            Type::String
        }
        &EType::INT2 => Type::Int16,
        &EType::INT4 => Type::Int32,
        &EType::INT8 => Type::Int64,
        &EType::FLOAT4 => Type::Float32,
        &EType::FLOAT8 => Type::Float64,
        &EType::UUID => Type::Uuid,
        &EType::DATE => Type::Date32,
        &EType::TIMESTAMP => Type::DateTime(Tz::UTC),
        &EType::TIMESTAMPTZ => Type::DateTime(Tz::UTC),
        &EType::JSON | &EType::JSONB => Type::String,
        &EType::OID => Type::UInt64,
        &EType::BYTEA => Type::Binary,
        &EType::BOOL_ARRAY => Type::Array(Box::new(Type::UInt8)),
        &EType::CHAR_ARRAY
        | &EType::BPCHAR_ARRAY
        | &EType::VARCHAR_ARRAY
        | &EType::NAME_ARRAY
        | &EType::TEXT_ARRAY => Type::Array(Box::new(Type::String)),
        &EType::INT2_ARRAY => Type::Array(Box::new(Type::Int16)),
        &EType::INT4_ARRAY => Type::Array(Box::new(Type::Int32)),
        &EType::INT8_ARRAY => Type::Array(Box::new(Type::Int64)),
        &EType::FLOAT4_ARRAY => Type::Array(Box::new(Type::Float32)),
        &EType::FLOAT8_ARRAY => Type::Array(Box::new(Type::Float64)),
        &EType::DATE_ARRAY => Type::Array(Box::new(Type::Date32)),
        &EType::TIMESTAMP_ARRAY => Type::Array(Box::new(Type::DateTime(Tz::UTC))),
        &EType::TIMESTAMPTZ_ARRAY => Type::Array(Box::new(Type::DateTime(Tz::UTC))),
        &EType::JSON_ARRAY | &EType::JSONB_ARRAY => Type::Array(Box::new(Type::String)),
        &EType::OID_ARRAY => Type::Array(Box::new(Type::UInt64)),
        &EType::UUID_ARRAY => Type::Array(Box::new(Type::Uuid)),
        &EType::BYTEA_ARRAY => Type::Array(Box::new(Type::Binary)),
        _ => {
            return Err(etl_error!(
                ErrorKind::ConversionError,
                "Unsupported column type",
                format!("Column '{}' has unsupported type '{:?}'", col.name, col.typ)
            ));
        }
    };
    Ok(match col.nullable {
        true => Type::Nullable(Box::new(typ)),
        false => typ,
    })
}

pub fn postgres_to_ch_schema(
    column_schemas: &[ColumnSchema],
) -> EtlResult<Vec<(String, clickhouse_arrow::native::types::Type)>> {
    let mut ch_columns = Vec::with_capacity(column_schemas.len());
    for col in column_schemas {
        let ch_type = postgres_col_type_to_ch_type(col)?;
        ch_columns.push((col.name.clone(), ch_type));
    }
    ch_columns.push(("is_deleted".to_string(), Type::UInt8));
    Ok(ch_columns)
}
