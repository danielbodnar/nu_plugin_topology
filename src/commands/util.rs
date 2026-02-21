use nu_protocol::{PipelineData, Record, Span, Value};

/// Normalize any PipelineData into a Vec<Value> of records.
///
/// Handles:
///   - Table (list of records) → pass through
///   - Single record → [record]
///   - List of strings → [{content: s1}, {content: s2}, ...]
///   - Single string → [{content: s}]
///   - List of non-records → [{value: v1}, {value: v2}, ...]
///   - Single non-record → [{value: v}]
///   - Empty/Nothing → []
pub fn normalize_input(input: PipelineData, span: Span) -> Vec<Value> {
    match input {
        PipelineData::Value(value, _) => normalize_value(value, span),
        other => {
            let values: Vec<Value> = other.into_iter().collect();
            if values.is_empty() {
                return vec![];
            }
            // Check if first item is already a record
            if matches!(values.first(), Some(Value::Record { .. })) {
                values
            } else {
                values.into_iter().map(|v| wrap_value(v, span)).collect()
            }
        }
    }
}

fn normalize_value(value: Value, span: Span) -> Vec<Value> {
    match value {
        Value::List { vals, .. } => {
            if vals.is_empty() {
                return vec![];
            }
            if matches!(vals.first(), Some(Value::Record { .. })) {
                vals
            } else {
                vals.into_iter().map(|v| wrap_value(v, span)).collect()
            }
        }
        Value::Record { .. } => vec![value],
        Value::Nothing { .. } => vec![],
        other => vec![wrap_value(other, span)],
    }
}

/// Wrap a non-record value into a record.
/// Strings get `{content: s}`, everything else gets `{value: v}`.
fn wrap_value(v: Value, span: Span) -> Value {
    let mut record = Record::new();
    match &v {
        Value::String { .. } => record.push("content", v),
        _ => record.push("value", v),
    }
    Value::record(record, span)
}

/// Append one column to a record Value. Non-records get wrapped first.
pub fn append_column(row: Value, col_name: &str, col_value: Value, span: Span) -> Value {
    match row {
        Value::Record { val, .. } => {
            let mut record = val.into_owned();
            record.push(col_name, col_value);
            Value::record(record, span)
        }
        other => {
            let mut record = Record::new();
            record.push("value", other);
            record.push(col_name, col_value);
            Value::record(record, span)
        }
    }
}

/// Append multiple columns to a record Value.
pub fn append_columns(row: Value, cols: &[(&str, Value)], span: Span) -> Value {
    match row {
        Value::Record { val, .. } => {
            let mut record = val.into_owned();
            for (name, value) in cols {
                record.push(*name, value.clone());
            }
            Value::record(record, span)
        }
        other => {
            let mut record = Record::new();
            record.push("value", other);
            for (name, value) in cols {
                record.push(*name, value.clone());
            }
            Value::record(record, span)
        }
    }
}
