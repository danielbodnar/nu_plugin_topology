use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, PipelineData, Record, Signature, Type, Value,
};
use std::collections::{HashMap, HashSet};

use crate::TopologyPlugin;

pub struct Analyze;

impl PluginCommand for Analyze {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology analyze"
    }

    fn description(&self) -> &str {
        "Analyze table structure: field stats, cardinality, patterns, and data quality"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::record())
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["analyze", "stats", "cardinality", "profile", "quality"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example:
                "[[name lang]; [foo rust] [bar go] [baz rust]] | topology analyze",
            description: "Analyze table structure and field statistics",
            result: None,
        }]
    }

    fn run(
        &self,
        _plugin: &TopologyPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let head = call.head;
        let rows: Vec<Value> = input.into_iter().collect();
        let total = rows.len();

        if total == 0 {
            let mut report = Record::new();
            report.push("total_rows", Value::int(0, head));
            report.push("columns", Value::list(vec![], head));
            report.push("fields", Value::record(Record::new(), head));
            return Ok(PipelineData::Value(Value::record(report, head), None));
        }

        // Discover columns from first row
        let columns: Vec<String> = match &rows[0] {
            Value::Record { val, .. } => val.columns().map(|c| c.to_string()).collect(),
            _ => vec!["value".into()],
        };

        // Analyze each column
        let mut field_reports = Record::new();

        for col in &columns {
            let mut values: Vec<String> = Vec::with_capacity(total);
            let mut null_count: usize = 0;
            let mut unique_vals: HashSet<String> = HashSet::new();
            let mut type_counts: HashMap<String, usize> = HashMap::new();
            let mut total_len: usize = 0;
            let mut min_len: usize = usize::MAX;
            let mut max_len: usize = 0;

            for row in &rows {
                let val = match row {
                    Value::Record { val, .. } => val.get(col).cloned(),
                    other => {
                        if col == "value" {
                            Some(other.clone())
                        } else {
                            None
                        }
                    }
                };

                match val {
                    Some(v) => {
                        let type_name = v.get_type().to_string();
                        *type_counts.entry(type_name).or_insert(0) += 1;

                        if let Ok(s) = v.coerce_string() {
                            let len = s.len();
                            total_len += len;
                            min_len = min_len.min(len);
                            max_len = max_len.max(len);
                            unique_vals.insert(s.clone());
                            values.push(s);
                        } else {
                            let s = format!("{v:?}");
                            unique_vals.insert(s.clone());
                            values.push(s);
                        }
                    }
                    None => {
                        null_count += 1;
                    }
                }
            }

            let non_null = total - null_count;
            let cardinality = unique_vals.len();
            let avg_len = if non_null > 0 {
                total_len as f64 / non_null as f64
            } else {
                0.0
            };

            if min_len == usize::MAX {
                min_len = 0;
            }

            // Top 5 most common values
            let mut freq: HashMap<&str, usize> = HashMap::new();
            for v in &values {
                *freq.entry(v.as_str()).or_insert(0) += 1;
            }
            let mut freq_vec: Vec<(&str, usize)> = freq.into_iter().collect();
            freq_vec.sort_by(|a, b| b.1.cmp(&a.1));
            freq_vec.truncate(5);

            let top_values: Vec<Value> = freq_vec
                .iter()
                .map(|(val, count)| {
                    let mut r = Record::new();
                    r.push("value", Value::string(val.to_string(), head));
                    r.push("count", Value::int(*count as i64, head));
                    Value::record(r, head)
                })
                .collect();

            // Type distribution
            let types: Vec<Value> = type_counts
                .iter()
                .map(|(t, c)| {
                    let mut r = Record::new();
                    r.push("type", Value::string(t.clone(), head));
                    r.push("count", Value::int(*c as i64, head));
                    Value::record(r, head)
                })
                .collect();

            let mut col_report = Record::new();
            col_report.push("non_null", Value::int(non_null as i64, head));
            col_report.push("null_count", Value::int(null_count as i64, head));
            col_report.push("cardinality", Value::int(cardinality as i64, head));
            col_report.push(
                "uniqueness",
                Value::float(
                    if non_null > 0 {
                        cardinality as f64 / non_null as f64
                    } else {
                        0.0
                    },
                    head,
                ),
            );
            col_report.push("avg_length", Value::float(avg_len, head));
            col_report.push("min_length", Value::int(min_len as i64, head));
            col_report.push("max_length", Value::int(max_len as i64, head));
            col_report.push("types", Value::list(types, head));
            col_report.push("top_values", Value::list(top_values, head));

            field_reports.push(col, Value::record(col_report, head));
        }

        let mut report = Record::new();
        report.push("total_rows", Value::int(total as i64, head));
        report.push(
            "columns",
            Value::list(
                columns.iter().map(|c| Value::string(c, head)).collect(),
                head,
            ),
        );
        report.push("num_columns", Value::int(columns.len() as i64, head));
        report.push("fields", Value::record(field_reports, head));

        Ok(PipelineData::Value(Value::record(report, head), None))
    }
}
