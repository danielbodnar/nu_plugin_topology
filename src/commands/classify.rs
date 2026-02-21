use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Record, Signature, Signals,
    SyntaxShape, Type, Value,
};

use crate::algo::{clustering, discover, taxonomy};
use crate::TopologyPlugin;

pub struct Classify;

impl PluginCommand for Classify {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology classify"
    }

    fn description(&self) -> &str {
        "Classify items into categories discovered automatically from content, or from a user-provided taxonomy file"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::table())
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text to classify (default: content)",
                Some('f'),
            )
            .named(
                "taxonomy",
                SyntaxShape::String,
                "Path to taxonomy JSON file. If omitted, taxonomy is discovered from the data",
                Some('t'),
            )
            .named(
                "clusters",
                SyntaxShape::Int,
                "Number of categories to discover (default: 15)",
                Some('k'),
            )
            .named(
                "sample",
                SyntaxShape::Int,
                "Max items to sample for discovery (HAC is O(n^2), default: 500)",
                None,
            )
            .named(
                "threshold",
                SyntaxShape::Float,
                "Minimum BM25 score to assign a category (default: 0.5)",
                None,
            )
            .named(
                "linkage",
                SyntaxShape::String,
                "HAC linkage: ward, complete, average, single (default: ward)",
                None,
            )
            .named(
                "seed",
                SyntaxShape::Int,
                "Random seed for sampling (default: 42)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["classify", "categorize", "label", "discover", "cluster", "taxonomy"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: r#"[[content]; ["rust memory safety"] ["python data science"] ["javascript web frontend"]] | topology classify --clusters 2"#,
                description: "Auto-discover 2 categories from content and classify",
                result: None,
            },
            Example {
                example: r#"open data.json | topology classify --taxonomy my-categories.json"#,
                description: "Classify using a user-provided taxonomy file",
                result: None,
            },
        ]
    }

    fn run(
        &self,
        _plugin: &TopologyPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let field: String = call
            .get_flag::<String>("field")?
            .unwrap_or_else(|| "content".into());
        let taxonomy_path: Option<String> = call.get_flag("taxonomy")?;
        let k: usize = call.get_flag::<i64>("clusters")?.unwrap_or(15) as usize;
        let sample_size: usize = call.get_flag::<i64>("sample")?.unwrap_or(500) as usize;
        let threshold: f64 = call.get_flag::<f64>("threshold")?.unwrap_or(0.5);
        let linkage_str: String = call
            .get_flag::<String>("linkage")?
            .unwrap_or_else(|| "ward".into());
        let seed: u64 = call.get_flag::<i64>("seed")?.unwrap_or(42) as u64;
        let head = call.head;

        let linkage = clustering::Linkage::from_str(&linkage_str).ok_or_else(|| {
            LabeledError::new(format!(
                "Unknown linkage '{linkage_str}'. Use: ward, complete, average, single"
            ))
        })?;

        let rows: Vec<Value> = input.into_iter().collect();
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        // Extract text from each row
        let texts: Vec<String> = rows
            .iter()
            .map(|row| {
                row.get_data_by_key(&field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_default()
            })
            .collect();

        // Get or discover taxonomy
        let tax = match taxonomy_path {
            Some(path) => taxonomy::load_taxonomy(&path)
                .map_err(|e| LabeledError::new(e))?,
            None => {
                let config = discover::DiscoverConfig {
                    k,
                    sample_size,
                    label_terms: 3,
                    keywords_per_cluster: 20,
                    linkage,
                    seed,
                };
                discover::discover_taxonomy(&texts, &config)
            }
        };

        // Classify all items against the taxonomy
        let classifications = discover::classify_against_taxonomy(&texts, &tax, threshold);

        // Append columns
        let results: Vec<Value> = rows
            .into_iter()
            .zip(classifications)
            .map(|(row, (category, hierarchy, confidence))| {
                append_columns(
                    row,
                    &[
                        ("_category", Value::string(&category, head)),
                        ("_hierarchy", Value::string(&hierarchy, head)),
                        ("_confidence", Value::float(confidence, head)),
                    ],
                    head,
                )
            })
            .collect();

        Ok(ListStream::new(results.into_iter(), head, Signals::empty()).into())
    }
}

fn append_columns(row: Value, cols: &[(&str, Value)], span: nu_protocol::Span) -> Value {
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
