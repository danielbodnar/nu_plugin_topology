use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Record, Signature, Signals,
    SyntaxShape, Type, Value,
};
use rayon::prelude::*;

use crate::algo::{taxonomy, tfidf, tokenizer};
use crate::TopologyPlugin;

pub struct Classify;

impl PluginCommand for Classify {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology classify"
    }

    fn description(&self) -> &str {
        "Classify items into taxonomy categories using BM25 scoring"
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
                "Taxonomy JSON string (default: built-in 17-category taxonomy)",
                Some('t'),
            )
            .named(
                "threshold",
                SyntaxShape::Float,
                "Minimum BM25 score to assign a category (default: 0.5)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["classify", "categorize", "label", "bm25", "taxonomy"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[{content: "rust memory safety systems programming"}] | topology classify"#,
            description: "Classify using built-in taxonomy",
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
        let field: String = call
            .get_flag::<String>("field")?
            .unwrap_or_else(|| "content".into());
        let taxonomy_json: Option<String> = call.get_flag("taxonomy")?;
        let threshold: f64 = call.get_flag::<f64>("threshold")?.unwrap_or(0.5);
        let head = call.head;

        let tax = match taxonomy_json {
            Some(json) => taxonomy::parse_taxonomy(&json)
                .map_err(|e| LabeledError::new(e))?,
            None => taxonomy::default_taxonomy(),
        };

        let flat = tax.flatten();

        let rows: Vec<Value> = input.into_iter().collect();
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        // Build a corpus from the taxonomy categories (each category = one "document")
        let mut corpus = tfidf::Corpus::new();
        for (_, keywords) in &flat {
            corpus.add_document(keywords);
        }

        // For each row, tokenize and score against each category
        let results: Vec<Value> = rows
            .into_par_iter()
            .map(|row| {
                let text = row
                    .get_data_by_key(&field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_default();

                let tokens = tokenizer::tokenize(&text);

                let mut best_score = 0.0f64;
                let mut best_category = String::new();
                let mut best_path = String::new();

                for (doc_idx, (path, keywords)) in flat.iter().enumerate() {
                    let score = corpus.bm25_score(doc_idx, &tokens);
                    if score > best_score {
                        best_score = score;
                        best_category = path.split(" > ").last().unwrap_or(path).to_string();
                        best_path = path.clone();
                    }
                }

                let (category, hierarchy, confidence) = if best_score >= threshold {
                    (best_category, best_path, best_score)
                } else {
                    ("Uncategorized".into(), "Uncategorized".into(), 0.0)
                };

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
