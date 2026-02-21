use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Record, Signature, Signals,
    SyntaxShape, Type, Value,
};
use rayon::prelude::*;

use crate::algo::{
    simhash::{fingerprint_to_hex, simhash, simhash_uniform},
    tfidf::Corpus,
    tokenizer,
};
use crate::TopologyPlugin;

pub struct Fingerprint;

impl PluginCommand for Fingerprint {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology fingerprint"
    }

    fn description(&self) -> &str {
        "Compute SimHash content fingerprints for deduplication"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::table())
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text to fingerprint (default: content)",
                Some('f'),
            )
            .switch(
                "weighted",
                "Use TF-IDF weighted SimHash (slower, more accurate)",
                Some('w'),
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec![
            "fingerprint",
            "simhash",
            "hash",
            "dedup",
            "duplicate",
            "similarity",
        ]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example:
                "[[content]; [\"hello world\"] [\"hello earth\"]] | topology fingerprint",
            description: "Compute SimHash fingerprints for content field",
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
        let weighted: bool = call.has_flag("weighted")?;
        let head = call.head;

        // Collect all rows
        let rows: Vec<Value> = input.into_iter().collect();

        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        // Extract text from each row and tokenize
        let texts: Vec<String> = rows
            .iter()
            .map(|row| {
                row.get_data_by_key(&field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_default()
            })
            .collect();

        let token_lists: Vec<Vec<String>> =
            texts.par_iter().map(|t| tokenizer::tokenize(t)).collect();

        // If weighted, build corpus for IDF
        let fingerprints: Vec<u64> = if weighted {
            let mut corpus = Corpus::new();
            for tokens in &token_lists {
                corpus.add_document(tokens);
            }
            token_lists
                .par_iter()
                .map(|tokens| {
                    let weights = corpus.token_weights(tokens);
                    simhash(tokens, &weights)
                })
                .collect()
        } else {
            token_lists
                .par_iter()
                .map(|tokens| simhash_uniform(tokens))
                .collect()
        };

        // Append _fingerprint column to each row
        let result: Vec<Value> = rows
            .into_iter()
            .zip(fingerprints)
            .map(|(row, fp)| {
                let hex = fingerprint_to_hex(fp);
                append_column(row, "_fingerprint", Value::string(hex, head), head)
            })
            .collect();

        Ok(ListStream::new(result.into_iter(), head, Signals::empty()).into())
    }
}

/// Append a column to a record Value, returning a new record.
fn append_column(row: Value, col_name: &str, col_value: Value, span: nu_protocol::Span) -> Value {
    match row {
        Value::Record { val, .. } => {
            let mut record = val.into_owned();
            record.push(col_name, col_value);
            Value::record(record, span)
        }
        other => {
            // Wrap non-record values
            let mut record = Record::new();
            record.push("value", other);
            record.push(col_name, col_value);
            Value::record(record, span)
        }
    }
}
