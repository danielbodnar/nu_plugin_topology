use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Record, Signature, Signals,
    SyntaxShape, Type, Value,
};

use crate::algo::{tfidf, tokenizer};
use crate::TopologyPlugin;

pub struct Tags;

impl PluginCommand for Tags {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology tags"
    }

    fn description(&self) -> &str {
        "Extract top TF-IDF tags from text content"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::table())
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text (default: content)",
                Some('f'),
            )
            .named(
                "count",
                SyntaxShape::Int,
                "Number of tags to extract per item (default: 5)",
                Some('n'),
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["tags", "keywords", "tfidf", "extract", "terms"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[content]; ["rust memory safety ownership borrow checker"] ["python data science pandas numpy"]] | topology tags --count 3"#,
            description: "Extract top 3 TF-IDF tags per item",
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
        let count: usize = call.get_flag::<i64>("count")?.unwrap_or(5) as usize;
        let head = call.head;

        let rows: Vec<Value> = input.into_iter().collect();
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        // Build corpus for IDF computation
        let mut corpus = tfidf::Corpus::new();
        let token_lists: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                let text = row
                    .get_data_by_key(&field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_default();
                tokenizer::tokenize(&text)
            })
            .collect();

        for tokens in &token_lists {
            corpus.add_document(tokens);
        }

        // Extract top terms for each document
        let results: Vec<Value> = rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| {
                let top = corpus.top_terms(i, count);
                let tags: Vec<Value> = top
                    .iter()
                    .map(|(term, _)| Value::string(term, head))
                    .collect();

                append_column(row, "_tags", Value::list(tags, head), head)
            })
            .collect();

        Ok(ListStream::new(results.into_iter(), head, Signals::empty()).into())
    }
}

fn append_column(row: Value, col_name: &str, col_value: Value, span: nu_protocol::Span) -> Value {
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
