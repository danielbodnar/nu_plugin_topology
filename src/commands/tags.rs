use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Signature, Signals, SyntaxShape,
    Type, Value,
};

use crate::algo::{tfidf, tokenizer};
use crate::TopologyPlugin;

use super::util;

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
            .input_output_types(vec![
                (Type::table(), Type::table()),
                (Type::list(Type::Any), Type::list(Type::Any)),
                (Type::String, Type::record()),
                (Type::Any, Type::Any),
            ])
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
            .named(
                "cache",
                SyntaxShape::String,
                "Path to SQLite cache database for persistent artifact caching",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["tags", "keywords", "tfidf", "extract", "terms"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: r#"[[content]; ["rust memory safety ownership borrow checker"]] | topology tags --count 3"#,
                description: "Extract top 3 tags from a table",
                result: None,
            },
            Example {
                example: r#""rust memory safety ownership borrow checker" | topology tags --count 3"#,
                description: "Extract tags from a single string",
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
        let count: usize = call.get_flag::<i64>("count")?.unwrap_or(5) as usize;
        let _cache_path: Option<String> = call.get_flag("cache")?;
        let head = call.head;

        let rows = util::normalize_input(input, head);
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

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

        let results: Vec<Value> = rows
            .into_iter()
            .enumerate()
            .map(|(i, row)| {
                let top = corpus.top_terms(i, count);
                let tags: Vec<Value> = top
                    .iter()
                    .map(|(term, _)| Value::string(term, head))
                    .collect();

                util::append_column(row, "_tags", Value::list(tags, head), head)
            })
            .collect();

        Ok(ListStream::new(results.into_iter(), head, Signals::empty()).into())
    }
}
