use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, PipelineData, Record, Signature, SyntaxShape, Type, Value,
};

use crate::algo::{nmf, tfidf, tokenizer};
use crate::TopologyPlugin;

pub struct Topics;

impl PluginCommand for Topics {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology topics"
    }

    fn description(&self) -> &str {
        "Discover topics using NMF (Non-negative Matrix Factorization)"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::record())
            .named(
                "field",
                SyntaxShape::String,
                "Field containing text (default: content)",
                Some('f'),
            )
            .named(
                "topics",
                SyntaxShape::Int,
                "Number of topics to discover (default: 5)",
                Some('k'),
            )
            .named(
                "terms",
                SyntaxShape::Int,
                "Number of top terms per topic (default: 10)",
                Some('n'),
            )
            .named(
                "iterations",
                SyntaxShape::Int,
                "NMF iterations (default: 200)",
                None,
            )
            .named(
                "vocab",
                SyntaxShape::Int,
                "Max vocabulary size (default: 5000)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["topics", "nmf", "model", "discover", "theme"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[content]; ["rust systems programming"] ["python data science"] ["javascript web browser"]] | topology topics --topics 2"#,
            description: "Discover 2 topics from content",
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
        let k: usize = call.get_flag::<i64>("topics")?.unwrap_or(5) as usize;
        let top_n: usize = call.get_flag::<i64>("terms")?.unwrap_or(10) as usize;
        let max_iter: usize = call.get_flag::<i64>("iterations")?.unwrap_or(200) as usize;
        let vocab_limit: usize = call.get_flag::<i64>("vocab")?.unwrap_or(5000) as usize;
        let head = call.head;

        let rows: Vec<Value> = input.into_iter().collect();
        if rows.is_empty() {
            return Err(LabeledError::new("Need at least 1 item for topic modeling"));
        }

        // Build TF-IDF vectors
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

        let vectors: Vec<std::collections::HashMap<String, f64>> = (0..rows.len())
            .map(|i| corpus.tfidf_vector(i))
            .collect();

        // Run NMF
        let result = nmf::nmf(&vectors, k, max_iter, vocab_limit);
        let dominant = result.dominant_topics();

        // Build output
        let topics: Vec<Value> = (0..k)
            .map(|t| {
                let top = result.top_terms(t, top_n);
                let terms: Vec<Value> = top
                    .iter()
                    .map(|(term, weight)| {
                        let mut r = Record::new();
                        r.push("term", Value::string(term, head));
                        r.push("weight", Value::float(*weight, head));
                        Value::record(r, head)
                    })
                    .collect();

                let members: Vec<Value> = dominant
                    .iter()
                    .enumerate()
                    .filter(|(_, &topic)| topic == t)
                    .map(|(i, _)| Value::int(i as i64, head))
                    .collect();

                let label = top
                    .iter()
                    .take(3)
                    .map(|(t, _)| t.as_str())
                    .collect::<Vec<&str>>()
                    .join(", ");

                let mut topic_rec = Record::new();
                topic_rec.push("id", Value::int(t as i64, head));
                topic_rec.push("label", Value::string(&label, head));
                topic_rec.push("size", Value::int(members.len() as i64, head));
                topic_rec.push("terms", Value::list(terms, head));
                topic_rec.push("members", Value::list(members, head));

                Value::record(topic_rec, head)
            })
            .collect();

        let assignments: Vec<Value> = dominant
            .iter()
            .enumerate()
            .map(|(i, &topic)| {
                let mut r = Record::new();
                r.push("item", Value::int(i as i64, head));
                r.push("topic", Value::int(topic as i64, head));
                Value::record(r, head)
            })
            .collect();

        let mut output = Record::new();
        output.push("num_topics", Value::int(k as i64, head));
        output.push("num_items", Value::int(rows.len() as i64, head));
        output.push("topics", Value::list(topics, head));
        output.push("assignments", Value::list(assignments, head));

        Ok(PipelineData::Value(Value::record(output, head), None))
    }
}
