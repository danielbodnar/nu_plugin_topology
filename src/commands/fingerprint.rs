use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Signature, Signals, SyntaxShape,
    Type, Value,
};
use rayon::prelude::*;

use crate::algo::{
    simhash::{fingerprint_to_hex, simhash, simhash_uniform},
    tfidf::Corpus,
    tokenizer,
};
use crate::TopologyPlugin;

use super::util;

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
            .input_output_types(vec![
                (Type::table(), Type::table()),
                (Type::list(Type::Any), Type::list(Type::Any)),
                (Type::record(), Type::record()),
                (Type::String, Type::record()),
                (Type::Any, Type::Any),
            ])
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
            .named(
                "cache",
                SyntaxShape::String,
                "Path to SQLite cache database for persistent artifact caching",
                None,
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
        vec![
            Example {
                example:
                    "[[content]; [\"hello world\"] [\"hello earth\"]] | topology fingerprint",
                description: "Fingerprint a table",
                result: None,
            },
            Example {
                example: "\"hello world\" | topology fingerprint",
                description: "Fingerprint a single string",
                result: None,
            },
            Example {
                example: "[\"hello world\" \"hello earth\"] | topology fingerprint",
                description: "Fingerprint a list of strings",
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
        let weighted: bool = call.has_flag("weighted")?;
        let _cache_path: Option<String> = call.get_flag("cache")?;
        let head = call.head;

        let rows = util::normalize_input(input, head);

        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

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

        let result: Vec<Value> = rows
            .into_iter()
            .zip(fingerprints)
            .map(|(row, fp)| {
                let hex = fingerprint_to_hex(fp);
                util::append_column(row, "_fingerprint", Value::string(hex, head), head)
            })
            .collect();

        Ok(ListStream::new(result.into_iter(), head, Signals::empty()).into())
    }
}
