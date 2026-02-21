use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, PipelineData, Record, Signature, SyntaxShape, Type, Value,
};

use crate::ops;
use crate::TopologyPlugin;

pub struct CacheCmd;

impl PluginCommand for CacheCmd {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology cache"
    }

    fn description(&self) -> &str {
        "Manage the persistent topology cache database"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_types(vec![(Type::Nothing, Type::record())])
            .required(
                "path",
                SyntaxShape::String,
                "Path to the SQLite cache database",
            )
            .switch(
                "clear",
                "Clear cached artifacts instead of showing info",
                Some('c'),
            )
            .named(
                "kind",
                SyntaxShape::String,
                "Artifact kind to clear: corpus, dendrogram, taxonomy, fingerprints (default: all)",
                Some('k'),
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["cache", "clear", "info", "artifacts", "sqlite"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "topology cache data.topology.db",
                description: "Show cache info",
                result: None,
            },
            Example {
                example: "topology cache data.topology.db --clear",
                description: "Clear all cached artifacts",
                result: None,
            },
            Example {
                example: "topology cache data.topology.db --clear --kind taxonomy",
                description: "Clear only cached taxonomy",
                result: None,
            },
        ]
    }

    fn run(
        &self,
        _plugin: &TopologyPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        _input: PipelineData,
    ) -> Result<PipelineData, LabeledError> {
        let cache_path: String = call.req(0)?;
        let clear: bool = call.has_flag("clear")?;
        let kind: Option<String> = call.get_flag("kind")?;
        let head = call.head;

        let result = if clear {
            ops::op_cache_clear(&cache_path, kind.as_deref())
        } else {
            ops::op_cache_info(&cache_path)
        };

        match result {
            Ok(json_val) => {
                let nu_val = json_to_nu(&json_val, head);
                Ok(PipelineData::Value(nu_val, None))
            }
            Err(e) => Err(LabeledError::new(e)),
        }
    }
}

/// Convert a serde_json::Value to a nu_protocol::Value
fn json_to_nu(val: &serde_json::Value, span: nu_protocol::Span) -> Value {
    match val {
        serde_json::Value::Null => Value::nothing(span),
        serde_json::Value::Bool(b) => Value::bool(*b, span),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::int(i, span)
            } else if let Some(f) = n.as_f64() {
                Value::float(f, span)
            } else {
                Value::string(n.to_string(), span)
            }
        }
        serde_json::Value::String(s) => Value::string(s, span),
        serde_json::Value::Array(arr) => {
            Value::list(arr.iter().map(|v| json_to_nu(v, span)).collect(), span)
        }
        serde_json::Value::Object(map) => {
            let mut record = Record::new();
            for (k, v) in map {
                record.push(k, json_to_nu(v, span));
            }
            Value::record(record, span)
        }
    }
}
