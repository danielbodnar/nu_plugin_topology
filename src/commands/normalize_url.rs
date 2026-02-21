use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, PipelineData, Record, Signature, SyntaxShape, Type, Value,
};

use crate::algo::url_normalize;
use crate::TopologyPlugin;

pub struct NormalizeUrl;

impl PluginCommand for NormalizeUrl {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology normalize-url"
    }

    fn description(&self) -> &str {
        "Normalize a URL for deduplication"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::record())
            .required("url", SyntaxShape::String, "URL to normalize")
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["url", "normalize", "canonical", "dedup", "tracking"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: r#"topology normalize-url "https://example.com/page?utm_source=twitter&id=42""#,
                description: "Strip tracking params and normalize",
                result: None,
            },
            Example {
                example: r#"topology normalize-url "https://www.Example.COM:443/path""#,
                description: "Strip www, default port, lowercase host",
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
        let url_str: String = call.req(0)?;
        let head = call.head;

        let normalized = url_normalize::normalize(&url_str).ok_or_else(|| {
            LabeledError::new(format!("Could not parse URL: {url_str}"))
        })?;

        let canonical = url_normalize::canonical_key(&url_str).unwrap_or_default();

        let mut result = Record::new();
        result.push("original", Value::string(&url_str, head));
        result.push("normalized", Value::string(&normalized, head));
        result.push("canonical_key", Value::string(&canonical, head));

        Ok(PipelineData::Value(Value::record(result, head), None))
    }
}
