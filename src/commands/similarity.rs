use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, PipelineData, Record, Signature, SyntaxShape, Type, Value,
};

use crate::algo::string_distance::{self, Metric};
use crate::TopologyPlugin;

pub struct Similarity;

impl PluginCommand for Similarity {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology similarity"
    }

    fn description(&self) -> &str {
        "Compute string similarity between two strings"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::Nothing, Type::record())
            .required("a", SyntaxShape::String, "First string")
            .required("b", SyntaxShape::String, "Second string")
            .named(
                "metric",
                SyntaxShape::String,
                "Metric: levenshtein, jaro-winkler, cosine (default: levenshtein)",
                Some('m'),
            )
            .switch("all", "Compute all metrics at once", Some('a'))
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec![
            "similarity",
            "distance",
            "levenshtein",
            "jaro",
            "cosine",
            "compare",
        ]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: r#"topology similarity "kitten" "sitting""#,
                description: "Levenshtein similarity between two strings",
                result: None,
            },
            Example {
                example: r#"topology similarity "martha" "marhta" --metric jaro-winkler"#,
                description: "Jaro-Winkler similarity",
                result: None,
            },
            Example {
                example: r#"topology similarity "night" "nacht" --all"#,
                description: "All metrics at once",
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
        let a: String = call.req(0)?;
        let b: String = call.req(1)?;
        let all: bool = call.has_flag("all")?;
        let metric_str: String = call
            .get_flag::<String>("metric")?
            .unwrap_or_else(|| "levenshtein".into());
        let head = call.head;

        let mut result = Record::new();
        result.push("a", Value::string(&a, head));
        result.push("b", Value::string(&b, head));

        if all {
            for name in Metric::all_names() {
                let metric = Metric::from_str(name).unwrap();
                let score = string_distance::similarity(&a, &b, metric);
                result.push(*name, Value::float(score, head));
            }
        } else {
            let metric = Metric::from_str(&metric_str).ok_or_else(|| {
                LabeledError::new(format!(
                    "Unknown metric '{metric_str}'. Use: {}",
                    Metric::all_names().join(", ")
                ))
            })?;
            let score = string_distance::similarity(&a, &b, metric);
            result.push("metric", Value::string(&metric_str, head));
            result.push("similarity", Value::float(score, head));
        }

        Ok(PipelineData::Value(Value::record(result, head), None))
    }
}
