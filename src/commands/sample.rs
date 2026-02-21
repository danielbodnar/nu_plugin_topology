use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Signature, Signals, SyntaxShape,
    Type, Value,
};
use std::collections::HashMap;

use crate::algo::sampling::{
    random_sample, reservoir_sample, stratified_sample, systematic_sample, Strategy,
};
use crate::TopologyPlugin;

pub struct Sample;

impl PluginCommand for Sample {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology sample"
    }

    fn description(&self) -> &str {
        "Sample rows from a table using various strategies"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_type(Type::table(), Type::table())
            .named(
                "size",
                SyntaxShape::Int,
                "Number of rows to sample (default: 100)",
                Some('n'),
            )
            .named(
                "strategy",
                SyntaxShape::String,
                "Sampling strategy: random, stratified, systematic, reservoir (default: random)",
                Some('s'),
            )
            .named(
                "field",
                SyntaxShape::String,
                "Field to stratify by (required for stratified strategy)",
                Some('f'),
            )
            .named(
                "seed",
                SyntaxShape::Int,
                "Random seed for reproducibility (default: 42)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["sample", "random", "stratified", "reservoir", "subset"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![
            Example {
                example: "[[name]; [a] [b] [c] [d] [e]] | topology sample --size 3",
                description: "Random sample of 3 rows",
                result: None,
            },
            Example {
                example: "[[lang value]; [rust 1] [go 2] [rust 3] [go 4]] | topology sample --size 2 --strategy stratified --field lang",
                description: "Stratified sample by language field",
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
        let size: usize = call.get_flag::<i64>("size")?.unwrap_or(100) as usize;
        let strategy_str: String = call
            .get_flag::<String>("strategy")?
            .unwrap_or_else(|| "random".into());
        let field: Option<String> = call.get_flag("field")?;
        let seed: u64 = call.get_flag::<i64>("seed")?.unwrap_or(42) as u64;
        let head = call.head;

        let strategy = Strategy::from_str(&strategy_str).ok_or_else(|| {
            LabeledError::new(format!(
                "Unknown strategy '{strategy_str}'. Use: random, stratified, systematic, reservoir"
            ))
        })?;

        // Collect all input rows
        let rows: Vec<Value> = input.into_iter().collect();
        let total = rows.len();

        if total == 0 {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        let indices = match strategy {
            Strategy::Random => random_sample(total, size, seed),
            Strategy::Systematic => systematic_sample(total, size, seed),
            Strategy::Reservoir => reservoir_sample(total, size, seed),
            Strategy::Stratified => {
                let field_name = field.ok_or_else(|| {
                    LabeledError::new("Stratified sampling requires --field flag")
                        .with_label("specify the field to stratify by", head)
                })?;

                // Group indices by field value
                let mut strata: HashMap<String, Vec<usize>> = HashMap::new();
                for (i, row) in rows.iter().enumerate() {
                    let key = row
                        .get_data_by_key(&field_name)
                        .map(|v| v.coerce_string().unwrap_or_else(|_| "unknown".into()))
                        .unwrap_or_else(|| "unknown".into());
                    strata.entry(key).or_default().push(i);
                }

                stratified_sample(&strata, size, seed)
            }
        };

        let sampled: Vec<Value> = indices
            .into_iter()
            .filter(|&i| i < total)
            .map(|i| rows[i].clone())
            .collect();

        Ok(ListStream::new(sampled.into_iter(), head, Signals::empty()).into())
    }
}
