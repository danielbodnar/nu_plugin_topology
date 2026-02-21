use nu_plugin::{EngineInterface, EvaluatedCall, PluginCommand};
use nu_protocol::{
    Category, Example, LabeledError, ListStream, PipelineData, Record, Signature, Signals,
    SyntaxShape, Type, Value,
};

use crate::algo::url_normalize::slugify;
use crate::TopologyPlugin;

use super::util;

pub struct Organize;

impl PluginCommand for Organize {
    type Plugin = TopologyPlugin;

    fn name(&self) -> &str {
        "topology organize"
    }

    fn description(&self) -> &str {
        "Generate output paths and structure from classified items"
    }

    fn signature(&self) -> Signature {
        Signature::build(self.name())
            .input_output_types(vec![
                (Type::table(), Type::table()),
                (Type::list(Type::Any), Type::list(Type::Any)),
                (Type::Any, Type::Any),
            ])
            .named(
                "format",
                SyntaxShape::String,
                "Output format: folders, flat, nested (default: folders)",
                None,
            )
            .named(
                "output-dir",
                SyntaxShape::String,
                "Base output directory path (default: ./organized)",
                Some('o'),
            )
            .named(
                "category-field",
                SyntaxShape::String,
                "Field containing category (default: _category)",
                None,
            )
            .named(
                "name-field",
                SyntaxShape::String,
                "Field to use for filename (default: id)",
                None,
            )
            .category(Category::Experimental)
    }

    fn search_terms(&self) -> Vec<&str> {
        vec!["organize", "structure", "folders", "arrange", "output"]
    }

    fn examples(&self) -> Vec<Example<'_>> {
        vec![Example {
            example: r#"[[id _category]; [item1 "Web Dev"] [item2 "AI"]] | topology organize"#,
            description: "Generate output paths based on category",
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
        let format: String = call
            .get_flag::<String>("format")?
            .unwrap_or_else(|| "folders".into());
        let output_dir: String = call
            .get_flag::<String>("output-dir")?
            .unwrap_or_else(|| "./organized".into());
        let category_field: String = call
            .get_flag::<String>("category-field")?
            .unwrap_or_else(|| "_category".into());
        let name_field: String = call
            .get_flag::<String>("name-field")?
            .unwrap_or_else(|| "id".into());
        let head = call.head;

        let rows = util::normalize_input(input, head);
        if rows.is_empty() {
            return Ok(PipelineData::Value(Value::list(vec![], head), None));
        }

        let results: Vec<Value> = rows
            .into_iter()
            .map(|row| {
                let category = row
                    .get_data_by_key(&category_field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_else(|| "Uncategorized".into());

                let name = row
                    .get_data_by_key(&name_field)
                    .and_then(|v| v.coerce_string().ok())
                    .unwrap_or_else(|| "unknown".into());

                let slug_cat = slugify(&category);
                let slug_name = slugify(&name);

                let output_path = match format.as_str() {
                    "flat" => format!("{output_dir}/{slug_cat}--{slug_name}"),
                    "nested" => {
                        let hierarchy = row
                            .get_data_by_key("_hierarchy")
                            .and_then(|v| v.coerce_string().ok())
                            .unwrap_or_else(|| category.clone());
                        let parts: Vec<&str> = hierarchy.split(" > ").collect();
                        let path = parts
                            .iter()
                            .map(|p| slugify(p))
                            .collect::<Vec<String>>()
                            .join("/");
                        format!("{output_dir}/{path}/{slug_name}")
                    }
                    _ => format!("{output_dir}/{slug_cat}/{slug_name}"),
                };

                let mut record = match row {
                    Value::Record { val, .. } => val.into_owned(),
                    other => {
                        let mut r = Record::new();
                        r.push("value", other);
                        r
                    }
                };
                record.push("_output_path", Value::string(&output_path, head));
                Value::record(record, head)
            })
            .collect();

        Ok(ListStream::new(results.into_iter(), head, Signals::empty()).into())
    }
}
